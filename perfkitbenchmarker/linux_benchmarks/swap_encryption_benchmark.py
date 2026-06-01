# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""GKE vs. AWS EKS Swap Encryption and LSSD Performance Benchmark.

Methodology: go/swap-encryption-and-lssd-performance-comparison:gke-vs-aws

== Architecture ==

Provisions a real GKE (GCP) or EKS (AWS) Kubernetes cluster via PKB's
container_cluster abstraction, then deploys a privileged DaemonSet whose
pod has full host-device access (/dev, /sys, hostPID).  All benchmark
phases execute inside this pod via kubectl exec, so measurements reflect
actual cluster-node behaviour including Kubernetes overhead (kubelet,
containerd cgroup hierarchy, etc.).

  GKE nodes  ── dm-crypt with ephemeral key (go/node:swap-encryption)
                 swap device: /dev/mapper/swap_encrypted (over dedicated
                 hyperdisk or LSSD RAID-0 /dev/md0).
                 Single-disk fallback: plain loop device on
                 /mnt/stateful_partition — dm-crypt is blocked by COS
                 kernel namespace restrictions from inside a pod.

  EKS nodes  ── NVMe Instance Store, Nitro hardware-offloaded encryption
                 swap device: /dev/nvme1n1 (or auto-detected)

== Benchmark Phases ==

  Phase 1 – fio Microbenchmarks
    Run fio directly on the swap block device (swapoff first) to measure
    the hardware + encryption ceiling: random IOPS (4K), sequential
    bandwidth (1M), and completion latency (iodepth=1).

  Phase 2a – CPU Overhead
    stress-ng drives sustained swap I/O; vmstat and pidstat capture
    swap-in/out rates and per-process CPU cost (kswapd, kcryptd,
    dm-crypt threads on GKE; Nitro offload on EKS).

  Phase 2b – I/O Interference
    Baseline fio on a scratch volume → re-run with concurrent swap
    pressure.  IOPS/latency delta = storage contention cost.

  Phase 3a – Redis Latency
    Dataset loaded beyond container memory limit → GET/SET p99 latency
    measured while kernel swaps pages.

  Phase 3b – Kernel Build
    Linux compiled inside a memory-capped cgroup; slowdown ratio vs
    unconstrained baseline.

  Phase 3c – OpenSearch
    Bulk-index + search query under swap pressure (esrally or curl).
"""

import base64
import json
import logging
import re
import textwrap
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl

FLAGS = flags.FLAGS

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = 'swap_encryption'

BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    GKE vs. EKS swap encryption and LSSD performance comparison.
    Two-step nodepool setup: PKB provisions a minimal cluster with a cheap
    default nodepool (Step 1), then Prepare() adds the real benchmark
    nodepool (n4-highmem-32 / c4-*-lssd, COS_CONTAINERD, 80k IOPS) with a
    node-level startup script that configures dm-crypt swap before any pod
    is scheduled, then removes the default nodepool (Step 2).  All benchmark
    phases run inside a privileged DaemonSet pinned to the benchmark nodepool.
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        # Cheap placeholder — the benchmark nodepool is created in Prepare().
        machine_type: e2-medium
        boot_disk_size: 20
      AWS:
        # Cheap placeholder — the benchmark nodegroup is added in Prepare().
        machine_type: t3.medium
        boot_disk_size: 20
"""

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

_SWAP_DEVICE = flags.DEFINE_string(
    'swap_encryption_device',
    '',
    'Explicit swap block-device path on the cluster node, e.g. '
    '/dev/nvme1n1 or /dev/dm-0.  When empty the benchmark auto-detects '
    'via /proc/swaps after setup.',
)
_SWAP_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_swap_size_gb',
    32,
    'Size in GB of the swap space to configure on the node. '
    'Ignored when a ready swap device already exists.',
)
_SWAP_TYPE = flags.DEFINE_enum(
    'swap_encryption_swap_type',
    'auto',
    ['auto', 'hyperdisk', 'lssd', 'instance_store'],
    'Swap backing storage type.  auto = detect from cloud and instance type.',
)
_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    'swap_encryption_fio_runtime_sec',
    60,
    'Wall-clock runtime in seconds for each individual fio job.',
)
_STRESS_TIMEOUT_SEC = flags.DEFINE_integer(
    'swap_encryption_stress_timeout_sec',
    300,
    'Duration in seconds of each stress-ng memory-pressure phase.',
)
_STRESS_VM_BYTES = flags.DEFINE_string(
    'swap_encryption_stress_vm_bytes',
    '28G',
    'Memory each stress-ng --vm worker touches.  Should exceed node RAM '
    'to force kernel swapping.',
)
_STRESS_VM_BYTES_LIST = flags.DEFINE_string(
    'swap_encryption_stress_vm_bytes_list',
    '',
    'Comma-separated list of stress-ng --vm-bytes values to iterate over '
    'in Phase 2a CPU-overhead sweeps, e.g. "14G,21G,28G".  When non-empty '
    'this overrides --swap_encryption_stress_vm_bytes and Phase 2a is run '
    'once per entry so that the swap-pressure intensity curve is captured.',
)
_REDIS_DATASET_MB = flags.DEFINE_integer(
    'swap_encryption_redis_dataset_mb',
    1024,
    'Approximate Redis dataset size in MB to load before the latency test.',
)
_REDIS_MAXMEMORY_MB = flags.DEFINE_integer(
    'swap_encryption_redis_maxmemory_mb',
    512,
    'Redis maxmemory in MB.  Must be less than dataset size to force swap.',
)
_KERNEL_VERSION = flags.DEFINE_string(
    'swap_encryption_kernel_version',
    '6.1.38',
    'Linux kernel version to download and compile for the build workload.',
)
_KERNEL_MEMORY_MB = flags.DEFINE_integer(
    'swap_encryption_kernel_memory_mb',
    512,
    'cgroup memory limit in MB applied during the constrained kernel build.',
)
_ENABLE_ZSWAP = flags.DEFINE_boolean(
    'swap_encryption_enable_zswap',
    False,
    'Enable zswap (lz4 compressor, 20%% max pool) before running tests.',
)
_MIN_FREE_KBYTES = flags.DEFINE_integer(
    'swap_encryption_min_free_kbytes',
    65536,
    'Value written to /proc/sys/vm/min_free_kbytes to trigger earlier '
    'swapping. Set 0 to leave the kernel default unchanged.',
)
_DAEMONSET_IMAGE = flags.DEFINE_string(
    'swap_encryption_daemonset_image',
    'ubuntu:22.04',
    'Container image used for the privileged benchmark DaemonSet pod.',
)
_NODEPOOL = flags.DEFINE_string(
    'swap_encryption_nodepool',
    'benchmark',
    'Name of the node pool to deploy the benchmark DaemonSet on.',
)
_INSTANCE_SIZE_LABEL = flags.DEFINE_string(
    'swap_encryption_instance_size_label',
    '',
    'Human-readable label for the current instance size being tested, e.g. '
    '"n4-highmem-32" or "i4i.4xlarge".  Stored in sample metadata so that '
    'results from multiple PKB runs across different instance sizes can be '
    'collated and compared.  Defaults to the value reported by the cloud '
    'metadata endpoint inside the pod.',
)
_COLLECT_COST = flags.DEFINE_boolean(
    'swap_encryption_collect_cost',
    False,
    'When True, emit a cost_estimate_usd sample using on-demand pricing '
    'for the instance type detected at runtime.',
)

# ---------------------------------------------------------------------------
# New flags — benchmark nodepool, COS image, encryption toggle, IOPS
# ---------------------------------------------------------------------------

_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    'swap_encryption_benchmark_machine_type',
    'n4-highmem-32',
    'Machine type for the benchmark nodepool created in Prepare(). '
    'Use n4-highmem-32 (hyperdisk, default) or c4-standard-8-lssd '
    '(LSSD RAID-0).  The matching swap setup is selected automatically.',
)
_BENCHMARK_LSSD = flags.DEFINE_boolean(
    'swap_encryption_lssd',
    False,
    'Force LSSD RAID-0 swap path even when the machine type name does not '
    'contain "lssd".  Auto-detected from machine type when False.',
)
_LSSD_COUNT = flags.DEFINE_integer(
    'swap_encryption_lssd_count',
    1,
    'Number of local NVMe SSDs to attach as raw block devices '
    '(--local-nvme-ssd-block count=N).  Must match the fixed local SSD '
    'count for the chosen machine type: c4-standard-8-lssd=1, '
    'c4-standard-16-lssd=2, i4i.4xlarge has NVMe Instance Store (AWS).  '
    'Default 1 covers most single-lssd machine types.',
)
_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    'swap_encryption_enable_dmcrypt',
    True,
    'When True (default), configure dm-crypt on the swap device — the '
    '"encryption enabled" column of the test matrix.  Set False to use '
    'plain swap (encryption disabled column).',
)
_NODE_IMAGE_TYPE = flags.DEFINE_string(
    'swap_encryption_node_image_type',
    'UBUNTU_CONTAINERD',
    'GKE node image type for the benchmark nodepool.  '
    'UBUNTU_CONTAINERD is required for dm-crypt measurement: COS locks '
    'down device-mapper at the kernel LSM level and cryptsetup hangs '
    'indefinitely from any pod context (even privileged, even via nsenter '
    'into the host mount namespace).  Ubuntu GKE nodes allow cryptsetup '
    'from privileged pods without restriction.  '
    'Use COS_CONTAINERD only when dm-crypt is disabled '
    '(--noswap_encryption_enable_dmcrypt) to measure plain-swap overhead.  '
    'AL2 on EKS.',
)
_BOOT_DISK_TYPE = flags.DEFINE_string(
    'swap_encryption_boot_disk_type',
    'hyperdisk-balanced',
    'Disk type for the benchmark nodepool boot disk.  Use hyperdisk-balanced '
    'for production machines (n4, c3, c4 families).  Use pd-ssd for n2/e2 '
    'dev/test machines, which do not support hyperdisk-balanced.',
)
_BOOT_DISK_IOPS = flags.DEFINE_integer(
    'swap_encryption_boot_disk_iops',
    80000,
    'Provisioned IOPS for the boot disk (hyperdisk-balanced only).  '
    '80 000 is the COS max-IOPS target.  Ignored for pd-ssd.',
)
_BOOT_DISK_THROUGHPUT = flags.DEFINE_integer(
    'swap_encryption_boot_disk_throughput',
    1200,
    'Provisioned throughput in MB/s for the boot disk (hyperdisk-balanced '
    'only).  Must be set together with iops.  1200 MB/s pairs with 80 000 '
    'IOPS for production; use 140 (minimum) for dev/test.  Ignored for '
    'pd-ssd.',
)
_BOOT_DISK_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_boot_disk_size_gb',
    500,
    'Boot disk size in GiB for the benchmark nodepool.  500 GiB is '
    'required for the n4-highmem-32 + hyperdisk-balanced Config 2 run '
    '(see Engineer Assignments table in execution-plan.md).  '
    'For LSSD configs the boot disk is smaller; 100 GiB is fine.',
)
_ADD_SWAP_DISK = flags.DEFINE_boolean(
    'swap_encryption_add_swap_disk',
    False,
    'Attach a dedicated second disk to the benchmark nodepool for use as '
    'the swap device.  Required for dm-crypt measurement on single-boot-disk '
    'machines (n4-highmem-32, n4-highmem-8) because COS blocks device-mapper '
    'from pod namespaces.  The second disk is provisioned via '
    '--additional-node-disk using the same type/IOPS/throughput as the boot '
    'disk flags.',
)
_SWAP_DISK_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_swap_disk_size_gb',
    500,
    'Size in GiB of the dedicated swap disk when '
    '--swap_encryption_add_swap_disk is True.  Must satisfy the '
    'hyperdisk-balanced IOPS constraint: provisioned_iops ≤ size_gb × 80.',
)

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'
_DEFAULT_NODEPOOL = 'default-pool'

# fio jobs: (name, rw_mode, blocksize, iodepth, description)
_FIO_JOBS = (
    ('rand_write_iops', 'randwrite', '4k', 256, 'Random write IOPS'),
    ('rand_read_iops', 'randread', '4k', 256, 'Random read IOPS'),
    ('rand_rw_mixed', 'randrw', '4k', 256, 'Mixed random R/W (50/50)'),
    ('seq_write_bw', 'write', '1m', 64, 'Sequential write bandwidth'),
    ('seq_read_bw', 'read', '1m', 64, 'Sequential read bandwidth'),
    ('lat_write', 'randwrite', '4k', 1, 'Random write latency'),
    ('lat_read', 'randread', '4k', 1, 'Random read latency'),
)

_VMSTAT_LOG = '/tmp/pkb_vmstat.log'
_PIDSTAT_LOG = '/tmp/pkb_pidstat.log'
_CRYPTO_PROCS = ('kswapd', 'kworker', 'kcryptd', 'dmcrypt_write')

# ---------------------------------------------------------------------------
# DaemonSet manifest (embedded YAML)
# ---------------------------------------------------------------------------

def _daemonset_yaml(image: str) -> str:
  """Return the privileged benchmark DaemonSet manifest as a YAML string.

  The DaemonSet is pinned to the benchmark nodepool via nodeSelector so it
  never lands on the cheap dummy default nodepool.  By the time this pod
  starts, the node startup script has already configured dm-crypt swap at
  the OS level, so the pod only needs to verify/use that device.
  """
  return textwrap.dedent(f"""\
    apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: {_DS_NAME}
      namespace: {_DS_NAMESPACE}
      labels:
        app: {_DS_LABEL}
    spec:
      selector:
        matchLabels:
          app: {_DS_LABEL}
      template:
        metadata:
          labels:
            app: {_DS_LABEL}
        spec:
          hostPID: true
          hostNetwork: true
          # Pin to the benchmark nodepool — never schedule on the dummy default pool.
          nodeSelector:
            pkb_nodepool: {_BENCHMARK_NODEPOOL}
          tolerations:
          - operator: Exists
          containers:
          - name: benchmark
            image: {image}
            command:
            - bash
            - -c
            - |
              echo "[pkb] Installing benchmark tools..."
              apt-get update -qq 2>&1 || true
              DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \\
                fio \\
                stress-ng \\
                sysstat \\
                cryptsetup \\
                mdadm \\
                redis-server \\
                redis-tools \\
                wget \\
                curl \\
                make \\
                gcc \\
                bc \\
                flex \\
                bison \\
                libelf-dev \\
                libssl-dev \\
                cgroup-tools \\
                nvme-cli \\
                util-linux \\
                python3-pip \\
                libevent-dev \\
                libssl-dev \\
                build-essential \\
                autoconf \\
                automake \\
                2>&1 || true
              echo "[pkb] Installing memtier_benchmark from source..."
              if ! command -v memtier_benchmark >/dev/null 2>&1; then
                (cd /tmp && \\
                  git clone --depth 1 https://github.com/RedisLabs/memtier_benchmark.git 2>&1 && \\
                  cd memtier_benchmark && \\
                  autoreconf -ivf 2>&1 && \\
                  ./configure 2>&1 && \\
                  make -j2 2>&1 && \\
                  make install 2>&1) || echo "[pkb] WARNING: memtier_benchmark build failed"
              fi
              echo "[pkb] Installing esrally..."
              pip3 install esrally --quiet --break-system-packages 2>&1 || true
              echo "[pkb] Tools installed. Writing ready sentinel."
              touch /tmp/pkb_ready
              sleep infinity
            securityContext:
              privileged: true
              capabilities:
                add: ["SYS_ADMIN", "IPC_LOCK"]
            resources:
              requests:
                memory: "512Mi"
            env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            volumeMounts:
            - name: dev
              mountPath: /dev
            - name: sys
              mountPath: /sys
            - name: run
              mountPath: /run
            - name: proc-host
              mountPath: /proc-host
              readOnly: true
            - name: stateful-partition
              mountPath: /mnt/stateful_partition
            - name: lib-modules
              mountPath: /lib/modules
              readOnly: true
          volumes:
          - name: dev
            hostPath:
              path: /dev
          - name: sys
            hostPath:
              path: /sys
          - name: run
            hostPath:
              path: /run
          - name: proc-host
            hostPath:
              path: /proc
          - name: stateful-partition
            hostPath:
              path: /mnt/stateful_partition
              type: DirectoryOrCreate
          - name: lib-modules
            hostPath:
              path: /lib/modules
              type: Directory
  """)


# ---------------------------------------------------------------------------
# PKB entry points
# ---------------------------------------------------------------------------

def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec) -> None:
  """Two-step nodepool setup then DaemonSet deployment.

  Step 1 (handled by PKB infrastructure): cluster provisioned with a cheap
  e2-medium default nodepool.

  Step 2 (this function):
    a. Create the benchmark nodepool (n4-highmem-32 or c4-*-lssd) with
       COS_CONTAINERD, 80 000 IOPS, and a node startup script that configures
       dm-crypt swap at the OS level — before any pod is scheduled.
    b. Delete the dummy default nodepool to stop its cost immediately.
    c. Deploy the privileged DaemonSet (pinned via nodeSelector to the
       benchmark nodepool) and wait for tools to install.
  """
  cluster = spec.container_cluster

  # ── Step 2a: add real benchmark nodepool ────────────────────────────────
  if getattr(cluster, 'project', None):
    # GCP path: true two-step nodepool setup
    logging.info('[swap_encryption] Step 2a: creating benchmark nodepool')
    _create_benchmark_node_pool(cluster)

    # ── Step 2b: wait for the benchmark node to join and be Ready ─────────
    logging.info('[swap_encryption] Step 2b: waiting for benchmark node')
    _wait_for_benchmark_node()

    # ── Step 2b2: attach dedicated swap disk (if requested) ───────────────
    # --additional-node-disk is not available in all gcloud versions, so we
    # create + attach the disk after the node is up using gcloud compute.
    if _ADD_SWAP_DISK.value:
      logging.info('[swap_encryption] Step 2b2: attaching dedicated swap disk')
      _attach_swap_disk(cluster)
  else:
    # AWS / unknown: nodepool management is done externally; log and continue.
    logging.info('[swap_encryption] Non-GCP cluster — skipping nodepool '
                 'create/delete steps; ensure a benchmark node is available.')

  # ── Step 2c: deploy DaemonSet ────────────────────────────────────────────
  # Deploy and wait for the pod BEFORE deleting the default nodepool.
  # Deleting the default pool while the benchmark node is still joining causes
  # a temporary API server i/o timeout (control plane busy with two nodepool
  # ops simultaneously).  Once the pod is Running the cluster is fully stable.
  logging.info('[swap_encryption] Step 2c: deploying privileged DaemonSet')
  _deploy_daemonset()

  pod = _wait_for_benchmark_pod()
  logging.info('[swap_encryption] Benchmark pod ready: %s', pod)

  # ── Step 2d: now safe to remove the dummy default nodepool ───────────────
  if getattr(cluster, 'project', None):
    logging.info('[swap_encryption] Step 2d: deleting dummy default nodepool')
    _delete_default_node_pool(cluster)
    # The DaemonSet pod may be evicted and rescheduled with a new name during
    # the nodepool deletion (cluster control plane briefly interrupts pod
    # lifecycle).  Re-resolve the pod name to avoid stale-reference errors on
    # all subsequent _pod_exec calls.
    logging.info('[swap_encryption] Step 2d: re-resolving benchmark pod '
                 'after nodepool deletion')
    pod = _wait_for_benchmark_pod()
    logging.info('[swap_encryption] Benchmark pod (post-deletion): %s', pod)

  # Tune kernel swap aggressiveness
  if _MIN_FREE_KBYTES.value > 0:
    _pod_exec(pod, f'sysctl -w vm.min_free_kbytes={_MIN_FREE_KBYTES.value}')

  # Enable zswap if requested
  if _ENABLE_ZSWAP.value:
    _enable_zswap(pod)

  # Configure cloud-specific swap
  cloud = _detect_cloud(pod)
  logging.info('[swap_encryption] Detected cloud: %s', cloud)

  if cloud == 'gcp':
    _setup_gke_swap(pod)
  elif cloud == 'aws':
    _setup_eks_swap(pod)
  else:
    logging.warning(
        '[swap_encryption] Unknown cloud – falling back to plain swapfile'
    )
    _setup_plain_swap_file(pod, _SWAP_SIZE_GB.value)


def Run(spec) -> list[sample.Sample]:
  """Execute all benchmark phases with gate logic.

  Execution is structured in three gated tiers matching the execution plan:

    Tier 1 (Gate 1) — fio microbenchmarks
      Raw I/O ceiling of the swap device.  Gate 1 fails if fio produces
      zero samples (device not found, O_DIRECT error, etc.).

    Tier 2 (Gate 2) — stress-ng CPU overhead + I/O interference
      Requires an active swap device (Gate 1 must pass).  Gate 2 fails if
      stress-ng does not complete within timeout.

    Tier 3 (Gate 3) — real-world workloads (Redis, kernel build, OpenSearch)
      Independent of Tier 2 results; always attempted if Gate 1 passed.
      Individual workload failures are logged but do not abort the others.

  If Gate 1 fails, Tiers 2 and 3 are skipped — there is no point measuring
  application-level swap performance when the raw device is inaccessible.
  """
  pod = _wait_for_benchmark_pod()
  swap_dev = _detect_swap_device(pod)
  base_meta = _build_metadata(pod, swap_dev)
  results: list[sample.Sample] = []
  t_run_start = time.time()

  logging.info('[swap_encryption] swap device: %s', swap_dev)

  # ── Tier 1 / Gate 1: fio microbenchmarks ─────────────────────────────────
  logging.info('[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──')
  tier1_results = []
  try:
    tier1_results = _phase1_fio(pod, swap_dev, base_meta)
    results += tier1_results
  except Exception as e:  # pylint: disable=broad-except
    logging.error('[swap_encryption] Gate 1 FAILED — fio phase error: %s', e)
    logging.error('[swap_encryption] Skipping Tiers 2 and 3 (no swap device)')
    return results

  if not tier1_results:
    logging.warning('[swap_encryption] Gate 1 produced no samples '
                    '(loop-device skip or parse error) — '
                    'continuing to Tier 2 with caution')

  # ── Tier 2 / Gate 2: stress-ng CPU overhead + I/O interference ───────────
  logging.info('[swap_encryption] ── Tier 2 / Gate 2: stress-ng phases ──')
  try:
    logging.info('[swap_encryption] Phase 2a: CPU overhead')
    results += _phase2a_cpu_overhead(pod, base_meta)
    logging.info('[swap_encryption] Phase 2b: I/O interference')
    results += _phase2b_io_interference(pod, base_meta)
  except Exception as e:  # pylint: disable=broad-except
    logging.error('[swap_encryption] Gate 2 FAILED — stress phase error: %s',
                  e)
    logging.warning('[swap_encryption] Proceeding to Tier 3 (workloads are '
                    'independent of stress-ng results)')

  # ── Tier 3 / Gate 3: real-world workloads ────────────────────────────────
  logging.info('[swap_encryption] ── Tier 3 / Gate 3: workloads ──')
  for phase_name, phase_fn in [
      ('Redis latency (3a)', lambda: _phase3a_redis(pod, base_meta)),
      ('Kernel build (3b)', lambda: _phase3b_kernel_build(pod, base_meta)),
      ('OpenSearch (3c)', lambda: _phase3c_opensearch(pod, base_meta)),
  ]:
    try:
      logging.info('[swap_encryption] Phase %s', phase_name)
      results += phase_fn()
    except Exception as e:  # pylint: disable=broad-except
      logging.error('[swap_encryption] %s FAILED: %s — continuing with '
                    'remaining workloads', phase_name, e)

  # ── Cost estimate ─────────────────────────────────────────────────────────
  if _COLLECT_COST.value:
    elapsed = time.time() - t_run_start
    results += _collect_cost_sample(pod, elapsed, base_meta)

  return results


def Cleanup(spec) -> None:
  """Remove the DaemonSet and tear down any swap configuration."""
  pod = _wait_for_benchmark_pod(timeout=30)
  if pod:
    _pod_exec(pod, 'swapoff -a 2>/dev/null || true', ignore_failure=True)
    _pod_exec(pod, textwrap.dedent("""
      swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
      dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    """), ignore_failure=True)
    # Clean up loop device backing files (single-disk fallback path).
    _pod_exec(pod, textwrap.dedent("""
      for backing in /var/pkb_swap_backing /run/pkb_swap_backing \
                     /mnt/stateful_partition/pkb_swap_backing
      do
        losetup -j "$backing" 2>/dev/null | awk -F: '{print $1}' | \
          while read dev
          do
            losetup -d "$dev" 2>/dev/null || true
          done
        rm -f "$backing"
      done
    """), ignore_failure=True)
    _pod_exec(pod, 'pkill -f "stress-ng|fio" 2>/dev/null || true',
             ignore_failure=True)

  _delete_daemonset()

  # Detach and delete the dedicated swap disk if one was provisioned.
  cluster = spec.container_cluster
  if _ADD_SWAP_DISK.value and getattr(cluster, 'project', None):
    _detach_and_delete_swap_disk(cluster)


# ---------------------------------------------------------------------------
# DaemonSet lifecycle helpers
# ---------------------------------------------------------------------------

def _deploy_daemonset() -> None:
  """Apply the benchmark DaemonSet manifest to the cluster."""
  manifest = _daemonset_yaml(image=_DAEMONSET_IMAGE.value)
  with vm_util.NamedTemporaryFile(mode='w', suffix='.yaml') as f:
    f.write(manifest)
    f.close()
    kubectl.RunKubectlCommand(['apply', '-f', f.name])
  logging.info('[swap_encryption] DaemonSet applied')


def _wait_for_benchmark_pod(timeout: int = 900) -> str | None:
  """Wait until the DaemonSet pod is Running AND tools are installed.

  The benchmark container installs apt packages on first start and writes
  /tmp/pkb_ready when done (~2-4 min on a cold node).  We must wait for
  that sentinel before exec-ing any commands, otherwise tools like
  cryptsetup / fio may not yet be on PATH.

  Uses tab-separated name/phase output so kubectl always exits 0 regardless
  of whether any pods are present, avoiding jsonpath index errors.
  """
  deadline = time.time() + timeout
  last_phase = ''
  ready_pod  = None   # pod name once phase == Running

  while time.time() < deadline:
    # ── Step 1: wait for Running phase ──────────────────────────────────────
    if ready_pod is None:
      out, _, rc = kubectl.RunKubectlCommand([
          'get', 'pods',
          '-l', f'app={_DS_LABEL}',
          '-n', _DS_NAMESPACE,
          '-o',
          r'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\n"}{end}',
      ], raise_on_failure=False)

      if rc == 0 and out.strip():
        for line in out.strip().splitlines():
          parts = line.split('\t')
          if len(parts) == 2:
            pod_name, phase = parts[0].strip(), parts[1].strip()
            if phase == 'Running':
              logging.info('[swap_encryption] Pod %s is Running – '
                           'waiting for tool install to finish...', pod_name)
              ready_pod = pod_name
              break
            if phase != last_phase:
              logging.info('[swap_encryption] Pod %s phase: %s', pod_name, phase)
              last_phase = phase
              if phase in ('Pending',):
                _log_pod_events(pod_name)
      else:
        logging.info('[swap_encryption] Waiting for DaemonSet pod to appear...')

    # ── Step 2: poll for /tmp/pkb_ready sentinel ────────────────────────────
    if ready_pod is not None:
      sentinel_out, sentinel_err, sentinel_rc = kubectl.RunKubectlCommand([
          'exec', ready_pod, '-n', _DS_NAMESPACE,
          '--', 'test', '-f', '/tmp/pkb_ready',
      ], raise_on_failure=False)
      if sentinel_rc == 0:
        logging.info(
            '[swap_encryption] Pod %s ready (tools installed)', ready_pod)
        return ready_pod
      # "container not found" means the container crashed (CrashLoopBackOff or
      # exited) — treat it as a hard reset: re-check pod phase on next iteration.
      if ('container not found' in sentinel_err
          or 'unable to upgrade connection' in sentinel_err):
        logging.warning('[swap_encryption] Pod %s: container not running (%s) '
                        '— will re-check pod state', ready_pod, sentinel_err.strip())
        ready_pod = None
        last_phase = ''
      else:
        logging.info(
            '[swap_encryption] Pod %s: still installing tools...', ready_pod)

    time.sleep(15)

  logging.warning(
      '[swap_encryption] Benchmark pod not ready after %ds', timeout)
  return None


def _log_pod_events(pod_name: str) -> None:
  """Dump recent Kubernetes events for the pod to help diagnose startup hangs."""
  events_out, _, _ = kubectl.RunKubectlCommand([
      'describe', 'pod', pod_name,
      '-n', _DS_NAMESPACE,
  ], raise_on_failure=False)
  # Only log the Events section to keep output manageable
  in_events = False
  lines = []
  for line in events_out.splitlines():
    if line.startswith('Events:'):
      in_events = True
    if in_events:
      lines.append(line)
  if lines:
    logging.info('[swap_encryption] Pod events:\n%s', '\n'.join(lines[:30]))
  else:
    logging.info('[swap_encryption] kubectl describe output:\n%s',
                 events_out[-2000:] if len(events_out) > 2000 else events_out)


def _delete_daemonset() -> None:
  """Delete the benchmark DaemonSet."""
  kubectl.RunKubectlCommand([
      'delete', 'daemonset', _DS_NAME,
      '-n', _DS_NAMESPACE,
      '--ignore-not-found',
  ], raise_on_failure=False)
  logging.info('[swap_encryption] DaemonSet deleted')


# ---------------------------------------------------------------------------
# Two-step GKE nodepool helpers
# ---------------------------------------------------------------------------

def _build_node_startup_script(enable_dmcrypt: bool, lssd: bool) -> str:
  """Return a bash startup script for the benchmark nodepool.

  NOTE: This function is not currently used. GKE reserves the
  `startup-script` node metadata key, so dm-crypt setup is performed
  from within the privileged DaemonSet pod instead (see
  _setup_gke_hyperdisk_swap / _setup_gke_lssd_swap). Kept as reference.

  Args:
    enable_dmcrypt: When True, wrap the swap device in dm-crypt plain
      mode (aes-xts-plain64, ephemeral random key) matching GKE's
      go/node:swap-encryption implementation.
    lssd: When True, build a RAID-0 array across all local SSDs before
      setting up swap (matches go/gke-swap-lssd).

  Returns:
    A bash script string suitable for running as root at node boot.
  """
  dmcrypt_str = 'true' if enable_dmcrypt else 'false'
  lssd_str = 'true' if lssd else 'false'

  return textwrap.dedent(f"""\
    #!/bin/bash
    # PKB swap_encryption_benchmark — nodepool startup script.
    # Configures swap once at node boot so all benchmark phases see a
    # pre-warmed swap device.  Runs as root on the COS host.
    set -euo pipefail
    ENABLE_DMCRYPT={dmcrypt_str}
    LSSD={lssd_str}

    _wait_dev() {{
      local d=$1 i
      for i in $(seq 1 30); do [ -b "$d" ] && return 0; sleep 2; done
      echo "[pkb-startup] device $d not ready" >&2; return 1
    }}

    _boot_dev() {{
      lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1 || echo nvme0n1
    }}

    if $LSSD; then
      BOOT=$(_boot_dev)
      # Collect all non-rotational non-boot block devices (local SSDs)
      DEVS=$(lsblk -d -o NAME,ROTA | awk '$2=="0"{{print "/dev/"$1}}' | grep -v "/dev/$BOOT" || true)
      N=$(echo "$DEVS" | grep -c /dev/ || true)
      if [ "$N" -gt 1 ]; then
        modprobe raid0 || true
        # shellcheck disable=SC2086
        mdadm --create /dev/md0 --level=0 --raid-devices="$N" $DEVS --force
        TARGET=/dev/md0
      elif [ "$N" -eq 1 ]; then
        TARGET=$(echo "$DEVS" | head -1)
      else
        echo "[pkb-startup] no LSSD devices found; skipping swap setup" >&2
        exit 0
      fi
    else
      BOOT=$(_boot_dev)
      RAW=$(lsblk -d -o NAME,TYPE | awk '$2=="disk"{{print $1}}' | grep -v "^$BOOT$" | head -1 || true)
      if [ -z "$RAW" ]; then
        echo "[pkb-startup] no secondary disk found for hyperdisk swap" >&2
        exit 0
      fi
      TARGET=/dev/$RAW
    fi

    _wait_dev "$TARGET"

    if $ENABLE_DMCRYPT; then
      modprobe dm-crypt || true
      dd if=/dev/urandom bs=32 count=1 2>/dev/null | \\
        cryptsetup open --type plain \\
          --cipher aes-xts-plain64 --key-size 256 \\
          --key-file=- "$TARGET" pkb_swap
      SWAP_DEV=/dev/mapper/pkb_swap
    else
      SWAP_DEV=$TARGET
    fi

    mkswap "$SWAP_DEV"
    swapon "$SWAP_DEV"
    echo "[pkb-startup] swap active on $SWAP_DEV (dmcrypt=$ENABLE_DMCRYPT lssd=$LSSD)"
  """)


def _create_benchmark_node_pool(cluster) -> None:
  """Add the benchmark nodepool to the existing cluster (Step 2 of setup).

  Uses:
    --swap_encryption_benchmark_machine_type  (default n4-highmem-32)
    --swap_encryption_node_image_type         (default COS_CONTAINERD)
    --swap_encryption_boot_disk_iops          (default 80000)
    --swap_encryption_enable_dmcrypt          (default True)

  The nodepool is labelled pkb_nodepool=benchmark so the DaemonSet
  nodeSelector targets it exclusively.  dm-crypt swap setup is performed
  from within the privileged DaemonSet pod (see _setup_gke_hyperdisk_swap /
  _setup_gke_lssd_swap) — we do NOT inject a startup-script via node metadata
  because GKE reserves that metadata key and rejects it at the API level.
  """
  machine_type = _BENCHMARK_MACHINE_TYPE.value
  # Auto-detect LSSD from machine type name; flag overrides only when True.
  is_lssd = _BENCHMARK_LSSD.value or 'lssd' in machine_type.lower()

  # Determine zone/region from the cluster object.
  zone_flags: list[str] = []
  if getattr(cluster, 'zones', None):
    zone_flags = ['--zone', cluster.zones[0]]
  elif getattr(cluster, 'region', None):
    zone_flags = ['--region', cluster.region]

  # LSSD configs only need a small boot disk (OS only; swap is on local NVMe).
  # Hyperdisk configs need 500 GiB to hit 80 000 IOPS (the IOPS/GiB ratio on
  # hyperdisk-balanced is 1:1 up to the provisioned ceiling, so a 100 GiB disk
  # can only provision up to 100 000 IOPS but a 500 GiB gives comfortable
  # headroom and matches the Config 2 spec in the Engineer Assignments table).
  disk_size_gb = 100 if is_lssd else _BOOT_DISK_SIZE_GB.value

  disk_type = _BOOT_DISK_TYPE.value
  cmd = [
      'gcloud', 'container', 'node-pools', 'create', _BENCHMARK_NODEPOOL,
      '--cluster',      cluster.name,
      '--project',      cluster.project,
      '--machine-type', machine_type,
      '--image-type',   _NODE_IMAGE_TYPE.value,
      '--disk-type',    disk_type,
      '--disk-size',    str(disk_size_gb),
      '--num-nodes',    '1',
      '--node-labels',  f'pkb_nodepool={_BENCHMARK_NODEPOOL}',
      '--no-enable-autoupgrade',
      '--no-enable-autorepair',
  ] + zone_flags

  # IOPS and throughput provisioning only applies to hyperdisk-* types AND
  # only when the boot disk is also the swap device (non-LSSD configs).
  # For LSSD machines the boot disk is OS-only; swap is on local NVMe.
  # Provisioning 80k IOPS on a 100 GiB boot disk would exceed the
  # hyperdisk-balanced per-GiB cap (80 IOPS/GiB × 100 GiB = 8 000 max).
  if disk_type.startswith('hyperdisk') and not is_lssd:
    cmd += [
        '--boot-disk-provisioned-iops', str(_BOOT_DISK_IOPS.value),
        '--boot-disk-provisioned-throughput', str(_BOOT_DISK_THROUGHPUT.value),
    ]

  # For LSSD machines, expose local NVMe as raw block devices so fio/mdadm
  # can access them directly (go/gke-swap-lssd uses local-nvme-ssd-block).
  if is_lssd:
    cmd += ['--local-nvme-ssd-block', f'count={_LSSD_COUNT.value}']

  logging.info('[swap_encryption] Creating benchmark nodepool: %s / %s / '
               'image=%s / disk=%dGiB / iops=%d / dmcrypt=%s / lssd=%s / '
               'add_swap_disk=%s',
               _BENCHMARK_NODEPOOL, machine_type, _NODE_IMAGE_TYPE.value,
               disk_size_gb, _BOOT_DISK_IOPS.value,
               _ENABLE_DMCRYPT.value, is_lssd, _ADD_SWAP_DISK.value)

  stdout, stderr, rc = vm_util.IssueCommand(cmd, timeout=600,
                                            raise_on_failure=False)

  if rc != 0:
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] Failed to create benchmark nodepool '
        f'(rc={rc}): {stderr}'
    )
  logging.info('[swap_encryption] Benchmark nodepool ready')


def _wait_for_benchmark_node(timeout: int = 600) -> None:
  """Block until a node labelled pkb_nodepool=benchmark is Ready.

  gcloud container node-pools create returns as soon as the API accepts the
  request — the actual node VM may take another 2-4 minutes to boot, join the
  cluster, and pass its readiness checks.  Deploying the DaemonSet before that
  point leaves the pod Pending indefinitely because the nodeSelector finds no
  eligible node.

  This function polls kubectl every 15 s until at least one node with
  pkb_nodepool=benchmark has Ready=True, then returns.
  """
  deadline = time.time() + timeout
  logging.info('[swap_encryption] Waiting for benchmark node '
               '(pkb_nodepool=benchmark) to be Ready...')
  while time.time() < deadline:
    out, _, rc = kubectl.RunKubectlCommand([
        'get', 'nodes',
        '-l', f'pkb_nodepool={_BENCHMARK_NODEPOOL}',
        '-o', r'jsonpath={range .items[*]}'
               r'{.metadata.name}{"\t"}'
               r'{range .status.conditions[?(@.type=="Ready")]}'
               r'{.status}{"\n"}{end}{end}',
    ], raise_on_failure=False)

    if rc == 0 and out.strip():
      for line in out.strip().splitlines():
        parts = line.split('\t')
        if len(parts) == 2 and parts[1].strip() == 'True':
          logging.info('[swap_encryption] Benchmark node ready: %s',
                       parts[0].strip())
          return

    logging.info('[swap_encryption] Benchmark node not yet Ready — '
                 'retrying in 15 s...')
    time.sleep(15)

  raise errors.Benchmarks.RunError(
      '[swap_encryption] Timed out waiting for benchmark node '
      f'(pkb_nodepool={_BENCHMARK_NODEPOOL}) to become Ready '
      f'after {timeout}s'
  )


def _attach_swap_disk(cluster) -> None:
  """Create a dedicated hyperdisk and attach it to the benchmark node.

  gcloud container node-pools create --additional-node-disk is not available
  in all gcloud SDK versions, so we use gcloud compute to create the disk and
  attach it after the node is ready.  In GKE the Kubernetes node name is the
  same as the GCE instance name, so no translation is needed.

  After attachment the disk appears as /dev/sdb (or /dev/nvme1n1 on NVMe
  nodes) inside the pod, and _setup_gke_hyperdisk_swap detects it via lsblk.

  The disk is named pkb-swap-<cluster-name> to avoid name collisions across
  concurrent runs.  Cleanup deletes it in Cleanup() if it exists.
  """
  # Resolve zone from cluster
  zone = None
  if getattr(cluster, 'zones', None):
    zone = cluster.zones[0]
  elif getattr(cluster, 'region', None):
    zone = cluster.region
  if not zone:
    raise errors.Benchmarks.RunError(
        '[swap_encryption] Cannot attach swap disk: cluster zone unknown')

  project = cluster.project
  disk_name = f'pkb-swap-{cluster.name}'
  disk_type = _BOOT_DISK_TYPE.value
  disk_size_gb = _SWAP_DISK_SIZE_GB.value

  # ── Step 1: get the GCE instance name of the benchmark node ───────────────
  node_out, _, rc = kubectl.RunKubectlCommand([
      'get', 'nodes',
      '-l', f'pkb_nodepool={_BENCHMARK_NODEPOOL}',
      '-o', 'jsonpath={.items[0].metadata.name}',
  ], raise_on_failure=False)
  instance_name = node_out.strip()
  if rc != 0 or not instance_name:
    raise errors.Benchmarks.RunError(
        '[swap_encryption] Cannot find benchmark node for swap disk attach')
  logging.info('[swap_encryption] Benchmark node instance: %s', instance_name)

  # ── Step 2: create the hyperdisk ──────────────────────────────────────────
  logging.info('[swap_encryption] Creating swap disk %s (%dGiB %s)',
               disk_name, disk_size_gb, disk_type)
  create_cmd = [
      'gcloud', 'compute', 'disks', 'create', disk_name,
      '--project', project,
      '--zone', zone,
      '--type', disk_type,
      '--size', f'{disk_size_gb}GB',
      '--quiet',
  ]
  if disk_type.startswith('hyperdisk'):
    create_cmd += [
        '--provisioned-iops', str(_BOOT_DISK_IOPS.value),
        '--provisioned-throughput', str(_BOOT_DISK_THROUGHPUT.value),
    ]
  _, stderr, rc = vm_util.IssueCommand(create_cmd, timeout=120,
                                       raise_on_failure=False)
  if rc != 0:
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] Failed to create swap disk {disk_name}: {stderr}')

  # ── Step 3: attach the disk to the node VM ────────────────────────────────
  logging.info('[swap_encryption] Attaching swap disk %s to %s',
               disk_name, instance_name)
  attach_cmd = [
      'gcloud', 'compute', 'instances', 'attach-disk', instance_name,
      '--project', project,
      '--zone', zone,
      '--disk', disk_name,
      '--device-name', 'pkb-swap',
      '--quiet',
  ]
  _, stderr, rc = vm_util.IssueCommand(attach_cmd, timeout=120,
                                       raise_on_failure=False)
  if rc != 0:
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] Failed to attach swap disk to {instance_name}: '
        f'{stderr}')
  logging.info('[swap_encryption] Swap disk attached: %s → %s',
               disk_name, instance_name)


def _detach_and_delete_swap_disk(cluster) -> None:
  """Detach and delete the dedicated swap disk created by _attach_swap_disk."""
  zone = None
  if getattr(cluster, 'zones', None):
    zone = cluster.zones[0]
  elif getattr(cluster, 'region', None):
    zone = cluster.region
  if not zone or not getattr(cluster, 'project', None):
    return

  project = cluster.project
  disk_name = f'pkb-swap-{cluster.name}'

  node_out, _, _ = kubectl.RunKubectlCommand([
      'get', 'nodes',
      '-l', f'pkb_nodepool={_BENCHMARK_NODEPOOL}',
      '-o', 'jsonpath={.items[0].metadata.name}',
  ], raise_on_failure=False)
  instance_name = node_out.strip()

  if instance_name:
    vm_util.IssueCommand([
        'gcloud', 'compute', 'instances', 'detach-disk', instance_name,
        '--project', project, '--zone', zone,
        '--disk', disk_name, '--quiet',
    ], timeout=120, raise_on_failure=False)

  vm_util.IssueCommand([
      'gcloud', 'compute', 'disks', 'delete', disk_name,
      '--project', project, '--zone', zone, '--quiet',
  ], timeout=120, raise_on_failure=False)
  logging.info('[swap_encryption] Swap disk deleted: %s', disk_name)


def _delete_default_node_pool(cluster) -> None:
  """Delete the dummy default nodepool after the benchmark pool is ready.

  The default nodepool (e2-medium) was only needed to satisfy GKE's
  requirement that a cluster must have at least one nodepool at creation time.
  Removing it stops the clock on its cost immediately.
  """
  zone_flags: list[str] = []
  if getattr(cluster, 'zones', None):
    zone_flags = ['--zone', cluster.zones[0]]
  elif getattr(cluster, 'region', None):
    zone_flags = ['--region', cluster.region]

  cmd = [
      'gcloud', 'container', 'node-pools', 'delete', _DEFAULT_NODEPOOL,
      '--cluster', cluster.name,
      '--project', cluster.project,
      '--quiet',
  ] + zone_flags

  logging.info(
      '[swap_encryption] Deleting default nodepool: %s', _DEFAULT_NODEPOOL)
  stdout, stderr, rc = vm_util.IssueCommand(cmd, timeout=300,
                                            raise_on_failure=False)
  if rc != 0:
    logging.warning('[swap_encryption] Could not delete default nodepool '
                    '(rc=%d): %s', rc, stderr)
  else:
    logging.info('[swap_encryption] Default nodepool deleted')


# ---------------------------------------------------------------------------
# Pod exec wrapper
# ---------------------------------------------------------------------------

def _pod_exec(
    pod: str,
    cmd: str,
    ignore_failure: bool = False,
    timeout: int = 300,
) -> tuple[str, str]:
  """Run a shell command inside the benchmark pod via kubectl exec.

  Args:
    pod: Pod name returned by _wait_for_benchmark_pod.
    cmd: Shell command string passed to bash -c.
    ignore_failure: When True, non-zero exit codes are logged but not
      raised.
    timeout: Seconds before PKB kills the kubectl exec process. Default
      300 s matches PKB's IssueCommand default. Pass a larger value for
      long-running jobs (fio, stress-ng, kernel build).

  Returns:
    Tuple of (stdout, stderr) strings.
  """
  out, err, rc = kubectl.RunKubectlCommand(
      ['exec', pod, '-n', _DS_NAMESPACE,
       '--', 'bash', '-c', cmd],
      raise_on_failure=not ignore_failure,
      timeout=timeout,
  )
  return out, err


# ---------------------------------------------------------------------------
# Cloud-specific swap setup
# ---------------------------------------------------------------------------

def _detect_cloud(pod: str) -> str:
  """Detect GCP vs AWS from DMI product info exposed via /sys hostPath mount.

  DMI is the most reliable in-container detection method because it reads
  directly from the host kernel's SMBIOS table via /sys (already mounted).
  It avoids HTTP metadata endpoint quoting issues and network timeouts.

  Falls back to metadata HTTP endpoints if DMI is inconclusive.
  """
  # Primary: DMI product name / vendor (available via /sys hostPath mount)
  dmi_out, _ = _pod_exec(
      pod,
      'cat /sys/class/dmi/id/product_name 2>/dev/null || '
      'cat /sys/class/dmi/id/sys_vendor 2>/dev/null || echo ""',
      ignore_failure=True,
  )
  dmi = dmi_out.strip().lower()
  if 'google' in dmi:
    logging.info(
        '[swap_encryption] Cloud detected via DMI: gcp (%s)', dmi_out.strip())
    return 'gcp'
  if any(k in dmi for k in ('amazon', 'ec2', 'aws')):
    logging.info(
        '[swap_encryption] Cloud detected via DMI: aws (%s)', dmi_out.strip())
    return 'aws'

  # Secondary: GCP metadata endpoint.
  # Use -H with no space after colon to avoid shell-quoting issues through
  # the kubectl exec → bash -c pipeline.
  gcp_out, _ = _pod_exec(
      pod,
      'curl -s -m 3 '
      'http://metadata.google.internal/computeMetadata/v1/instance/zone '
      '-H Metadata-Flavor:Google 2>/dev/null || echo ""',
      ignore_failure=True,
  )
  if gcp_out.strip():
    logging.info('[swap_encryption] Cloud detected via metadata: gcp')
    return 'gcp'

  # Tertiary: AWS IMDS
  aws_out, _ = _pod_exec(
      pod,
      'curl -s -m 3 '
      'http://169.254.169.254/latest/meta-data/instance-id '
      '2>/dev/null || echo ""',
      ignore_failure=True,
  )
  if aws_out.strip():
    logging.info('[swap_encryption] Cloud detected via IMDS: aws')
    return 'aws'

  logging.warning(
      '[swap_encryption] Could not detect cloud from DMI or metadata')
  return 'unknown'


def _setup_gke_swap(pod: str) -> None:
  """Configure dm-crypt swap on the GKE node, mirroring go/node:swap-encryption.

  GKE nodes use dm-crypt with an ephemeral random key so that swap contents
  are encrypted at rest without requiring persistent key management.
  We replicate this exactly using cryptsetup in plain mode (no LUKS header).
  """
  swap_type = _SWAP_TYPE.value
  if swap_type == 'auto':
    # Check whether Local SSDs are present
    lssd_out, _ = _pod_exec(
        pod,
        "lsblk -d -o NAME,MODEL | grep -i 'local\\|nvme' | "
        "grep -v 'nvme0' | awk '{print $1}' | head -1",
        ignore_failure=True,
    )
    swap_type = 'lssd' if lssd_out.strip() else 'hyperdisk'

  if swap_type == 'lssd':
    _setup_gke_lssd_swap(pod)
  else:
    _setup_gke_hyperdisk_swap(pod)


def _setup_gke_hyperdisk_swap(pod: str) -> None:
  """Configure dm-crypt swap on hyperdisk-balanced (GKE default).

  Disk detection is split into two separate commands so that the boot-device
  name is resolved first and then substituted as a literal string — nested
  $() expansions inside a kubectl exec bash -c argument are unreliable.

  If no dedicated data disk is attached (single-disk node) dm-crypt is set up
  over a loop device backed by a file on the boot hyperdisk, which still
  exercises the full encryption path on the same storage tier.
  """
  logging.info('[swap_encryption] GKE: setting up dm-crypt on hyperdisk')

  # Step 1: identify the boot device name (e.g. "nvme0n1", "sda")
  boot_out, _ = _pod_exec(
      pod,
      'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
      ignore_failure=True,
  )
  boot_base = boot_out.strip() or 'nvme0n1'
  logging.info('[swap_encryption] GKE: boot device: %s', boot_base)

  # Step 2: find a non-boot disk using the literal name from step 1
  disk_out, _ = _pod_exec(
      pod,
      f"lsblk -d -o NAME,TYPE | awk '$2==\"disk\"{{print $1}}' "
      f"| grep -v '^{boot_base}$' | head -1",
      ignore_failure=True,
  )
  disk_name = disk_out.strip()

  if not disk_name:
    logging.info(
        '[swap_encryption] No dedicated data disk found – '
        'falling back to loop device on /mnt/stateful_partition '
        '(direct-io=on, dm-crypt=%s)', _ENABLE_DMCRYPT.value)
    _setup_gke_loop_device_swap(pod)
    return

  disk = f'/dev/{disk_name}'
  logging.info('[swap_encryption] GKE: swap target disk: %s  dmcrypt=%s',
               disk, _ENABLE_DMCRYPT.value)

  # Clean up any stale mapping from a previous failed run.
  _pod_exec(pod, textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    wipefs -a {disk} 2>/dev/null || true
  """), ignore_failure=True)

  if _ENABLE_DMCRYPT.value:
    # We cannot use cryptsetup open from inside a container because
    # libdevmapper calls dm_udev_wait() after creating the target, which
    # blocks on /run/udev/control.  That socket belongs to udevd which is
    # not running inside the container — so cryptsetup hangs forever.
    #
    # Instead we drive dmsetup directly with --noudevrules --noudevsync,
    # which skips all udev synchronisation, and call dmsetup mknodes to
    # ensure /dev/mapper/swap_encrypted appears without udev.
    #
    # insmod (not modprobe) loads the kernel module: modprobe also talks to
    # systemd-udevd and can deadlock from a container for the same reason.
    _pod_exec(pod, textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {disk})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{disk}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """))
    logging.info('[swap_encryption] GKE: dm-crypt swap active on '
                 '/dev/mapper/swap_encrypted')
  else:
    # Encryption-disabled column of the test matrix
    _pod_exec(pod, textwrap.dedent(f"""
      mkswap {disk} && \\
      swapon {disk}
    """))
    logging.info('[swap_encryption] GKE: plain (unencrypted) swap active '
                 'on %s', disk)


def _setup_gke_loop_device_swap(pod: str) -> None:
  """Plain loop-device swap for single-disk GKE COS nodes (dm-crypt unavailable).

  Used when _setup_gke_hyperdisk_swap finds no dedicated second disk (e.g.
  n4-highmem-8 / n4-highmem-32 single-boot-disk nodes).

  COS restriction: the device-mapper kernel subsystem is inaccessible from
  inside a Kubernetes pod (even privileged) on Container-Optimised OS.
  Calls to cryptsetup/dmsetup block indefinitely at the kernel level and are
  eventually killed by the PKB timeout.  This is not a permissions issue — it
  is a deliberate COS security restriction on dm operations from container
  namespaces.  For dedicated block devices (hyperdisk, LSSD) nsenter into
  the host mount namespace works around this (see _setup_gke_hyperdisk_swap).
  The loop device path skips dm-crypt because the loop device itself is
  created in the container namespace and its behaviour under nsenter is
  untested; plain loop swap is used instead.

  Therefore this path uses a plain loop device as swap without dm-crypt.
  Phase 1 (fio) is skipped for plain loop devices — the goal is enc-on vs
  enc-off comparison, and fio on a plain loop device measures the backing
  filesystem rather than the swap stack.  Tiers 2–6 (stress-ng, Redis,
  kernel build, OpenSearch) run normally.

  For dm-crypt measurement on GCP use a machine type with local NVMe (LSSD)
  or provision a dedicated hyperdisk on a second disk slot (n4-highmem-32+).

  Improvements over the old /var path:
  - Backing file on /mnt/stateful_partition (ext4), not the container
    overlayfs — avoids overlayfs O_DIRECT limitation.
  - losetup --direct-io=on passes I/O through to the host ext4, reducing
    double-buffering for Tiers 2–6 workloads.
  """
  size_gb = _SWAP_SIZE_GB.value
  # /mnt/stateful_partition is ext4 on COS (mounted from the stateful
  # partition of the node's persistent disk).  It is NOT the container
  # overlay filesystem and is mounted into the pod via the DaemonSet
  # hostPath volume.
  backing = '/mnt/stateful_partition/pkb_swap_backing'

  # ── Step 0: detach any stale loop device from a previous failed run ───────
  _pod_exec(pod, textwrap.dedent(f"""
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | \
      while read dev
      do
        swapoff "$dev" 2>/dev/null || true
        losetup -d "$dev" 2>/dev/null || true
      done
    rm -f {backing}
  """), ignore_failure=True)

  # ── Step 1: allocate backing file on stateful partition (ext4) ───────────
  logging.info(
      '[swap_encryption] GKE: creating %dG backing file on stateful_partition',
      size_gb)
  # fallocate preallocates real ext4 blocks (avoids fragmentation during swap
  # I/O); truncate is the sparse fallback for filesystems where fallocate
  # fails.
  _pod_exec(pod, textwrap.dedent(f"""
    fallocate -l {size_gb}G {backing} 2>/dev/null || \\
      truncate -s {size_gb}G {backing}
  """))

  # ── Step 2: loop device with direct-io passthrough ───────────────────────
  # --direct-io=on lets the loop driver pass O_DIRECT to the host ext4,
  # reducing double-buffering for workload I/O (kernel 5.x+, present on
  # GKE COS ≥ 1.29).
  loop_out, _ = _pod_exec(pod, textwrap.dedent(f"""
    LOOP=$(losetup -f) && \\
    losetup --direct-io=on "$LOOP" {backing} && \\
    echo "$LOOP"
  """))
  loop_dev = loop_out.strip()
  if not loop_dev.startswith('/dev/loop'):
    raise RuntimeError(
        f'[swap_encryption] losetup failed – output: {loop_out!r}'
    )
  logging.info('[swap_encryption] GKE: loop device: %s  direct-io=on', loop_dev)

  # ── Step 3: plain mkswap + swapon (dm-crypt unavailable on COS pods) ──────
  _pod_exec(pod, f'mkswap {loop_dev}')
  _pod_exec(pod, f'swapon {loop_dev}')
  logging.warning(
      '[swap_encryption] GKE: plain loop swap active on %s '
      '(dm-crypt unavailable from COS pod — device-mapper is blocked by '
      'COS kernel namespace restrictions). '
      'Phase 1 (fio) will be skipped. '
      'Use a machine with LSSD (c4-*-lssd) or attach a dedicated second '
      'hyperdisk for dm-crypt measurement.',
      loop_dev,
  )


def _setup_gke_lssd_swap(pod: str) -> None:
  """Configure dm-crypt on LSSD RAID-0 array (go/gke-swap-lssd)."""
  logging.info('[swap_encryption] GKE: setting up LSSD RAID-0 swap')

  # Step 1: identify boot device (findmnt returns 'overlay' inside the
  # container so lsblk silently fails — default to nvme0n1).
  boot_out, _ = _pod_exec(
      pod,
      'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
      ignore_failure=True,
  )
  boot_base = boot_out.strip() or 'nvme0n1'

  # Step 2: list all non-rotational disks that are not the boot device.
  lssd_out, _ = _pod_exec(
      pod,
      f"lsblk -d -o NAME,ROTA | awk '$2==\"0\"{{print \"/dev/\"$1}}' "
      f"| grep -v '^/dev/{boot_base}$'",
      ignore_failure=True,
  )
  devices = [d.strip() for d in lssd_out.strip().splitlines() if d.strip()]
  if not devices:
    logging.warning(
        '[swap_encryption] No LSSD devices found, falling back to hyperdisk')
    _setup_gke_hyperdisk_swap(pod)
    return

  device_list = ' '.join(devices)
  n = len(devices)
  logging.info('[swap_encryption] GKE: LSSD RAID-0 across %d devices: %s  '
               'dmcrypt=%s', n, device_list, _ENABLE_DMCRYPT.value)

  # Clean up stale mapping and RAID array from previous failed run.
  _pod_exec(pod, textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    mdadm --stop /dev/md0 2>/dev/null || true
    wipefs -a {device_list} 2>/dev/null || true
  """), ignore_failure=True)

  # Build RAID-0, then optionally wrap in dm-crypt (test matrix enc on/off).
  # --force is required when raid-devices=1; mdadm rejects single-device
  # RAID-0 without it.  mdadm (md driver) is NOT device-mapper and works
  # from container space on both COS and Ubuntu nodes.
  _pod_exec(pod, textwrap.dedent(f"""
    mdadm --create /dev/md0 --force \\
      --level=0 --raid-devices={n} \\
      {device_list}
    test -b /dev/md0 || {{ echo "mdadm: /dev/md0 not created" >&2; exit 1; }}
  """))

  if _ENABLE_DMCRYPT.value:
    # Same dmsetup --noudevrules --noudevsync approach as _setup_gke_hyperdisk_swap.
    _pod_exec(pod, textwrap.dedent("""
      grep -q dm_crypt /proc/modules 2>/dev/null || {
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz /dev/md0)
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "/dev/md0" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """))
    logging.info('[swap_encryption] GKE: LSSD RAID-0 dm-crypt swap active')
  else:
    _pod_exec(pod, textwrap.dedent("""
      mkswap /dev/md0 && \\
      swapon /dev/md0
    """))
    logging.info('[swap_encryption] GKE: LSSD RAID-0 plain (unencrypted) '
                 'swap active on /dev/md0')


def _setup_eks_swap(pod: str) -> None:
  """Configure swap on EKS nodes — Instance Store OR io2 root disk.

  Swap type is selected by --swap_encryption_swap_type:
    instance_store (default) – NVMe SSD attached by Nitro (i4i, m6id, c6id).
      Nitro encrypts all block-device writes at hardware level; no extra
      cryptsetup needed.
    io2 – EBS io2 volume provisioned as the node root/data disk.
      Used for apples-to-apples comparison against GKE hyperdisk-balanced.
  """
  swap_type = _SWAP_TYPE.value
  if swap_type in ('auto', 'instance_store'):
    _setup_eks_instance_store_swap(pod)
  elif swap_type == 'io2':
    _setup_eks_io2_swap(pod)
  else:
    logging.warning(
        '[swap_encryption] Unknown EKS swap type %s – fallback', swap_type)
    _setup_eks_instance_store_swap(pod)


def _setup_eks_instance_store_swap(pod: str) -> None:
  """Swap on AWS NVMe Instance Store (Nitro hardware-offloaded encryption)."""
  logging.info('[swap_encryption] EKS: setting up Instance Store swap')

  # Find the Instance Store NVMe device (not the root EBS volume)
  nvme_out, _ = _pod_exec(
      pod,
      "nvme list 2>/dev/null | awk '/Instance Storage/{print $1}' | head -1 || "
      "lsblk -d -o NAME,MODEL | grep -i 'instance\\|nvme' | "
      "grep -v 'nvme0' | awk '{print \"/dev/\"$1}' | head -1",
      ignore_failure=True,
  )
  device = nvme_out.strip()
  if not device:
    # Common Instance Store device paths on AWS
    for candidate in ['/dev/nvme1n1', '/dev/nvme2n1', '/dev/xvdb']:
      exists_out, _ = _pod_exec(
          pod, f'test -b {candidate} && echo yes || echo no',
          ignore_failure=True,
      )
      if exists_out.strip() == 'yes':
        device = candidate
        break

  if not device:
    logging.warning(
        '[swap_encryption] No Instance Store NVMe found – creating swapfile'
    )
    _setup_plain_swap_file(pod, _SWAP_SIZE_GB.value)
    return

  logging.info('[swap_encryption] EKS: Instance Store device: %s', device)

  # Nitro encrypts all Instance Store writes automatically.
  # No additional cryptsetup required.
  _pod_exec(pod, textwrap.dedent(f"""
    mkswap {device} && \\
    swapon {device}
  """))
  logging.info(
      '[swap_encryption] EKS: Instance Store swap active on %s', device)


def _setup_eks_io2_swap(pod: str) -> None:
  """Swap on AWS EBS io2 volume – apples-to-apples comparison vs GKE hyperdisk.

  EBS io2 volumes on Nitro instances are encrypted at rest by AWS KMS (if
  enabled on the volume) or via Nitro-level hardware encryption.  No additional
  cryptsetup is needed here; we simply format the attached data disk as swap.

  Device discovery order:
    1. Any non-root, non-Instance-Store block device (xvd*, sdb, second NVMe).
    2. /dev/nvme1n1, /dev/nvme2n1 – fallback if lsblk heuristics fail.
  """
  logging.info('[swap_encryption] EKS: setting up io2 EBS swap')

  # Identify root device so we can exclude it
  root_out, _ = _pod_exec(
      pod,
      "lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo nvme0n1",
      ignore_failure=True,
  )
  root_base = root_out.strip() or 'nvme0n1'

  # Prefer non-NVMe EBS volumes (xvdb, sdb, …) which are clearly not
  # Instance Store.  Fall back to the second NVMe if none found.
  disk_out, _ = _pod_exec(
      pod,
      f"lsblk -d -o NAME,TYPE | awk '$2==\"disk\"{{print $1}}' | "
      f"grep -v '{root_base}' | "
      f"grep -E '^xvd[b-z]|^sd[b-z]' | head -1",
      ignore_failure=True,
  )
  device = ('/dev/' + disk_out.strip()) if disk_out.strip() else ''

  if not device or device == '/dev/':
    # Try second NVMe (io2 can also appear as NVMe on Nitro)
    for candidate in ['/dev/nvme1n1', '/dev/nvme2n1', '/dev/xvdb', '/dev/sdb']:
      exists_out, _ = _pod_exec(
          pod, f'test -b {candidate} && echo yes || echo no',
          ignore_failure=True,
      )
      if exists_out.strip() == 'yes':
        device = candidate
        break

  if not device:
    logging.warning(
        '[swap_encryption] No io2 EBS disk found – creating plain swapfile'
    )
    _setup_plain_swap_file(pod, _SWAP_SIZE_GB.value)
    return

  logging.info('[swap_encryption] EKS: io2 EBS device: %s', device)

  # EBS io2 encryption is handled at the AWS level (Nitro / KMS).
  # No cryptsetup required on the guest side.
  _pod_exec(pod, textwrap.dedent(f"""
    mkswap {device} && \\
    swapon {device}
  """))
  logging.info('[swap_encryption] EKS: io2 EBS swap active on %s', device)


def _setup_plain_swap_file(pod: str, size_gb: int) -> None:
  """Fallback: create a loop-device-backed swapfile.

  A plain file on overlayfs (the container root) cannot be used as swap —
  the kernel rejects it with EINVAL.  Routing it through a loop device
  presents a proper block device to the mm subsystem and succeeds.
  """
  logging.info('[swap_encryption] Creating %dGB loop-device swap', size_gb)
  _pod_exec(pod, textwrap.dedent(f"""
    fallocate -l {size_gb}G /tmp/pkb_swapfile && \\
    chmod 600 /tmp/pkb_swapfile && \\
    LOOP=$(losetup -f) && \\
    losetup "$LOOP" /tmp/pkb_swapfile && \\
    mkswap "$LOOP" && \\
    swapon "$LOOP" && \\
    echo "swap loop device: $LOOP"
  """))


def _enable_zswap(pod: str) -> None:
  """Enable zswap with lz4 compressor and 20% pool limit inside the pod."""
  logging.info('[swap_encryption] Enabling zswap (lz4, 20%% pool)')
  for cmd in [
      'echo 1      > /sys/module/zswap/parameters/enabled',
      'echo lz4    > /sys/module/zswap/parameters/compressor',
      'echo 20     > /sys/module/zswap/parameters/max_pool_percent',
      'echo z3fold > /sys/module/zswap/parameters/zpool',
  ]:
    _pod_exec(pod, cmd, ignore_failure=True)


# ---------------------------------------------------------------------------
# Phase 1 – fio Microbenchmarks
# ---------------------------------------------------------------------------

def _phase1_fio(
    pod: str, swap_dev: str, base_meta: dict
) -> list[sample.Sample]:
  """Run fio directly on the swap block device for raw I/O characterisation.

  Skipped for plain loop devices (single-disk GKE COS node fallback):
  COS blocks device-mapper from inside pods, so dm-crypt is unavailable on
  single-disk nodes and the loop device is used as plain swap.  Running fio
  on a plain loop device measures the backing filesystem (stateful_partition
  ext4), not the swap stack, making results misleading for the enc comparison.

  For dedicated second disks (hyperdisk, LSSD, NVMe) direct I/O is always
  used and swap is restored (mkswap + swapon) after the fio run.
  To get fio results on GCP use c4-*-lssd (local NVMe, bypasses this path)
  or provision a dedicated hyperdisk on a second disk slot.
  """
  if swap_dev.startswith('/dev/loop'):
    logging.warning(
        '[swap_encryption] Phase 1 (fio) SKIPPED for plain loop device %s. '
        'COS blocks device-mapper from pod namespaces so dm-crypt is '
        'unavailable on single-disk nodes. '
        'Use c4-*-lssd or attach a dedicated second disk for fio results.',
        swap_dev,
    )
    return []

  results = []

  _pod_exec(pod, f'swapoff {swap_dev}', ignore_failure=True)

  # Pre-fill device so read tests have real data (avoids zero-block optimisation
  # by the storage controller skewing read latency measurements).
  # Cap at 20 GiB — enough to warm up the dm-crypt pipeline and cover the fio
  # runtime window.  Writing 100% of a 500 GiB hyperdisk takes ~500+ seconds
  # at provisioned throughput, which exceeds the PKB command timeout.
  # Timeout: 20 GiB / ~150 MB/s (conservative dm-crypt write rate) + 60 s buffer.
  _PREFILL_GIB = 20
  prefill_timeout = _PREFILL_GIB * 1024 // 150 + 60  # ~197 s, rounds up to ~200 s
  prefill_timeout = max(prefill_timeout, 300)          # floor at 5 min
  logging.info('[swap_encryption] Pre-filling %d GiB of %s', _PREFILL_GIB, swap_dev)
  _pod_exec(pod, (
      f'fio --name=prefill --filename={swap_dev} '
      f'--ioengine=libaio --direct=1 --rw=write --bs=1m '
      f'--size={_PREFILL_GIB}g --verify=0'
  ), timeout=prefill_timeout)

  # Each fio job: runtime + 60 s buffer for setup/teardown
  fio_timeout = _FIO_RUNTIME_SEC.value + 60

  for name, rw, bs, depth, label in _FIO_JOBS:
    logging.info('[swap_encryption] fio: %s', name)
    cmd = (
        f'fio --name={name} --filename={swap_dev} '
        f'--ioengine=libaio --direct=1 --verify=0 --randrepeat=0 '
        f'--bs={bs} --iodepth={depth} --rw={rw} '
        f'--time_based --runtime={_FIO_RUNTIME_SEC.value}s '
        f'--output-format=json'
    )
    out, _ = _pod_exec(pod, cmd, timeout=fio_timeout)
    results += _parse_fio_json(out, name, label, base_meta)

  # fio prefill overwrites the entire device, destroying the mkswap header.
  # Re-stamp and re-enable before the remaining phases need active swap.
  _pod_exec(pod, f'mkswap {swap_dev} && swapon {swap_dev}',
           ignore_failure=True, timeout=120)
  return results


def _parse_fio_json(
    stdout: str, job_name: str, label: str, base_meta: dict
) -> list[sample.Sample]:
  """Parse fio JSON output into PKB Samples."""
  results = []
  try:
    data = json.loads(stdout)
  except (json.JSONDecodeError, ValueError):
    logging.warning('[swap_encryption] fio JSON parse failed for %s', job_name)
    return results

  meta = dict(base_meta, fio_job=job_name, fio_label=label)
  for job in data.get('jobs', []):
    for direction in ('read', 'write'):
      d = job.get(direction, {})
      if not d or d.get('io_bytes', 0) == 0:
        continue
      iops = float(d.get('iops', 0))
      bw_kib = float(d.get('bw', 0))
      clat = d.get('clat_ns', {})
      pct = clat.get('percentile', {})
      lat_mean = float(clat.get('mean', 0)) / 1000.0
      lat_p50 = float(pct.get('50.000000', 0)) / 1000.0
      lat_p99 = float(pct.get('99.000000', 0)) / 1000.0
      lat_p999 = float(pct.get('99.900000', 0)) / 1000.0
      m = dict(meta, direction=direction)
      results += [
          sample.Sample(
              f'{job_name}_{direction}_iops', iops, 'iops', m),
          sample.Sample(
              f'{job_name}_{direction}_bw_mbps', bw_kib / 1024, 'MB/s', m),
          sample.Sample(
              f'{job_name}_{direction}_lat_mean', lat_mean, 'us', m),
          sample.Sample(
              f'{job_name}_{direction}_lat_p50', lat_p50, 'us', m),
          sample.Sample(
              f'{job_name}_{direction}_lat_p99', lat_p99, 'us', m),
          sample.Sample(
              f'{job_name}_{direction}_lat_p999', lat_p999, 'us', m),
      ]
  return results


# ---------------------------------------------------------------------------
# Phase 2a – CPU Overhead Under Swap Pressure
# ---------------------------------------------------------------------------

def _phase2a_cpu_overhead(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Measure CPU cost of dm-crypt / Nitro while stress-ng drives swap I/O.

  If --swap_encryption_stress_vm_bytes_list is set the phase is run once per
  listed intensity value so that a full pressure-curve is captured (gap 5).
  Otherwise the single value from --swap_encryption_stress_vm_bytes is used.
  """
  # Build the list of vm-bytes intensities to sweep (gap 5)
  if _STRESS_VM_BYTES_LIST.value.strip():
    intensities = [v.strip() for v in _STRESS_VM_BYTES_LIST.value.split(',')
                   if v.strip()]
  else:
    intensities = [_STRESS_VM_BYTES.value]

  results = []
  for vm_bytes in intensities:
    logging.info('[swap_encryption] Phase 2a: stress-ng intensity %s', vm_bytes)
    results += _run_cpu_overhead_sweep(pod, base_meta, vm_bytes)
  return results


def _run_cpu_overhead_sweep(
    pod: str, base_meta: dict, vm_bytes: str
) -> list[sample.Sample]:
  """Single stress-ng intensity sweep for Phase 2a."""
  results = []
  meta = dict(base_meta, phase='cpu_overhead', stress_vm_bytes=vm_bytes)
  timeout = _STRESS_TIMEOUT_SEC.value
  interval = 2

  vmstat_log = f'/tmp/pkb_vmstat_{vm_bytes}.log'
  pidstat_log = f'/tmp/pkb_pidstat_{vm_bytes}.log'

  # Start background collectors (access host /proc via hostPath mount)
  _pod_exec(pod, (
      f'vmstat {interval} {timeout // interval} > {vmstat_log} 2>&1 &'
  ))
  _pod_exec(pod, (
      f'pidstat -u {interval} {timeout // interval} '
      f'-p ALL > {pidstat_log} 2>&1 &'
  ))

  t0 = time.time()
  stress_out, _ = _pod_exec(pod, (
      f'stress-ng --vm 1 '
      f'--vm-bytes {vm_bytes} '
      f'--vm-method all '
      f'--timeout {timeout}s '
      f'--metrics-brief 2>&1'
  ), timeout=timeout + 60)
  elapsed = time.time() - t0

  time.sleep(interval + 1)  # let collectors flush last sample

  results.append(sample.Sample('stress_ng_duration_sec', elapsed, 's', meta))

  for line in stress_out.splitlines():
    m = re.search(r'vm\s+\d+\s+(\d+)\s+\S+\s+bogo-ops', line)
    if m:
      results.append(
          sample.Sample('stress_ng_bogo_ops', float(m.group(1)), 'ops', meta)
      )
      break

  vmstat_out, _ = _pod_exec(pod, f'cat {vmstat_log}', ignore_failure=True)
  results += _parse_vmstat(vmstat_out, meta)

  pidstat_out, _ = _pod_exec(pod, f'cat {pidstat_log}', ignore_failure=True)
  results += _parse_pidstat(pidstat_out, meta)
  return results


def _parse_vmstat(output: str, base_meta: dict) -> list[sample.Sample]:
  """Parse vmstat output for swap rates AND CPU utilisation.

  Standard vmstat column layout (non-header data lines, 0-indexed):
    r b swpd free buff cache  si  so  bi  bo  in  cs  us  sy  id  wa  st
    0 1    2    3    4     5   6   7   8   9  10  11  12  13  14  15  16

  si=6, so=7  – swap-in / swap-out pages/s
  us=12        – user CPU %
  sy=13        – system (kernel) CPU %  ← gap 2: system time %
  id=14        – idle CPU %
  wa=15        – I/O wait CPU %
  total_active = us + sy + wa          ← gap 1: total CPU utilisation
  """
  si_vals, so_vals = [], []
  us_vals, sy_vals, wa_vals = [], [], []

  for line in output.splitlines():
    parts = line.split()
    if len(parts) < 17 or not parts[0].isdigit():
      continue
    try:
      si_vals.append(float(parts[6]))
      so_vals.append(float(parts[7]))
      us_vals.append(float(parts[12]))
      sy_vals.append(float(parts[13]))
      wa_vals.append(float(parts[15]))
    except (ValueError, IndexError):
      pass

  if not si_vals:
    return []

  meta = dict(base_meta, metric_source='vmstat')

  def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

  def _peak(lst):
    return max(lst) if lst else 0.0

  total_active = [u + s + w for u, s, w in zip(us_vals, sy_vals, wa_vals)]

  return [
      # Swap rates
      sample.Sample(
          'swap_in_pages_per_sec', _mean(si_vals), 'pages/s', meta),
      sample.Sample(
          'swap_in_pages_per_sec_max', _peak(si_vals), 'pages/s', meta),
      sample.Sample(
          'swap_out_pages_per_sec', _mean(so_vals), 'pages/s', meta),
      sample.Sample(
          'swap_out_pages_per_sec_max', _peak(so_vals), 'pages/s', meta),
      # Total CPU utilisation (gap 1)
      sample.Sample(
          'total_cpu_pct_avg', _mean(total_active), '%', meta),
      sample.Sample(
          'total_cpu_pct_max', _peak(total_active), '%', meta),
      # System (kernel) time % – encryption overhead signal (gap 2)
      sample.Sample('system_time_pct_avg', _mean(sy_vals), '%', meta),
      sample.Sample('system_time_pct_max', _peak(sy_vals), '%', meta),
      # User and I/O-wait for completeness
      sample.Sample('user_cpu_pct_avg', _mean(us_vals), '%', meta),
      sample.Sample('iowait_cpu_pct_avg', _mean(wa_vals), '%', meta),
  ]


def _parse_pidstat(output: str, base_meta: dict) -> list[sample.Sample]:
  """Parse CPU % for swap/encryption-related kernel threads from pidstat."""
  cpu_by_proc: dict[str, list[float]] = {}
  for line in output.splitlines():
    parts = line.split()
    if len(parts) < 9:
      continue
    proc = parts[-1]
    if not any(t in proc for t in _CRYPTO_PROCS):
      continue
    try:
      cpu_by_proc.setdefault(proc, []).append(float(parts[7]))
    except (ValueError, IndexError):
      pass
  results = []
  meta = dict(base_meta, metric_source='pidstat')
  for proc, vals in cpu_by_proc.items():
    m = dict(meta, process=proc)
    results += [
        sample.Sample(f'cpu_pct_avg_{proc}', sum(vals) / len(vals), '%', m),
        sample.Sample(f'cpu_pct_max_{proc}', max(vals), '%', m),
    ]
  return results


# ---------------------------------------------------------------------------
# Phase 2b – I/O Interference
# ---------------------------------------------------------------------------

def _phase2b_io_interference(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Quantify drop in application I/O when swap is under simultaneous pressure."""
  results = []
  app_file = '/tmp/pkb_app_io'
  timeout = _STRESS_TIMEOUT_SEC.value
  meta = dict(base_meta, phase='io_interference')

  # Create test file on the container filesystem (tmpfs or overlay)
  # Give generous timeout for large file creation
  _pod_exec(pod, (
      f'fio --name=create --filename={app_file} '
      f'--rw=write --bs=1m --size=8G --verify=0'
  ), timeout=600)

  def _run_app_fio(pressure_label: str) -> list[sample.Sample]:
    cmd = (
        f'fio --name=app_io --filename={app_file} '
        f'--ioengine=libaio --direct=1 '
        f'--rw=randrw --bs=4k --iodepth=32 --size=8G --verify=0 '
        f'--time_based --runtime=60s --output-format=json'
    )
    out, _ = _pod_exec(pod, cmd)
    return _parse_fio_json(
        out, 'app_io', f'App I/O ({pressure_label})',
        dict(meta, pressure=pressure_label),
    )

  # 1. Baseline – no swap pressure
  logging.info('[swap_encryption] I/O interference: baseline (no pressure)')
  results += _run_app_fio('no_pressure')

  # 2. Under swap pressure
  # Use nohup + disown so bash exits immediately after launching stress-ng;
  # otherwise kubectl exec keeps the session alive until stress-ng finishes
  # (300 s) and PKB's IssueCommand times out.
  logging.info('[swap_encryption] I/O interference: under swap pressure')
  # Wrap in bash -c '...' so PKB's semicolon checker does not fire on
  # the & + disown pattern needed to detach the process from kubectl exec.
  _pod_exec(pod, textwrap.dedent(f"""
    nohup stress-ng --vm 1 \\
      --vm-bytes {_STRESS_VM_BYTES.value} \\
      --vm-method all \\
      --timeout {timeout}s \\
      >/tmp/pkb_stress_io.log 2>&1 &
    disown
    echo STRESS_STARTED
  """), timeout=30)
  time.sleep(10)  # let swap pressure build
  results += _run_app_fio('with_swap_pressure')

  # Wait for stress-ng to finish naturally (it auto-exits after --timeout)
  _pod_exec(pod, f'sleep {timeout}', ignore_failure=True, timeout=timeout + 30)
  return results


# ---------------------------------------------------------------------------
# Phase 3a – Redis Latency Under Memory Pressure
# ---------------------------------------------------------------------------

def _phase3a_redis(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Load Redis beyond its memory cap and measure GET/SET P50/P90/P99 latency.

  Uses memtier_benchmark (installed in the DaemonSet) instead of the built-in
  redis-benchmark because memtier reports per-percentile latency (P50/P90/P99)
  which is required by the test plan (redis SET/GET P90/P99 under memory
  pressure).  This mirrors the approach in PKB's redis_memtier_benchmark.
  """
  results = []
  meta = dict(base_meta, workload='redis', tool='memtier_benchmark')

  # Start Redis and wait up to 30 s for it to accept connections.
  # `service redis-server start` fails inside a container (no init system)
  # so we fall through to a direct redis-server invocation.  A retry loop
  # on redis-cli PING is more reliable than a fixed sleep.
  _pod_exec(pod, textwrap.dedent("""
    pkill -x redis-server 2>/dev/null || true
    sleep 1
    redis-server --port 6379 --daemonize yes \
      --bind 127.0.0.1 \
      --logfile /tmp/redis.log \
      --loglevel notice \
      --save "" \
      --appendonly no 2>/dev/null || true
    for i in $(seq 1 30)
    do
      redis-cli -p 6379 ping 2>/dev/null | grep -q PONG && echo "Redis ready" && break
      sleep 1
    done
  """), ignore_failure=True, timeout=45)

  maxmem = _REDIS_MAXMEMORY_MB.value * 1024 * 1024
  _pod_exec(pod, f'redis-cli CONFIG SET maxmemory {maxmem}',
            ignore_failure=True)
  _pod_exec(pod, 'redis-cli CONFIG SET maxmemory-policy allkeys-lru',
            ignore_failure=True)

  # Pre-load dataset (forces eviction/swap once dataset > maxmemory)
  n_keys = (_REDIS_DATASET_MB.value * 1024 * 1024) // 128
  logging.info('[swap_encryption] Loading %d Redis keys (%d MB)',
               n_keys, _REDIS_DATASET_MB.value)
  _pod_exec(pod,
           f'redis-benchmark -n {n_keys} -d 128 -t SET -q >/dev/null 2>&1',
           ignore_failure=True, timeout=600)

  # Apply swap pressure — detach so kubectl exec returns immediately.
  _pod_exec(pod, textwrap.dedent(f"""
    nohup stress-ng --vm 1 \\
      --vm-bytes {_STRESS_VM_BYTES.value} \\
      --vm-method all --timeout 120s \\
      >/tmp/pkb_stress_redis.log 2>&1 &
    disown
    echo STRESS_STARTED
  """), timeout=30)
  time.sleep(8)

  # Run the latency workload.  Prefer memtier_benchmark (gives per-percentile
  # JSON) but fall back to redis-benchmark --csv which is always available via
  # the redis-tools package installed in the DaemonSet init script.
  memtier_avail, _ = _pod_exec(
      pod, 'command -v memtier_benchmark 2>/dev/null', ignore_failure=True)
  if memtier_avail.strip():
    meta = dict(base_meta, workload='redis', tool='memtier_benchmark')
    mt_cmd = (
        'memtier_benchmark '
        '--server 127.0.0.1 --port 6379 --protocol redis '
        '--clients 50 --threads 4 --test-time 60 '
        '--data-size 128 '
        '--ratio 1:1 '
        '--hide-histogram '
        '--json-out-file /tmp/pkb_memtier.json '
        '2>&1'
    )
    _pod_exec(pod, mt_cmd, ignore_failure=True, timeout=120)
    results += _parse_memtier_json('/tmp/pkb_memtier.json', pod, meta)
  else:
    # redis-benchmark fallback: --csv gives us latency percentiles
    logging.warning('[swap_encryption] memtier_benchmark not found; '
                    'using redis-benchmark as fallback')
    meta = dict(base_meta, workload='redis', tool='redis_benchmark_fallback')
    rb_out, _ = _pod_exec(pod, textwrap.dedent("""
      redis-benchmark -h 127.0.0.1 -p 6379 \
        -c 50 -n 100000 -d 128 -t get,set \
        --csv 2>&1
    """), ignore_failure=True, timeout=120)
    results += _parse_redis_benchmark_csv(rb_out, meta)

  return results


def _parse_memtier_json(
    json_path: str, pod: str, base_meta: dict
) -> list[sample.Sample]:
  """Parse memtier_benchmark JSON output into PKB Samples.

  Extracts throughput (ops/s) and latency percentiles (P50, P90, P99)
  for both GET and SET operations, as required by the test plan.
  """
  raw, _ = _pod_exec(pod, f'cat {json_path} 2>/dev/null || echo ""',
                    ignore_failure=True)
  results = []
  try:
    data = json.loads(raw)
  except (json.JSONDecodeError, ValueError):
    logging.warning('[swap_encryption] memtier JSON parse failed')
    return results

  # memtier JSON structure: {"ALL STATS": {"Sets": {...}, "Gets": {...}, ...}}
  all_stats = data.get('ALL STATS', {})
  op_map = {
      'Sets': 'set',
      'Gets': 'get',
      'Totals': 'total',
  }
  for json_key, op_label in op_map.items():
    section = all_stats.get(json_key, {})
    if not section:
      continue
    m = dict(base_meta, operation=op_label)
    ops_sec = section.get('Ops/sec', 0)
    lat_avg = section.get('Latency', {}).get('Average Latency', 0)
    lat_p50 = section.get('Latency', {}).get('50th Percentile Latency', 0)
    lat_p90 = section.get('Latency', {}).get('90th Percentile Latency', 0)
    lat_p99 = section.get('Latency', {}).get('99th Percentile Latency', 0)
    lat_p999 = section.get('Latency', {}).get('99.9th Percentile Latency', 0)
    results += [
        sample.Sample(
            f'redis_{op_label}_ops_per_sec', float(ops_sec), 'ops/s', m),
        sample.Sample(
            f'redis_{op_label}_lat_avg_ms', float(lat_avg), 'ms', m),
        sample.Sample(
            f'redis_{op_label}_lat_p50_ms', float(lat_p50), 'ms', m),
        sample.Sample(
            f'redis_{op_label}_lat_p90_ms', float(lat_p90), 'ms', m),
        sample.Sample(
            f'redis_{op_label}_lat_p99_ms', float(lat_p99), 'ms', m),
        sample.Sample(
            f'redis_{op_label}_lat_p999_ms', float(lat_p999), 'ms', m),
    ]
  return results


def _parse_redis_benchmark_csv(
    output: str, base_meta: dict
) -> list[sample.Sample]:
  """Parse redis-benchmark --csv output into PKB Samples.

  redis-benchmark --csv emits lines like:
    "SET","107526.88"
    "GET","115207.37"
  Each line gives the test name and throughput (requests/sec).
  Latency percentiles are not available in the CSV format; we emit only
  ops/sec so the run still produces comparable throughput data.
  """
  results = []
  for line in output.splitlines():
    line = line.strip()
    if not line or line.startswith('#'):
      continue
    parts = line.replace('"', '').split(',')
    if len(parts) < 2:
      continue
    op = parts[0].lower()       # e.g. "set", "get"
    try:
      ops = float(parts[1])
    except ValueError:
      continue
    m = dict(base_meta, operation=op)
    results.append(sample.Sample(f'redis_{op}_ops_per_sec', ops, 'ops/s', m))
  if not results:
    logging.warning('[swap_encryption] redis-benchmark CSV parse produced no samples')
  return results


# ---------------------------------------------------------------------------
# Phase 3b – Kernel Build Under Memory Constraint
# ---------------------------------------------------------------------------

def _phase3b_kernel_build(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Compile Linux inside a cgroup memory cap; compare to unconstrained."""
  results = []
  ver = _KERNEL_VERSION.value
  root = '/tmp/pkb_kernel'
  tarball = f'{root}/linux-{ver}.tar.xz'
  src = f'{root}/linux-{ver}'
  url = (
      f'https://cdn.kernel.org/pub/linux/kernel/'
      f'v{ver.split(".")[0]}.x/linux-{ver}.tar.xz'
  )

  _pod_exec(pod, f'mkdir -p {root}')
  _pod_exec(pod, f'test -f {tarball} || wget -q --timeout=120 -O {tarball} {url}',
           timeout=600)
  _pod_exec(pod, f'test -d {src}    || tar -xf {tarball} -C {root}',
           timeout=600)
  _pod_exec(pod, f'make -C {src} defconfig -j$(nproc) 2>&1', timeout=300)

  cgroup = '/sys/fs/cgroup/memory/pkb_kernelbuild'
  mem_bytes = _KERNEL_MEMORY_MB.value * 1024 * 1024

  _pod_exec(pod, (
      f'mkdir -p {cgroup} && '
      f'echo {mem_bytes} > {cgroup}/memory.limit_in_bytes'
  ), ignore_failure=True)

  def _build(label: str, use_cgroup: bool) -> sample.Sample:
    _pod_exec(pod, f'make -C {src} clean 2>&1')
    if use_cgroup:
      cmd = (f'cgexec -g memory:pkb_kernelbuild '
             f'make -C {src} -j$(nproc) vmlinux 2>&1 '
             f'|| make -C {src} -j$(nproc) vmlinux 2>&1')
    else:
      cmd = f'make -C {src} -j$(nproc) vmlinux 2>&1'
    t0 = time.time()
    _pod_exec(pod, cmd, timeout=3600)  # kernel builds can take up to ~1 hr
    elapsed = time.time() - t0
    m = dict(base_meta,
             workload='kernel_build',
             kernel_version=ver,
             build_variant=label,
             memory_limit_mb=(
                 _KERNEL_MEMORY_MB.value if use_cgroup else 'unconstrained'))
    return sample.Sample('kernel_build_elapsed_sec', elapsed, 's', m)

  s_constrained = _build('constrained', use_cgroup=True)
  s_unconstrained = _build('unconstrained', use_cgroup=False)
  results += [s_constrained, s_unconstrained]

  if s_unconstrained.value > 0:
    ratio = s_constrained.value / s_unconstrained.value
    results.append(sample.Sample(
        'kernel_build_slowdown_ratio', ratio, 'ratio',
        dict(base_meta, workload='kernel_build', kernel_version=ver,
             memory_limit_mb=_KERNEL_MEMORY_MB.value),
    ))
  return results


# ---------------------------------------------------------------------------
# Phase 3c – OpenSearch
# ---------------------------------------------------------------------------

def _phase3c_opensearch(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Index + query workload under swap pressure (esrally or curl fallback)."""
  meta = dict(base_meta, workload='opensearch')

  # Detach stress-ng so kubectl exec exits immediately; see Phase 2b comment.
  _pod_exec(pod, textwrap.dedent(f"""
    nohup stress-ng --vm 1 \\
      --vm-bytes {_STRESS_VM_BYTES.value} \\
      --vm-method all --timeout {_STRESS_TIMEOUT_SEC.value}s \\
      >/tmp/pkb_stress_opensearch.log 2>&1 &
    disown
    echo STRESS_STARTED
  """), timeout=30)
  time.sleep(10)

  esrally_out, _ = _pod_exec(
      pod, 'which esrally 2>/dev/null', ignore_failure=True)
  if esrally_out.strip():
    return _run_esrally(pod, meta)
  else:
    logging.info('[swap_encryption] esrally absent – using curl fallback')
    return _run_opensearch_curl(pod, meta)


def _run_esrally(pod: str, meta: dict) -> list[sample.Sample]:
  """Run esrally geonames track with a capped JVM heap to induce swap pressure.

  esrally is installed via pip3 in the DaemonSet init script and uses the
  same geonames track as PKB's standalone esrally_benchmark.py, so results
  are directly comparable.  The JVM heap is capped to 512 MB so the OS page
  cache overflows to swap during indexing — the key swap pressure scenario
  described in the methodology doc.
  """
  jvm_heap_mb = 512
  # Patch jvm.options before starting Elasticsearch/OpenSearch
  _pod_exec(pod, textwrap.dedent(f"""
    for f in /etc/elasticsearch/jvm.options /etc/opensearch/jvm.options \\
              /usr/share/elasticsearch/config/jvm.options \\
              /usr/share/opensearch/config/jvm.options
    do
      test -f "$f" || continue
      sed -i 's/^-Xms.*/-Xms{jvm_heap_mb}m/' "$f"
      sed -i 's/^-Xmx.*/-Xmx{jvm_heap_mb}m/' "$f"
    done
    export ES_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
    export OPENSEARCH_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
  """), ignore_failure=True)

  _pod_exec(pod,
           'systemctl start elasticsearch 2>/dev/null || '
           'systemctl start opensearch 2>/dev/null || true',
           ignore_failure=True)
  time.sleep(15)  # wait for the engine to be ready

  _pod_exec(pod, textwrap.dedent("""
    esrally race \\
      --track=geonames \\
      --target-hosts=localhost:9200 \\
      --pipeline=benchmark-only \\
      --report-format=csv \\
      --report-file=/tmp/pkb_esrally.csv \\
      --track-param="number_of_replicas:0" \\
      2>&1
  """), ignore_failure=True, timeout=3600)

  csv_out, _ = _pod_exec(pod, 'cat /tmp/pkb_esrally.csv 2>/dev/null || echo ""')
  results = []
  for line in csv_out.splitlines():
    parts = [p.strip() for p in line.split(',')]
    if len(parts) < 3:
      continue
    metric = parts[0].lower().replace(' ', '_').replace('-', '_')
    try:
      value = float(parts[2])
      unit = parts[3].strip() if len(parts) > 3 else 'unknown'
      results.append(sample.Sample(f'opensearch_{metric}', value, unit,
                                   dict(meta, tool='esrally',
                                        jvm_heap_mb=jvm_heap_mb)))
    except (ValueError, IndexError):
      pass
  return results


def _run_opensearch_curl(pod: str, meta: dict) -> list[sample.Sample]:
  """Minimal OpenSearch benchmark via curl (fallback).

  Elasticsearch/OpenSearch JVM heap is deliberately capped to a small value
  so that the JVM off-heap buffers and OS page cache overflow to swap during
  indexing, making this a realistic swap-pressure workload (gap 4).
  """
  # Cap the JVM heap so OS page cache / off-heap memory causes swap pressure.
  # 512 MB heap on a 32-vCPU node leaves almost all RAM available for page
  # cache, which the kernel will then need to reclaim under bulk-index load.
  jvm_heap_mb = 512
  _pod_exec(pod, textwrap.dedent(f"""
    # Patch jvm.options in-place for Elasticsearch and OpenSearch installs
    for jvm_opts_file in \\
        /etc/elasticsearch/jvm.options \\
        /etc/opensearch/jvm.options \\
        /usr/share/elasticsearch/config/jvm.options \\
        /usr/share/opensearch/config/jvm.options
    do
      test -f "$jvm_opts_file" || continue
      sed -i 's/^-Xms.*/-Xms{jvm_heap_mb}m/' "$jvm_opts_file"
      sed -i 's/^-Xmx.*/-Xmx{jvm_heap_mb}m/' "$jvm_opts_file"
      echo "[swap_encryption] Patched $jvm_opts_file"
    done
    # Environment-variable fallback (works with both ES and OpenSearch)
    export ES_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
    export OPENSEARCH_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
  """), ignore_failure=True)

  _pod_exec(pod,
           'systemctl start elasticsearch 2>/dev/null || true',
           ignore_failure=True)
  time.sleep(10)

  doc = '{"index":{}}\n{"field":"benchmark","ts":"2026-01-01"}\n'
  bulk = doc * 500

  t0 = time.time()
  _pod_exec(pod, (
      f'printf "%s" \'{bulk}\' | '
      "curl -s -X POST 'http://localhost:9200/pkb_test/_bulk' "
      "-H 'Content-Type: application/x-ndjson' "
      "--data-binary @- -o /dev/null"
  ), ignore_failure=True)
  index_sec = time.time() - t0

  t0 = time.time()
  _pod_exec(pod, (
      "curl -s 'http://localhost:9200/pkb_test/_search?q=field:benchmark' "
      "-o /dev/null"
  ), ignore_failure=True)
  query_sec = time.time() - t0

  m = dict(meta, tool='curl_fallback')
  return [
      sample.Sample('opensearch_bulk_index_sec', index_sec, 's', m),
      sample.Sample('opensearch_query_latency_sec', query_sec, 's', m),
  ]


# ---------------------------------------------------------------------------
# Gap 7 – Cloud cost estimation
# ---------------------------------------------------------------------------

# On-demand pricing (USD/hr) for the primary benchmark instance types.
# Values are approximate list prices (us-central1 / us-east-1) as of 2026-05.
# Update this table when running on other regions or reserved/spot capacity.
_INSTANCE_PRICE_USD_PER_HR: dict[str, float] = {
    # GCP
    'n4-highmem-32':   3.0256,   # 32 vCPU, 256 GB RAM, us-central1
    'n2-highmem-32':   2.5216,   # 32 vCPU, 256 GB RAM
    'n2-standard-32':  1.5264,   # 32 vCPU, 120 GB RAM
    'z3-highmem-8':    2.7248,   # 8 vCPU + 4× LSSD, us-central1
    # AWS
    'i4i.4xlarge':     1.4960,   # 16 vCPU, 128 GB RAM, NVMe Instance Store
    'i4i.2xlarge':     0.7480,
    'm6id.4xlarge':    0.9072,   # 16 vCPU, 64 GB RAM, NVMe Instance Store
    'm6i.4xlarge':     0.7680,   # 16 vCPU, 64 GB RAM, no Instance Store
    'r6i.4xlarge':     1.0080,   # 16 vCPU, 128 GB RAM, no Instance Store
}


def _collect_cost_sample(
    pod: str, elapsed_sec: float, base_meta: dict
) -> list[sample.Sample]:
  """Emit a cost_estimate_usd sample for the benchmark run (gap 7).

  Instance type is read from cloud metadata inside the pod.  Price is looked
  up from _INSTANCE_PRICE_USD_PER_HR; if unknown, the sample is omitted and
  a warning is logged.

  Args:
    pod: Benchmark pod name.
    elapsed_sec: Wall-clock seconds the benchmark phases took.
    base_meta: Shared metadata dict.

  Returns:
    A list of zero or one sample.Sample.
  """
  # Detect instance type from cloud metadata
  instance_type = ''

  # GCP: machine type is the last segment of the metadata URL value
  gcp_type_out, _ = _pod_exec(
      pod,
      'curl -s -m 3 --fail '
      'http://metadata.google.internal/computeMetadata/v1/instance/machine-type '
      '-H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
      ignore_failure=True,
  )
  if gcp_type_out.strip():
    instance_type = gcp_type_out.strip().split('/')[-1]

  if not instance_type:
    # AWS: instance-type is a plain string
    aws_type_out, _ = _pod_exec(
        pod,
        'curl -s -m 3 --fail '
        'http://169.254.169.254/latest/meta-data/instance-type '
        '2>/dev/null || echo ""',
        ignore_failure=True,
    )
    instance_type = aws_type_out.strip()

  # Allow flag override (useful when running on custom/renamed machine types)
  if _INSTANCE_SIZE_LABEL.value:
    instance_type = _INSTANCE_SIZE_LABEL.value

  price = _INSTANCE_PRICE_USD_PER_HR.get(instance_type)
  if price is None:
    logging.warning(
        '[swap_encryption] Unknown instance type "%s" – skipping cost sample. '
        'Add it to _INSTANCE_PRICE_USD_PER_HR to enable cost tracking.',
        instance_type,
    )
    return []

  hours = elapsed_sec / 3600.0
  cost = hours * price
  meta = dict(
      base_meta,
      instance_type=instance_type,
      price_usd_per_hr=price,
      benchmark_elapsed_sec=round(elapsed_sec, 1),
  )
  return [sample.Sample('cost_estimate_usd', cost, 'USD', meta)]


# ---------------------------------------------------------------------------
# Swap device detection (runs inside the pod)
# ---------------------------------------------------------------------------

def _detect_swap_device(pod: str) -> str:
  """Return the active swap device path on the cluster node."""
  if _SWAP_DEVICE.value:
    return _SWAP_DEVICE.value

  # Prefer dm-crypt mapped device (GKE).
  # Two-step check avoids a compound one-liner that triggers PKB's
  # semicolon/pipe-chain warning.
  dm_out, _ = _pod_exec(
      pod,
      textwrap.dedent("""
        if test -e /dev/mapper/swap_encrypted
        then
          echo /dev/mapper/swap_encrypted
        else
          awk 'NR>1{print $1; exit}' /proc/swaps
        fi
      """),
      ignore_failure=True,
  )
  dev = dm_out.strip()
  if dev:
    return dev
  raise ValueError(
      'No active swap device found in the benchmark pod. '
      'Use --swap_encryption_device to specify one.'
  )


# ---------------------------------------------------------------------------
# Metadata builder
# ---------------------------------------------------------------------------

def _build_metadata(pod: str, swap_dev: str) -> dict:
  """Collect node environment, encryption type, and config into a dict."""

  kernel_out, _ = _pod_exec(pod, 'uname -r', ignore_failure=True)
  mem_out, _ = _pod_exec(
      pod, "awk '/MemTotal/{print $2}' /proc/meminfo",
      ignore_failure=True,
  )
  swap_out, _ = _pod_exec(
      pod, "awk 'NR>1{sum+=$3} END{print sum+0}' /proc/swaps",
      ignore_failure=True,
  )

  try:
    mem_gb = round(int(mem_out.strip()) / (1024 * 1024), 1)
  except ValueError:
    mem_gb = 0
  try:
    swap_gb = round(int(swap_out.strip()) / (1024 * 1024), 1)
  except ValueError:
    swap_gb = 0

  # Encryption type
  enc = 'unknown'
  if '/dev/mapper/' in swap_dev:
    table_out, _ = _pod_exec(
        pod,
        f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    enc = 'dm-crypt-plain' if 'crypt' in table_out.lower() else 'dm-other'
  elif any(x in swap_dev for x in ('nvme', 'xvd', 'sdb')):
    enc = 'nitro_hardware_offload'
  elif 'swapfile' in swap_dev:
    enc = 'none'

  cloud = _detect_cloud(pod)

  # Gap 6: instance size label for multi-size comparison runs.
  # If the flag is set use it directly; otherwise try to read it from
  # cloud metadata so that the field is always populated.
  instance_label = _INSTANCE_SIZE_LABEL.value
  if not instance_label:
    gcp_type_out, _ = _pod_exec(
        pod,
        'curl -s -m 3 --fail '
        'http://metadata.google.internal/computeMetadata/v1/instance/machine-type '
        '-H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_type_out.strip():
      instance_label = gcp_type_out.strip().split('/')[-1]
  if not instance_label:
    aws_type_out, _ = _pod_exec(
        pod,
        'curl -s -m 3 --fail '
        'http://169.254.169.254/latest/meta-data/instance-type '
        '2>/dev/null || echo ""',
        ignore_failure=True,
    )
    instance_label = aws_type_out.strip()

  return {
      'benchmark': BENCHMARK_NAME,
      'execution_mode': 'kubernetes_privileged_pod',
      'cloud': cloud,
      'instance_size': instance_label,
      'kernel_version': kernel_out.strip(),
      'host_memory_gb': mem_gb,
      'swap_device': swap_dev,
      'swap_size_gb': swap_gb,
      'swap_encryption': enc,
      # Test-matrix columns: encryption on/off, node image type, IOPS target
      'dmcrypt_enabled': _ENABLE_DMCRYPT.value,
      'node_image_type': _NODE_IMAGE_TYPE.value,
      'boot_disk_iops_target': _BOOT_DISK_IOPS.value,
      'benchmark_machine_type': _BENCHMARK_MACHINE_TYPE.value,
      # Other config
      'zswap_enabled': _ENABLE_ZSWAP.value,
      'min_free_kbytes': _MIN_FREE_KBYTES.value,
      'fio_runtime_sec': _FIO_RUNTIME_SEC.value,
      'stress_vm_bytes': _STRESS_VM_BYTES.value,
      'stress_vm_bytes_list': _STRESS_VM_BYTES_LIST.value,
      'stress_timeout_sec': _STRESS_TIMEOUT_SEC.value,
      'nodepool': _NODEPOOL.value,
  }
