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
                 swap device: /dev/mapper/swap_encrypted (over hyperdisk or
                 LSSD RAID-0 /dev/md0)

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
import time
import textwrap
from typing import Any

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = 'swap_encryption'

BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    GKE vs. EKS swap encryption and LSSD performance comparison.
    Provisions a Kubernetes cluster, deploys a privileged DaemonSet,
    and runs fio microbenchmarks, stress-ng CPU overhead, I/O
    interference, and real-world workloads (Redis, kernel build,
    OpenSearch) on the cluster node.
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: n4-highmem-32
        boot_disk_size: 100
        boot_disk_type: hyperdisk-balanced
      AWS:
        machine_type: i4i.4xlarge
        boot_disk_size: 100
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
# Internal constants
# ---------------------------------------------------------------------------

_DS_NAME      = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL     = 'pkb-swap-benchmark'

# fio jobs: (name, rw_mode, blocksize, iodepth, description)
_FIO_JOBS = [
    ('rand_write_iops', 'randwrite', '4k',  256, 'Random write IOPS'),
    ('rand_read_iops',  'randread',  '4k',  256, 'Random read IOPS'),
    ('rand_rw_mixed',   'randrw',    '4k',  256, 'Mixed random R/W (50/50)'),
    ('seq_write_bw',    'write',     '1m',   64, 'Sequential write bandwidth'),
    ('seq_read_bw',     'read',      '1m',   64, 'Sequential read bandwidth'),
    ('lat_write',       'randwrite', '4k',    1, 'Random write latency'),
    ('lat_read',        'randread',  '4k',    1, 'Random read latency'),
]

_VMSTAT_LOG  = '/tmp/pkb_vmstat.log'
_PIDSTAT_LOG = '/tmp/pkb_pidstat.log'
_CRYPTO_PROCS = ('kswapd', 'kworker', 'kcryptd', 'dmcrypt_write')

# ---------------------------------------------------------------------------
# DaemonSet manifest (embedded YAML)
# ---------------------------------------------------------------------------

def _DaemonSetYaml(image: str) -> str:
  """Return the privileged benchmark DaemonSet manifest as a YAML string."""
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
          tolerations:
          - operator: Exists
          containers:
          - name: benchmark
            image: {image}
            command:
            - bash
            - -c
            - |
              set -e
              echo "[pkb] Installing benchmark tools..."
              apt-get update -qq
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
                2>&1
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
  """)


# ---------------------------------------------------------------------------
# PKB entry points
# ---------------------------------------------------------------------------

def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec) -> None:
  """Deploy privileged DaemonSet and configure swap on the cluster node."""
  cluster = spec.container_cluster

  logging.info('[swap_encryption] Deploying privileged DaemonSet on cluster')
  _DeployDaemonSet()

  pod = _WaitForBenchmarkPod()
  logging.info('[swap_encryption] Benchmark pod ready: %s', pod)

  # Tune kernel swap aggressiveness
  if _MIN_FREE_KBYTES.value > 0:
    _PodExec(pod, f'sysctl -w vm.min_free_kbytes={_MIN_FREE_KBYTES.value}')

  # Enable zswap if requested
  if _ENABLE_ZSWAP.value:
    _EnableZswap(pod)

  # Configure cloud-specific swap
  cloud = _DetectCloud(pod)
  logging.info('[swap_encryption] Detected cloud: %s', cloud)

  if cloud == 'gcp':
    _SetupGKESwap(pod)
  elif cloud == 'aws':
    _SetupEKSSwap(pod)
  else:
    logging.warning(
        '[swap_encryption] Unknown cloud – falling back to plain swapfile'
    )
    _SetupPlainSwapFile(pod, _SWAP_SIZE_GB.value)


def Run(spec) -> list[sample.Sample]:
  """Execute all benchmark phases and return collected samples."""
  pod = _WaitForBenchmarkPod()
  swap_dev = _DetectSwapDevice(pod)
  base_meta = _BuildMetadata(pod, swap_dev)
  results: list[sample.Sample] = []
  t_run_start = time.time()

  logging.info('[swap_encryption] swap device: %s', swap_dev)

  logging.info('[swap_encryption] ── Phase 1: fio microbenchmarks ──')
  results += _Phase1_Fio(pod, swap_dev, base_meta)

  logging.info('[swap_encryption] ── Phase 2a: CPU overhead ──')
  results += _Phase2a_CpuOverhead(pod, base_meta)

  logging.info('[swap_encryption] ── Phase 2b: I/O interference ──')
  results += _Phase2b_IoInterference(pod, base_meta)

  logging.info('[swap_encryption] ── Phase 3a: Redis latency ──')
  results += _Phase3a_Redis(pod, base_meta)

  logging.info('[swap_encryption] ── Phase 3b: Kernel build ──')
  results += _Phase3b_KernelBuild(pod, base_meta)

  logging.info('[swap_encryption] ── Phase 3c: OpenSearch ──')
  results += _Phase3c_OpenSearch(pod, base_meta)

  # Gap 7: cloud cost estimate for the full benchmark run
  if _COLLECT_COST.value:
    elapsed = time.time() - t_run_start
    results += _CollectCostSample(pod, elapsed, base_meta)

  return results


def Cleanup(spec) -> None:
  """Remove the DaemonSet and tear down any swap configuration."""
  pod = _WaitForBenchmarkPod(timeout=30)
  if pod:
    _PodExec(pod, 'swapoff -a || true', ignore_failure=True)
    _PodExec(pod,
             'cryptsetup close swap_encrypted 2>/dev/null || true',
             ignore_failure=True)
    _PodExec(pod, 'pkill stress-ng fio || true', ignore_failure=True)

  _DeleteDaemonSet()


# ---------------------------------------------------------------------------
# DaemonSet lifecycle helpers
# ---------------------------------------------------------------------------

def _DeployDaemonSet() -> None:
  """Apply the benchmark DaemonSet manifest to the cluster."""
  manifest = _DaemonSetYaml(image=_DAEMONSET_IMAGE.value)
  with vm_util.NamedTemporaryFile(mode='w', suffix='.yaml') as f:
    f.write(manifest)
    f.close()
    kubectl.RunKubectlCommand(['apply', '-f', f.name])
  logging.info('[swap_encryption] DaemonSet applied')


def _WaitForBenchmarkPod(timeout: int = 900) -> str | None:
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
                _LogPodEvents(pod_name)
      else:
        logging.info('[swap_encryption] Waiting for DaemonSet pod to appear...')

    # ── Step 2: poll for /tmp/pkb_ready sentinel ────────────────────────────
    if ready_pod is not None:
      sentinel_out, _, sentinel_rc = kubectl.RunKubectlCommand([
          'exec', ready_pod, '-n', _DS_NAMESPACE,
          '--', 'test', '-f', '/tmp/pkb_ready',
      ], raise_on_failure=False)
      if sentinel_rc == 0:
        logging.info('[swap_encryption] Pod %s ready (tools installed)', ready_pod)
        return ready_pod
      logging.info('[swap_encryption] Pod %s: still installing tools...', ready_pod)

    time.sleep(15)

  logging.warning('[swap_encryption] Benchmark pod not ready after %ds', timeout)
  return None


def _LogPodEvents(pod_name: str) -> None:
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


def _DeleteDaemonSet() -> None:
  """Delete the benchmark DaemonSet."""
  kubectl.RunKubectlCommand([
      'delete', 'daemonset', _DS_NAME,
      '-n', _DS_NAMESPACE,
      '--ignore-not-found',
  ], raise_on_failure=False)
  logging.info('[swap_encryption] DaemonSet deleted')


# ---------------------------------------------------------------------------
# Pod exec wrapper
# ---------------------------------------------------------------------------

def _PodExec(
    pod: str,
    cmd: str,
    ignore_failure: bool = False,
    timeout: int = 300,
) -> tuple[str, str]:
  """Run a shell command inside the benchmark pod via kubectl exec.

  Args:
    pod:            Pod name returned by _WaitForBenchmarkPod.
    cmd:            Shell command string passed to bash -c.
    ignore_failure: When True, non-zero exit codes are logged but not raised.
    timeout:        Seconds before PKB kills the kubectl exec process.
                    Default 300 s matches PKB's IssueCommand default.
                    Pass a larger value for long-running jobs (fio, stress-ng,
                    kernel build).

  Returns:
    (stdout, stderr) strings.
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

def _DetectCloud(pod: str) -> str:
  """Detect GCP vs AWS from DMI product info exposed via /sys hostPath mount.

  DMI is the most reliable in-container detection method because it reads
  directly from the host kernel's SMBIOS table via /sys (already mounted).
  It avoids HTTP metadata endpoint quoting issues and network timeouts.

  Falls back to metadata HTTP endpoints if DMI is inconclusive.
  """
  # Primary: DMI product name / vendor (available via /sys hostPath mount)
  dmi_out, _ = _PodExec(
      pod,
      'cat /sys/class/dmi/id/product_name 2>/dev/null || '
      'cat /sys/class/dmi/id/sys_vendor 2>/dev/null || echo ""',
      ignore_failure=True,
  )
  dmi = dmi_out.strip().lower()
  if 'google' in dmi:
    logging.info('[swap_encryption] Cloud detected via DMI: gcp (%s)', dmi_out.strip())
    return 'gcp'
  if any(k in dmi for k in ('amazon', 'ec2', 'aws')):
    logging.info('[swap_encryption] Cloud detected via DMI: aws (%s)', dmi_out.strip())
    return 'aws'

  # Secondary: GCP metadata endpoint.
  # Use -H with no space after colon to avoid shell-quoting issues through
  # the kubectl exec → bash -c pipeline.
  gcp_out, _ = _PodExec(
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
  aws_out, _ = _PodExec(
      pod,
      'curl -s -m 3 '
      'http://169.254.169.254/latest/meta-data/instance-id '
      '2>/dev/null || echo ""',
      ignore_failure=True,
  )
  if aws_out.strip():
    logging.info('[swap_encryption] Cloud detected via IMDS: aws')
    return 'aws'

  logging.warning('[swap_encryption] Could not detect cloud from DMI or metadata')
  return 'unknown'


def _SetupGKESwap(pod: str) -> None:
  """Configure dm-crypt swap on the GKE node, mirroring go/node:swap-encryption.

  GKE nodes use dm-crypt with an ephemeral random key so that swap contents
  are encrypted at rest without requiring persistent key management.
  We replicate this exactly using cryptsetup in plain mode (no LUKS header).
  """
  swap_type = _SWAP_TYPE.value
  if swap_type == 'auto':
    # Check whether Local SSDs are present
    lssd_out, _ = _PodExec(
        pod,
        "lsblk -d -o NAME,MODEL | grep -i 'local\\|nvme' | "
        "grep -v 'nvme0' | awk '{print $1}' | head -1",
        ignore_failure=True,
    )
    swap_type = 'lssd' if lssd_out.strip() else 'hyperdisk'

  if swap_type == 'lssd':
    _SetupGKELSSDSwap(pod)
  else:
    _SetupGKEHyperdiskSwap(pod)


def _SetupGKEHyperdiskSwap(pod: str) -> None:
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
  boot_out, _ = _PodExec(
      pod,
      'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
      ignore_failure=True,
  )
  boot_base = boot_out.strip() or 'nvme0n1'
  logging.info('[swap_encryption] GKE: boot device: %s', boot_base)

  # Step 2: find a non-boot disk using the literal name from step 1
  disk_out, _ = _PodExec(
      pod,
      f"lsblk -d -o NAME,TYPE | awk '$2==\"disk\"{{print $1}}' "
      f"| grep -v '^{boot_base}$' | head -1",
      ignore_failure=True,
  )
  disk_name = disk_out.strip()

  if not disk_name:
    logging.info(
        '[swap_encryption] No dedicated data disk found – '
        'using dm-crypt loop device on boot hyperdisk'
    )
    _SetupGKELoopDeviceSwap(pod)
    return

  disk = f'/dev/{disk_name}'
  logging.info('[swap_encryption] GKE: dm-crypt target disk: %s', disk)

  _PodExec(pod, textwrap.dedent(f"""
    dd if=/dev/urandom bs=32 count=1 2>/dev/null | \\
    cryptsetup open --type plain \\
      --cipher aes-xts-plain64 \\
      --key-size 256 \\
      --key-file=- \\
      {disk} swap_encrypted && \\
    mkswap /dev/mapper/swap_encrypted && \\
    swapon /dev/mapper/swap_encrypted
  """))
  logging.info('[swap_encryption] GKE: dm-crypt swap active on /dev/mapper/swap_encrypted')


def _SetupGKELoopDeviceSwap(pod: str) -> None:
  """dm-crypt swap via a loop device, executed in the HOST mount namespace.

  Root cause of the cryptsetup hang: when the loop device's backing file
  lives on the container's overlayfs layer, the kernel dm-crypt probe stalls
  trying to read device geometry through the overlayfs→loop chain.

  Fix: run the entire setup script inside the host mount namespace using
    nsenter --mount --target 1
  The host's root filesystem IS the hyperdisk-balanced volume (no overlayfs),
  so all block operations complete normally.  The privileged pod with
  hostPID: true makes PID 1 visible and nsenter work.

  The setup script is base64-encoded before being written to the pod so that
  no shell-quoting issues occur passing it through kubectl exec → bash -c.
  """
  size_gb = _SWAP_SIZE_GB.value
  backing  = '/var/pkb_swap_backing'

  script = '\n'.join([
      '#!/bin/bash',
      'set -euo pipefail',
      f'truncate -s {size_gb}G {backing}',
      'LOOP=$(losetup -f)',
      f'losetup "$LOOP" {backing}',
      # --batch-mode suppresses any interactive confirmation prompts
      'dd if=/dev/urandom bs=32 count=1 2>/dev/null |'
      ' cryptsetup open --type plain'
      ' --cipher aes-xts-plain64 --key-size 256'
      ' --batch-mode --key-file=- "$LOOP" swap_encrypted',
      'mkswap /dev/mapper/swap_encrypted',
      'swapon /dev/mapper/swap_encrypted',
      'echo "SWAP_ACTIVE:$LOOP"',
  ])

  # Write script via base64 to avoid multi-level quoting through kubectl exec
  b64 = base64.b64encode(script.encode()).decode()
  _PodExec(pod,
           f'echo {b64} | base64 -d > /tmp/pkb_swap_setup.sh'
           ' && chmod +x /tmp/pkb_swap_setup.sh')
  logging.info('[swap_encryption] GKE: running dm-crypt setup in host mount namespace')

  # Execute in host mount namespace – should complete in < 10 s
  out, _ = _PodExec(
      pod,
      'nsenter --mount --target 1 -- bash /tmp/pkb_swap_setup.sh 2>&1',
      timeout=120,
  )
  logging.info('[swap_encryption] GKE: setup output: %s', out.strip())

  if 'SWAP_ACTIVE' not in out:
    raise RuntimeError(
        '[swap_encryption] dm-crypt loop-device swap setup failed.\n'
        f'Script output:\n{out}'
    )
  loop_dev = out.strip().split('SWAP_ACTIVE:')[-1].split()[0]
  logging.info('[swap_encryption] GKE: dm-crypt loop-device swap active on %s', loop_dev)


def _SetupGKELSSDSwap(pod: str) -> None:
  """Configure dm-crypt on LSSD RAID-0 array (go/gke-swap-lssd)."""
  logging.info('[swap_encryption] GKE: setting up LSSD RAID-0 swap')

  # Discover all Local SSD devices (non-boot NVMe or ssd* devices)
  lssd_out, _ = _PodExec(
      pod,
      "lsblk -d -o NAME,ROTA | awk '$2==\"0\"{print \"/dev/\"$1}' | "
      "grep -v $(lsblk -no pkname $(findmnt -n -o SOURCE /))",
      ignore_failure=True,
  )
  devices = [d.strip() for d in lssd_out.strip().splitlines() if d.strip()]
  if not devices:
    logging.warning('[swap_encryption] No LSSD devices found, falling back to hyperdisk')
    _SetupGKEHyperdiskSwap(pod)
    return

  device_list = ' '.join(devices)
  n = len(devices)
  logging.info('[swap_encryption] GKE: LSSD RAID-0 across %d devices: %s', n, device_list)

  # Create RAID-0 array then dm-crypt on top (matches GKE node provisioner)
  _PodExec(pod, textwrap.dedent(f"""
    modprobe dm-crypt || true
    yes | mdadm --create /dev/md0 \\
      --level=0 --raid-devices={n} \\
      {device_list} 2>&1 || true
    dd if=/dev/urandom bs=32 count=1 2>/dev/null | \\
    cryptsetup open --type plain \\
      --cipher aes-xts-plain64 \\
      --key-size 256 \\
      --key-file=- \\
      /dev/md0 swap_encrypted && \\
    mkswap /dev/mapper/swap_encrypted && \\
    swapon /dev/mapper/swap_encrypted
  """))
  logging.info('[swap_encryption] GKE: LSSD RAID-0 dm-crypt swap active')


def _SetupEKSSwap(pod: str) -> None:
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
    _SetupEKSInstanceStoreSwap(pod)
  elif swap_type == 'io2':
    _SetupEKSIo2Swap(pod)
  else:
    logging.warning('[swap_encryption] Unknown EKS swap type %s – fallback', swap_type)
    _SetupEKSInstanceStoreSwap(pod)


def _SetupEKSInstanceStoreSwap(pod: str) -> None:
  """Swap on AWS NVMe Instance Store (Nitro hardware-offloaded encryption)."""
  logging.info('[swap_encryption] EKS: setting up Instance Store swap')

  # Find the Instance Store NVMe device (not the root EBS volume)
  nvme_out, _ = _PodExec(
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
      exists_out, _ = _PodExec(
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
    _SetupPlainSwapFile(pod, _SWAP_SIZE_GB.value)
    return

  logging.info('[swap_encryption] EKS: Instance Store device: %s', device)

  # Nitro encrypts all Instance Store writes automatically.
  # No additional cryptsetup required.
  _PodExec(pod, textwrap.dedent(f"""
    mkswap {device} && \\
    swapon {device}
  """))
  logging.info('[swap_encryption] EKS: Instance Store swap active on %s', device)


def _SetupEKSIo2Swap(pod: str) -> None:
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
  root_out, _ = _PodExec(
      pod,
      "lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo nvme0n1",
      ignore_failure=True,
  )
  root_base = root_out.strip() or 'nvme0n1'

  # Prefer non-NVMe EBS volumes (xvdb, sdb, …) which are clearly not
  # Instance Store.  Fall back to the second NVMe if none found.
  disk_out, _ = _PodExec(
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
      exists_out, _ = _PodExec(
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
    _SetupPlainSwapFile(pod, _SWAP_SIZE_GB.value)
    return

  logging.info('[swap_encryption] EKS: io2 EBS device: %s', device)

  # EBS io2 encryption is handled at the AWS level (Nitro / KMS).
  # No cryptsetup required on the guest side.
  _PodExec(pod, textwrap.dedent(f"""
    mkswap {device} && \\
    swapon {device}
  """))
  logging.info('[swap_encryption] EKS: io2 EBS swap active on %s', device)


def _SetupPlainSwapFile(pod: str, size_gb: int) -> None:
  """Fallback: create a loop-device-backed swapfile.

  A plain file on overlayfs (the container root) cannot be used as swap —
  the kernel rejects it with EINVAL.  Routing it through a loop device
  presents a proper block device to the mm subsystem and succeeds.
  """
  logging.info('[swap_encryption] Creating %dGB loop-device swap', size_gb)
  _PodExec(pod, textwrap.dedent(f"""
    fallocate -l {size_gb}G /tmp/pkb_swapfile && \\
    chmod 600 /tmp/pkb_swapfile && \\
    LOOP=$(losetup -f) && \\
    losetup "$LOOP" /tmp/pkb_swapfile && \\
    mkswap "$LOOP" && \\
    swapon "$LOOP" && \\
    echo "swap loop device: $LOOP"
  """))


def _EnableZswap(pod: str) -> None:
  """Enable zswap with lz4 compressor and 20% pool limit inside the pod."""
  logging.info('[swap_encryption] Enabling zswap (lz4, 20%% pool)')
  for cmd in [
      'echo 1      > /sys/module/zswap/parameters/enabled',
      'echo lz4    > /sys/module/zswap/parameters/compressor',
      'echo 20     > /sys/module/zswap/parameters/max_pool_percent',
      'echo z3fold > /sys/module/zswap/parameters/zpool',
  ]:
    _PodExec(pod, cmd, ignore_failure=True)


# ---------------------------------------------------------------------------
# Phase 1 – fio Microbenchmarks
# ---------------------------------------------------------------------------

def _Phase1_Fio(
    pod: str, swap_dev: str, base_meta: dict
) -> list[sample.Sample]:
  """Run fio directly on the swap block device for raw I/O characterisation."""
  results = []

  _PodExec(pod, f'swapoff {swap_dev}', ignore_failure=True)

  # Pre-fill device so read tests have real data.
  # Timeout = swap_size_gb / ~200 MB/s (conservative hyperdisk write rate) + buffer.
  prefill_timeout = max(600, _SWAP_SIZE_GB.value * 10)
  logging.info('[swap_encryption] Pre-filling swap device: %s', swap_dev)
  _PodExec(pod, (
      f'fio --name=prefill --filename={swap_dev} '
      f'--ioengine=libaio --direct=1 --rw=write --bs=1m --size=100% --verify=0'
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
    out, _ = _PodExec(pod, cmd, timeout=fio_timeout)
    results += _ParseFioJson(out, name, label, base_meta)

  _PodExec(pod, f'swapon {swap_dev}', ignore_failure=True)
  return results


def _ParseFioJson(
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
      iops     = float(d.get('iops', 0))
      bw_kib   = float(d.get('bw', 0))
      clat     = d.get('clat_ns', {})
      pct      = clat.get('percentile', {})
      lat_mean = float(clat.get('mean', 0)) / 1000.0
      lat_p50  = float(pct.get('50.000000', 0)) / 1000.0
      lat_p99  = float(pct.get('99.000000', 0)) / 1000.0
      lat_p999 = float(pct.get('99.900000', 0)) / 1000.0
      m = dict(meta, direction=direction)
      results += [
          sample.Sample(f'{job_name}_{direction}_iops',     iops,          'iops', m),
          sample.Sample(f'{job_name}_{direction}_bw_mbps',  bw_kib / 1024, 'MB/s', m),
          sample.Sample(f'{job_name}_{direction}_lat_mean', lat_mean,      'us',   m),
          sample.Sample(f'{job_name}_{direction}_lat_p50',  lat_p50,       'us',   m),
          sample.Sample(f'{job_name}_{direction}_lat_p99',  lat_p99,       'us',   m),
          sample.Sample(f'{job_name}_{direction}_lat_p999', lat_p999,      'us',   m),
      ]
  return results


# ---------------------------------------------------------------------------
# Phase 2a – CPU Overhead Under Swap Pressure
# ---------------------------------------------------------------------------

def _Phase2a_CpuOverhead(pod: str, base_meta: dict) -> list[sample.Sample]:
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
    results += _RunCpuOverheadSweep(pod, base_meta, vm_bytes)
  return results


def _RunCpuOverheadSweep(
    pod: str, base_meta: dict, vm_bytes: str
) -> list[sample.Sample]:
  """Single stress-ng intensity sweep for Phase 2a."""
  results = []
  meta = dict(base_meta, phase='cpu_overhead', stress_vm_bytes=vm_bytes)
  timeout  = _STRESS_TIMEOUT_SEC.value
  interval = 2

  vmstat_log  = f'/tmp/pkb_vmstat_{vm_bytes}.log'
  pidstat_log = f'/tmp/pkb_pidstat_{vm_bytes}.log'

  # Start background collectors (access host /proc via hostPath mount)
  _PodExec(pod, (
      f'vmstat {interval} {timeout // interval} > {vmstat_log} 2>&1 &'
  ))
  _PodExec(pod, (
      f'pidstat -u {interval} {timeout // interval} '
      f'-p ALL > {pidstat_log} 2>&1 &'
  ))

  t0 = time.time()
  stress_out, _ = _PodExec(pod, (
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

  vmstat_out, _ = _PodExec(pod, f'cat {vmstat_log}', ignore_failure=True)
  results += _ParseVmstat(vmstat_out, meta)

  pidstat_out, _ = _PodExec(pod, f'cat {pidstat_log}', ignore_failure=True)
  results += _ParsePidstat(pidstat_out, meta)
  return results


def _ParseVmstat(output: str, base_meta: dict) -> list[sample.Sample]:
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

  def _avg(lst): return sum(lst) / len(lst) if lst else 0.0
  def _max(lst): return max(lst) if lst else 0.0

  total_active = [u + s + w for u, s, w in zip(us_vals, sy_vals, wa_vals)]

  return [
      # Swap rates
      sample.Sample('swap_in_pages_per_sec',     _avg(si_vals), 'pages/s', meta),
      sample.Sample('swap_in_pages_per_sec_max',  _max(si_vals), 'pages/s', meta),
      sample.Sample('swap_out_pages_per_sec',    _avg(so_vals), 'pages/s', meta),
      sample.Sample('swap_out_pages_per_sec_max', _max(so_vals), 'pages/s', meta),
      # Total CPU utilisation (gap 1)
      sample.Sample('total_cpu_pct_avg',          _avg(total_active), '%', meta),
      sample.Sample('total_cpu_pct_max',          _max(total_active), '%', meta),
      # System (kernel) time % – encryption overhead signal (gap 2)
      sample.Sample('system_time_pct_avg',        _avg(sy_vals), '%', meta),
      sample.Sample('system_time_pct_max',        _max(sy_vals), '%', meta),
      # User and I/O-wait for completeness
      sample.Sample('user_cpu_pct_avg',           _avg(us_vals), '%', meta),
      sample.Sample('iowait_cpu_pct_avg',         _avg(wa_vals), '%', meta),
  ]


def _ParsePidstat(output: str, base_meta: dict) -> list[sample.Sample]:
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
        sample.Sample(f'cpu_pct_max_{proc}', max(vals),             '%', m),
    ]
  return results


# ---------------------------------------------------------------------------
# Phase 2b – I/O Interference
# ---------------------------------------------------------------------------

def _Phase2b_IoInterference(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Quantify drop in application I/O when swap is under simultaneous pressure."""
  results = []
  app_file = '/tmp/pkb_app_io'
  timeout  = _STRESS_TIMEOUT_SEC.value
  meta     = dict(base_meta, phase='io_interference')

  # Create test file on the container filesystem (tmpfs or overlay)
  _PodExec(pod, (
      f'fio --name=create --filename={app_file} '
      f'--rw=write --bs=1m --size=8G --verify=0'
  ))

  def _run_app_fio(pressure_label: str) -> list[sample.Sample]:
    cmd = (
        f'fio --name=app_io --filename={app_file} '
        f'--ioengine=libaio --direct=1 '
        f'--rw=randrw --bs=4k --iodepth=32 --size=8G --verify=0 '
        f'--time_based --runtime=60s --output-format=json'
    )
    out, _ = _PodExec(pod, cmd)
    return _ParseFioJson(
        out, 'app_io', f'App I/O ({pressure_label})',
        dict(meta, pressure=pressure_label),
    )

  # 1. Baseline – no swap pressure
  logging.info('[swap_encryption] I/O interference: baseline (no pressure)')
  results += _run_app_fio('no_pressure')

  # 2. Under swap pressure
  logging.info('[swap_encryption] I/O interference: under swap pressure')
  _PodExec(pod, (
      f'stress-ng --vm 1 '
      f'--vm-bytes {_STRESS_VM_BYTES.value} '
      f'--vm-method all '
      f'--timeout {timeout}s &'
  ))
  time.sleep(10)  # let swap pressure build
  results += _run_app_fio('with_swap_pressure')

  _PodExec(pod, f'sleep {timeout}', ignore_failure=True)
  return results


# ---------------------------------------------------------------------------
# Phase 3a – Redis Latency Under Memory Pressure
# ---------------------------------------------------------------------------

def _Phase3a_Redis(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Load Redis beyond its memory cap and measure GET/SET throughput."""
  results = []
  meta = dict(base_meta, workload='redis')

  _PodExec(pod,
           'service redis-server start 2>/dev/null || redis-server --daemonize yes',
           ignore_failure=True)
  time.sleep(3)

  maxmem = _REDIS_MAXMEMORY_MB.value * 1024 * 1024
  _PodExec(pod, f'redis-cli CONFIG SET maxmemory {maxmem}',      ignore_failure=True)
  _PodExec(pod,  'redis-cli CONFIG SET maxmemory-policy allkeys-lru', ignore_failure=True)

  n_keys = (_REDIS_DATASET_MB.value * 1024 * 1024) // 128
  logging.info('[swap_encryption] Loading %d Redis keys (%d MB)',
               n_keys, _REDIS_DATASET_MB.value)
  _PodExec(pod,
           f'redis-benchmark -n {n_keys} -d 128 -t SET -q > /dev/null 2>&1',
           ignore_failure=True)

  # Apply swap pressure while benchmarking
  _PodExec(pod, (
      f'stress-ng --vm 1 --vm-bytes {_STRESS_VM_BYTES.value} '
      f'--vm-method all --timeout 120s &'
  ))
  time.sleep(8)

  out, _ = _PodExec(
      pod,
      'redis-benchmark -n 100000 -d 128 -t GET,SET --csv -q 2>&1',
      ignore_failure=True,
  )
  results += _ParseRedisBenchmark(out, meta)
  return results


def _ParseRedisBenchmark(output: str, base_meta: dict) -> list[sample.Sample]:
  """Parse redis-benchmark --csv output (op, rps)."""
  results = []
  for line in output.splitlines():
    line = line.strip()
    if not line or not line.startswith('"'):
      continue
    parts = [p.strip('"') for p in line.split(',')]
    if len(parts) < 2:
      continue
    op = parts[0].lower()
    try:
      results.append(
          sample.Sample(f'redis_{op}_rps', float(parts[1]), 'req/s',
                        dict(base_meta, operation=op))
      )
    except ValueError:
      pass
  return results


# ---------------------------------------------------------------------------
# Phase 3b – Kernel Build Under Memory Constraint
# ---------------------------------------------------------------------------

def _Phase3b_KernelBuild(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Compile Linux inside a cgroup memory cap; compare to unconstrained."""
  results = []
  ver      = _KERNEL_VERSION.value
  root     = '/tmp/pkb_kernel'
  tarball  = f'{root}/linux-{ver}.tar.xz'
  src      = f'{root}/linux-{ver}'
  url      = (f'https://cdn.kernel.org/pub/linux/kernel/'
              f'v{ver.split(".")[0]}.x/linux-{ver}.tar.xz')

  _PodExec(pod, f'mkdir -p {root}')
  _PodExec(pod, f'test -f {tarball} || wget -q -O {tarball} {url}')
  _PodExec(pod, f'test -d {src}    || tar -xf {tarball} -C {root}')
  _PodExec(pod, f'make -C {src} defconfig -j$(nproc) 2>&1')

  cgroup   = '/sys/fs/cgroup/memory/pkb_kernelbuild'
  mem_bytes = _KERNEL_MEMORY_MB.value * 1024 * 1024

  _PodExec(pod, (
      f'mkdir -p {cgroup} && '
      f'echo {mem_bytes} > {cgroup}/memory.limit_in_bytes'
  ), ignore_failure=True)

  def _build(label: str, use_cgroup: bool) -> sample.Sample:
    _PodExec(pod, f'make -C {src} clean 2>&1')
    if use_cgroup:
      cmd = (f'cgexec -g memory:pkb_kernelbuild '
             f'make -C {src} -j$(nproc) vmlinux 2>&1 '
             f'|| make -C {src} -j$(nproc) vmlinux 2>&1')
    else:
      cmd = f'make -C {src} -j$(nproc) vmlinux 2>&1'
    t0 = time.time()
    _PodExec(pod, cmd, timeout=3600)  # kernel builds can take up to ~1 hr
    elapsed = time.time() - t0
    m = dict(base_meta,
             workload='kernel_build',
             kernel_version=ver,
             build_variant=label,
             memory_limit_mb=(
                 _KERNEL_MEMORY_MB.value if use_cgroup else 'unconstrained'))
    return sample.Sample('kernel_build_elapsed_sec', elapsed, 's', m)

  s_constrained   = _build('constrained',   use_cgroup=True)
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

def _Phase3c_OpenSearch(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Index + query workload under swap pressure (esrally or curl fallback)."""
  meta = dict(base_meta, workload='opensearch')

  _PodExec(pod, (
      f'stress-ng --vm 1 --vm-bytes {_STRESS_VM_BYTES.value} '
      f'--vm-method all --timeout {_STRESS_TIMEOUT_SEC.value}s &'
  ))
  time.sleep(10)

  esrally_out, _ = _PodExec(pod, 'which esrally 2>/dev/null', ignore_failure=True)
  if esrally_out.strip():
    return _RunEsrally(pod, meta)
  else:
    logging.info('[swap_encryption] esrally absent – using curl fallback')
    return _RunOpenSearchCurl(pod, meta)


def _RunEsrally(pod: str, meta: dict) -> list[sample.Sample]:
  _PodExec(pod, (
      'esrally race --track=geonames '
      '--target-hosts=localhost:9200 '
      '--pipeline=benchmark-only '
      '--report-format=csv '
      '--report-file=/tmp/pkb_esrally.csv 2>&1'
  ), ignore_failure=True)
  csv_out, _ = _PodExec(pod, 'cat /tmp/pkb_esrally.csv 2>/dev/null || echo ""')
  results = []
  for line in csv_out.splitlines():
    parts = [p.strip() for p in line.split(',')]
    if len(parts) < 3:
      continue
    metric = parts[0].lower().replace(' ', '_').replace('-', '_')
    try:
      value = float(parts[2])
      unit  = parts[3].strip() if len(parts) > 3 else 'unknown'
      results.append(sample.Sample(f'opensearch_{metric}', value, unit,
                                   dict(meta, tool='esrally')))
    except (ValueError, IndexError):
      pass
  return results


def _RunOpenSearchCurl(pod: str, meta: dict) -> list[sample.Sample]:
  """Minimal OpenSearch benchmark via curl (fallback).

  Elasticsearch/OpenSearch JVM heap is deliberately capped to a small value
  so that the JVM off-heap buffers and OS page cache overflow to swap during
  indexing, making this a realistic swap-pressure workload (gap 4).
  """
  # Cap the JVM heap so OS page cache / off-heap memory causes swap pressure.
  # 512 MB heap on a 32-vCPU node leaves almost all RAM available for page
  # cache, which the kernel will then need to reclaim under bulk-index load.
  jvm_heap_mb = 512
  _PodExec(pod, textwrap.dedent(f"""
    # Patch jvm.options in-place for Elasticsearch and OpenSearch installs
    for jvm_opts_file in \\
        /etc/elasticsearch/jvm.options \\
        /etc/opensearch/jvm.options \\
        /usr/share/elasticsearch/config/jvm.options \\
        /usr/share/opensearch/config/jvm.options; do
      if [ -f "$jvm_opts_file" ]; then
        sed -i 's/^-Xms.*/-Xms{jvm_heap_mb}m/' "$jvm_opts_file"
        sed -i 's/^-Xmx.*/-Xmx{jvm_heap_mb}m/' "$jvm_opts_file"
        echo "[swap_encryption] Patched $jvm_opts_file → -Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
      fi
    done
    # Environment-variable fallback (works with both ES and OpenSearch)
    export ES_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
    export OPENSEARCH_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
  """), ignore_failure=True)

  _PodExec(pod,
           'systemctl start elasticsearch 2>/dev/null || true',
           ignore_failure=True)
  time.sleep(10)

  doc = '{"index":{}}\n{"field":"benchmark","ts":"2026-01-01"}\n'
  bulk = doc * 500

  t0 = time.time()
  _PodExec(pod, (
      f'printf "%s" \'{bulk}\' | '
      "curl -s -X POST 'http://localhost:9200/pkb_test/_bulk' "
      "-H 'Content-Type: application/x-ndjson' "
      "--data-binary @- -o /dev/null"
  ), ignore_failure=True)
  index_sec = time.time() - t0

  t0 = time.time()
  _PodExec(pod, (
      "curl -s 'http://localhost:9200/pkb_test/_search?q=field:benchmark' "
      "-o /dev/null"
  ), ignore_failure=True)
  query_sec = time.time() - t0

  m = dict(meta, tool='curl_fallback')
  return [
      sample.Sample('opensearch_bulk_index_sec',  index_sec, 's', m),
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


def _CollectCostSample(
    pod: str, elapsed_sec: float, base_meta: dict
) -> list[sample.Sample]:
  """Emit a cost_estimate_usd sample for the benchmark run (gap 7).

  Instance type is read from cloud metadata inside the pod.  Price is looked
  up from _INSTANCE_PRICE_USD_PER_HR; if unknown, the sample is omitted and
  a warning is logged.

  Args:
    pod:         Benchmark pod name.
    elapsed_sec: Wall-clock seconds the benchmark phases took.
    base_meta:   Shared metadata dict.

  Returns:
    A list of zero or one sample.Sample.
  """
  # Detect instance type from cloud metadata
  instance_type = ''

  # GCP: machine type is the last segment of the metadata URL value
  gcp_type_out, _ = _PodExec(
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
    aws_type_out, _ = _PodExec(
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

  hours  = elapsed_sec / 3600.0
  cost   = hours * price
  meta   = dict(base_meta,
                instance_type=instance_type,
                price_usd_per_hr=price,
                benchmark_elapsed_sec=round(elapsed_sec, 1))
  return [sample.Sample('cost_estimate_usd', cost, 'USD', meta)]


# ---------------------------------------------------------------------------
# Swap device detection (runs inside the pod)
# ---------------------------------------------------------------------------

def _DetectSwapDevice(pod: str) -> str:
  """Return the active swap device path on the cluster node."""
  if _SWAP_DEVICE.value:
    return _SWAP_DEVICE.value

  # Prefer dm-crypt mapped device (GKE)
  dm_out, _ = _PodExec(
      pod,
      "test -e /dev/mapper/swap_encrypted && echo /dev/mapper/swap_encrypted || "
      "awk 'NR>1{print $1; exit}' /proc/swaps",
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

def _BuildMetadata(pod: str, swap_dev: str) -> dict:
  """Collect node environment, encryption type, and config into a dict."""

  kernel_out, _ = _PodExec(pod, 'uname -r', ignore_failure=True)
  mem_out, _    = _PodExec(pod,
                            "awk '/MemTotal/{print $2}' /proc/meminfo",
                            ignore_failure=True)
  swap_out, _   = _PodExec(pod,
                            "awk 'NR>1{sum+=$3} END{print sum+0}' /proc/swaps",
                            ignore_failure=True)

  try:
    mem_gb  = round(int(mem_out.strip()) / (1024 * 1024), 1)
  except ValueError:
    mem_gb  = 0
  try:
    swap_gb = round(int(swap_out.strip()) / (1024 * 1024), 1)
  except ValueError:
    swap_gb = 0

  # Encryption type
  enc = 'unknown'
  if '/dev/mapper/' in swap_dev:
    table_out, _ = _PodExec(
        pod,
        f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    enc = 'dm-crypt-plain' if 'crypt' in table_out.lower() else 'dm-other'
  elif any(x in swap_dev for x in ('nvme', 'xvd', 'sdb')):
    enc = 'nitro_hardware_offload'
  elif 'swapfile' in swap_dev:
    enc = 'none'

  cloud = _DetectCloud(pod)

  # Gap 6: instance size label for multi-size comparison runs.
  # If the flag is set use it directly; otherwise try to read it from
  # cloud metadata so that the field is always populated.
  instance_label = _INSTANCE_SIZE_LABEL.value
  if not instance_label:
    gcp_type_out, _ = _PodExec(
        pod,
        'curl -s -m 3 --fail '
        'http://metadata.google.internal/computeMetadata/v1/instance/machine-type '
        '-H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_type_out.strip():
      instance_label = gcp_type_out.strip().split('/')[-1]
  if not instance_label:
    aws_type_out, _ = _PodExec(
        pod,
        'curl -s -m 3 --fail '
        'http://169.254.169.254/latest/meta-data/instance-type '
        '2>/dev/null || echo ""',
        ignore_failure=True,
    )
    instance_label = aws_type_out.strip()

  return {
      'benchmark':             BENCHMARK_NAME,
      'execution_mode':        'kubernetes_privileged_pod',
      'cloud':                 cloud,
      'instance_size':         instance_label,   # gap 6
      'kernel_version':        kernel_out.strip(),
      'host_memory_gb':        mem_gb,
      'swap_device':           swap_dev,
      'swap_size_gb':          swap_gb,
      'swap_encryption':       enc,
      'zswap_enabled':         _ENABLE_ZSWAP.value,
      'min_free_kbytes':       _MIN_FREE_KBYTES.value,
      'fio_runtime_sec':       _FIO_RUNTIME_SEC.value,
      'stress_vm_bytes':       _STRESS_VM_BYTES.value,
      'stress_vm_bytes_list':  _STRESS_VM_BYTES_LIST.value,  # gap 5
      'stress_timeout_sec':    _STRESS_TIMEOUT_SEC.value,
      'nodepool':              _NODEPOOL.value,
  }
