# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Benchmark for measuring time to start up a deployment on Kubernetes.

PR 1 — Metrics & Observability (Layer 0)
==========================================================================
See PR 1 section further down / git history: max_pod_ready_time (existing),
per_pod_ready_time, startup_latency/per_pod_startup_latency,
cpu_utilization_peak/mean_millicores + cpu_utilization_reading_count.

PR 2 — vLLM Workload Support (Layer 1)
==========================================================================
Adds a second workload option alongside the existing slow-starting JVM
app: a CPU-only vLLM OpenAI-compatible server
(public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo), selected via
--kubernetes_deployment_startup_workload={jvm,vllm}.

  - New WORKLOAD flag (default 'jvm').
  - New VLLM_IMAGE / VLLM_YAML flags pointing at the vLLM container image
    and its Deployment+Service manifest (vllm.yaml.j2).
  - GetConfig() swaps the container spec's image to VLLM_IMAGE when
    workload=vllm.
  - Prepare()/Run() branch on WORKLOAD to apply/wait-on the right
    Deployment (startup vs vllm-startup).
  - Sample metadata's 'workload' field is now flag-driven instead of the
    hardcoded 'jvm' literal from PR 1 ('scenario' stays hardcoded
    'baseline' until PR 3 introduces the scenario flag).
  - New VLLM_GPU_MEMORY_UTILIZATION / VLLM_MEMORY_LIMIT flags, forwarded
    to vllm.yaml.j2 as --gpu-memory-utilization and
    requests/limits.memory. These are bundled into this layer (rather
    than landing as later hotfixes) because vLLM's own defaults
    (~0.9 utilization, no explicit memory limit) are not viable defaults
    for this benchmark at all: they deterministically crash-loop
    ("ValueError: Available memory ... is less than desired CPU memory
    utilization") or OOMKill during compile/warmup against a small
    container memory limit. A vLLM workload option that ships without
    working defaults isn't a usable PR 2.

No scenario/CPU-Startup-Boost logic yet -- that's PR 3.
"""

import collections
import logging
import threading
from collections.abc import Callable
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.container_service import kubernetes_conditions

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_deployment_startup'
BENCHMARK_CONFIG = """
kubernetes_deployment_startup:
  description: >
    Measures the time it takes for a slow-starting JVM application or vLLM
    to become ready in a Kubernetes cluster.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_spec: *default_dual_core
  container_specs:
    kubernetes_deployment_startup:
      image: slowjvmstartup
  container_registry:
    cloud: GCP
    spec:
      GCP:
        zone: 'us-central1'
"""

# ── Existing flags (PR 1) ────────────────────────────────────────────────
DEPLOYMENT_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_yaml',
    'container/kubernetes_deployment_startup/slowjvmstartup.yaml.j2',
    'Deployment yaml for JVM workload.',
)
DEPLOYMENT_IMAGE = flags.DEFINE_string(
    'kubernetes_deployment_startup_image',
    None,
    'Image name for JVM workload. If omitted, "slowjvmstartup" will be used.',
)

# ── New flags (PR 2) ──────────────────────────────────────────────────────
WORKLOAD = flags.DEFINE_enum(
    'kubernetes_deployment_startup_workload',
    'jvm',
    ['jvm', 'vllm'],
    'Workload type to deploy.',
)
VLLM_IMAGE = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_image',
    'public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:latest',
    'Container image for the vLLM CPU workload.',
)
VLLM_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_yaml',
    'container/kubernetes_deployment_startup/vllm.yaml.j2',
    'Deployment yaml for the vLLM workload.',
)
VLLM_GPU_MEMORY_UTILIZATION = flags.DEFINE_float(
    'kubernetes_deployment_startup_vllm_gpu_memory_utilization',
    0.5,
    "Fraction of the vLLM container's memory limit to reserve for model "
    + "weights/KV cache -- vLLM's --gpu-memory-utilization flag, which "
    + 'despite the name also governs the CPU backend. vLLM defaults to '
    + "~0.9, which assumes far more headroom than this benchmark's "
    + 'container memory limit provides once Python/PyTorch/runtime '
    + 'overhead is subtracted -- an unconfigured default here '
    + 'deterministically crash-loops ("ValueError: Available memory ... is '
    + 'less than desired CPU memory utilization"). 0.5 keeps the '
    + 'reservation within the observed available headroom.',
    lower_bound=0.05,
    upper_bound=0.95,
)
VLLM_MEMORY_LIMIT = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_memory_limit',
    '8Gi',
    "vLLM container's requests/limits.memory (Kubernetes quantity, e.g."
    + ' "8Gi"). vLLM OOMKills (exit 137) during its "Warming up model for'
    + ' the compilation..." phase against too small a limit -- 8Gi keeps'
    + ' the pod Guaranteed QoS (requests == limits) and fits comfortably'
    + ' on the n2-standard-4 nodes used for baseline vLLM runs (~16GiB'
    + ' allocatable).',
)

_JVM_DEPLOYMENT_NAME = 'startup'
_VLLM_DEPLOYMENT_NAME = 'vllm-startup'
_CPU_POLL_INTERVAL_SECS = 5


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Returns merged benchmark config.

  For workload=vllm, swaps the container spec's image to VLLM_IMAGE.

  Args:
    user_config: User-supplied configuration.

  Returns:
    Loaded benchmark configuration.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if WORKLOAD.value == 'vllm':
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = VLLM_IMAGE.value
  elif DEPLOYMENT_IMAGE.value is not None:
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = DEPLOYMENT_IMAGE.value

  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the Kubernetes cluster for the benchmark.

  Deploys the JVM or vLLM workload depending on WORKLOAD.

  Args:
    benchmark_spec: The benchmark specification.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image
  workload = WORKLOAD.value

  if workload == 'vllm':
    logging.info('[startup] Deploying vLLM workload (image=%s)', image)
    kubernetes_commands.ApplyManifest(
        VLLM_YAML.value,
        name=_VLLM_DEPLOYMENT_NAME,
        image=image,
        gpu_memory_utilization=VLLM_GPU_MEMORY_UTILIZATION.value,
        memory_limit=VLLM_MEMORY_LIMIT.value,
    )
  else:
    logging.info('[startup] Deploying JVM workload (image=%s)', image)
    kubernetes_commands.ApplyManifest(
        DEPLOYMENT_YAML.value,
        name=_JVM_DEPLOYMENT_NAME,
        image=image,
    )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark and collects startup metrics.

  Collects all PR 1 metrics (max_pod_ready_time, per_pod_ready_time,
  startup_latency/per_pod_startup_latency, cpu_utilization_*) against
  whichever workload's Deployment is active.

  Required metrics fail loudly rather than silently degrading: if a
  metric can't be computed at all for the whole run, this raises instead
  of logging a warning and returning partial results.

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    RuntimeError: If no pods become ready, if no pod had both a
      PodRunning and Ready timestamp (startup_latency uncomputable), or
      if zero CPU utilization readings were collected all run.

  Returns:
    List of sample.Sample objects.
  """
  del benchmark_spec  # Image/deployment name are resolved via flags below.
  workload = WORKLOAD.value
  deployment_name = (
      _VLLM_DEPLOYMENT_NAME if workload == 'vllm' else _JVM_DEPLOYMENT_NAME
  )

  base_metadata: Dict[str, Any] = {
      'scenario': 'baseline',
      'workload': workload,
      'cloud': FLAGS.cloud,
  }

  # ── CPU background collector (PR 1) ──────────────────────────────────
  all_samples: List[sample.Sample] = []
  stop = threading.Event()
  cpu_collector = _CpuUtilizationCollector(all_samples, stop)
  collector_errors: List[BaseException] = []

  def _RunCollector() -> None:
    # Runs in a background thread: exceptions raised here (e.g. zero CPU
    # readings collected all run) don't propagate to the main thread on
    # their own, so capture and re-raise below once the thread is joined.
    try:
      cpu_collector.ObserveCpuUtilization()
    except Exception as e:  # pylint: disable=broad-except
      collector_errors.append(e)

  try:
    collector_thread = threading.Thread(
        target=_RunCollector,
        daemon=True,
    )
    collector_thread.start()

    kubernetes_commands.WaitForRollout(
        f'deployment/{deployment_name}', timeout=600
    )

  finally:
    stop.set()
    collector_thread.join(timeout=_CPU_POLL_INTERVAL_SECS * 3)

  if collector_errors:
    raise collector_errors[0]

  # ── Parse pod conditions ──────────────────────────────────────────────
  # max_pod_ready_time uses PodReadyToStartContainers -> Ready (existing).
  # startup_latency uses PodRunning -> Ready (container process started ->
  # app passed its readiness probe). PodRunning is synthesized by
  # kubernetes_conditions from containerStatuses[].state.running.startedAt,
  # since it isn't a real pod condition.
  pod_name_to_start_end_times: dict[str, tuple[int, int]] = (
      collections.defaultdict(lambda: (0, 0))
  )
  pod_name_to_running_ready_times: dict[str, tuple[int, int]] = (
      collections.defaultdict(lambda: (0, 0))
  )
  for c in kubernetes_conditions.GetStatusConditionsForResourceType('pod'):
    if c.event == 'PodReadyToStartContainers':
      prev_end_time = pod_name_to_start_end_times[c.resource_name][1]
      pod_name_to_start_end_times[c.resource_name] = (
          c.epoch_time,
          prev_end_time,
      )
    elif c.event == 'PodRunning':
      prev_end_time = pod_name_to_running_ready_times[c.resource_name][1]
      pod_name_to_running_ready_times[c.resource_name] = (
          c.epoch_time,
          prev_end_time,
      )
    elif c.event == 'Ready':
      prev_start_time = pod_name_to_start_end_times[c.resource_name][0]
      pod_name_to_start_end_times[c.resource_name] = (
          prev_start_time,
          c.epoch_time,
      )
      prev_running_start_time = pod_name_to_running_ready_times[
          c.resource_name
      ][0]
      pod_name_to_running_ready_times[c.resource_name] = (
          prev_running_start_time,
          c.epoch_time,
      )

  if not pod_name_to_start_end_times:
    raise RuntimeError('No pods became ready')

  # ── Metric 1: max_pod_ready_time ─────────────────────────────────────
  max_pod_ready_t = -1
  for _, times in pod_name_to_start_end_times.items():
    t = times[1] - times[0]
    max_pod_ready_t = max(max_pod_ready_t, t)

  if max_pod_ready_t < 0:
    raise RuntimeError('No pods became ready')

  all_samples.append(
      sample.Sample(
          'max_pod_ready_time', max_pod_ready_t, 'seconds', {**base_metadata}
      )
  )

  # ── Metric 2: per_pod_ready_time (PR 1) ──────────────────────────────
  for pod_name, (start_t, end_t) in pod_name_to_start_end_times.items():
    pod_ready_t = end_t - start_t
    if pod_ready_t >= 0:
      all_samples.append(
          sample.Sample(
              'per_pod_ready_time',
              pod_ready_t,
              'seconds',
              {**base_metadata, 'pod_name': pod_name},
          )
      )

  # ── Metric 3: startup_latency (PodRunning -> Ready) ──────────────────
  max_startup_latency = -1
  for pod_name, (running_t, ready_t) in pod_name_to_running_ready_times.items():
    if running_t <= 0 or ready_t <= 0:
      continue
    latency = ready_t - running_t
    if latency < 0:
      continue
    max_startup_latency = max(max_startup_latency, latency)
    all_samples.append(
        sample.Sample(
            'per_pod_startup_latency',
            latency,
            'seconds',
            {**base_metadata, 'pod_name': pod_name},
        )
    )

  if max_startup_latency < 0:
    raise RuntimeError(
        'Could not compute startup_latency: no pod had both a'
        ' PodRunning and Ready timestamp (container runtime may not report'
        ' containerStatuses[].state.running.startedAt on this cluster).'
    )

  all_samples.append(
      sample.Sample(
          'startup_latency',
          max_startup_latency,
          'seconds',
          {**base_metadata},
      )
  )

  logging.info(
      '[startup] workload=%s max_pod_ready_time=%.2fs'
      + ' startup_latency=%.2fs pods=%d',
      workload,
      max_pod_ready_t,
      max_startup_latency,
      len(pod_name_to_start_end_times),
  )

  return all_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up the Kubernetes cluster after the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec


# ---------------------------------------------------------------------------
# CPU Utilization Background Collector (PR 1 — unchanged)
# ---------------------------------------------------------------------------


class _CpuUtilizationCollector:
  """Polls CPU utilization in a background thread during the startup window."""

  def __init__(self, samples, stop):
    self._samples = samples
    self._stop = stop
    self._readings: List[float] = []
    self._lock = threading.Lock()

  def ObserveCpuUtilization(self) -> None:
    """Polls CPU utilization for the duration of the run.

    Transient poll failures (e.g. the Kubernetes Metrics API still
    warming up on a freshly created cluster) are tolerated by _Observe
    and simply retried. This only raises if the metric ends up with zero
    data for the entire run.

    Raises:
      RuntimeError: If not a single CPU reading was collected all run.
    """
    self._Observe(self._PollCpuMillicoresSample)
    with self._lock:
      readings = list(self._readings)
    if not readings:
      raise RuntimeError(
          'Collected zero CPU utilization readings for the entire run --'
          ' the Kubernetes Metrics API may never have become available.'
          ' cpu_utilization_peak/mean_millicores cannot be computed.'
      )
    peak = max(readings)
    mean = sum(readings) / len(readings)
    self._samples.extend(
        [
            sample.Sample(
                'cpu_utilization_peak_millicores', peak, 'millicores', {}
            ),
            sample.Sample(
                'cpu_utilization_mean_millicores', mean, 'millicores', {}
            ),
            sample.Sample(
                'cpu_utilization_reading_count', len(readings), 'count', {}
            ),
        ]
    )

  def _PollCpuMillicoresSample(self) -> List[sample.Sample]:
    cpu_m = _GetTotalCpuMillicores()
    if cpu_m is None:
      return []
    with self._lock:
      self._readings.append(cpu_m)
    return []

  def _Observe(self, observe_fn: Callable[[], List[sample.Sample]]) -> None:
    while True:
      try:
        self._samples.extend(observe_fn())
      except (
          errors.VmUtil.IssueCommandError,
          errors.VmUtil.IssueCommandTimeoutError,
      ) as e:
        logging.warning('[startup/cpu] Poll error: %s', e)
      if self._stop.wait(timeout=_CPU_POLL_INTERVAL_SECS):
        return


def _GetTotalCpuMillicores() -> float | None:
  """Returns summed CPU millicores across pods from `kubectl top`, or None.

  Returns None (rather than raising) on any parse/command failure so a
  single bad poll doesn't take down the whole collector loop; _Observe
  above retries on the next poll interval regardless.
  """
  try:
    stdout, _, rc = kubectl.RunKubectlCommand(
        ['top', 'pods', '--no-headers'], raise_on_failure=False
    )
    if rc != 0 or not stdout.strip():
      return None
    total_m = 0.0
    for line in stdout.strip().splitlines():
      parts = line.split()
      if len(parts) < 2:
        continue
      cpu_str = parts[1]
      if cpu_str.endswith('m'):
        total_m += float(cpu_str[:-1])
      else:
        total_m += float(cpu_str) * 1000.0
    return total_m
  except (ValueError, IndexError):
    return None
