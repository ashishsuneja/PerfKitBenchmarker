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
Extends the existing benchmark with two new metrics and per-sample
metadata, with no workload or scenario logic changes:

  1. startup_latency (+ per_pod_startup_latency) — PodRunning -> Ready,
     i.e. container-process-started -> app-passed-readiness-probe.
     PodRunning is synthesized in kubernetes_conditions.py from
     containerStatuses[].state.running.startedAt, since Kubernetes
     doesn't report it as a real pod condition. Distinct from the
     existing max_pod_ready_time (PodReadyToStartContainers -> Ready),
     which also includes scheduling + image pull time.
  2. cpu_utilization_{peak,mean}_millicores + cpu_utilization_reading_count
     — CPU sampled in a background thread during the startup window via
     `kubectl top pods`, following the same background-collector pattern
     used by kubernetes_hpa_benchmark.py / kubernetes_vpa_benchmark.py.
  3. per_pod_ready_time — per-pod max_pod_ready_time breakdown, useful for
     percentile analysis across replicas.
  4. metadata (scenario/workload/cloud) added to every sample for
     cross-config comparison. scenario/workload are static "baseline"/
     "jvm" string literals here since the actual --scenario/--workload
     flags don't exist until PR 3 / PR 2; those PRs replace these
     hardcoded values with real flag-driven ones.

Required metrics fail loudly rather than silently degrading: if a metric
can't be computed at all for the whole run, this raises instead of
logging a warning and returning partial results.

Nothing in Prepare() or Cleanup() changes. PR 2 adds vLLM workload
support; PR 3 adds the VPA CPU Startup Boost scenario flag.
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
    Measures the time it takes for a slow-starting JVM application
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

DEPLOYMENT_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_yaml',
    'container/kubernetes_deployment_startup/slowjvmstartup.yaml.j2',
    'Deployment yaml',
)
DEPLOYMENT_IMAGE = flags.DEFINE_string(
    'kubernetes_deployment_startup_image',
    None,
    'Image name. If omitted, "slowjvmstartup" will be used',
)

_CPU_POLL_INTERVAL_SECS = 5


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if DEPLOYMENT_IMAGE.value is not None:
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = DEPLOYMENT_IMAGE.value
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the Kubernetes cluster for the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark and collects startup metrics.

  Collects:
    1. max_pod_ready_time     — PodReadyToStartContainers -> Ready
       (existing metric, preserved).
    2. per_pod_ready_time     — per-pod breakdown of the above (PR 1).
    3. startup_latency        — PodRunning -> Ready (per-pod:
       per_pod_startup_latency) (PR 1).
    4. cpu_utilization_*      — background CPU collector (PR 1).

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
    A list of sample.Sample objects.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image
  kubernetes_commands.ApplyManifest(
      DEPLOYMENT_YAML.value,
      name='startup',
      image=image,
  )

  base_metadata: Dict[str, Any] = {
      'scenario': 'baseline',
      'workload': 'jvm',
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

    kubernetes_commands.WaitForRollout('deployment/startup', timeout=600)

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
      '[startup] max_pod_ready_time=%.2fs startup_latency=%.2fs pods=%d',
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
# CPU Utilization Background Collector (PR 1)
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
    data for the entire run -- the same standard applied to
    startup_latency, so a total collection failure is surfaced as a
    benchmark failure instead of silently shipping incomplete results.

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
