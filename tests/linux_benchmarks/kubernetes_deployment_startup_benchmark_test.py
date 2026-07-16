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
"""Tests for kubernetes_deployment_startup_benchmark (PR 1).

Scope: metrics + observability only (max_pod_ready_time, per_pod_ready_time,
startup_latency/per_pod_startup_latency, cpu_utilization_*). No
workload/scenario flags exist yet -- those are added in PR 2/PR 3 -- so
sample metadata here is asserted against the hardcoded 'baseline'/'jvm'
literals Run() emits at this layer.
"""

import threading
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import (
    kubernetes_deployment_startup_benchmark as bench,
)
from tests import pkb_common_test_case


def _MakeCondition(resource_name, event, epoch_time):
  """Returns a mock KubernetesStatusCondition."""
  c = mock.MagicMock()
  c.resource_name = resource_name
  c.event = event
  c.epoch_time = epoch_time
  return c


def _MakeSpec(image='slowjvmstartup'):
  """Returns a mock BenchmarkSpec."""
  bm = mock.MagicMock()
  bm.container_specs = {
      'kubernetes_deployment_startup': mock.MagicMock(image=image)
  }
  return bm


def _DefaultConditions():
  # Includes a PodRunning entry so startup_latency is always computable by
  # default -- tests that specifically exercise the "PodRunning missing"
  # failure path build their own conditions list without it.
  return [
      _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
      _MakeCondition('pod-0', 'PodRunning', 1015),
      _MakeCondition('pod-0', 'Ready', 1030),
  ]


def _RunWithConditions(conditions, flag_kwargs=None, cpu_millicores=100.0):
  """Runs bench.Run() with mocked kubectl calls.

  Args:
    conditions: Pod status conditions to return from
      GetStatusConditionsForResourceType.
    flag_kwargs: Flags to set via flagsaver.
    cpu_millicores: Value _GetTotalCpuMillicores should return on every
      poll. Defaults to a real value (not None) so cpu_utilization is
      computable by default -- tests exercising the "zero CPU readings"
      failure path override this to None explicitly.
  """
  flag_kwargs = flag_kwargs or {'cloud': 'GCP'}
  with mock.patch.object(
      bench.kubernetes_commands, 'ApplyManifest'
  ), mock.patch.object(
      bench.kubernetes_commands, 'WaitForRollout'
  ), mock.patch.object(
      bench.kubernetes_conditions,
      'GetStatusConditionsForResourceType',
      return_value=conditions,
  ), mock.patch.object(
      bench, '_GetTotalCpuMillicores', return_value=cpu_millicores
  ), flagsaver.flagsaver(
      **flag_kwargs
  ):
    return bench.Run(_MakeSpec())


# ---------------------------------------------------------------------------
# PR 1: existing + new metrics
# ---------------------------------------------------------------------------


class MaxPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for max_pod_ready_time metric (existing, preserved)."""

  def testEmitsMaxPodReadyTime(self):
    samples = _RunWithConditions(_DefaultConditions())
    self.assertIn('max_pod_ready_time', {s.metric for s in samples})

  def testMaxPodReadyTimeValue(self):
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1010),
        _MakeCondition('pod-0', 'Ready', 1020),
        _MakeCondition('pod-1', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-1', 'PodRunning', 1010),
        _MakeCondition('pod-1', 'Ready', 1035),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['max_pod_ready_time'], 35)

  def testRaisesWhenNoPodsReady(self):
    with self.assertRaises(RuntimeError):
      _RunWithConditions([])


class PerPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for per_pod_ready_time metric (PR 1)."""

  def testEmitsOnePerPod(self):
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1010),
        _MakeCondition('pod-0', 'Ready', 1025),
        _MakeCondition('pod-1', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-1', 'PodRunning', 1010),
        _MakeCondition('pod-1', 'Ready', 1040),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertLen(per_pod, 2)

  def testPerPodCarriesPodName(self):
    conditions = [
        _MakeCondition('pod-abc', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-abc', 'PodRunning', 1010),
        _MakeCondition('pod-abc', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertEqual(per_pod[0].metadata['pod_name'], 'pod-abc')


class StartupLatencyTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the startup_latency metric (PodRunning -> Ready)."""

  def testEmitsStartupLatency(self):
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1005),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    self.assertIn('startup_latency', {s.metric for s in samples})

  def testStartupLatencyValue(self):
    conditions = [
        _MakeCondition('pod-0', 'PodRunning', 1005),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['startup_latency'], 25)

  def testStartupLatencyTakesMaxAcrossPods(self):
    conditions = [
        _MakeCondition('pod-0', 'PodRunning', 1000),
        _MakeCondition('pod-0', 'Ready', 1020),
        _MakeCondition('pod-1', 'PodRunning', 1000),
        _MakeCondition('pod-1', 'Ready', 1040),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['startup_latency'], 40)

  def testEmitsOnePerPodStartupLatency(self):
    conditions = [
        _MakeCondition('pod-0', 'PodRunning', 1000),
        _MakeCondition('pod-0', 'Ready', 1020),
        _MakeCondition('pod-1', 'PodRunning', 1000),
        _MakeCondition('pod-1', 'Ready', 1040),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_startup_latency']
    self.assertLen(per_pod, 2)

  def testDistinctFromMaxPodReadyTime(self):
    # PodReadyToStartContainers is earlier than PodRunning (scheduling +
    # image pull happen first), so startup_latency should be smaller than
    # max_pod_ready_time for the same pod.
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1015),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['max_pod_ready_time'], 30)
    self.assertAlmostEqual(by_metric['startup_latency'], 15)
    self.assertLess(
        by_metric['startup_latency'], by_metric['max_pod_ready_time']
    )

  def testRaisesWhenPodRunningMissing(self):
    # Per review: if the cluster/runtime never reports containerStatuses
    # startedAt for any pod, startup_latency can't be computed at all --
    # this must fail loudly rather than silently succeeding with the
    # metric missing.
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    with self.assertRaises(RuntimeError):
      _RunWithConditions(conditions)


class SampleMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for sample metadata (PR 1).

  scenario/workload aren't flag-driven yet at this layer (that's PR 2/PR 3)
  -- Run() emits the hardcoded 'baseline'/'jvm' literals for every sample.
  """

  def testAllSamplesCarryScenarioWorkloadCloud(self):
    samples = _RunWithConditions(
        _DefaultConditions(), flag_kwargs={'cloud': 'GCP'}
    )
    pod_samples = [
        s
        for s in samples
        if s.metric in ('max_pod_ready_time', 'per_pod_ready_time')
    ]
    self.assertNotEmpty(pod_samples)
    for s in pod_samples:
      self.assertEqual(s.metadata['scenario'], 'baseline')
      self.assertEqual(s.metadata['workload'], 'jvm')
      self.assertEqual(s.metadata['cloud'], 'GCP')


# ---------------------------------------------------------------------------
# PR 1: CPU utilization collector
# ---------------------------------------------------------------------------


class CpuUtilizationCollectorTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _CpuUtilizationCollector (PR 1)."""

  def _MakeCollector(self):
    samples = []
    stop = threading.Event()
    collector = bench._CpuUtilizationCollector(samples, stop)
    return collector, samples, stop

  def testEmitsPeakMeanCount(self):
    collector, samples, stop = self._MakeCollector()
    collector._readings = [100.0, 200.0, 300.0]
    stop.set()
    collector.ObserveCpuUtilization()
    metrics = {s.metric for s in samples}
    self.assertIn('cpu_utilization_peak_millicores', metrics)
    self.assertIn('cpu_utilization_mean_millicores', metrics)
    self.assertIn('cpu_utilization_reading_count', metrics)

  def testPeakAndMeanValues(self):
    collector, samples, stop = self._MakeCollector()
    collector._readings = [100.0, 200.0, 300.0]
    stop.set()
    collector.ObserveCpuUtilization()
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['cpu_utilization_peak_millicores'], 300.0)
    self.assertAlmostEqual(by_metric['cpu_utilization_mean_millicores'], 200.0)
    self.assertEqual(by_metric['cpu_utilization_reading_count'], 3)

  def testRaisesWhenNoReadings(self):
    # Per review: zero CPU readings for the whole run must fail loudly
    # rather than silently shipping results with cpu_utilization missing.
    collector, samples, stop = self._MakeCollector()
    collector._readings = []
    stop.set()
    with self.assertRaises(RuntimeError):
      collector.ObserveCpuUtilization()
    self.assertEqual(samples, [])

  def testObserveIgnoresIssueCommandError(self):
    collector, _, stop = self._MakeCollector()
    call_count = [0]

    def flaky():
      call_count[0] += 1
      if call_count[0] < 3:
        raise errors.VmUtil.IssueCommandError('transient')
      stop.set()
      return []

    collector._Observe(flaky)
    self.assertEqual(call_count[0], 3)

  def testRunRaisesWhenCpuCollectionFailsEntirely(self):
    # End-to-end: bench.Run() runs the collector on a background thread,
    # so this also verifies the collector's RuntimeError is captured and
    # re-raised on the main thread rather than silently disappearing.
    with self.assertRaises(RuntimeError):
      _RunWithConditions(_DefaultConditions(), cpu_millicores=None)

  def testThreadSafety(self):
    collector, _, _ = self._MakeCollector()
    errs = []

    def append_readings():
      try:
        for _ in range(50):
          with collector._lock:
            collector._readings.append(1.0)
      except Exception as e:  # pylint: disable=broad-except
        errs.append(e)

    threads = [threading.Thread(target=append_readings) for _ in range(4)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    self.assertEqual(errs, [])
    self.assertEqual(len(collector._readings), 200)


# ---------------------------------------------------------------------------
# PR 1: _GetTotalCpuMillicores
# ---------------------------------------------------------------------------


class GetTotalCpuMillicoresTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _GetTotalCpuMillicores helper (PR 1)."""

  def _MockKubectl(self, stdout, rc=0):
    return mock.patch.object(
        bench.kubectl,
        'RunKubectlCommand',
        return_value=(stdout, '', rc),
    )

  def testParsesMiliSuffix(self):
    with self._MockKubectl('pod-abc   250m   128Mi\n'):
      self.assertAlmostEqual(bench._GetTotalCpuMillicores(), 250.0)

  def testParsesCoreSuffix(self):
    with self._MockKubectl('pod-abc   1   512Mi\n'):
      self.assertAlmostEqual(bench._GetTotalCpuMillicores(), 1000.0)

  def testSumsMultiplePods(self):
    with self._MockKubectl('pod-0   100m   64Mi\npod-1   150m   64Mi\n'):
      self.assertAlmostEqual(bench._GetTotalCpuMillicores(), 250.0)

  def testReturnsNoneOnError(self):
    with self._MockKubectl('', rc=1):
      self.assertIsNone(bench._GetTotalCpuMillicores())

  def testReturnsNoneOnEmpty(self):
    with self._MockKubectl(''):
      self.assertIsNone(bench._GetTotalCpuMillicores())
