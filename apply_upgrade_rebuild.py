#!/usr/bin/env python3
"""Rebuild the upgrade follow-up on the post-redesign base (f431c3f1e).

Re-applies the upgrade delta (Phase 2 upgrade + pipelined mode + the
--k8s_mgmt_target_version / --k8s_mgmt_pipeline_scenario_a flags) expressed in
the NEW fail-hard API:
  - _TimedAsync returns OpTiming and PROPAGATES exceptions (no error capture)
  - _RunAsync returns list[(name, OpTiming)]
  - _OpSamples takes (name, OpTiming) pairs, latency-only
  - ThreadSafeResults collects (name, OpTiming)

Apply on a branch reset to current mgmt_plane_benchmark_base (f431c3f1e).
Idempotent: [ok]/[SKIP] per edit; sentinel guards on pyink-sensitive edits.
"""

import pathlib

BENCH = pathlib.Path(
    "perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py"
)
TEST = pathlib.Path(
    "tests/linux_benchmarks/kubernetes_management_benchmark_test.py"
)


def edit(path, old, new, label):
  text = path.read_text()
  if new in text and old not in text:
    print(f"[SKIP] {label} (already applied)")
    return
  if old not in text:
    raise SystemExit(f"[FAIL] anchor not found: {label}\n---\n{old[:200]}")
  if text.count(old) != 1:
    raise SystemExit(f"[FAIL] anchor not unique ({text.count(old)}x): {label}")
  path.write_text(text.replace(old, new, 1))
  print(f"[ok]   {label}")


def edit_if_absent(path, sentinel, old, new, label):
  if sentinel in path.read_text():
    print(f"[SKIP] {label} (sentinel present)")
    return
  edit(path, old, new, label)


# ───────────────────────── docstrings / config text ───────────────────────
edit(
    BENCH,
    "  concurrent_node_pool_ops: concurrent node-pool create/delete.",
    "  concurrent_node_pool_ops: concurrent node-pool create/upgrade/delete.",
    "doc: scenario line (upgrade)",
)
edit(
    BENCH,
    "    create/delete, overlapping cluster + node-pool ops, and large-scale",
    "    create/upgrade/delete, overlapping cluster + node-pool ops, and "
    "large-scale",
    "config description (upgrade)",
)
edit(
    BENCH,
    "#   concurrent_node_pool_ops: concurrently create and delete N node "
    "pools;\n"
    "#     measures control-plane throughput under parallel ops.",
    "#   concurrent_node_pool_ops: concurrently create, upgrade, and delete N\n"
    "#     node pools; measures control-plane throughput under parallel ops.",
    "scenario comment block (upgrade)",
)
edit(
    BENCH,
    '    "Number of node pools to create and delete concurrently in the "\n'
    '    + "concurrent_node_pool_ops scenario.",',
    '    "Number of node pools to create/upgrade/delete concurrently in the "\n'
    '    + "concurrent_node_pool_ops scenario.",',
    "_CONCURRENT_NODEPOOLS help (upgrade)",
)

# ───────────────────────── flags: target + pipeline ───────────────────────
edit_if_absent(
    BENCH,
    "_TARGET_VERSION = flags.DEFINE_string(",
    '_INITIAL_VERSION = flags.DEFINE_string(\n    "k8s_mgmt_initial_version",\n'
    '    None,\n    "Kubernetes version for newly-created node pools (N-1).'
    ' None = auto.",\n)',
    '_INITIAL_VERSION = flags.DEFINE_string(\n    "k8s_mgmt_initial_version",\n'
    '    None,\n    "Kubernetes version for newly-created node pools (N-1).'
    ' None = auto.",\n)\n_TARGET_VERSION = flags.DEFINE_string(\n   '
    ' "k8s_mgmt_target_version",\n    None,\n    "Kubernetes version to upgrade'
    ' node pools to (N). None = cluster version.",\n)\n_PIPELINE_CONCURRENT_OPS'
    ' = flags.DEFINE_boolean(\n    "k8s_mgmt_pipeline_scenario_a",\n    True,\n'
    '    "If True, run concurrent_node_pool_ops as a per-pool pipeline "\n    +'
    ' "(create->upgrade->delete back-to-back per thread) to minimize "\n    +'
    ' "wall time; else run phase-by-phase.",\n)',
    "add _TARGET_VERSION + _PIPELINE_CONCURRENT_OPS flags",
)

# ───────────────────────── CheckPrerequisites cross-check ──────────────────
edit_if_absent(
    BENCH,
    "apply only to the concurrent_node_pool_ops",
    "  selected = {s.strip() for s in _SCENARIOS.value}\n"
    '  if _SCALE_SWEEP.value and "large_scale_provisioning" not in selected:',
    "  selected = {s.strip() for s in _SCENARIOS.value}\n"
    "  if (\n"
    "      _INITIAL_VERSION.value or _TARGET_VERSION.value\n"
    '  ) and "concurrent_node_pool_ops" not in selected:\n'
    "    raise errors.Config.InvalidValue(\n"
    '        "--k8s_mgmt_initial_version / --k8s_mgmt_target_version apply "\n'
    '        + "only to the concurrent_node_pool_ops scenario, which is not "\n'
    '        + "selected."\n'
    "    )\n"
    '  if _SCALE_SWEEP.value and "large_scale_provisioning" not in selected:',
    "CheckPrerequisites: version cross-check",
)

# ───────────────────────── Run: resolve target + thread it ─────────────────
edit(
    BENCH,
    "  # Resolve the initial node-pool version once; log clearly; tag every"
    " sample.\n  flag_initial = _INITIAL_VERSION.value\n  if not"
    " flag_initial:\n    resolved_initial, _ ="
    " cluster.ResolveNodePoolVersions()\n    flag_initial = resolved_initial\n "
    ' initial = flag_initial\n  source = "flag" if _INITIAL_VERSION.value else'
    ' "auto-resolved"\n\n  logging.info(\n      "NodePool version (%s):'
    ' initial=%s "\n      + "(cluster k8s_version=%s) | nodes_per_pool=%d |'
    ' machine_type=%s",\n      source,\n      initial,\n     '
    " cluster.k8s_version,",
    "  # Resolve versions once; log clearly; tag every sample.\n  # Google"
    " spec: initial=N-1, target=N (adjacent minor upgrade).\n  flag_initial ="
    " _INITIAL_VERSION.value\n  flag_target = _TARGET_VERSION.value\n  if not"
    " (flag_initial and flag_target):\n    resolved_initial, resolved_target ="
    " cluster.ResolveNodePoolVersions()\n    flag_initial = flag_initial or"
    " resolved_initial\n    flag_target = flag_target or resolved_target\n "
    " initial, target = flag_initial, flag_target\n  if _INITIAL_VERSION.value"
    ' and _TARGET_VERSION.value:\n    source = "flags"\n  elif not'
    " (_INITIAL_VERSION.value or _TARGET_VERSION.value):\n    source ="
    ' "auto-resolved"\n  else:\n    source = "mixed"\n\n  logging.info(\n     '
    ' "NodePool versions (%s): initial=%s -> target=%s "\n      + "(cluster'
    ' k8s_version=%s) | nodes_per_pool=%d | machine_type=%s",\n      source,\n '
    "     initial,\n      target,\n      cluster.k8s_version,",
    "Run: resolve target version + source logic",
)
edit(
    BENCH,
    '  if "concurrent_node_pool_ops" in scenarios:\n'
    "    samples += _RunConcurrentNodePoolOps(cluster, initial)\n"
    "    # Each scenario leaves the cluster clean for the next one.\n"
    "    _ClearNodePools(cluster)",
    '  if "concurrent_node_pool_ops" in scenarios:\n'
    "    samples += _RunConcurrentNodePoolOps(cluster, initial, target)\n"
    "    # Each scenario leaves the cluster clean for the next one.\n"
    "    _ClearNodePools(cluster)",
    "Run dispatch: thread target into concurrent ops",
)
edit(
    BENCH,
    '      "initial_version": str(initial),\n'
    '      "cluster_k8s_version": str(cluster.k8s_version),',
    '      "initial_version": str(initial),\n'
    '      "target_version": str(target),\n'
    '      "cluster_k8s_version": str(cluster.k8s_version),',
    "Run meta: add target_version",
)

# ───────────────────────── _RunConcurrentNodePoolOps: target + pipelined ───
edit(
    BENCH,
    "def _RunConcurrentNodePoolOps(\n"
    "    cluster: kubernetes_cluster.KubernetesCluster,\n"
    "    initial: str,\n"
    ") -> list[sample.Sample]:\n"
    '  """Concurrent CreateNodePool then DeleteNodePool."""\n'
    "  n = _CONCURRENT_NODEPOOLS.value\n"
    '  logging.info("concurrent_node_pool_ops: %d pools, initial=%s", n, '
    "initial)\n"
    "  pool_names = [_ConcurrentPoolName(i) for i in range(n)]",
    "def _RunConcurrentNodePoolOps(\n    cluster:"
    " kubernetes_cluster.KubernetesCluster,\n    initial: str,\n    target:"
    ' str,\n) -> list[sample.Sample]:\n  """Concurrent CreateNodePool,'
    ' UpgradeNodePool, then DeleteNodePool."""\n  n ='
    " _CONCURRENT_NODEPOOLS.value\n  if _PIPELINE_CONCURRENT_OPS.value:\n   "
    ' logging.info(\n        "concurrent_node_pool_ops pipelined: %d pools'
    ' %s->%s",\n        n,\n        initial,\n        target,\n    )\n   '
    " return _RunConcurrentNodePoolOpsPipelined(cluster, n, initial,"
    ' target)\n\n  logging.info(\n      "concurrent_node_pool_ops phased: %d'
    ' pools %s->%s", n, initial, target\n  )\n  pool_names ='
    " [_ConcurrentPoolName(i) for i in range(n)]",
    "_RunConcurrentNodePoolOps: target param + pipelined branch",
)
edit(
    BENCH,
    '  samples += _OpSamples("ConcurrentOps_Create", create_results)\n'
    "\n"
    "  # ── Phase 2: concurrent deletes (live-list; all creates succeeded) "
    "──────\n"
    '  alive = _LiveNodePoolNames(cluster, f"{_PREFIX}a")',
    '  samples += _OpSamples("ConcurrentOps_Create", create_results)\n'
    "\n"
    "  # ── Phase 2: concurrent upgrades (fail-hard) ────────────────────────\n"
    "  created = [name for name, _ in create_results]\n"
    "  upgrade_results = _RunAsync(\n"
    "      kickoff=lambda name: cluster.UpgradeNodePoolAsync(name, target),\n"
    "      wait_fn=cluster.WaitForOperation,\n"
    "      items=created,\n"
    "      get_name=str,\n"
    "  )\n"
    '  samples += _OpSamples("ConcurrentOps_Upgrade", upgrade_results)\n'
    "\n"
    "  # ── Phase 3: concurrent deletes (live-list; all ops succeeded) ──────\n"
    '  alive = _LiveNodePoolNames(cluster, f"{_PREFIX}a")',
    "_RunConcurrentNodePoolOps: insert upgrade phase",
)

# ───────────────────────── new pipelined function ─────────────────────────
edit_if_absent(
    BENCH,
    "def _RunConcurrentNodePoolOpsPipelined(",
    '  samples += _OpSamples("ConcurrentOps_Delete", delete_results)\n'
    "  return samples\n"
    "\n"
    "\n"
    "def _RunOverlappingClusterUpdate(",
    '  samples += _OpSamples("ConcurrentOps_Delete", delete_results)\n  return'
    " samples\n\n\ndef _RunConcurrentNodePoolOpsPipelined(\n    cluster:"
    " kubernetes_cluster.KubernetesCluster,\n    n: int,\n    initial: str,\n  "
    '  target: str,\n) -> list[sample.Sample]:\n  """Per-pool pipeline:'
    " create->upgrade->delete back-to-back per thread.\n\n  Minimizes wall time"
    " vs phase-by-phase (max per-pool sum rather than\n  sum of per-phase"
    ' maxima). Fail-hard: any op raising aborts the run.\n  """\n  pool_names ='
    " [_ConcurrentPoolName(i) for i in range(n)]\n  creates ="
    " ThreadSafeResults()\n  upgrades = ThreadSafeResults()\n  deletes ="
    ' ThreadSafeResults()\n\n  def DoPool(pool_name: str):\n    """Runs timed'
    ' create/upgrade/delete for one pool (fail-hard)."""\n    cfg ='
    " _MakeNodePoolConfig(cluster, pool_name)\n    creates.add(\n       "
    " pool_name,\n        _TimedAsync(\n            lambda:"
    " cluster.CreateNodePoolAsync(cfg, node_version=initial),\n           "
    " cluster.WaitForOperation,\n        ),\n    )\n    upgrades.add(\n       "
    " pool_name,\n        _TimedAsync(\n            lambda:"
    " cluster.UpgradeNodePoolAsync(pool_name, target),\n           "
    " cluster.WaitForOperation,\n        ),\n    )\n    deletes.add(\n       "
    " pool_name,\n        _TimedAsync(\n            lambda:"
    " cluster.DeleteNodePoolAsync(pool_name),\n           "
    " cluster.WaitForOperation,\n        ),\n    )\n\n "
    " background_tasks.RunThreaded(\n      DoPool,\n      pool_names,\n     "
    " max_concurrent_threads=min(n, _MAX_CONCURRENT.value),\n  )\n  samples:"
    ' list[sample.Sample] = []\n  samples += _OpSamples("ConcurrentOps_Create",'
    ' creates.entries)\n  samples += _OpSamples("ConcurrentOps_Upgrade",'
    ' upgrades.entries)\n  samples += _OpSamples("ConcurrentOps_Delete",'
    " deletes.entries)\n  return samples\n\n\ndef"
    " _RunOverlappingClusterUpdate(",
    "add _RunConcurrentNodePoolOpsPipelined",
)

print("\nBENCH upgrade rebuild complete.\n")

# ════════════════════════ TEST FILE ════════════════════════════════════════

# 1) Version cross-check test (after the scale-sweep cross-check test).
edit_if_absent(
    TEST,
    "testVersionFlagWithoutConcurrentRaises",
    "  def testScaleSweepWithoutLargeScaleRaises(self):",
    "  def testVersionFlagWithoutConcurrentRaises(self):\n"
    "    with flagsaver.flagsaver(\n"
    "        k8s_mgmt_scenarios=['large_scale_provisioning'],\n"
    "        k8s_mgmt_target_version='1.34',\n"
    "    ):\n"
    "      with self.assertRaises(errors.Config.InvalidValue):\n"
    "        kubernetes_management_benchmark.CheckPrerequisites(\n"
    "            _make_mock_config()\n"
    "        )\n"
    "\n"
    "  def testScaleSweepWithoutLargeScaleRaises(self):",
    "test: version cross-check test",
)

# 2) RunTest: add target_version to the meta-key assertion list.
edit(
    TEST,
    "    for key in (\n"
    "        'initial_version',\n"
    "        'cluster_k8s_version',\n"
    "        'nodes_per_nodepool',\n"
    "        'concurrent_nodepools',\n"
    "    ):",
    "    for key in (\n"
    "        'initial_version',\n"
    "        'target_version',\n"
    "        'cluster_k8s_version',\n"
    "        'nodes_per_nodepool',\n"
    "        'concurrent_nodepools',\n"
    "    ):",
    "test: meta-key list adds target_version",
)

# 3) explicit-flags test: add target flag + assert.
edit(
    TEST,
    "      k8s_mgmt_scenarios=['concurrent_node_pool_ops'],\n"
    "      k8s_mgmt_initial_version='1.30',\n"
    "      k8s_mgmt_scale_sweep=[],\n"
    "      k8s_mgmt_large_scale_nodepools=10,\n"
    "  )\n"
    "  def testRunUsesExplicitVersionFlags(self):",
    "      k8s_mgmt_scenarios=['concurrent_node_pool_ops'],\n"
    "      k8s_mgmt_initial_version='1.30',\n"
    "      k8s_mgmt_target_version='1.31',\n"
    "      k8s_mgmt_scale_sweep=[],\n"
    "      k8s_mgmt_large_scale_nodepools=10,\n"
    "  )\n"
    "  def testRunUsesExplicitVersionFlags(self):",
    "test: explicit-flags add target flag",
)
edit(
    TEST,
    "    cluster.ResolveNodePoolVersions.assert_not_called()\n"
    "    self.assertEqual('1.30', samples[0].metadata['initial_version'])",
    "    cluster.ResolveNodePoolVersions.assert_not_called()\n"
    "    self.assertEqual('1.30', samples[0].metadata['initial_version'])\n"
    "    self.assertEqual('1.31', samples[0].metadata['target_version'])",
    "test: explicit-flags assert target_version",
)

# 4) auto-resolve test: assert target_version too.
edit(
    TEST,
    "    cluster.ResolveNodePoolVersions.assert_called_once()\n"
    "    self.assertEqual('1.33', samples[0].metadata['initial_version'])",
    "    cluster.ResolveNodePoolVersions.assert_called_once()\n"
    "    self.assertEqual('1.33', samples[0].metadata['initial_version'])\n"
    "    self.assertEqual('1.34', samples[0].metadata['target_version'])",
    "test: auto-resolve assert target_version",
)

# 5) Replace RunScenarioATest with upgrade-aware (phase + pipelined) + new class.
old_cls = '''class RunScenarioATest(pkb_common_test_case.PkbCommonTestCase):
  """Tests the _RunConcurrentNodePoolOps create/delete path."""

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testProducesCreateAndDeleteSamples(self):
    """Tests Scenario A produces Create and Delete samples."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = kubernetes_management_benchmark._RunConcurrentNodePoolOps(
        cluster, '1.33'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ConcurrentOps_Create' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Delete' in m for m in metrics))
    self.assertFalse(any('ConcurrentOps_Upgrade' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPassesInitialVersionToCreate(self):
    """_RunConcurrentNodePoolOps passes initial_version to creates."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    kubernetes_management_benchmark._RunConcurrentNodePoolOps(cluster, '1.33')
    for call in cluster.CreateNodePoolAsync.call_args_list:
      kw = call.kwargs if call.kwargs else {}
      pos = call.args
      node_version = kw.get('node_version') or (
          pos[1] if len(pos) > 1 else None
      )
      self.assertEqual('1.33', node_version)

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testDeleteUsesLivePoolList(self):
    """_RunConcurrentNodePoolOps deletes only pools found at runtime."""
    cluster = _make_mock_cluster(pool_names=['pkbma000'])
    kubernetes_management_benchmark._RunConcurrentNodePoolOps(cluster, '1.33')
    self.assertEqual(1, cluster.DeleteNodePoolAsync.call_count)'''

new_cls = '''class RunScenarioATest(pkb_common_test_case.PkbCommonTestCase):
  """Tests the _RunConcurrentNodePoolOps phase-by-phase and pipelined modes."""

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhasedProducesCreateUpgradeDeleteSamples(self):
    """Phase-by-phase produces Create, Upgrade, and Delete samples."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = kubernetes_management_benchmark._RunConcurrentNodePoolOps(
        cluster, '1.33', '1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ConcurrentOps_Create' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Upgrade' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Delete' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhasedPassesInitialVersionToCreate(self):
    """Phase-by-phase passes initial_version to CreateNodePoolAsync."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    kubernetes_management_benchmark._RunConcurrentNodePoolOps(
        cluster, '1.33', '1.34'
    )
    for call in cluster.CreateNodePoolAsync.call_args_list:
      kw = call.kwargs if call.kwargs else {}
      pos = call.args
      node_version = kw.get('node_version') or (
          pos[1] if len(pos) > 1 else None
      )
      self.assertEqual('1.33', node_version)

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhasedPassesTargetVersionToUpgrade(self):
    """Phase-by-phase passes target_version to UpgradeNodePoolAsync."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    kubernetes_management_benchmark._RunConcurrentNodePoolOps(
        cluster, '1.33', '1.34'
    )
    for call in cluster.UpgradeNodePoolAsync.call_args_list:
      pos, kw = call.args, (call.kwargs if call.kwargs else {})
      target = kw.get('target_version') or (pos[1] if len(pos) > 1 else None)
      self.assertEqual('1.34', target)

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=True,
  )
  def testPipelinedModeProducesAllThreePhases(self):
    """Pipelined mode (default) emits Create/Upgrade/Delete samples."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = kubernetes_management_benchmark._RunConcurrentNodePoolOps(
        cluster, '1.33', '1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ConcurrentOps_Create' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Upgrade' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Delete' in m for m in metrics))


class RunScenarioAPipelinedTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _RunConcurrentNodePoolOpsPipelined directly."""

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testProducesAllThreePhases(self):
    """Pipelined run produces Create/Upgrade/Delete samples per pool."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = (
        kubernetes_management_benchmark._RunConcurrentNodePoolOpsPipelined(
            cluster, n=2, initial='1.33', target='1.34'
        )
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ConcurrentOps_Create' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Upgrade' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Delete' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testCreateFailurePropagates(self):
    """Fail-hard: a create failure aborts (raises) the pipeline."""
    cluster = _make_mock_cluster(pool_names=['pkbma000'])
    cluster.CreateNodePoolAsync.side_effect = RuntimeError('create failed')
    with self.assertRaises(Exception):
      kubernetes_management_benchmark._RunConcurrentNodePoolOpsPipelined(
          cluster, n=1, initial='1.33', target='1.34'
      )'''

edit(
    TEST,
    old_cls,
    new_cls,
    "test: upgrade-aware RunScenarioATest + pipelined class",
)

print("\nTEST upgrade rebuild complete.\n")
