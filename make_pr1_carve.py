#!/usr/bin/env python3
"""Carve the upgrade path out of kubernetes_management for the PR split (#9).

PR-1 keeps the create/delete path: concurrent_node_pool_ops becomes
create+delete only, plus overlapping_cluster_update and
large_scale_provisioning. The upgrade phase, the pipelined mode, the
--k8s_mgmt_target_version / --k8s_mgmt_pipeline_scenario_a flags, and their
tests move to the follow-up PR. Run from repo root on the PR-1 branch. Idempotent.
"""

import pathlib

B = pathlib.Path(
    'perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py'
)
T = pathlib.Path(
    'tests/linux_benchmarks/kubernetes_management_benchmark_test.py'
)


def edit(text, old, new, label):
  if old in text:
    print(f'[ok]   {label}')
    return text.replace(old, new, 1)
  if new and new in text:
    print(f'[SKIP] {label}')
    return text
  print(f'[WARN] {label}: anchor not found')
  return text


s = B.read_text()

# docstring scenario line
s = edit(
    s,
    '  concurrent_node_pool_ops: concurrent node-pool create/upgrade/delete.\n',
    '  concurrent_node_pool_ops: concurrent node-pool create/delete.\n',
    'doc: scenario A line',
)
# docstring optimization note (pipelined) — remove
s = edit(
    s,
    '  - Optional pipelined concurrent_node_pool_ops (create/upgrade/delete)\n',
    '',
    'doc: remove pipelined optimization note',
)
# BENCHMARK_CONFIG description
s = edit(
    s,
    '    create/upgrade/delete, overlapping cluster + node-pool ops, and'
    ' large-scale\n',
    '    create/delete, overlapping cluster + node-pool ops, and large-scale\n',
    'config description',
)
# scenario comment block
s = edit(
    s,
    '#   concurrent_node_pool_ops: concurrently create, upgrade, and delete N\n'
    '#     node pools; measures control-plane throughput under parallel ops.\n',
    '#   concurrent_node_pool_ops: concurrently create and delete N node'
    ' pools;\n'
    '#     measures control-plane throughput under parallel ops.\n',
    'scenario comment block',
)
# _CONCURRENT_NODEPOOLS help
s = edit(
    s,
    '    "Number of node pools to create/upgrade/delete concurrently in the "\n'
    '    + "concurrent_node_pool_ops scenario.",\n',
    '    "Number of node pools to create and delete concurrently in the "\n'
    '    + "concurrent_node_pool_ops scenario.",\n',
    '_CONCURRENT_NODEPOOLS help',
)
# remove _TARGET_VERSION + _PIPELINE_SCENARIO_A flags
s = edit(
    s,
    '_TARGET_VERSION = flags.DEFINE_string(\n'
    '    "k8s_mgmt_target_version",\n'
    '    None,\n'
    '    "Kubernetes version to upgrade node pools to (N). None = cluster'
    ' version.",\n'
    ')\n'
    '_PIPELINE_SCENARIO_A = flags.DEFINE_boolean(\n'
    '    "k8s_mgmt_pipeline_scenario_a",\n'
    '    True,\n'
    '    "If True, run concurrent_node_pool_ops as a per-pool pipeline "\n'
    '    + "(create->upgrade->delete back-to-back per thread). Minimizes wall'
    ' time. "\n'
    '    + "Default False for spec-strict phase-by-phase.",\n'
    ')\n',
    '',
    'remove target + pipeline flags',
)
# Run() version resolution -> initial only
s = edit(
    s,
    '  # Resolve versions once; log clearly; tag every sample.\n  # Google'
    ' spec: initial=N-1, target=N (adjacent minor upgrade).\n  flag_initial ='
    ' _INITIAL_VERSION.value\n  flag_target = _TARGET_VERSION.value\n  if not'
    ' (flag_initial and flag_target):\n    resolved_initial, resolved_target ='
    ' cluster.ResolveNodePoolVersions()\n    flag_initial = flag_initial or'
    ' resolved_initial\n    flag_target = flag_target or resolved_target\n '
    ' initial, target = flag_initial, flag_target\n  if _INITIAL_VERSION.value'
    ' and _TARGET_VERSION.value:\n    source = "flags"\n  elif not'
    ' (_INITIAL_VERSION.value or _TARGET_VERSION.value):\n    source ='
    ' "auto-resolved"\n  else:\n    source = "mixed"\n\n  logging.info(\n     '
    ' "NodePool versions (%s): initial=%s -> target=%s "\n      + "(cluster'
    ' k8s_version=%s) | nodes_per_pool=%d | machine_type=%s",\n      source,\n '
    '     initial,\n      target,\n      cluster.k8s_version,\n',
    '  # Resolve the initial node-pool version once; log clearly; tag every'
    ' sample.\n  flag_initial = _INITIAL_VERSION.value\n  if not'
    ' flag_initial:\n    resolved_initial, _ ='
    ' cluster.ResolveNodePoolVersions()\n    flag_initial = resolved_initial\n '
    ' initial = flag_initial\n  source = "flag" if _INITIAL_VERSION.value else'
    ' "auto-resolved"\n\n  logging.info(\n      "NodePool version (%s):'
    ' initial=%s "\n      + "(cluster k8s_version=%s) | nodes_per_pool=%d |'
    ' machine_type=%s",\n      source,\n      initial,\n     '
    ' cluster.k8s_version,\n',
    'Run version resolution -> initial only',
)
# dispatch: drop target arg
s = edit(
    s,
    '    samples += _RunScenarioA(cluster, initial, target)\n',
    '    samples += _RunScenarioA(cluster, initial)\n',
    'Run dispatch drop target arg',
)
# run_meta: drop target_version
s = edit(
    s,
    '      "initial_version": str(initial),\n'
    '      "target_version": str(target),\n'
    '      "cluster_k8s_version": str(cluster.k8s_version),\n',
    '      "initial_version": str(initial),\n'
    '      "cluster_k8s_version": str(cluster.k8s_version),\n',
    'run_meta drop target_version',
)
# CheckPrerequisites: remove version cross-check (keep scale_sweep + selected)
s = edit(
    s,
    '  selected = {s.strip() for s in _SCENARIOS.value}\n'
    '  if (\n'
    '      _INITIAL_VERSION.value or _TARGET_VERSION.value\n'
    '  ) and "concurrent_node_pool_ops" not in selected:\n'
    '    raise errors.Config.InvalidValue(\n'
    '        "--k8s_mgmt_initial_version / --k8s_mgmt_target_version apply only'
    ' to "\n'
    '        + "the concurrent_node_pool_ops scenario, which is not'
    ' selected."\n'
    '    )\n'
    '  if _SCALE_SWEEP.value and "large_scale_provisioning" not in selected:\n',
    '  selected = {s.strip() for s in _SCENARIOS.value}\n'
    '  if _SCALE_SWEEP.value and "large_scale_provisioning" not in selected:\n',
    'CheckPrerequisites remove version cross-check',
)
# Replace _RunScenarioA + remove _RunScenarioAPipelined
old_funcs = '''def _RunScenarioA(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
    target: str,
) -> list[sample.Sample]:
  """Concurrent CreateNodePool, UpgradeNodePool, DeleteNodePool."""
  n = _CONCURRENT_NODEPOOLS.value
  if _PIPELINE_SCENARIO_A.value:
    logging.info(
        "Scenario A (pipelined): %d pools, initial=%s, target=%s",
        n,
        initial,
        target,
    )
    return _RunScenarioAPipelined(cluster, n, initial, target)

  logging.info(
      "Scenario A (phase-by-phase): %d pools, initial=%s, target=%s",
      n,
      initial,
      target,
  )
  pool_names = [_ScenarioAName(i) for i in range(n)]
  configs_ = [_MakeNodePoolConfig(cluster, name) for name in pool_names]
  samples: list[sample.Sample] = []

  # ── Phase 1: concurrent creates ─────────────────────────────────────────
  create_results = _RunAsync(
      kickoff=lambda cfg: cluster.CreateNodePoolAsync(
          cfg, node_version=initial
      ),
      wait_fn=cluster.WaitForOperation,
      items=configs_,
      get_name=lambda cfg: cfg.name,
  )
  samples += _OpSamples(
      "ScenarioA_Create", create_results, attempted_ops=len(pool_names)
  )

  # ── Phase 2: concurrent upgrades (only successfully created pools) ───────
  created = [r.name for r in create_results if r.error is None]
  logging.info(
      "Scenario A: %d/%d pools created — proceeding to upgrade", len(created), n
  )
  upgrade_results = _RunAsync(
      kickoff=lambda name: cluster.UpgradeNodePoolAsync(name, target),
      wait_fn=cluster.WaitForOperation,
      items=created,
      get_name=str,
  )
  samples += _OpSamples(
      "ScenarioA_Upgrade", upgrade_results, attempted_ops=len(created)
  )

  # ── Phase 3: concurrent deletes (live-list to catch EKS rollbacks) ──────
  alive = [p for p in cluster.GetNodePoolNames() if p.startswith(f"{_PREFIX}a")]
  logging.info(
      "Scenario A: %d live pools found for delete (originally %d)",
      len(alive),
      n,
  )
  delete_results = _RunAsync(
      kickoff=cluster.DeleteNodePoolAsync,
      wait_fn=cluster.WaitForOperation,
      items=alive,
      get_name=str,
  )
  # attempted_ops=n: success rate reflects original request, not just live.
  # EKS rolls back timed-out pools silently — without this shows 100%.
  samples += _OpSamples("ScenarioA_Delete", delete_results, attempted_ops=n)
  return samples


def _RunScenarioAPipelined(
    cluster: kubernetes_cluster.KubernetesCluster,
    n: int,
    initial: str,
    target: str,
) -> list[sample.Sample]:
  """Per-pool pipeline: create->upgrade->delete back-to-back per thread.

  Minimizes wall time: max_i(create_i + upgrade_i + delete_i) vs
  max(creates)+max(upgrades)+max(deletes) in phase-by-phase mode.
  Trade-off: ops run under mixed-type concurrent load.
  """
  pool_names = [_ScenarioAName(i) for i in range(n)]
  creates = _Results()
  upgrades = _Results()
  deletes = _Results()

  def DoPool(pool_name: str):
    """Runs timed create/upgrade/delete for one pool."""
    cfg = _MakeNodePoolConfig(cluster, pool_name)
    init, e2e, err = _TimedAsync(
        lambda: cluster.CreateNodePoolAsync(cfg, node_version=initial),
        cluster.WaitForOperation,
    )
    creates.add(pool_name, init, e2e, err)
    if err is not None:
      return
    init, e2e, err = _TimedAsync(
        lambda: cluster.UpgradeNodePoolAsync(pool_name, target),
        cluster.WaitForOperation,
    )
    upgrades.add(pool_name, init, e2e, err)
    init, e2e, err = _TimedAsync(
        lambda: cluster.DeleteNodePoolAsync(pool_name),
        cluster.WaitForOperation,
    )
    deletes.add(pool_name, init, e2e, err)

  background_tasks.RunThreaded(
      DoPool,
      pool_names,
      max_concurrent_threads=min(n, _MAX_CONCURRENT.value),
  )
  samples: list[sample.Sample] = []
  samples += _OpSamples("ScenarioA_Create", creates.entries, attempted_ops=n)
  samples += _OpSamples("ScenarioA_Upgrade", upgrades.entries, attempted_ops=n)
  samples += _OpSamples("ScenarioA_Delete", deletes.entries, attempted_ops=n)
  return samples'''

new_func = '''def _RunScenarioA(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
) -> list[sample.Sample]:
  """Concurrent CreateNodePool then DeleteNodePool."""
  n = _CONCURRENT_NODEPOOLS.value
  logging.info(
      "concurrent_node_pool_ops: %d pools, initial=%s", n, initial
  )
  pool_names = [_ScenarioAName(i) for i in range(n)]
  configs_ = [_MakeNodePoolConfig(cluster, name) for name in pool_names]
  samples: list[sample.Sample] = []

  # ── Phase 1: concurrent creates ─────────────────────────────────────────
  create_results = _RunAsync(
      kickoff=lambda cfg: cluster.CreateNodePoolAsync(
          cfg, node_version=initial
      ),
      wait_fn=cluster.WaitForOperation,
      items=configs_,
      get_name=lambda cfg: cfg.name,
  )
  samples += _OpSamples(
      "ScenarioA_Create", create_results, attempted_ops=len(pool_names)
  )

  # ── Phase 2: concurrent deletes (live-list to catch EKS rollbacks) ──────
  alive = [p for p in cluster.GetNodePoolNames() if p.startswith(f"{_PREFIX}a")]
  logging.info(
      "concurrent_node_pool_ops: %d live pools for delete (originally %d)",
      len(alive),
      n,
  )
  delete_results = _RunAsync(
      kickoff=cluster.DeleteNodePoolAsync,
      wait_fn=cluster.WaitForOperation,
      items=alive,
      get_name=str,
  )
  # attempted_ops=n: success rate reflects original request, not just live.
  # EKS rolls back timed-out pools silently — without this shows 100%.
  samples += _OpSamples("ScenarioA_Delete", delete_results, attempted_ops=n)
  return samples'''
s = edit(s, old_funcs, new_func, 'carve _RunScenarioA + remove pipelined')

B.write_text(s)

# ---------------- tests ----------------
t = T.read_text()

# remove the version cross-validation test (uses target_version)
t = edit(
    t,
    '\n'
    '  def testVersionFlagWithoutConcurrentRaises(self):\n'
    '    with flagsaver.flagsaver(\n'
    "        k8s_mgmt_scenarios=['large_scale_provisioning'],\n"
    "        k8s_mgmt_target_version='1.34',\n"
    '    ):\n'
    '      with self.assertRaises(errors.Config.InvalidValue):\n'
    '        kubernetes_management_benchmark.CheckPrerequisites('
    '_make_mock_config())\n',
    '',
    'test: remove testVersionFlagWithoutConcurrentRaises',
)
# run-meta key list: drop target_version
t = edit(
    t,
    "        'initial_version',\n"
    "        'target_version',\n"
    "        'cluster_k8s_version',\n",
    "        'initial_version',\n        'cluster_k8s_version',\n",
    'test: meta key list drop target_version',
)
# testRunUsesExplicitVersionFlags: drop target flag + assert
t = edit(
    t,
    "      k8s_mgmt_initial_version='1.30',\n"
    "      k8s_mgmt_target_version='1.31',\n",
    "      k8s_mgmt_initial_version='1.30',\n",
    'test: explicit flags drop target set',
)
t = edit(
    t,
    "    self.assertEqual('1.30', samples[0].metadata['initial_version'])\n"
    "    self.assertEqual('1.31', samples[0].metadata['target_version'])\n",
    "    self.assertEqual('1.30', samples[0].metadata['initial_version'])\n",
    'test: explicit flags drop target assert',
)
# testRunAutoResolves: drop target assert
t = edit(
    t,
    "    self.assertEqual('1.33', samples[0].metadata['initial_version'])\n"
    "    self.assertEqual('1.34', samples[0].metadata['target_version'])\n",
    "    self.assertEqual('1.33', samples[0].metadata['initial_version'])\n",
    'test: auto-resolve drop target assert',
)
# RunScenarioATest: replace class body (drop pipeline flag, upgrade asserts,
# target args, and the pipelined-activation test)
old_class_a = '''class RunScenarioATest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunScenarioA phase-by-phase and pipelined modes."""

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhaseByPhaseProducesCreateUpgradeDeleteSamples(self):
    """Tests Scenario A produces Create, Upgrade, and Delete samples."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = kubernetes_management_benchmark._RunScenarioA(
        cluster, '1.33', '1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioA_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Upgrade' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Delete' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhaseByPhasePassesInitialVersionToCreate(self):
    """Tests _RunScenarioA passes initial_version to CreateNodePoolAsync."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    kubernetes_management_benchmark._RunScenarioA(cluster, '1.33', '1.34')
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
  def testPhaseByPhaseDeleteUsesLivePoolList(self):
    """Tests that _RunScenarioA deletes only the pools it finds at runtime."""
    cluster = _make_mock_cluster(pool_names=['pkbma000'])
    kubernetes_management_benchmark._RunScenarioA(cluster, '1.33', '1.34')
    self.assertEqual(1, cluster.DeleteNodePoolAsync.call_count)

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=True,
  )
  def testPipelinedModeActivatedByFlag(self):
    """Tests pipelined mode is activated by the pipeline_scenario_a flag."""
    cluster = _make_mock_cluster(pool_names=[])
    samples = kubernetes_management_benchmark._RunScenarioA(
        cluster, '1.33', '1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioA_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Upgrade' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Delete' in m for m in metrics))


class RunScenarioAPipelinedTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunScenarioAPipelined pipelined execution path."""

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPipelinedProducesAllThreePhases(self):
    """Tests pipelined Scenario A produces Create/Upgrade/Delete samples."""
    cluster = _make_mock_cluster(pool_names=[])
    samples = kubernetes_management_benchmark._RunScenarioAPipelined(
        cluster, n=2, initial='1.33', target='1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioA_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Upgrade' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Delete' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPipelinedSkipsUpgradeAfterCreateFailure(self):
    """Tests pipelined mode skips upgrade when create fails."""
    cluster = _make_mock_cluster(pool_names=[])
    cluster.CreateNodePoolAsync.side_effect = RuntimeError('create failed')
    samples = kubernetes_management_benchmark._RunScenarioAPipelined(
        cluster, n=1, initial='1.33', target='1.34'
    )
    cluster.UpgradeNodePoolAsync.assert_not_called()
    upgrade_rate = next(
        (s for s in samples if s.metric == 'ScenarioA_Upgrade_SuccessRate'),
        None,
    )
    if upgrade_rate is not None:
      self.assertEqual(0.0, upgrade_rate.value)'''

new_class_a = '''class RunScenarioATest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunScenarioA phase-by-phase create/delete path."""

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testProducesCreateAndDeleteSamples(self):
    """Tests Scenario A produces Create and Delete samples."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = kubernetes_management_benchmark._RunScenarioA(cluster, '1.33')
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioA_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Delete' in m for m in metrics))
    self.assertFalse(any('ScenarioA_Upgrade' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPassesInitialVersionToCreate(self):
    """Tests _RunScenarioA passes initial_version to CreateNodePoolAsync."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    kubernetes_management_benchmark._RunScenarioA(cluster, '1.33')
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
    """Tests that _RunScenarioA deletes only the pools it finds at runtime."""
    cluster = _make_mock_cluster(pool_names=['pkbma000'])
    kubernetes_management_benchmark._RunScenarioA(cluster, '1.33')
    self.assertEqual(1, cluster.DeleteNodePoolAsync.call_count)'''
t = edit(
    t,
    old_class_a,
    new_class_a,
    'test: carve RunScenarioATest + drop pipelined class',
)

T.write_text(t)
print('\nDone.')
