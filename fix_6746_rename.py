#!/usr/bin/env python3
"""PR #6746 review comments #2, #3, #5 (Zach / hubatish).

  #2 Rename scenario identifiers A/B/C -> descriptive names + documenting block:
        A -> concurrent_node_pool_ops
        B -> overlapping_cluster_update
        C -> large_scale_provisioning
     (User-facing identifiers, flag default/help, dispatch, docstring. Internal
      _RunScenario* function names and ScenarioA_* metric tags are intentionally
      kept for now; they're internal and best harmonized during the PR split.)
  #3 Group scenario-specific flags by scenario (shared / concurrent / large).
  #5 CheckPrerequisites cross-validation: a scenario-specific flag set while its
     scenario isn't selected -> fail.

Run from repo root on branch mgmt_plane_benchmark_base. Edits the benchmark and
its test file. Idempotent.
"""

import pathlib

B = pathlib.Path(
    "perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py"
)
T = pathlib.Path(
    "tests/linux_benchmarks/kubernetes_management_benchmark_test.py"
)


def edit(text, old, new, label, count=1):
  if old in text:
    print(f"[ok]   {label}")
    return text.replace(old, new, count)
  if new in text:
    print(f"[SKIP] {label} (already applied)")
    return text
  print(f"[WARN] {label}: anchor not found — review manually")
  return text


src = B.read_text()

# ── #2: module docstring scenario list ──
src = edit(
    src,
    "Measures GKE/EKS/AKS control-plane API responsiveness via three"
    " scenarios:\n"
    "  A. Concurrent node-pool create/upgrade/delete.\n"
    "  B. Node-pool create overlapping with a long-running cluster update.\n"
    "  C. Large-scale node-pool provisioning (single scale or sweep).\n",
    "Measures GKE/EKS/AKS control-plane API responsiveness via three"
    " scenarios:\n"
    "  concurrent_node_pool_ops: concurrent node-pool create/upgrade/delete.\n"
    "  overlapping_cluster_update: node-pool create overlapping a cluster"
    " update.\n"
    "  large_scale_provisioning: large-scale node-pool provisioning"
    " (scale/sweep).\n",
    "#2 docstring scenario list",
)
src = edit(
    src,
    "  - Streaming concurrency in Scenario C (no batch barriers)\n"
    "  - Optional pipelined Scenario A (create->upgrade->delete per thread)\n",
    "  - Streaming concurrency in large_scale_provisioning (no batch"
    " barriers)\n"
    "  - Optional pipelined concurrent_node_pool_ops"
    " (create/upgrade/delete)\n",
    "#2 docstring optimization notes",
)

# ── #2 + #3: _VALID_SCENARIOS + documenting comment + grouped/renamed flags ──
old_block = """_VALID_SCENARIOS = frozenset({"A", "B", "C"})

_CONCURRENT_NODEPOOLS = flags.DEFINE_integer(
    "k8s_mgmt_concurrent_nodepools",
    5,
    "Number of node pools to create/upgrade/delete concurrently in Scenario A.",
)
_LARGE_SCALE_NODEPOOLS = flags.DEFINE_integer(
    "k8s_mgmt_large_scale_nodepools",
    1000,
    "Number of node pools to provision in the large-scale Scenario C. "
    + "Spec target is 1000; ensure VPC/quota is available before running.",
)
_NODES_PER_NODEPOOL = flags.DEFINE_integer(
    "k8s_mgmt_nodes_per_nodepool",
    2,
    "Number of nodes per node pool. Google spec: 2 nodes per pool.",
)
_INITIAL_VERSION = flags.DEFINE_string(
    "k8s_mgmt_initial_version",
    None,
    "Kubernetes version for newly-created node pools (N-1). None = auto.",
)
_TARGET_VERSION = flags.DEFINE_string(
    "k8s_mgmt_target_version",
    None,
    "Kubernetes version to upgrade node pools to (N). None = cluster version.",
)
_SCENARIOS = flags.DEFINE_list(
    "k8s_mgmt_scenarios",
    ["A", "B", "C"],
    "Comma-separated subset of scenarios to run. Valid values: A, B, C.",
)
_SCALE_SWEEP = flags.DEFINE_list(
    "k8s_mgmt_scale_sweep",
    [],
    "Comma-separated list of node-pool counts for Scenario C scale sweep. "
    + "Each scale runs as a separate sub-run with full create/delete cycle. "
    + "Example: --k8s_mgmt_scale_sweep=10,50,100,500,1000. "
    + "If empty, uses --k8s_mgmt_large_scale_nodepools.",
)
_MAX_CONCURRENT = flags.DEFINE_integer(
    "k8s_mgmt_max_concurrent",
    50,
    "Cap on concurrent provider API calls within a batch. "
    + "Higher = faster but more aggressive on connection pools.",
)
_PIPELINE_SCENARIO_A = flags.DEFINE_boolean(
    "k8s_mgmt_pipeline_scenario_a",
    True,
    "If True, run Scenario A as per-pool pipeline (create->upgrade->delete "
    + "back-to-back per thread). Minimizes wall time. "
    + "Default False for spec-strict phase-by-phase.",
)"""

new_block = """# Scenarios measured by this benchmark (select via --k8s_mgmt_scenarios):
#   concurrent_node_pool_ops: concurrently create, upgrade, and delete N
#     node pools; measures control-plane throughput under parallel ops.
#   overlapping_cluster_update: run a cluster update and a node-pool create
#     simultaneously; measures behaviour when a cluster-scoped op overlaps a
#     node-pool-scoped one.
#   large_scale_provisioning: create then delete a large number of node pools
#     (optionally swept via --k8s_mgmt_scale_sweep); measures scaling limits
#     and large-batch provisioning latency.
_VALID_SCENARIOS = frozenset({
    "concurrent_node_pool_ops",
    "overlapping_cluster_update",
    "large_scale_provisioning",
})

# ── Shared flags (apply across all scenarios) ──
_SCENARIOS = flags.DEFINE_list(
    "k8s_mgmt_scenarios",
    [
        "concurrent_node_pool_ops",
        "overlapping_cluster_update",
        "large_scale_provisioning",
    ],
    "Comma-separated subset of scenarios to run. Valid values: "
    + "concurrent_node_pool_ops, overlapping_cluster_update, "
    + "large_scale_provisioning.",
)
_NODES_PER_NODEPOOL = flags.DEFINE_integer(
    "k8s_mgmt_nodes_per_nodepool",
    2,
    "Number of nodes per node pool. Google spec: 2 nodes per pool.",
)
_MAX_CONCURRENT = flags.DEFINE_integer(
    "k8s_mgmt_max_concurrent",
    50,
    "Cap on concurrent provider API calls within a batch. "
    + "Higher = faster but more aggressive on connection pools.",
)

# ── concurrent_node_pool_ops flags ──
_CONCURRENT_NODEPOOLS = flags.DEFINE_integer(
    "k8s_mgmt_concurrent_nodepools",
    5,
    "Number of node pools to create/upgrade/delete concurrently in the "
    + "concurrent_node_pool_ops scenario.",
)
_INITIAL_VERSION = flags.DEFINE_string(
    "k8s_mgmt_initial_version",
    None,
    "Kubernetes version for newly-created node pools (N-1). None = auto.",
)
_TARGET_VERSION = flags.DEFINE_string(
    "k8s_mgmt_target_version",
    None,
    "Kubernetes version to upgrade node pools to (N). None = cluster version.",
)
_PIPELINE_SCENARIO_A = flags.DEFINE_boolean(
    "k8s_mgmt_pipeline_scenario_a",
    True,
    "If True, run concurrent_node_pool_ops as a per-pool pipeline "
    + "(create->upgrade->delete back-to-back per thread). Minimizes wall time. "
    + "Default False for spec-strict phase-by-phase.",
)

# ── large_scale_provisioning flags ──
_LARGE_SCALE_NODEPOOLS = flags.DEFINE_integer(
    "k8s_mgmt_large_scale_nodepools",
    1000,
    "Number of node pools to provision in the large_scale_provisioning "
    + "scenario. Spec target is 1000; ensure VPC/quota is available before "
    + "running.",
)
_SCALE_SWEEP = flags.DEFINE_list(
    "k8s_mgmt_scale_sweep",
    [],
    "Comma-separated list of node-pool counts for the large_scale_provisioning "
    + "scale sweep. Each scale runs as a separate sub-run with full "
    + "create/delete cycle. Example: --k8s_mgmt_scale_sweep=10,50,100,500,1000. "
    + "If empty, uses --k8s_mgmt_large_scale_nodepools.",
)"""
src = edit(src, old_block, new_block, "#2/#3 valid set + grouped/renamed flags")

# ── #2: Run() dispatch (drop .upper(), descriptive identifiers) ──
src = edit(
    src,
    "  scenarios = {s.strip().upper() for s in _SCENARIOS.value}\n"
    "  samples: list[sample.Sample] = []\n"
    "\n"
    '  if "A" in scenarios:\n'
    "    samples += _RunScenarioA(cluster, initial, target)\n"
    '  if "B" in scenarios:\n'
    "    samples += _RunScenarioB(cluster, initial)\n"
    '  if "C" in scenarios:\n',
    "  scenarios = {s.strip() for s in _SCENARIOS.value}\n"
    "  samples: list[sample.Sample] = []\n"
    "\n"
    '  if "concurrent_node_pool_ops" in scenarios:\n'
    "    samples += _RunScenarioA(cluster, initial, target)\n"
    '  if "overlapping_cluster_update" in scenarios:\n'
    "    samples += _RunScenarioB(cluster, initial)\n"
    '  if "large_scale_provisioning" in scenarios:\n',
    "#2 Run dispatch",
)

# ── #5: CheckPrerequisites cross-validation ──
src = edit(
    src,
    '        + f"Valid options: {sorted(_VALID_SCENARIOS)}."\n'
    "    )\n"
    "  for s in _SCALE_SWEEP.value:\n",
    '        + f"Valid options: {sorted(_VALID_SCENARIOS)}."\n'
    "    )\n"
    "  selected = {s.strip() for s in _SCENARIOS.value}\n"
    "  if (\n"
    "      _INITIAL_VERSION.value or _TARGET_VERSION.value\n"
    '  ) and "concurrent_node_pool_ops" not in selected:\n'
    "    raise errors.Config.InvalidValue(\n"
    '        "--k8s_mgmt_initial_version / --k8s_mgmt_target_version apply only'
    ' to "\n'
    '        + "the concurrent_node_pool_ops scenario, which is not'
    ' selected."\n'
    "    )\n"
    '  if _SCALE_SWEEP.value and "large_scale_provisioning" not in selected:\n'
    "    raise errors.Config.InvalidValue(\n"
    '        "--k8s_mgmt_scale_sweep applies only to the'
    ' large_scale_provisioning "\n'
    '        + "scenario, which is not selected."\n'
    "    )\n"
    "  for s in _SCALE_SWEEP.value:\n",
    "#5 CheckPrerequisites cross-validation",
)

B.write_text(src)

# ── Test file: update flag values + add cross-validation tests ──
t = T.read_text()
for old, new, lbl in [
    (
        "k8s_mgmt_scenarios=['A', 'B', 'C']",
        (
            "k8s_mgmt_scenarios=[\n"
            "            'concurrent_node_pool_ops',\n"
            "            'overlapping_cluster_update',\n"
            "            'large_scale_provisioning',\n"
            "        ]"
        ),
        "tests: ['A','B','C']",
    ),
    (
        "k8s_mgmt_scenarios=['A', 'Z']",
        "k8s_mgmt_scenarios=['concurrent_node_pool_ops', 'Z']",
        "tests: ['A','Z']",
    ),
    (
        "k8s_mgmt_scenarios=['A']",
        "k8s_mgmt_scenarios=['concurrent_node_pool_ops']",
        "tests: ['A']",
    ),
    (
        "k8s_mgmt_scenarios=['B']",
        "k8s_mgmt_scenarios=['overlapping_cluster_update']",
        "tests: ['B']",
    ),
    (
        "k8s_mgmt_scenarios=['C']",
        "k8s_mgmt_scenarios=['large_scale_provisioning']",
        "tests: ['C']",
    ),
]:
  if old in t:
    n = t.count(old)
    t = t.replace(old, new)
    print(f"[ok]   {lbl} ({n} site(s))")
  elif new in t:
    print(f"[SKIP] {lbl} (already applied)")
  else:
    print(f"[WARN] {lbl}: not found")

# add cross-validation tests after testLowercaseScenarioRaises
xval_anchor = (
    "  def testLowercaseScenarioRaises(self):\n"
    "    with flagsaver.flagsaver(k8s_mgmt_scenarios=['a']):\n"
    "      with self.assertRaises(errors.Config.InvalidValue):\n"
    "        kubernetes_management_benchmark.CheckPrerequisites("
    "_make_mock_config())\n"
)
xval_new = xval_anchor + (
    "\n"
    "  def testVersionFlagWithoutConcurrentRaises(self):\n"
    "    with flagsaver.flagsaver(\n"
    "        k8s_mgmt_scenarios=['large_scale_provisioning'],\n"
    "        k8s_mgmt_target_version='1.34',\n"
    "    ):\n"
    "      with self.assertRaises(errors.Config.InvalidValue):\n"
    "        kubernetes_management_benchmark.CheckPrerequisites("
    "_make_mock_config())\n"
    "\n"
    "  def testScaleSweepWithoutLargeScaleRaises(self):\n"
    "    with flagsaver.flagsaver(\n"
    "        k8s_mgmt_scenarios=['concurrent_node_pool_ops'],\n"
    "        k8s_mgmt_scale_sweep=['10', '50'],\n"
    "    ):\n"
    "      with self.assertRaises(errors.Config.InvalidValue):\n"
    "        kubernetes_management_benchmark.CheckPrerequisites("
    "_make_mock_config())\n"
)
if "testVersionFlagWithoutConcurrentRaises" in t:
  print("[SKIP] cross-validation tests (already added)")
elif xval_anchor in t:
  t = t.replace(xval_anchor, xval_new, 1)
  print("[ok]   added 3 cross-validation tests")
else:
  print("[WARN] cross-validation test anchor not found — review manually")

T.write_text(t)
print("\nDone.")
