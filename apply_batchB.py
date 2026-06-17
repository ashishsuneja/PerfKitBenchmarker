#!/usr/bin/env python3
"""Batch B: relocate the node-pool sweep (hubatish 242 / 275 / 292).

Behavioral change requested by review:
  - Don't sweep at the START of Run (cluster should start clean).
  - Each scenario cleans up after itself: sweep AFTER each scenario so the
    next one starts clean, rather than the next scenario fixing the mess.
  - Cleanup() performs the same sweep (reviewer: "cleanup doesn't call this").
  - Rename _CleanStartSweep -> _SweepNodePools (no longer start-specific).

Run on top of Batch A (commit b0c04ec6d). Idempotent: [ok]/[SKIP] per edit.
"""

import pathlib

BENCH = pathlib.Path(
    "perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py"
)
TEST = pathlib.Path(
    "tests/linux_benchmarks/kubernetes_management_benchmark_test.py"
)


def edit(path, old, new, label, required=True):
  text = path.read_text()
  if new in text and old not in text:
    print(f"[SKIP] {label} (already applied)")
    return
  if old not in text:
    if required:
      raise SystemExit(f"[FAIL] anchor not found: {label}\n---\n{old[:160]}")
    print(f"[SKIP] {label} (anchor absent, optional)")
    return
  if text.count(old) != 1 and required:
    raise SystemExit(f"[FAIL] anchor not unique ({text.count(old)}x): {label}")
  path.write_text(text.replace(old, new, 1))
  print(f"[ok]   {label}")


def replace_all(path, old, new, label):
  text = path.read_text()
  if old not in text:
    print(f"[SKIP] {label} (absent)")
    return
  n = text.count(old)
  path.write_text(text.replace(old, new))
  print(f"[ok]   {label} ({n}x)")


# ───────────────────────── BENCH: rename helper ────────────────────────────
edit(
    BENCH,
    "def _CleanStartSweep(cluster: kubernetes_cluster.KubernetesCluster) ->"
    ' None:\n  """Deletes any stale pkbm* node pools so each run starts clean'
    ' (spec C.2)."""\n  stale = [n for n in cluster.GetNodePoolNames() if'
    ' n.startswith(_PREFIX)]\n  if not stale:\n    logging.info("CleanStart: no'
    ' stale pools found — clean start confirmed.")\n    return\n '
    ' logging.info("CleanStart: deleting %d stale pools: %s", len(stale),'
    " stale)\n  background_tasks.RunThreaded(cluster.DeleteNodePool, stale)",
    "def _SweepNodePools(cluster: kubernetes_cluster.KubernetesCluster) ->"
    ' None:\n  """Deletes all pkbm* node pools, blocking until each delete'
    " completes.\n\n  Called after each scenario so the next one starts from a"
    " clean cluster,\n  and from Cleanup() as a final best-effort teardown.\n "
    ' """\n  pools = [n for n in cluster.GetNodePoolNames() if'
    ' n.startswith(_PREFIX)]\n  if not pools:\n    logging.info("Sweep: no'
    ' pkbm* pools present — cluster is clean.")\n    return\n '
    ' logging.info("Sweep: deleting %d pkbm* pools: %s", len(pools), pools)\n '
    " background_tasks.RunThreaded(cluster.DeleteNodePool, pools)",
    "rename _CleanStartSweep -> _SweepNodePools (242)",
)

# ───────────────────────── BENCH: Run — drop start sweep ───────────────────
edit(
    BENCH,
    "  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)\n"
    "\n"
    "  # Spec C.2: start clean.\n"
    "  _CleanStartSweep(cluster)\n"
    "\n"
    "  # Resolve the initial node-pool version once",
    "  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)\n"
    "\n"
    "  # Resolve the initial node-pool version once",
    "Run: remove start-of-run sweep (242)",
)

# ───────────────────────── BENCH: Run — per-scenario sweep ─────────────────
edit(
    BENCH,
    '  if "concurrent_node_pool_ops" in scenarios:\n    samples +='
    " _RunConcurrentNodePoolOps(cluster, initial)\n  if"
    ' "overlapping_cluster_update" in scenarios:\n    samples +='
    " _RunOverlappingClusterUpdate(cluster, initial)\n  if"
    ' "large_scale_provisioning" in scenarios:\n    # Stale pools from earlier'
    " scenarios may still be in Deleting state and\n    # count toward AKS's"
    " 100-pool cluster limit; sweep before the scale work\n    # so we don't"
    " hit MaxAgentPoolCountReached mid-run.\n    _CleanStartSweep(cluster)\n   "
    " samples += _SweepScales(cluster, initial)\n",
    '  if "concurrent_node_pool_ops" in scenarios:\n'
    "    samples += _RunConcurrentNodePoolOps(cluster, initial)\n"
    "    # Each scenario leaves the cluster clean for the next one.\n"
    "    _SweepNodePools(cluster)\n"
    '  if "overlapping_cluster_update" in scenarios:\n'
    "    samples += _RunOverlappingClusterUpdate(cluster, initial)\n"
    "    _SweepNodePools(cluster)\n"
    '  if "large_scale_provisioning" in scenarios:\n'
    "    samples += _SweepScales(cluster, initial)\n"
    "    _SweepNodePools(cluster)\n",
    "Run: per-scenario sweep instead of start/pre-scale (275)",
)

# ───────────────────────── BENCH: Cleanup uses the sweep ───────────────────
edit(
    BENCH,
    '  kubectl.RunKubectlCommand(\n      ["delete", "pod", _SLEEP_POD_NAME,'
    ' "--ignore-not-found"],\n      raise_on_failure=False,\n  )\n  leftover ='
    " [n for n in cluster.GetNodePoolNames() if n.startswith(_PREFIX)]\n  if"
    ' not leftover:\n    return\n  logging.info("Cleanup: deleting %d leftover'
    ' node pools", len(leftover))\n '
    " background_tasks.RunThreaded(cluster.DeleteNodePool, leftover)",
    "  kubectl.RunKubectlCommand(\n"
    '      ["delete", "pod", _SLEEP_POD_NAME, "--ignore-not-found"],\n'
    "      raise_on_failure=False,\n"
    "  )\n"
    "  # Final teardown reuses the same sweep the scenarios use.\n"
    "  _SweepNodePools(cluster)",
    "Cleanup: delegate to _SweepNodePools (242)",
)

print("\nBENCH edits complete.\n")

# ════════════════════════ TEST FILE ════════════════════════════════════════

# Rename the helper everywhere in tests.
replace_all(
    TEST,
    "_CleanStartSweep",
    "_SweepNodePools",
    "test: rename _CleanStartSweep -> _SweepNodePools",
)

# Rename the dedicated test class + its docstring.
edit(
    TEST,
    "class CleanStartSweepTest(pkb_common_test_case.PkbCommonTestCase):",
    "class SweepNodePoolsTest(pkb_common_test_case.PkbCommonTestCase):",
    "test: rename CleanStartSweepTest class",
    required=False,
)
edit(
    TEST,
    "  def testCleanStartSweepRaisesOnGetNodePoolNamesException(self):",
    "  def testSweepRaisesOnGetNodePoolNamesException(self):",
    "test: rename raises-on-exception test method",
    required=False,
)

# testRunCallsCleanStartSweep: behavior changed. The sweep now runs AFTER each
# scenario (3 scenarios default-on -> 3 sweeps), not at start + pre-scale (2).
edit(
    TEST,
    '  def testRunCallsCleanStartSweep(self):\n    """Tests that Run invokes'
    ' _SweepNodePools before executing scenarios."""',
    "  def testRunSweepsAfterEachScenario(self):\n"
    '    """Run sweeps node pools after each scenario that executes."""',
    "test: rename+repurpose testRunCallsCleanStartSweep",
    required=False,
)
edit(
    TEST,
    "      kubernetes_management_benchmark.Run(bm_spec)\n"
    "    self.assertEqual(mock_clean.call_count, 2)",
    "      kubernetes_management_benchmark.Run(bm_spec)\n"
    "    # All three scenarios run by default -> one sweep after each.\n"
    "    self.assertEqual(mock_clean.call_count, 3)",
    "test: sweep count 2 -> 3 (per-scenario)",
    required=False,
)

print("\nTEST edits complete.\n")
