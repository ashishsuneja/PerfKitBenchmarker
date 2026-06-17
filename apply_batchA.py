#!/usr/bin/env python3
"""Batch A: address hubatish Jun-9 review on kubernetes_management_benchmark.

Covers:
  - Full A/B/C purge (function names, name helpers, metric tags, metadata key)
  - 347: extract live-by-prefix node-pool listing into _LiveNodePoolNames helper
  - 276: split _RunScenarioC into _SweepScales (loop) + _ScaleToPoolCount (one scale)
  - 623: fail the benchmark if total attempted ops == 0 (was silent skip)
  - 597: remove dead tuple->_OpResult defensive conversion
  - 630: expose op counts (Total/Executed/Successful/Skipped) as separate metrics
  - tests updated to match (incl. parameterized name tests, new dispatch seam)

Idempotent: prints [ok]/[SKIP] per edit; safe to re-run.
"""

import pathlib
import re

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
      raise SystemExit(f"[FAIL] anchor not found: {label}\n---\n{old[:200]}")
    print(f"[SKIP] {label} (anchor absent, optional)")
    return
  cnt = text.count(old)
  if cnt != 1 and required:
    raise SystemExit(f"[FAIL] anchor not unique ({cnt}x): {label}")
  path.write_text(text.replace(old, new, 1))
  print(f"[ok]   {label}")


def sub(path, pattern, repl, label, expected=None):
  text = path.read_text()
  new_text, n = re.subn(pattern, repl, text)
  if n == 0:
    # treat as idempotent skip if the replacement target already present
    print(f"[SKIP] {label} (0 matches — likely already applied)")
    return
  if expected is not None and n != expected:
    raise SystemExit(f"[FAIL] {label}: expected {expected} subs, got {n}")
  path.write_text(new_text)
  print(f"[ok]   {label} ({n} subs)")


# ───────────────────────── BENCH: name helpers ─────────────────────────────
edit(
    BENCH,
    'def _ScenarioAName(i):\n  return f"{_PREFIX}a{i:03d}"',
    'def _ConcurrentPoolName(i):\n  return f"{_PREFIX}a{i:03d}"',
    "rename _ScenarioAName -> _ConcurrentPoolName",
)
edit(
    BENCH,
    '_SCENARIO_B_NAME = f"{_PREFIX}b"',
    '_OVERLAPPING_POOL_NAME = f"{_PREFIX}b"',
    "rename _SCENARIO_B_NAME -> _OVERLAPPING_POOL_NAME",
)
edit(
    BENCH,
    'def _ScenarioCName(i):\n  return f"{_PREFIX}c{i:04d}"',
    'def _ScalePoolName(i):\n  return f"{_PREFIX}c{i:04d}"',
    "rename _ScenarioCName -> _ScalePoolName",
)

# ───────────────────────── BENCH: shared live-list helper (347) ────────────
# Insert _LiveNodePoolNames just after _MakeNodePoolConfig.
edit(
    BENCH,
    "  cfg.max_nodes = _NODES_PER_NODEPOOL.value\n  return cfg\n",
    "  cfg.max_nodes = _NODES_PER_NODEPOOL.value\n  return cfg\n\n\n"
    "def _LiveNodePoolNames(\n"
    "    cluster: kubernetes_cluster.KubernetesCluster, prefix: str\n"
    ") -> list[str]:\n"
    '  """Returns current node-pool names matching the given prefix."""\n'
    "  return [p for p in cluster.GetNodePoolNames() if"
    " p.startswith(prefix)]\n",
    "add _LiveNodePoolNames helper (347)",
)

# ───────────────────────── BENCH: Run dispatch ─────────────────────────────
edit(
    BENCH,
    '  if "concurrent_node_pool_ops" in scenarios:\n    samples +='
    ' _RunScenarioA(cluster, initial)\n  if "overlapping_cluster_update" in'
    " scenarios:\n    samples += _RunScenarioB(cluster, initial)\n  if"
    ' "large_scale_provisioning" in scenarios:\n    # fix: Scenario A/B pools'
    " may still be in Deleting state and count\n    # toward AKS's 100-pool"
    " cluster limit.  Sweep them out before Scenario C\n    # so we don't hit"
    " MaxAgentPoolCountReached mid-run.\n    _CleanStartSweep(cluster)\n   "
    " scales = (\n        [int(x.strip()) for x in _SCALE_SWEEP.value]\n       "
    " if _SCALE_SWEEP.value\n        else [_LARGE_SCALE_NODEPOOLS.value]\n   "
    ' )\n    logging.info("Scenario C: scale sweep = %s", scales)\n    for'
    " scale in scales:\n      scenario_c_samples = _RunScenarioC(cluster,"
    " initial, scale)\n      for s in scenario_c_samples:\n       "
    ' s.metadata["scenario_c_scale"] = str(scale)\n      samples +='
    " scenario_c_samples\n",
    '  if "concurrent_node_pool_ops" in scenarios:\n    samples +='
    " _RunConcurrentNodePoolOps(cluster, initial)\n  if"
    ' "overlapping_cluster_update" in scenarios:\n    samples +='
    " _RunOverlappingClusterUpdate(cluster, initial)\n  if"
    ' "large_scale_provisioning" in scenarios:\n    # Stale pools from earlier'
    " scenarios may still be in Deleting state and\n    # count toward AKS's"
    " 100-pool cluster limit; sweep before the scale work\n    # so we don't"
    " hit MaxAgentPoolCountReached mid-run.\n    _CleanStartSweep(cluster)\n   "
    " samples += _SweepScales(cluster, initial)\n",
    "Run dispatch: rename A/B + extract _SweepScales (276)",
)

# ───────────────────────── BENCH: _SweepScales helper (276) ────────────────
# Insert _SweepScales just before _RunScenarioA's section banner.
edit(
    BENCH,
    "# ---------------------------------------------------------------------------\n#"
    " Scenario A\n#"
    " ---------------------------------------------------------------------------\n\n\ndef"
    " _RunScenarioA(",
    "def _SweepScales(\n    cluster: kubernetes_cluster.KubernetesCluster,\n   "
    ' initial: str,\n) -> list[sample.Sample]:\n  """Runs large-scale'
    " provisioning across each requested scale.\n\n  Scales come from"
    " --k8s_mgmt_scale_sweep when set, else the single\n "
    " --k8s_mgmt_large_scale_nodepools value. Each scale's samples are"
    " tagged\n  with large_scale_scale so results stay distinguishable.\n "
    ' """\n  scales = (\n      [int(x.strip()) for x in _SCALE_SWEEP.value]\n  '
    "    if _SCALE_SWEEP.value\n      else [_LARGE_SCALE_NODEPOOLS.value]\n "
    ' )\n  logging.info("large_scale_provisioning: scale sweep = %s", scales)\n'
    "  samples: list[sample.Sample] = []\n  for scale in scales:\n   "
    " scale_samples = _ScaleToPoolCount(cluster, initial, scale)\n    for s in"
    ' scale_samples:\n      s.metadata["large_scale_scale"] = str(scale)\n   '
    " samples += scale_samples\n  return samples\n\n\ndef"
    " _RunConcurrentNodePoolOps(",
    "add _SweepScales + rename _RunScenarioA -> _RunConcurrentNodePoolOps"
    " (276)",
)

# ───────────────────────── BENCH: Scenario A body ──────────────────────────
edit(
    BENCH,
    "  pool_names = [_ScenarioAName(i) for i in range(n)]",
    "  pool_names = [_ConcurrentPoolName(i) for i in range(n)]",
    "ScenarioA: _ConcurrentPoolName",
)
edit(
    BENCH,
    '      "ScenarioA_Create", create_results, attempted_ops=len(pool_names)',
    '      "ConcurrentOps_Create", create_results,'
    " attempted_ops=len(pool_names)",
    "ScenarioA: metric ConcurrentOps_Create",
)
edit(
    BENCH,
    "  alive = [p for p in cluster.GetNodePoolNames() if"
    ' p.startswith(f"{_PREFIX}a")]',
    '  alive = _LiveNodePoolNames(cluster, f"{_PREFIX}a")',
    "ScenarioA: use _LiveNodePoolNames (347)",
)
edit(
    BENCH,
    '  samples += _OpSamples("ScenarioA_Delete", delete_results,'
    " attempted_ops=n)",
    '  samples += _OpSamples("ConcurrentOps_Delete", delete_results,'
    " attempted_ops=n)",
    "ScenarioA: metric ConcurrentOps_Delete",
)

# ───────────────────────── BENCH: Scenario B body ──────────────────────────
edit(
    BENCH,
    "# ---------------------------------------------------------------------------\n#"
    " Scenario B\n#"
    " ---------------------------------------------------------------------------\n\n\ndef"
    " _RunScenarioB(",
    "def _RunOverlappingClusterUpdate(",
    "rename _RunScenarioB -> _RunOverlappingClusterUpdate",
)
edit(
    BENCH,
    "  cfg = _MakeNodePoolConfig(cluster, _SCENARIO_B_NAME)",
    "  cfg = _MakeNodePoolConfig(cluster, _OVERLAPPING_POOL_NAME)",
    "ScenarioB: _OVERLAPPING_POOL_NAME (config)",
)
edit(
    BENCH,
    '    results.add("ScenarioB_ClusterUpdate", init, e2e, err)',
    '    results.add("OverlappingUpdate_ClusterUpdate", init, e2e, err)',
    "ScenarioB: metric OverlappingUpdate_ClusterUpdate",
)
edit(
    BENCH,
    '    results.add("ScenarioB_NodePoolCreate", init, e2e, err)',
    '    results.add("OverlappingUpdate_NodePoolCreate", init, e2e, err)',
    "ScenarioB: metric OverlappingUpdate_NodePoolCreate",
)
edit(
    BENCH,
    "  cluster.DeleteNodePool(_SCENARIO_B_NAME)",
    "  cluster.DeleteNodePool(_OVERLAPPING_POOL_NAME)",
    "ScenarioB: _OVERLAPPING_POOL_NAME (delete)",
)

# ───────────────────────── BENCH: Scenario C body ──────────────────────────
edit(
    BENCH,
    "# ---------------------------------------------------------------------------\n#"
    " Scenario C\n#"
    " ---------------------------------------------------------------------------\n\n\ndef"
    " _RunScenarioC(",
    "def _ScaleToPoolCount(",
    "rename _RunScenarioC -> _ScaleToPoolCount (276)",
)
edit(
    BENCH,
    "  pool_names = [_ScenarioCName(i) for i in range(scale)]",
    "  pool_names = [_ScalePoolName(i) for i in range(scale)]",
    "ScenarioC: _ScalePoolName",
)
edit(
    BENCH,
    '  samples += _OpSamples("ScenarioC_Create", create_results,'
    " attempted_ops=scale)",
    '  samples += _OpSamples("LargeScale_Create", create_results,'
    " attempted_ops=scale)",
    "ScenarioC: metric LargeScale_Create",
)
edit(
    BENCH,
    "  alive = [p for p in cluster.GetNodePoolNames() if"
    ' p.startswith(f"{_PREFIX}c")]',
    '  alive = _LiveNodePoolNames(cluster, f"{_PREFIX}c")',
    "ScenarioC: use _LiveNodePoolNames (347)",
)
edit(
    BENCH,
    '    samples += _OpSamples("ScenarioC_Delete", [], attempted_ops=scale)',
    '    samples += _OpSamples("LargeScale_Delete", [], attempted_ops=scale)',
    "ScenarioC: metric LargeScale_Delete (empty)",
)
edit(
    BENCH,
    '  samples += _OpSamples("ScenarioC_Delete", delete_results,'
    " attempted_ops=scale)",
    '  samples += _OpSamples("LargeScale_Delete", delete_results,'
    " attempted_ops=scale)",
    "ScenarioC: metric LargeScale_Delete",
)

# ───────────────────────── BENCH: _OpSamples (597, 623, 630) ───────────────
# 597: remove dead tuple->_OpResult conversion.
edit(
    BENCH,
    "  for r in results:\n"
    "    if isinstance(r, tuple):\n"
    "      r = _OpResult(*r)\n"
    '    meta = {"operation_name": r.name, "success": str(r.error is None)}',
    "  for r in results:\n"
    '    meta = {"operation_name": r.name, "success": str(r.error is None)}',
    "_OpSamples: remove dead tuple conversion (597)",
)

# 623 + 630: fail on total==0; emit count metrics as separate samples.
edit(
    BENCH,
    "  # ── Success rate"
    " ─────────────────────────────────────────────────────────\n  total ="
    " attempted_ops if attempted_ops is not None else len(results)\n  executed"
    " = len(results)\n  if total > 0:\n    samples.append(\n       "
    ' sample.Sample(\n            f"{metric_prefix}_SuccessRate",\n           '
    ' 100.0 * success / total,\n            "percent",\n            {\n        '
    '        "total_ops": str(total),\n                "executed_ops":'
    ' str(executed),\n                "successful_ops": str(success),\n        '
    '        "skipped_ops": str(total - executed),\n            },\n        )\n'
    "    )\n",
    "  # ── Counts + success rate"
    " ──────────────────────────────────────────────\n  total = attempted_ops"
    " if attempted_ops is not None else len(results)\n  executed ="
    " len(results)\n  if total == 0:\n    raise errors.Benchmarks.RunError(\n  "
    '      f"{metric_prefix}: zero operations attempted — the scenario "\n     '
    '   "produced no work, which indicates a setup or dispatch failure."\n   '
    " )\n  # Expose each count as its own metric (not just SuccessRate"
    ' metadata).\n  count_meta = {\n      "total_ops": str(total),\n     '
    ' "executed_ops": str(executed),\n      "successful_ops": str(success),\n  '
    '    "skipped_ops": str(total - executed),\n  }\n  for count_label,'
    ' count_value in (\n      ("TotalOps", total),\n      ("ExecutedOps",'
    ' executed),\n      ("SuccessfulOps", success),\n      ("SkippedOps", total'
    " - executed),\n  ):\n    samples.append(\n        sample.Sample(\n        "
    '    f"{metric_prefix}_{count_label}",\n            count_value,\n         '
    '   "count",\n            dict(count_meta),\n        )\n    )\n '
    " samples.append(\n      sample.Sample(\n         "
    ' f"{metric_prefix}_SuccessRate",\n          100.0 * success / total,\n    '
    '      "percent",\n          dict(count_meta),\n      )\n  )\n',
    "_OpSamples: fail on total==0 (623) + separate count metrics (630)",
)

print("\nBENCH edits complete.\n")

# ════════════════════════ TEST FILE ════════════════════════════════════════

# 1) Mechanical name swaps that are unambiguous everywhere.
for old, new, label in [
    ("_ScenarioAName", "_ConcurrentPoolName", "test: _ConcurrentPoolName"),
    ("_ScenarioCName", "_ScalePoolName", "test: _ScalePoolName"),
    (
        "_SCENARIO_B_NAME",
        "_OVERLAPPING_POOL_NAME",
        "test: _OVERLAPPING_POOL_NAME",
    ),
    (
        "_RunScenarioA",
        "_RunConcurrentNodePoolOps",
        "test: _RunConcurrentNodePoolOps",
    ),
    (
        "_RunScenarioB",
        "_RunOverlappingClusterUpdate",
        "test: _RunOverlappingClusterUpdate",
    ),
    # NOTE: _RunScenarioC handled separately below (split into two seams).
    (
        "ScenarioA_Create",
        "ConcurrentOps_Create",
        "test: metric ConcurrentOps_Create",
    ),
    (
        "ScenarioA_Delete",
        "ConcurrentOps_Delete",
        "test: metric ConcurrentOps_Delete",
    ),
    (
        "ScenarioA_Upgrade",
        "ConcurrentOps_Upgrade",
        "test: metric ConcurrentOps_Upgrade (neg-assert)",
    ),
    (
        "ScenarioB_ClusterUpdate",
        "OverlappingUpdate_ClusterUpdate",
        "test: metric OverlappingUpdate_ClusterUpdate",
    ),
    (
        "ScenarioB_NodePoolCreate",
        "OverlappingUpdate_NodePoolCreate",
        "test: metric OverlappingUpdate_NodePoolCreate",
    ),
    ("ScenarioC_Create", "LargeScale_Create", "test: metric LargeScale_Create"),
    ("ScenarioC_Delete", "LargeScale_Delete", "test: metric LargeScale_Delete"),
    (
        "scenario_c_scale",
        "large_scale_scale",
        "test: metadata large_scale_scale",
    ),
]:
  t = TEST.read_text()
  if old in t:
    TEST.write_text(t.replace(old, new))
    print(f"[ok]   {label} ({t.count(old)}x)")
  else:
    print(f"[SKIP] {label} (absent)")

# 2) Remaining _RunScenarioC -> _ScaleToPoolCount in the direct unit tests
#    (the scenario-C unit-test class calls the single-scale function directly).
t = TEST.read_text()
if "_RunScenarioC" in t:
  n = t.count("_RunScenarioC")
  TEST.write_text(t.replace("_RunScenarioC", "_ScaleToPoolCount"))
  print(f"[ok]   test: _RunScenarioC -> _ScaleToPoolCount ({n}x)")
else:
  print("[SKIP] test: _RunScenarioC (absent)")

# 3) Fix the docstring of the name-test class.
edit(
    TEST,
    '"""Tests for _SCENARIO_A_NAME, _SCENARIO_B_NAME, _SCENARIO_C_NAME."""',
    '"""Tests for node-pool name helpers."""',
    "test: name-class docstring",
    required=False,
)

print("\nTEST edits complete.\n")

# 4) 597 follow-through: convert the exact raw-tuple results literals in
#    OpSamplesTest into _OpResult(...) objects (precise, no regex).
_kb = "kubernetes_management_benchmark"
_R = f"{_kb}._OpResult"

precise = [
    (
        "    results = [('op1', 0.1, 1.0, None), ('op2', 0.2, 2.0, None)]",
        (
            f"    results = [{_R}('op1', 0.1, 1.0, None), {_R}('op2', 0.2, 2.0,"
            " None)]"
        ),
    ),
    (
        "    results = [('op1', 1.0, 2.0, None), ('op2', 0.5, 1.5, None)]",
        (
            f"    results = [{_R}('op1', 1.0, 2.0, None), {_R}('op2', 0.5, 1.5,"
            " None)]"
        ),
    ),
    (
        (
            "        ('op1', 1.0, 2.0, None),\n        ('op2', 0.5, 0.5,"
            " RuntimeError('fail')),"
        ),
        (
            f"        {_R}('op1', 1.0, 2.0, None),\n        {_R}('op2', 0.5,"
            " 0.5, RuntimeError('fail')),"
        ),
    ),
    (
        (
            "    results = [('op1', 1.0, 2.0, None), ('op2', 0.5, 0.5,"
            " Exception('err'))]"
        ),
        (
            f"    results = [{_R}('op1', 1.0, 2.0, None), {_R}('op2', 0.5, 0.5,"
            " Exception('err'))]"
        ),
    ),
    (
        "    results = [('fail-op', 0.5, 0.5, RuntimeError('oops'))]",
        f"    results = [{_R}('fail-op', 0.5, 0.5, RuntimeError('oops'))]",
    ),
    (
        (
            "    results = [(f'op{i}', float(i), float(i) * 2, None) for i in"
            " range(1, 6)]"
        ),
        (
            f"    results = [{_R}(f'op{{i}}', float(i), float(i) * 2, None) for"
            " i in range(1, 6)]"
        ),
    ),
]
# The single-op literal appears twice (lines 524, 561) — replace both.
single_old = "    results = [('op1', 1.0, 2.0, None)]"
single_new = f"    results = [{_R}('op1', 1.0, 2.0, None)]"

tt = TEST.read_text()
for old, new in precise:
  if new in tt:
    print(f"[SKIP] test tuple: {old.strip()[:40]} (applied)")
  elif old in tt:
    tt = tt.replace(old, new, 1)
    print(f"[ok]   test tuple: {old.strip()[:40]}")
  else:
    raise SystemExit(f"[FAIL] test tuple anchor missing: {old.strip()[:60]}")
# range(1,4) comprehension appears twice (aggregates + outliers tests)
r14_old = (
    "    results = [(f'op{i}', float(i), float(i) * 2, None) for i in"
    " range(1, 4)]"
)
r14_new = (
    f"    results = [{_R}(f'op{{i}}', float(i), float(i) * 2, None) for i in"
    " range(1, 4)]"
)
c14 = tt.count(r14_old)
if c14:
  tt = tt.replace(r14_old, r14_new)
  print(f"[ok]   test tuple: range(1,4) comprehension ({c14}x)")
else:
  print("[SKIP] test tuple: range(1,4) comprehension (applied)")
TEST.write_text(tt)
tt = TEST.read_text()
# both single-op occurrences
c = tt.count(single_old)
if c:
  tt = tt.replace(single_old, single_new)
  print(f"[ok]   test tuple: single-op literal ({c}x)")
else:
  print("[SKIP] test tuple: single-op literal (applied)")
TEST.write_text(tt)

# 5) Add 623 fail-fast test.
anchor = "  def testFailedOpIncludesErrorMessage(self):"
addition = (
    '  def testZeroAttemptedOpsRaisesRunError(self):\n    """total==0'
    ' indicates a dispatch/setup failure; fail loudly (623)."""\n    with'
    " self.assertRaises(errors.Benchmarks.RunError):\n     "
    f" {_kb}._OpSamples('Op', [], attempted_ops=0)\n\n"
)
edit(
    TEST,
    anchor,
    addition + anchor,
    "test: add testZeroAttemptedOpsRaisesRunError (623)",
    required=False,
)

# 6) Add 630 separate-count-metrics test.
anchor2 = "  def testSuccessRateMetadataFields(self):"
addition2 = (
    '  def testCountsExposedAsSeparateMetrics(self):\n    """630:'
    ' Total/Executed/Successful/Skipped each emitted as a metric."""\n   '
    f" results = [\n        {_R}('op1', 1.0, 2.0, None),\n        {_R}('op2',"
    f" 0.5, 0.5, Exception('e')),\n    ]\n    samples = {_kb}._OpSamples('Op',"
    " results, attempted_ops=3)\n    metrics = {s.metric: s.value for s in"
    " samples}\n    self.assertEqual(3, metrics['Op_TotalOps'])\n   "
    " self.assertEqual(2, metrics['Op_ExecutedOps'])\n    self.assertEqual(1,"
    " metrics['Op_SuccessfulOps'])\n    self.assertEqual(1,"
    " metrics['Op_SkippedOps'])\n\n"
)
edit(
    TEST,
    anchor2,
    addition2 + anchor2,
    "test: add testCountsExposedAsSeparateMetrics (630)",
    required=False,
)

# 7) Ensure errors import present in test file.
tt = TEST.read_text()
if "from perfkitbenchmarker import errors" not in tt:
  tt = tt.replace(
      "from perfkitbenchmarker import sample",
      "from perfkitbenchmarker import errors\n"
      "from perfkitbenchmarker import sample",
      1,
  )
  TEST.write_text(tt)
  print("[ok]   test: add errors import")
else:
  print("[SKIP] test: errors import (present)")

print("\nTEST follow-through complete.\n")

# ════════════════════ LINT CLEANUP (pylint diff-gate) ══════════════════════
# The rename + tuple edits pulled these lines into the lint diff, exposing
# pre-existing long docstrings (C0301) and missing docstrings (C0116).
# Wrap the long ones; add docstrings to the four flagged methods.

lint_fixes = [
    # (old single-line docstring, new wrapped docstring)
    (
        (
            '    """Tests that Run only calls _RunConcurrentNodePoolOps when'
            ' scenarios=[\'A\']."""'
        ),
        (
            '    """Run dispatches only to _RunConcurrentNodePoolOps for that'
            ' scenario."""'
        ),
    ),
    (
        (
            '    """Tests that Run only calls _RunOverlappingClusterUpdate when'
            ' scenarios=[\'B\']."""'
        ),
        (
            '    """Run dispatches only to _RunOverlappingClusterUpdate for'
            ' that scenario."""'
        ),
    ),
    (
        (
            '    """Tests that Run passes the large-scale-nodepools flag to'
            ' _ScaleToPoolCount."""'
        ),
        (
            '    """Run passes the large-scale-nodepools flag down to'
            ' _ScaleToPoolCount."""'
        ),
    ),
    (
        (
            '  """Tests for the _RunConcurrentNodePoolOps phase-by-phase'
            ' create/delete path."""'
        ),
        '  """Tests the _RunConcurrentNodePoolOps create/delete path."""',
    ),
    (
        (
            '    """Tests _RunConcurrentNodePoolOps passes initial_version to'
            ' CreateNodePoolAsync."""'
        ),
        (
            '    """_RunConcurrentNodePoolOps passes initial_version to'
            ' creates."""'
        ),
    ),
    (
        (
            '    """Tests that _RunConcurrentNodePoolOps deletes only the pools'
            ' it finds at runtime."""'
        ),
        (
            '    """_RunConcurrentNodePoolOps deletes only pools found at'
            ' runtime."""'
        ),
    ),
    (
        (
            '  """Tests for the _RunOverlappingClusterUpdate cluster-update +'
            ' nodepool-create scenario."""'
        ),
        '  """Tests the _RunOverlappingClusterUpdate overlap scenario."""',
    ),
    (
        (
            '    """Tests _RunOverlappingClusterUpdate passes initial_version'
            ' to CreateNodePoolAsync."""'
        ),
        (
            '    """_RunOverlappingClusterUpdate passes initial_version to the'
            ' create."""'
        ),
    ),
]
tt = TEST.read_text()
for old, new in lint_fixes:
  if new in tt:
    print(f"[SKIP] lint wrap: {new.strip()[:45]}")
  elif old in tt:
    tt = tt.replace(old, new, 1)
    print(f"[ok]   lint wrap: {new.strip()[:45]}")
  else:
    raise SystemExit(f"[FAIL] lint wrap anchor missing:\n{old}")
TEST.write_text(tt)

# Add docstrings to the four methods missing them (C0116).
docstring_adds = [
    (
        "  def testSuccessRateMetadataFields(self):\n",
        (
            '  def testSuccessRateMetadataFields(self):\n    """SuccessRate'
            ' sample carries the op-count metadata fields."""\n'
        ),
    ),
    (
        "  def testAggregatesGeneratedForTwoOrMoreSuccesses(self):\n",
        (
            "  def testAggregatesGeneratedForTwoOrMoreSuccesses(self):\n   "
            ' """Aggregate stat samples appear once there are >=2'
            ' successes."""\n'
        ),
    ),
    (
        "  def testOutliersGeneratedForFourOrMoreSuccesses(self):\n",
        (
            "  def testOutliersGeneratedForFourOrMoreSuccesses(self):\n   "
            ' """Outlier-count samples appear once there are >=4'
            ' successes."""\n'
        ),
    ),
    (
        "  def testProducesClusterUpdateAndNodePoolCreateSamples(self):\n",
        (
            "  def testProducesClusterUpdateAndNodePoolCreateSamples(self):\n  "
            '  """Overlap scenario emits both cluster-update and create'
            ' samples."""\n'
        ),
    ),
]
tt = TEST.read_text()
for old, new in docstring_adds:
  # Idempotent: skip if a docstring already follows the def.
  idx = tt.find(old)
  if idx == -1:
    raise SystemExit(f"[FAIL] docstring anchor missing: {old.strip()}")
  after = tt[idx + len(old) : idx + len(old) + 80]
  if after.lstrip().startswith('"""'):
    print(f"[SKIP] docstring add: {old.strip()[:45]}")
    continue
  tt = tt.replace(old, new, 1)
  print(f"[ok]   docstring add: {old.strip()[:45]}")
TEST.write_text(tt)

print("\nLINT cleanup complete.\n")
