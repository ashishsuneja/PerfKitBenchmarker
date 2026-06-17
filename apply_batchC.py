#!/usr/bin/env python3
"""Batch C: small Jun-9 review items on kubernetes_management.

  - 18:06 CHANGES.next.md: collapse to one line, drop author credit + the
    abstract-methods detail (reviewer: one line suffices for the PR stack).
  - 155: full attribute descriptions (g3doc) on _OpResult fields; clarify
    init_dur ("initiation") vs e2e_dur ("end-to-end") and rename for clarity.
  - 122: condense the repetitive node-pool-name tests with parameterized cases.

Run on top of Batch B (commit 0e1dba309). Idempotent: [ok]/[SKIP] per edit.
"""

import pathlib

BENCH = pathlib.Path(
    "perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py"
)
TEST = pathlib.Path(
    "tests/linux_benchmarks/kubernetes_management_benchmark_test.py"
)
CHANGES = pathlib.Path("CHANGES.next.md")


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
  if text.count(old) != 1 and required:
    raise SystemExit(f"[FAIL] anchor not unique ({text.count(old)}x): {label}")
  path.write_text(text.replace(old, new, 1))
  print(f"[ok]   {label}")


# ───────────────────────── CHANGES.next.md (18:06) ─────────────────────────
edit(
    CHANGES,
    "-   Add kubernetes_management benchmark for measuring GKE/EKS/AKS"
    " management\n    plane API responsiveness. (from @ashishsuneja)\n-   Add"
    " KubernetesCluster base class management plane abstract methods:\n   "
    " CreateNodePool, DeleteNodePool, UpgradeNodePool, UpdateCluster and\n   "
    " their async counterparts. (from @ashishsuneja)\n",
    "-   Add kubernetes_management benchmark for measuring GKE/EKS/AKS"
    " management\n    plane API responsiveness.\n",
    "CHANGES.next.md: one line, drop author credit + abstract-methods detail",
)

# ───────────────────────── _OpResult g3doc fields (155) ────────────────────
# Rename init_dur->initiation_latency, e2e_dur->end_to_end_latency with
# attribute descriptions, and update all usages.
edit(
    BENCH,
    '@dataclasses.dataclass\nclass _OpResult:\n  """Holds timing and outcome'
    ' for a single async management-plane operation."""\n\n  name: str\n '
    " init_dur: float\n  e2e_dur: float\n  error: Exception | None = None\n\n "
    " def __iter__(self):\n    yield self.name\n    yield self.init_dur\n   "
    " yield self.e2e_dur\n    yield self.error",
    '@dataclasses.dataclass\nclass _OpResult:\n  """Timing and outcome for a'
    " single async management-plane operation.\n\n  Attributes:\n    name:"
    " Node-pool (or operation) name the result is for.\n    initiation_latency:"
    " Seconds from issuing the async API call until it is\n      accepted and"
    " an operation handle is returned (time to *start*).\n   "
    " end_to_end_latency: Seconds from issuing the call until the operation\n  "
    "    fully completes (initiation plus server-side execution).\n    error:"
    ' The exception raised if the operation failed, else None.\n  """\n\n '
    " name: str\n  initiation_latency: float\n  end_to_end_latency: float\n "
    " error: Exception | None = None\n\n  def __iter__(self):\n    yield"
    " self.name\n    yield self.initiation_latency\n    yield"
    " self.end_to_end_latency\n    yield self.error",
    "_OpResult: g3doc field descriptions + clearer names (155)",
)

# Update _OpResult field usages in _OpSamples and _Results.add.
edit(
    BENCH,
    "      success += 1\n"
    "      init_latencies.append(r.init_dur)\n"
    "      e2e_latencies.append(r.e2e_dur)",
    "      success += 1\n"
    "      init_latencies.append(r.initiation_latency)\n"
    "      e2e_latencies.append(r.end_to_end_latency)",
    "_OpSamples: use renamed _OpResult fields",
)
edit(
    BENCH,
    '            r.init_dur,\n            "seconds",\n            dict(meta),\n'
    "        )\n    )\n    samples.append(\n        sample.Sample(\n           "
    ' f"{metric_prefix}_EndToEndLatency", r.e2e_dur, "seconds", dict(meta)',
    "            r.initiation_latency,\n"
    '            "seconds",\n'
    "            dict(meta),\n"
    "        )\n"
    "    )\n"
    "    samples.append(\n"
    "        sample.Sample(\n"
    '            f"{metric_prefix}_EndToEndLatency",\n'
    "            r.end_to_end_latency,\n"
    '            "seconds",\n'
    "            dict(meta),",
    "_OpSamples: per-op sample uses renamed fields",
)

print("\nBENCH edits complete.\n")

# ───────────────────────── parameterized name tests (122) ──────────────────
# Replace the six repetitive name tests + the stale class docstring with two
# parameterized tests. Keep testAllNamesWithinAksLimit + the B-name test.
old_block = '''class ScenarioNameTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _SCENARIO_A_NAME, _OVERLAPPING_POOL_NAME, _SCENARIO_C_NAME."""

  def testScenarioANameZeroPadsToThreeDigits(self):
    self.assertEqual(
        'pkbma000',
        kubernetes_management_benchmark._ConcurrentPoolName(0),
    )

  def testScenarioANameTwoDigitIndex(self):
    self.assertEqual(
        'pkbma042',
        kubernetes_management_benchmark._ConcurrentPoolName(42),
    )

  def testScenarioANameMaxThreeDigits(self):
    self.assertEqual(
        'pkbma999',
        kubernetes_management_benchmark._ConcurrentPoolName(999),
    )

  def testScenarioBNameIsConstant(self):
    self.assertEqual(
        'pkbmb',
        kubernetes_management_benchmark._OVERLAPPING_POOL_NAME,
    )

  def testScenarioCNameZeroPadsToFourDigits(self):
    self.assertEqual(
        'pkbmc0000',
        kubernetes_management_benchmark._ScalePoolName(0),
    )

  def testScenarioCNameSingleDigitIndex(self):
    self.assertEqual(
        'pkbmc0007',
        kubernetes_management_benchmark._ScalePoolName(7),
    )

  def testScenarioCNameFourDigitIndex(self):
    self.assertEqual(
        'pkbmc1000',
        kubernetes_management_benchmark._ScalePoolName(1000),
    )

  def testAllNamesWithinAksLimit(self):'''

new_block = '''class NodePoolNameTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the node-pool name-generation helpers."""

  @parameterized.named_parameters(
      ('zero', 0, 'pkbma000'),
      ('two_digit', 42, 'pkbma042'),
      ('max_three_digit', 999, 'pkbma999'),
  )
  def testConcurrentPoolNameZeroPadsToThreeDigits(self, index, expected):
    self.assertEqual(
        expected, kubernetes_management_benchmark._ConcurrentPoolName(index)
    )

  @parameterized.named_parameters(
      ('zero', 0, 'pkbmc0000'),
      ('single_digit', 7, 'pkbmc0007'),
      ('four_digit', 1000, 'pkbmc1000'),
  )
  def testScalePoolNameZeroPadsToFourDigits(self, index, expected):
    self.assertEqual(
        expected, kubernetes_management_benchmark._ScalePoolName(index)
    )

  def testOverlappingPoolNameIsConstant(self):
    self.assertEqual(
        'pkbmb', kubernetes_management_benchmark._OVERLAPPING_POOL_NAME
    )

  def testAllNamesWithinAksLimit(self):'''

edit(TEST, old_block, new_block, "test: parameterize name tests (122)")

# Ensure parameterized is imported.
edit(
    TEST,
    "from absl.testing import flagsaver",
    "from absl.testing import flagsaver\nfrom absl.testing import"
    " parameterized",
    "test: import parameterized",
    required=False,
)

print("\nTEST edits complete.\n")
