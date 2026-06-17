#!/usr/bin/env python3
"""Idempotent patch for PR #6746 review comments 1, 4, 6 (Zach / hubatish).

Run from the PerfKitBenchmarker repo root on branch mgmt_plane_benchmark_base.
  - Comment 1: zone & machine type are "detail for a design doc" and are
               overridden at call time, so don't hardcode them. Replace the
               per-cloud block with the shared `*default_dual_core` anchor
               (configs/default_config_constants.yaml) — the idiomatic pattern
               used by provision_node_pools and other kubernetes benchmarks.
               It's the same 2-vCPU/8GB tier this benchmark intended, and more
               cross-cloud-consistent than the old hand-picked types (the old
               AWS t3.medium was 4GB, a mismatch with the 8GB GCP/Azure picks).
  - Comment 4: Prepare() assert is type-narrowing for pytype, not a
               reachability probe. Fix the misleading docstring + annotate.
  - Comment 6: remove the commented-out "Synchronization Barrier" dead block.
Safe to re-run: each edit is skipped if already applied.
"""

import re
import pathlib

BENCH = pathlib.Path(
    "perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py"
)


def edit(text, old, new, label):
  if old in text:
    print(f"[ok]   {label}")
    return text.replace(old, new, 1)
  if new in text:
    print(f"[SKIP] {label} (already applied)")
    return text
  print(f"[WARN] {label}: anchor not found — review manually")
  return text


src = BENCH.read_text()

# ── Comment 1: don't hardcode zone/machine_type; use shared default anchor ──
old_cfg = '''BENCHMARK_CONFIG = """
kubernetes_management:
  description: >
    Benchmarks GKE/EKS/AKS management plane operations: concurrent node pool
    create/upgrade/delete, overlapping cluster + node-pool ops, and large-scale
    provisioning. Focused on control-plane API responsiveness.
    Spec regions: GCP us-central1, AWS us-east-1 (closest), Azure eastus.
    Equivalent machine types across clouds per Google benchmark spec.
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        # us-central1-a: spec primary region for GCP
        # e2-standard-2: 2 vCPU 8GB \u2014 equivalent to t3.medium / D2s_v3
        machine_type: e2-standard-2
        zone: us-central1-a
      AWS:
        # us-east-1a: closest comparable region to GCP us-central1
        # t3.medium: 2 vCPU 4GB \u2014 closest equivalent to e2-standard-2
        machine_type: t3.medium
        zone: us-east-1a
      Azure:
        # eastus: closest comparable region to GCP us-central1
        # Standard_D2s_v3: 2 vCPU 8GB \u2014 equivalent to e2-standard-2
        machine_type: Standard_D2s_v3
        zone: eastus
"""'''
new_cfg = '''BENCHMARK_CONFIG = """
kubernetes_management:
  description: >
    Benchmarks GKE/EKS/AKS management plane operations: concurrent node pool
    create/upgrade/delete, overlapping cluster + node-pool ops, and large-scale
    provisioning. Focused on control-plane API responsiveness.
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
"""'''
src = edit(src, old_cfg, new_cfg, "Comment 1: use *default_dual_core anchor")

# ── Comment 4: Prepare() docstring + assert annotation ──
old_prep = (
    '  """Asserts the cluster is reachable; deploys spec-defined sleep'
    ' workload."""\n'
    "  cluster = benchmark_spec.container_cluster\n"
    "  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)\n"
)
new_prep = (
    '  """Deploys a sleep pod to confirm data-plane reachability."""\n'
    "  cluster = benchmark_spec.container_cluster\n"
    "  # Type narrowing for pytype; reachability is confirmed by the sleep pod"
    " below.\n"
    "  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)\n"
)
src = edit(
    src, old_prep, new_prep, "Comment 4: Prepare docstring + assert note"
)

# ── Comment 6: remove commented-out Synchronization Barrier dead block ──
barrier = re.compile(
    r"\n\n[ \t]*# # \u2500\u2500 Idiomatic Control Plane Synchronization"
    r" Barrier"
    r".*?#   time\.sleep\(5\)\n",
    re.DOTALL,
)
if barrier.search(src):
  src = barrier.sub("\n", src)
  print("[ok]   Comment 6: removed Synchronization Barrier dead block")
elif "Idiomatic Control Plane Synchronization Barrier" not in src:
  print("[SKIP] Comment 6 (already applied)")
else:
  print("[WARN] Comment 6: pattern not matched — review manually")

BENCH.write_text(src)
print("\nDone.")
