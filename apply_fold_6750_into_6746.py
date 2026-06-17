#!/usr/bin/env python3
"""Fold the AddNodepool refactor (#6750) into #6746.

Removes the BaseEksCluster.AddNodepool no-op `pass` stub so EKS inherits the
base KubernetesCluster.AddNodepool -> CreateNodePool delegation. This brings
the complete Add/CreateNodepool refactor into #6746 (the PR under review),
satisfying the "refactor at front of chain" ask, and lets #6750 be closed.

The EksKarpenterCluster.AddNodepool override (manifest apply) is unaffected —
that's the path the existing provision_node_pools benchmark uses.

Run on mgmt_plane_benchmark_base. Idempotent: [ok]/[SKIP].
"""

import pathlib

EKS = pathlib.Path(
    "perfkitbenchmarker/providers/aws/elastic_kubernetes_service.py"
)

old = (
    "    nodegroups = json.loads(stdout)\n"
    "    return [ng['Name'] for ng in nodegroups]\n"
    "\n"
    "  def AddNodepool(self, batch_name, pool_id):\n"
    "    pass\n"
    "\n"
)
new = (
    "    nodegroups = json.loads(stdout)\n"
    "    return [ng['Name'] for ng in nodegroups]\n"
    "\n"
)

text = EKS.read_text()
if "def AddNodepool(self, batch_name, pool_id):\n    pass" not in text:
  print("[SKIP] BaseEksCluster.AddNodepool stub already removed")
elif old in text:
  EKS.write_text(text.replace(old, new, 1))
  print("[ok]   removed BaseEksCluster.AddNodepool no-op stub")
else:
  raise SystemExit("[FAIL] stub present but surrounding anchor changed")

print("\nDone.\n")
