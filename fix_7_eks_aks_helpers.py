#!/usr/bin/env python3
"""PR #6748 (EKS) / #6749 (AKS): reuse version helpers in ResolveNodePoolVersions.

Addresses the spirit of Zach's comment #7: the EKS and AKS overrides duplicate
the bare-minor parse inline instead of using the existing BareMinor /
AdjacentMinorBelow helpers in kubernetes_cluster. This refactor calls the
helpers (behaviour-equivalent, with validation), removing the duplication while
keeping the provider override (still needed; GKE's override is not removable).

Run from the repo root on branch mgmt_plane_eks, then again on mgmt_plane_aks.
The script only touches the override present on the current branch. Idempotent.
"""

import pathlib

EKS = pathlib.Path(
    "perfkitbenchmarker/providers/aws/elastic_kubernetes_service.py"
)
AKS = pathlib.Path(
    "perfkitbenchmarker/providers/azure/azure_kubernetes_service.py"
)

EKS_OLD = (
    "    cluster_ver = self.cluster_version or self.k8s_version\n    # Strip"
    " any patch suffix e.g. '1.34.7' -> '1.34'\n    parts ="
    " cluster_ver.lstrip('v').split('.')\n    major, minor = int(parts[0]),"
    " int(parts[1])\n    target = f'{major}.{minor}'\n    initial ="
    " f'{major}.{minor - 1}'\n    logging.info(\n        '[EKS]"
    " ResolveNodePoolVersions: cluster=%s initial=%s target=%s',\n"
)
EKS_NEW = (
    "    cluster_ver = self.cluster_version or self.k8s_version\n    target ="
    " kubernetes_cluster.BareMinor(cluster_ver)\n    initial ="
    " kubernetes_cluster.AdjacentMinorBelow(cluster_ver)\n    logging.info(\n  "
    "      '[EKS] ResolveNodePoolVersions: cluster=%s initial=%s target=%s',\n"
)
AKS_OLD = (
    "    cluster_ver = self.cluster_version or self.k8s_version\n    parts ="
    " cluster_ver.lstrip('v').split('.')\n    major, minor = int(parts[0]),"
    " int(parts[1])\n    target = f'{major}.{minor}'\n    initial ="
    " f'{major}.{minor - 1}'\n    logging.info(\n        '[AKS]"
    " ResolveNodePoolVersions: cluster=%s initial=%s target=%s',\n"
)
AKS_NEW = (
    "    cluster_ver = self.cluster_version or self.k8s_version\n    target ="
    " kubernetes_cluster.BareMinor(cluster_ver)\n    initial ="
    " kubernetes_cluster.AdjacentMinorBelow(cluster_ver)\n    logging.info(\n  "
    "      '[AKS] ResolveNodePoolVersions: cluster=%s initial=%s target=%s',\n"
)


def apply(path, old, new, marker, label):
  # marker = the override's log tag; if absent, the override isn't on this
  # branch, so stay silent rather than warn about an anchor that never existed.
  if not path.exists():
    return
  src = path.read_text()
  if new in src:
    print(f"[SKIP] {label} (already applied)")
  elif old in src:
    path.write_text(src.replace(old, new, 1))
    print(f"[ok]   {label}")
  elif marker in src:
    print(f"[WARN] {label}: code changed since patch was written — review")


apply(
    EKS,
    EKS_OLD,
    EKS_NEW,
    "'[EKS] ResolveNodePoolVersions",
    "EKS ResolveNodePoolVersions -> helpers",
)
apply(
    AKS,
    AKS_OLD,
    AKS_NEW,
    "'[AKS] ResolveNodePoolVersions",
    "AKS ResolveNodePoolVersions -> helpers",
)
