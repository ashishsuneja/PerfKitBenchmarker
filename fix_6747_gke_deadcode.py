#!/usr/bin/env python3
"""Idempotent patch for PR #6747: remove leftover commented-out code.

Run from the repo root on branch mgmt_plane_gke. Removes the dead
'# status = out.strip()' line (superseded by the json.loads line below it).
This was NOT yet flagged by Zach; found during the cross-branch dead-code sweep.
"""

import pathlib

GKE = pathlib.Path(
    "perfkitbenchmarker/providers/gcp/google_kubernetes_engine.py"
)
src = GKE.read_text()
dead = "      # status = out.strip()\n"
if dead in src:
  src = src.replace(dead, "", 1)
  GKE.write_text(src)
  print("[ok]   removed leftover '# status = out.strip()'")
else:
  print("[SKIP] already removed")
