#!/usr/bin/env python3
"""Follow-up dead-code removal for PR #6747 (mgmt_plane_gke).

Removes two commented-out code remnants in google_kubernetes_engine.py that a
stricter sweep caught (the first pass only removed '# status = out.strip()'):
  1. An orphaned 5-line `cmd.flags[...]` block stranded after the raise at the
     end of _GetLatestOperationName.
  2. '# describe.flags['format'] = 'value(status)'' in WaitForOperation,
     superseded by the '= 'json'' line below it.
Run from the repo root on branch mgmt_plane_gke. Idempotent.
"""

import pathlib

GKE = pathlib.Path(
    "perfkitbenchmarker/providers/gcp/google_kubernetes_engine.py"
)
src = GKE.read_text()

block1 = (
    "    #     cmd.flags['zone'] = self.zone\n"
    "    #     cmd.flags['filter'] = 'operationType=UPGRADE_NODES AND"
    " status=RUNNING'\n"
    "    #     cmd.flags['sort-by'] = '~startTime'\n"
    "    #     cmd.flags['limit'] = 1\n"
    "    #     cmd.flags['format'] = 'value(name)'\n"
    "\n"
)
block2 = "      # describe.flags['format'] = 'value(status)'\n"

for label, dead in [
    ("orphaned cmd.flags[] block", block1),
    ("describe.flags 'value(status)' leftover", block2),
]:
  if dead in src:
    src = src.replace(dead, "", 1)
    print(f"[ok]   removed {label}")
  else:
    print(f"[SKIP] {label} (already removed)")

GKE.write_text(src)
