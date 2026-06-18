#!/usr/bin/env python3
"""Script to split kubernetes management plane PRs into -A (approvable) and
-B (create nodepool, to be discussed) versions.

Run from the root of the PerfKitBenchmarker repository:
  python split_prs.py

For each affected file it writes:
  <original_file>.split_A  — the version to keep in the current PR
  <original_file>.split_B  — the methods to move to the new PR
"""

import ast
import os
import re
import textwrap

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def read_file(path):
  with open(path, 'r', encoding='utf-8') as f:
    return f.read()


def write_file(path, content):
  with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
  print(f'  Written: {path}')


def extract_method_source(source, method_names):
  """Extracts the full source text of named methods from a class body.

  Uses the AST to find exact line ranges, then slices the original source
  so decorators, docstrings and blank lines are preserved exactly.

  Returns:
    (kept_source, extracted_source) where extracted_source contains the
    removed methods and kept_source has them replaced with a one-line
    comment placeholder.
  """
  lines = source.splitlines(keepends=True)
  tree = ast.parse(source)

  # Collect (start_line, end_line) for each target method (1-indexed).
  ranges_to_remove = []  # list of (start, end) 1-indexed inclusive

  for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
      if node.name in method_names:
        # Find decorator start if present
        start = node.lineno
        if node.decorator_list:
          start = node.decorator_list[0].lineno
        # end_lineno available in Python 3.8+
        end = node.end_lineno
        ranges_to_remove.append((start, end))

  if not ranges_to_remove:
    print(f'  WARNING: None of {method_names} found in source.')
    return source, ''

  # Sort by start line
  ranges_to_remove.sort()

  # Build kept and extracted line sets
  remove_set = set()
  for start, end in ranges_to_remove:
    for i in range(start, end + 1):
      remove_set.add(i)

  kept_lines = []
  extracted_lines = []
  i = 1
  placeholder_inserted = False
  while i <= len(lines):
    if i in remove_set:
      extracted_lines.append(lines[i - 1])
      # Check if this is the first line of a removed block and insert
      # placeholder comment before it (only once)
      if not placeholder_inserted and i == ranges_to_remove[0][0]:
        # Detect indentation from the line being removed
        indent = len(lines[i - 1]) - len(lines[i - 1].lstrip())
        placeholder = ' ' * indent + (
            '# CreateNodePool / CreateNodePoolAsync intentionally omitted —\n'
            + ' ' * indent
            + '# split to a separate PR for discussion.\n'
        )
        kept_lines.append(placeholder)
        placeholder_inserted = True
    else:
      kept_lines.append(lines[i - 1])
    i += 1

  return ''.join(kept_lines), ''.join(extracted_lines)


def extract_functions_toplevel(source, func_names):
  """Like extract_method_source but for module-level functions."""
  return extract_method_source(source, func_names)


def extract_flag_definitions(source, flag_var_names):
  """Removes DEFINE_* flag assignments by variable name.

  Matches patterns like:
    _MYFLAG = flags.DEFINE_integer(
        ...
    )
  using a simple brace-depth counter so multi-line calls are handled.

  Returns (kept_source, extracted_source).
  """
  lines = source.splitlines(keepends=True)
  # Build a map: line_index (0-based) -> flag_var_name if that line starts
  # the flag definition.
  flag_start = {}
  for idx, line in enumerate(lines):
    for name in flag_var_names:
      if re.match(rf'^\s*{re.escape(name)}\s*=\s*flags\.DEFINE_', line):
        flag_start[idx] = name
        break

  if not flag_start:
    print(f'  WARNING: None of flag vars {flag_var_names} found.')
    return source, ''

  # For each flag start, walk forward until the parentheses balance.
  ranges_to_remove = []  # (start_idx, end_idx) inclusive 0-based
  for start_idx in sorted(flag_start):
    depth = 0
    end_idx = start_idx
    for j in range(start_idx, len(lines)):
      depth += lines[j].count('(') - lines[j].count(')')
      end_idx = j
      if depth <= 0:
        break
    # Include trailing blank line if present
    if end_idx + 1 < len(lines) and lines[end_idx + 1].strip() == '':
      end_idx += 1
    ranges_to_remove.append((start_idx, end_idx))

  remove_set = set()
  for s, e in ranges_to_remove:
    for i in range(s, e + 1):
      remove_set.add(i)

  kept_lines = []
  extracted_lines = []
  placeholder_inserted = False
  for idx, line in enumerate(lines):
    if idx in remove_set:
      extracted_lines.append(line)
      if not placeholder_inserted and idx == ranges_to_remove[0][0]:
        kept_lines.append(
            '# CreateNodePool-related flags intentionally omitted —\n'
            '# split to a separate PR for discussion.\n'
        )
        placeholder_inserted = True
    else:
      kept_lines.append(line)

  return ''.join(kept_lines), ''.join(extracted_lines)


# ---------------------------------------------------------------------------
# Per-file split functions
# ---------------------------------------------------------------------------


def split_kubernetes_cluster(path):
  """Splits kubernetes_cluster.py: removes CreateNodePool/Async."""
  print(f'\nSplitting {path}')
  source = read_file(path)

  methods_to_move = {'CreateNodePool', 'CreateNodePoolAsync'}
  kept, extracted = extract_method_source(source, methods_to_move)

  write_file(path + '.split_A', kept)
  write_file(
      path + '.split_B',
      _make_split_b_header(
          'kubernetes_cluster.py',
          'CreateNodePool / CreateNodePoolAsync base stubs',
      )
      + extracted,
  )
  _print_summary('kubernetes_cluster.py', methods_to_move)


def split_gke(path):
  """Splits google_kubernetes_engine.py: removes CreateNodePoolAsync."""
  print(f'\nSplitting {path}')
  source = read_file(path)

  methods_to_move = {'CreateNodePoolAsync'}
  kept, extracted = extract_method_source(source, methods_to_move)

  write_file(path + '.split_A', kept)
  write_file(
      path + '.split_B',
      _make_split_b_header(
          'google_kubernetes_engine.py',
          'GKE CreateNodePoolAsync',
      )
      + extracted,
  )
  _print_summary('google_kubernetes_engine.py', methods_to_move)


def split_eks(path):
  """Splits elastic_kubernetes_service.py: removes CreateNodePoolAsync
  and its four helper methods."""
  print(f'\nSplitting {path}')
  source = read_file(path)

  methods_to_move = {
      'CreateNodePoolAsync',
      '_DiscoverSubnets',
      '_DiscoverSubnetsPerAZ',
      '_ResolveReleaseVersion',
      '_DiscoverNodeRoleArn',
  }
  kept, extracted = extract_method_source(source, methods_to_move)

  write_file(path + '.split_A', kept)
  write_file(
      path + '.split_B',
      _make_split_b_header(
          'elastic_kubernetes_service.py',
          'EKS CreateNodePoolAsync + helper methods',
      )
      + extracted,
  )
  _print_summary('elastic_kubernetes_service.py', methods_to_move)


def split_aks(path):
  """Splits azure_kubernetes_service.py: removes CreateNodePool (sync)
  and CreateNodePoolAsync."""
  print(f'\nSplitting {path}')
  source = read_file(path)

  methods_to_move = {'CreateNodePool', 'CreateNodePoolAsync'}
  kept, extracted = extract_method_source(source, methods_to_move)

  write_file(path + '.split_A', kept)
  write_file(
      path + '.split_B',
      _make_split_b_header(
          'azure_kubernetes_service.py',
          'AKS CreateNodePool (sync) + CreateNodePoolAsync',
      )
      + extracted,
  )
  _print_summary('azure_kubernetes_service.py', methods_to_move)


def split_benchmark_6746(path):
  """Splits kubernetes_management_benchmark.py for PR #6746:
  removes scenario flag definitions and scenario implementation functions,
  leaving only scaffolding/helpers."""
  print(f'\nSplitting benchmark (PR #6746) {path}')
  source = read_file(path)

  # Step 1: remove scenario-specific flags
  flags_to_move = [
      '_CONCURRENT_NODEPOOLS',
      '_INITIAL_VERSION',
      '_LARGE_SCALE_NODEPOOLS',
      '_SCALE_SWEEP',
  ]
  kept_after_flags, extracted_flags = extract_flag_definitions(
      source, flags_to_move
  )

  # Step 2: remove scenario implementation functions
  funcs_to_move = {
      'Run',
      '_RunConcurrentNodePoolOps',
      '_RunOverlappingClusterUpdate',
      '_ScaleToPoolCount',
      '_SweepScales',
  }
  kept_final, extracted_funcs = extract_functions_toplevel(
      kept_after_flags, funcs_to_move
  )

  write_file(path + '.split_A', kept_final)
  write_file(
      path + '.split_B',
      _make_split_b_header(
          'kubernetes_management_benchmark.py (PR #6746)',
          'Scenario flags + Run() + scenario implementations',
      )
      + extracted_flags
      + '\n\n'
      + extracted_funcs,
  )
  _print_summary(
      'kubernetes_management_benchmark.py (#6746)',
      flags_to_move + list(funcs_to_move),
  )


def split_benchmark_6751(path):
  """Splits kubernetes_management_benchmark.py for PR #6751:
  removes scenario implementation functions (A/B/C + pipelined),
  leaving scaffolding/helpers."""
  print(f'\nSplitting benchmark (PR #6751) {path}')
  source = read_file(path)

  # Step 1: remove _PIPELINE_SCENARIO_A flag
  flags_to_move = ['_PIPELINE_SCENARIO_A']
  kept_after_flags, extracted_flags = extract_flag_definitions(
      source, flags_to_move
  )

  # Step 2: remove scenario functions
  funcs_to_move = {
      'Run',
      '_RunScenarioA',
      '_RunScenarioAPipelined',
      '_RunScenarioB',
      '_RunScenarioC',
  }
  kept_final, extracted_funcs = extract_functions_toplevel(
      kept_after_flags, funcs_to_move
  )

  write_file(path + '.split_A', kept_final)
  write_file(
      path + '.split_B',
      _make_split_b_header(
          'kubernetes_management_benchmark.py (PR #6751)',
          '_PIPELINE_SCENARIO_A flag + Run() + Scenario A/B/C implementations',
      )
      + extracted_flags
      + '\n\n'
      + extracted_funcs,
  )
  _print_summary(
      'kubernetes_management_benchmark.py (#6751)',
      flags_to_move + list(funcs_to_move),
  )


def split_kubernetes_cluster_6751(path):
  """Splits kubernetes_cluster.py for PR #6751:
  removes the CreateNodePool sync wrapper (the one that calls
  CreateNodePoolAsync + WaitForOperation)."""
  print(f'\nSplitting kubernetes_cluster.py (PR #6751) {path}')
  source = read_file(path)

  methods_to_move = {'CreateNodePool'}
  kept, extracted = extract_method_source(source, methods_to_move)

  write_file(path + '.split_A', kept)
  write_file(
      path + '.split_B',
      _make_split_b_header(
          'kubernetes_cluster.py (PR #6751)',
          'CreateNodePool sync wrapper (calls CreateNodePoolAsync +'
          ' WaitForOperation)',
      )
      + extracted,
  )
  _print_summary('kubernetes_cluster.py (#6751)', methods_to_move)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _make_split_b_header(filename, description):
  border = '#' * 72
  return textwrap.dedent(f"""\
        {border}
        # SPLIT-B: {filename}
        # Description: {description}
        #
        # These methods have been split out from the main PR for separate
        # discussion (per hubatish's review comment about splitting the
        # create-nodepools scenario).
        #
        # To apply: copy these methods back into the appropriate class/file
        # in the new -B PR branch.
        {border}

        """)


def _print_summary(filename, items):
  print(f'  Removed from {filename}:')
  for item in sorted(items):
    print(f'    - {item}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

FILES = {
    # PR #6746
    '6746_kubernetes_cluster': (
        'perfkitbenchmarker/resources/container_service/kubernetes_cluster.py',
        split_kubernetes_cluster,
    ),
    '6746_benchmark': (
        'perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py',
        split_benchmark_6746,
    ),
    # PR #6747
    '6747_gke': (
        'perfkitbenchmarker/providers/gcp/google_kubernetes_engine.py',
        split_gke,
    ),
    # PR #6748
    '6748_eks': (
        'perfkitbenchmarker/providers/aws/elastic_kubernetes_service.py',
        split_eks,
    ),
    # PR #6749
    '6749_aks': (
        'perfkitbenchmarker/providers/azure/azure_kubernetes_service.py',
        split_aks,
    ),
    # PR #6751
    '6751_kubernetes_cluster': (
        'perfkitbenchmarker/resources/container_service/kubernetes_cluster.py',
        split_kubernetes_cluster_6751,
    ),
    '6751_benchmark': (
        'perfkitbenchmarker/linux_benchmarks/kubernetes_management_benchmark.py',
        split_benchmark_6751,
    ),
}


def main():
  print('=' * 72)
  print('PerfKitBenchmarker PR Split Script')
  print('Splitting CreateNodePool out of management plane PRs')
  print('=' * 72)

  # Check we are in the right directory
  if not os.path.exists('perfkitbenchmarker'):
    print('\nERROR: Run this script from the PerfKitBenchmarker root dir.')
    return

  import argparse

  parser = argparse.ArgumentParser(
      description='Split kubernetes management plane PRs'
  )
  parser.add_argument(
      '--pr',
      choices=list(FILES.keys()) + ['all'],
      default='all',
      help='Which PR/file to split. Default: all',
  )
  args = parser.parse_args()

  targets = FILES.items() if args.pr == 'all' else [(args.pr, FILES[args.pr])]

  results = []
  for key, (filepath, split_fn) in targets:
    if not os.path.exists(filepath):
      print(f'\nSKIPPED ({key}): {filepath} not found.')
      print('  -> You may be on a branch that does not include this PR yet.')
      results.append((key, filepath, 'SKIPPED'))
      continue
    try:
      split_fn(filepath)
      results.append((key, filepath, 'OK'))
    except Exception as e:
      print(f'\nERROR splitting {filepath}: {e}')
      results.append((key, filepath, f'ERROR: {e}'))

  print('\n' + '=' * 72)
  print('SUMMARY')
  print('=' * 72)
  for key, filepath, status in results:
    print(f'  [{status:8s}] {key}: {filepath}')
    if status == 'OK':
      print(f'             -> {filepath}.split_A  (keep in current PR)')
      print(f'             -> {filepath}.split_B  (move to new -B PR)')

  print('\nNext steps:')
  print('  1. Review each .split_A file — this replaces the original')
  print('  2. Review each .split_B file — these methods go into new -B PRs')
  print('  3. On your -A branch: cp <file>.split_A <file>')
  print('  4. On your -B branch: add the .split_B methods back to the file')
  print('  5. Run tests to verify both splits compile correctly:')
  print(
      '     python -m pytest tests/linux_benchmarks/'
      'kubernetes_management_benchmark_test.py -v'
  )
  print(
      '     python -m pytest tests/providers/gcp/'
      'google_kubernetes_engine_test.py -v'
  )


if __name__ == '__main__':
  main()
