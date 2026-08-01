#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT

cc -std=c99 -Wall -Wextra -Werror \
    -I"$repo_dir" \
    "$repo_dir/tests/test_safe_metadata_scan.c" \
    -o "$build_dir/test_safe_metadata_scan"

"$build_dir/test_safe_metadata_scan"
