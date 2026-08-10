#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT

"${CC:-gcc}" \
    -std=c99 \
    -Wall -Wextra -Werror \
    -fsanitize=address,undefined \
    -fno-omit-frame-pointer \
    -I"$repo_root" \
    "$repo_root/tests/test_safe_input_boundaries.c" \
    -o "$build_dir/test_safe_input_boundaries"

ASAN_OPTIONS=detect_leaks=1 "$build_dir/test_safe_input_boundaries"
