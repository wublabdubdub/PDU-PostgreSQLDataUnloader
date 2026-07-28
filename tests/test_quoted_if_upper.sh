#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_dir="$(mktemp -d /isoTest/opensourcexman-quoted-test.XXXXXX)"

cleanup()
{
    case "$test_dir" in
        /isoTest/opensourcexman-quoted-test.*)
            rm -rf -- "$test_dir"
            ;;
    esac
}
trap cleanup EXIT

gcc -std=c99 -g -Wall -Wextra \
    -Wno-unused-parameter -Wno-sign-compare \
    -Wno-missing-field-initializers -Wno-unused-variable \
    -Wno-unused-function -Wno-unused-but-set-variable \
    -ffunction-sections -fdata-sections \
    -I"$repo_dir" -c "$repo_dir/tools.c" -o "$test_dir/tools.o"

gcc -std=c99 -g -Wall -Wextra \
    -ffunction-sections -fdata-sections \
    "$repo_dir/tests/test_quoted_if_upper.c" "$test_dir/tools.o" \
    -Wl,--gc-sections -lm -lz -ldl -llz4 -lpthread \
    -o "$test_dir/test_quoted_if_upper"

"$test_dir/test_quoted_if_upper"
