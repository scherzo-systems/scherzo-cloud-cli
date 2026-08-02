#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture=$(mktemp -d)
cleanup() {
  rm -rf "$fixture"
}
trap cleanup EXIT

mkdir -p "$fixture/scripts" "$fixture/src" "$fixture/tests/fixtures/lint-policy"
cp "$root/Cargo.toml" "$root/Cargo.lock" "$root/clippy.toml" "$fixture/"
cp "$root/scripts/check-lint-policy" "$fixture/scripts/"
cp "$root/tests/fixtures/lint-policy/"*.rs "$fixture/tests/fixtures/lint-policy/"

cat >"$fixture/src/main.rs" <<'RS'
#[expect(
    clippy::disallowed_methods,
    reason = "a module-wide expectation must not bypass the deterministic timing policy"
)]
mod ordinary;

fn main() {
    ordinary::timing_dependent_synchronization();
}
RS
cat >"$fixture/src/ordinary.rs" <<'RS'
pub(crate) fn timing_dependent_synchronization() {
    std::thread::sleep(std::time::Duration::from_millis(1));
}
RS

target_dir=${CARGO_TARGET_DIR:-$root/target}
CARGO_TARGET_DIR="$target_dir" cargo clippy \
  --quiet \
  --locked \
  --manifest-path "$fixture/Cargo.toml" \
  --all-targets \
  --all-features

set +e
output=$(CARGO_TARGET_DIR="$target_dir" "$fixture/scripts/check-lint-policy" 2>&1)
status=$?
set -e
if ((status == 0)); then
  printf 'lint-policy check accepted a module-wide expectation and direct sleep:\n%s\n' "$output" >&2
  exit 1
fi
printf 'lint-policy check rejected the fixture as expected:\n%s\n' "$output"
