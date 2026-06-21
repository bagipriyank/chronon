#!/usr/bin/env bash
#
# Smoke test for `zipline admin init` + `zipline compile`.
#
# Builds the python wheel, installs it into a throwaway venv, then for each
# cloud provider scaffolds a fresh project and compiles it. Exits 0 only if
# every cloud succeeds. Designed to run in CI as a merge gate.
#
# Usage: scripts/ci/scaffold_smoke_test.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLOUDS=("gcp" "aws" "azure")

echo "==> Building zipline wheel via mill"
./mill python.wheel

WHEEL_PATH="$(find out/python/wheel.dest/dist -maxdepth 1 -name 'zipline_ai-*.whl' -type f -print -quit)"
if [[ -z "${WHEEL_PATH:-}" || ! -f "$WHEEL_PATH" ]]; then
    echo "ERROR: could not locate built wheel under out/python/wheel.dest/dist" >&2
    exit 1
fi
echo "    wheel: $WHEEL_PATH"

TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT
echo "==> Scratch dir: $TMP_ROOT"

VENV_DIR="$TMP_ROOT/venv"
echo "==> Creating venv and installing wheel"
python3 -m venv "$VENV_DIR"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet "$WHEEL_PATH"

if ! command -v zipline >/dev/null 2>&1; then
    echo "ERROR: \`zipline\` CLI not found after installing wheel" >&2
    exit 1
fi
echo "    zipline at: $(command -v zipline)"

overall_status=0
for cloud in "${CLOUDS[@]}"; do
    echo
    echo "==> [$cloud] scaffold + compile"
    project_dir="$TMP_ROOT/proj-$cloud"
    mkdir -p "$project_dir"

    # Piping "n" answers the "Add PYTHONPATH to <shell rc>?" prompt so the
    # script never edits the user's / runner's shell config.
    if ! printf 'n\n' | zipline admin init "$cloud" --chronon-root "$project_dir"; then
        echo "    FAILED: zipline admin init $cloud" >&2
        overall_status=1
        continue
    fi

    # `zipline compile` reads teams.py from cwd and walks the chronon root.
    if ! (cd "$project_dir" && zipline compile); then
        echo "    FAILED: zipline compile ($cloud)" >&2
        overall_status=1
        continue
    fi

    echo "    OK: $cloud"
done

deactivate

echo
if [[ $overall_status -ne 0 ]]; then
    echo "==> Scaffold smoke test FAILED"
    exit 1
fi

echo "==> Scaffold smoke test PASSED for all clouds: ${CLOUDS[*]}"
