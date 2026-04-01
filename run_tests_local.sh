#!/usr/bin/env bash
set -euo pipefail

# Always restore the placeholder on exit — even on failure, ctrl+c, kill
cleanup() {
    sed -i 's/^version = ".*"/version = "__HOLEFINDER_VERSION__"/' pyproject.toml
    echo "[run_tests_local] Restored __HOLEFINDER_VERSION__ placeholder in pyproject.toml"
}
trap cleanup EXIT

# Inject real version from info.yml (single source of truth)
VERSION=$(grep '^version:' info.yml | awk '{print $2}')
sed -i "s/__HOLEFINDER_VERSION__/$VERSION/" pyproject.toml
echo "[run_tests_local] Injected version $VERSION into pyproject.toml"

# Install deps if needed and run tests
uv sync --extra dev
uv run pytest tests/unit/ -v --tb=short "$@"
