#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="${HOME}/flutter/bin:${PATH}"
cd "$ROOT"
flutter_rust_bridge_codegen generate
cargo test
cd app && flutter run "$@"
