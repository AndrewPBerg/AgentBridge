#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

command -v task >/dev/null || {
  echo "task is required; run 'mise install' or install Task directly" >&2
  exit 127
}

case "${1:-check}" in
format) task format ;;
check) task quality ;;
lsp) task lsp-quality ;;
*)
  echo 'Usage: scripts/quality.sh [format|check|lsp]' >&2
  exit 2
  ;;
esac
