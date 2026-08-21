#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source_dir="$root/packages/pi-extension"
target_dir="${PI_HOME:-$HOME/.pi}/agent/extensions/agent-bridge"
mkdir -p "$target_dir"
for file in client.ts git.ts herdr.ts index.ts intent.ts jj.ts protocol.ts README.md; do
  install -m 0644 "$source_dir/$file" "$target_dir/$file"
done
printf 'Installed Agent Bridge Pi adapter to %s\n' "$target_dir"
