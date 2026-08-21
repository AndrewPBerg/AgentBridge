#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source_dir="$root/packages/pi-extension"
target_dir="${PI_HOME:-$HOME/.pi}/agent/extensions/agent-bridge"
mkdir -p "$target_dir"
for file in client.ts git.ts herdr.ts index.ts intent.ts jj.ts provenance.ts protocol.ts talk-modal.ts README.md; do
  install -m 0644 "$source_dir/$file" "$target_dir/$file"
done
for file in "$source_dir"/*.test.ts; do
  install -m 0644 "$file" "$target_dir/$(basename "$file")"
done
mkdir -p "$target_dir/test/mocks"
install -m 0644 "$source_dir/test/mocks/pi-coding-agent.ts" "$target_dir/test/mocks/pi-coding-agent.ts"
install -m 0644 "$source_dir/test/mocks/typebox.ts" "$target_dir/test/mocks/typebox.ts"
install -m 0644 "$source_dir/test/mocks/pi-tui.ts" "$target_dir/test/mocks/pi-tui.ts"
skill_source="$root/skills/agent-bridge"
skill_target="${PI_HOME:-$HOME/.pi}/agent/skills/agent-bridge"
mkdir -p "$skill_target/references"
install -m 0644 "$skill_source/SKILL.md" "$skill_target/SKILL.md"
install -m 0644 "$skill_source/references/provenance.md" "$skill_target/references/provenance.md"
printf 'Installed Agent Bridge Pi adapter, tests, and skill to %s\n' "$target_dir"
