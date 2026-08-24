#!/usr/bin/env bash
set -euo pipefail
umask 077
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source_dir="$root/packages/pi-extension"
skill_source="$root/skills/agent-bridge"
pi_root="${PI_HOME:-$HOME/.pi}"
target_dir="$pi_root/agent/extensions/agent-bridge"
skill_target="$pi_root/agent/skills/agent-bridge"
marker='.agent-bridge-deployment'
files=(client.ts git.ts herdr.ts index.ts intent.ts jj.ts provenance.ts protocol.ts talk-modal.ts README.md)
skill_files=(SKILL.md references/provenance.md)
test_files=(client.test.ts git.test.ts index.test.ts install-pi-extension.test.ts intent.test.ts jj.test.ts pi-events.test.ts provenance.test.ts talk-modal.test.ts)
mock_files=(test/mocks/pi-coding-agent.ts test/mocks/pi-ai.ts test/mocks/typebox.ts test/mocks/pi-tui.ts)
ext_files=("${files[@]}" "${test_files[@]}" "${mock_files[@]}")

hash_files() {
  local base=$1; shift
  (cd "$base" && sha256sum "$@" | sha256sum | awk '{print $1}')
}
source_digest() { hash_files "$root" "${ext_files[@]/#/packages/pi-extension/}" "${skill_files[@]/#/skills/agent-bridge/}"; }
check_tree() {
  local base=$1 expected=$2; shift 2
  local f
  for f in "$@"; do [[ -f "$base/$f" ]] || return 1; done
  [[ -f "$base/$marker" ]] || return 1
  [[ "$(<"$base/$marker")" == "agent-bridge-deployment-v1 $digest" ]] || return 1
  [[ "$(hash_files "$base" "$@")" == "$expected" ]]
}

digest=$(source_digest)
ext_digest=$(hash_files "$source_dir" "${ext_files[@]}")
skill_digest=$(hash_files "$skill_source" "${skill_files[@]}")
if [[ "${1:-}" == "--check" ]]; then
  [[ $# -eq 1 ]] || { echo 'usage: install-pi-extension.sh [--check]' >&2; exit 2; }
  check_tree "$target_dir" "$ext_digest" "${ext_files[@]}" || { echo "Pi extension is missing or drifted: $target_dir" >&2; exit 1; }
  check_tree "$skill_target" "$skill_digest" "${skill_files[@]}" || { echo "Agent Bridge skill is missing or drifted: $skill_target" >&2; exit 1; }
  echo "Agent Bridge Pi deployment verified ($digest)."
  exit 0
fi
[[ $# -eq 0 ]] || { echo 'usage: install-pi-extension.sh [--check]' >&2; exit 2; }

parent=$(dirname "$target_dir")
skill_parent=$(dirname "$skill_target")
mkdir -p "$pi_root"
stage=$(mktemp -d "${pi_root}/.agent-bridge-install.XXXXXX")
backup=$(mktemp -d "${pi_root}/.agent-bridge-backup.XXXXXX")
backed_ext=0
backed_skill=0
installed_ext=0
installed_skill=0
rollback() {
  local code=$?
  if (( code != 0 )); then
    (( installed_skill )) && rm -rf "$skill_target" || true
    (( backed_skill )) && mv -T "$backup/skill" "$skill_target" || true
    (( installed_ext )) && rm -rf "$target_dir" || true
    (( backed_ext )) && mv -T "$backup/ext" "$target_dir" || true
  fi
  rm -rf "$stage" "$backup"
  return "$code"
}
trap rollback EXIT
mkdir -p "$stage/ext/test/mocks" "$stage/skill/references" "$parent" "$skill_parent"
# Preserve unrelated files in an existing installation while replacing the managed payload.
[[ -d "$target_dir" ]] && cp -a "$target_dir"/. "$stage/ext"/
[[ -d "$skill_target" ]] && cp -a "$skill_target"/. "$stage/skill"/
for f in "${files[@]}"; do install -m 0644 "$source_dir/$f" "$stage/ext/$f"; done
for f in "$source_dir"/*.test.ts; do install -m 0644 "$f" "$stage/ext/$(basename "$f")"; done
for f in "${mock_files[@]}"; do install -m 0644 "$source_dir/$f" "$stage/ext/$f"; done
for f in "${skill_files[@]}"; do install -m 0644 "$skill_source/$f" "$stage/skill/$f"; done
printf 'agent-bridge-deployment-v1 %s\n' "$digest" > "$stage/ext/$marker"
install -m 0644 "$stage/ext/$marker" "$stage/skill/$marker"
# Validate the complete staged payload before touching the installed trees.
check_tree "$stage/ext" "$ext_digest" "${ext_files[@]}"
check_tree "$stage/skill" "$skill_digest" "${skill_files[@]}"
if [[ -e "$target_dir" ]]; then mv -T "$target_dir" "$backup/ext"; backed_ext=1; fi
mv -T "$stage/ext" "$target_dir"
installed_ext=1
if [[ -e "$skill_target" ]]; then mv -T "$skill_target" "$backup/skill"; backed_skill=1; fi
mv -T "$stage/skill" "$skill_target"
installed_skill=1
# Verify after both renames; a partial deployment is never reported as success.
check_tree "$target_dir" "$ext_digest" "${ext_files[@]}" || exit 1
check_tree "$skill_target" "$skill_digest" "${skill_files[@]}" || exit 1
echo "Installed and verified Agent Bridge Pi adapter, skill, and shared deployment digest ($digest)."
