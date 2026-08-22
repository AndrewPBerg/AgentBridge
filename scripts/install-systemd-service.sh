#!/usr/bin/env bash
set -euo pipefail
umask 077
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$unit_dir" "$HOME/.local/bin"
(cd "$root" && go install ./cmd/agent-bridge)
go_bin=$(go env GOBIN)
if [[ -z "$go_bin" ]]; then go_bin="$(go env GOPATH)/bin"; fi
install -m 0755 "$go_bin/agent-bridge" "$HOME/.local/bin/agent-bridge"
install -m 0644 "$root/deploy/systemd/agent-bridge.service" "$unit_dir/agent-bridge.service"
"$root/scripts/install-pi-extension.sh"
systemctl --user daemon-reload
systemctl --user enable --now agent-bridge.service
for _ in $(seq 1 120); do
  if "$HOME/.local/bin/agent-bridge" ping >/dev/null 2>&1; then
    echo 'Agent Bridge systemd service is healthy.'
    exit 0
  fi
  sleep 1
done
systemctl --user status --no-pager agent-bridge.service || true
journalctl --user -u agent-bridge.service -n 50 --no-pager || true
exit 1
