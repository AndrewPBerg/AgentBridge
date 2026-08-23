# TODO

- [ ] After installing the target-only external-change notification patch, restart the user daemon so it loads the new binary:

  ```sh
  go install ./cmd/agent-bridge
  systemctl --user restart agent-bridge.service
  ```
