# Quality gates

The repository uses a strict Go profile in [`.golangci.yml`](../.golangci.yml).
The merge gate is intentionally broader than linting:

```text
mise install
task quality
```

This runs gofmt/gofumpt checks, `go vet`, all configured golangci-lint analyzers,
normal tests, the race detector, shuffled tests, and `govulncheck`. The pinned toolchain is declared in [`mise.toml`](../mise.toml); run `mise install`
once on a new machine. CI uses the same mise configuration. `gosec` is initially restricted to
high-confidence/high-severity findings; every finding should still be reviewed.

For editor/agent feedback, `task lsp-quality` is the fast gate. Pi's project LSP
configuration in [`.pi/lsp.json`](../.pi/lsp.json) enables gofumpt and gopls's
staticcheck and additional analyses. It applies to Go files through `gopls`;
Pyright is a Python server and does not analyze this Go module. The full
repository checks remain authoritative because an LSP cannot provide race,
shuffle, or vulnerability analysis.

After changing LSP configuration, restart/reload the Pi session so the LSP
process is recreated (`/reload`). A normal source edit does not require an LSP
restart; diagnostics refresh automatically. Use `task lsp-quality` for
agent-triggered fast checks and `task quality` for the complete gate.
