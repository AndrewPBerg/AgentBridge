package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var jsonOutput bool

func execute(args []string) error {
	root := &cobra.Command{
		Use:           "agent-bridge",
		Short:         "Local coordination daemon for coding agents",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	root.PersistentFlags().BoolVar(&jsonOutput, "json", false, "format output as JSON")
	root.SetArgs(args)

	add := func(use, short string, run func([]string) error) {
		command := &cobra.Command{Use: use, Short: short, FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true}, RunE: func(_ *cobra.Command, args []string) error {
			return run(args)
		}}
		root.AddCommand(command)
	}
	root.AddCommand(&cobra.Command{
		Use:                "serve",
		Short:              "Run the coordination daemon",
		DisableFlagParsing: true,
		RunE: func(_ *cobra.Command, args []string) error {
			return serve(args)
		},
	})
	add("ping", "Check daemon connectivity", func(args []string) error {
		if len(args) != 0 {
			return errors.New("usage: agent-bridge ping")
		}
		return callAndPrint("ping", map[string]any{})
	})
	add("stop", "Stop the running daemon", func(args []string) error {
		if len(args) != 0 {
			return errors.New("usage: agent-bridge stop")
		}
		return callAndPrint("daemon.shutdown", map[string]any{})
	})
	add("sessions", "List agent sessions", sessions)
	add("scopes", "List authority scopes", func(args []string) error {
		if len(args) != 0 {
			return errors.New("usage: agent-bridge scopes")
		}
		return callAndPrint("provenance.scopes", map[string]any{})
	})
	add("send", "Send a durable peer message", send)
	root.AddCommand(&cobra.Command{
		Use:                "worker",
		Short:              "Coordinate work from a non-Pi worker",
		DisableFlagParsing: true,
		RunE: func(_ *cobra.Command, args []string) error {
			return worker(args)
		},
	})
	add("request", "Send a raw JSON-RPC request", request)
	add("provenance", "Query causal provenance", provenanceCommand)
	add("doctor", "Check deployment integrity", doctor)
	daemon := &cobra.Command{Use: "daemon", Short: "Manage the coordination daemon"}
	recoverCommand := &cobra.Command{Use: "recover", Short: "Repair an unreachable daemon", FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true}, RunE: func(_ *cobra.Command, args []string) error { return recoverDaemon(args) }}
	daemon.AddCommand(recoverCommand)
	root.AddCommand(daemon)
	add("version", "Print version information", versionCommand)

	err := root.Execute()
	if err == nil {
		return nil
	}
	return reportExecutionError(err)
}

func reportExecutionError(err error) error {
	if !jsonOutput {
		return err
	}
	if failure, ok := errors.AsType[doctorFailure](err); ok && failure.Error() != "" {
		return err
	}
	if printErr := printJSON(map[string]any{"ok": false, "error": map[string]string{"message": err.Error()}}); printErr != nil {
		return errors.Join(err, printErr)
	}
	return err
}

func versionCommand(args []string) error {
	if len(args) != 0 {
		return errors.New("usage: agent-bridge version")
	}
	if jsonOutput {
		return printJSON(map[string]any{"ok": true, "version": "dev"})
	}
	fmt.Println("agent-bridge dev")
	return nil
}

func printJSON(value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(encoded))
	return err
}
