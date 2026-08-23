package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTicketsCanonicalAndInvalid(t *testing.T) {
	var tickets Tickets
	if err := json.Unmarshal([]byte(` [ { "z": 2, "a": 1 } ] `), &tickets); err != nil {
		t.Fatal(err)
	}
	if got, want := string(tickets), `[{"a":1,"z":2}]`; got != want {
		t.Fatalf("canonical tickets = %s, want %s", got, want)
	}
	for _, input := range []string{`{}`, `[1]`, `[{"a":1,"a":2}]`, `[] []`, `[}]`} {
		t.Run(input, func(t *testing.T) {
			var value Tickets
			if err := json.Unmarshal([]byte(input), &value); err == nil {
				t.Fatal("invalid tickets accepted")
			}
		})
	}
	if _, err := NormalizeTickets(Tickets(strings.Repeat("x", MaxTicketsJSONBytes+1))); err == nil {
		t.Fatal("oversized tickets accepted")
	}
}
