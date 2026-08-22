package protocol

import "testing"

func TestValidateUUID(t *testing.T) {
	valid := []string{
		"11111111-1111-5111-8111-111111111111",
		"pi:22222222-2222-4222-8222-222222222222",
	}
	for _, value := range valid {
		if err := ValidateUUID(value); err != nil {
			t.Errorf("ValidateUUID(%q) = %v", value, err)
		}
	}
	invalid := []string{"", "repo:test", "11111111-1111-0111-8111-111111111111", "11111111-1111-5111-7111-111111111111"}
	for _, value := range invalid {
		if err := ValidateUUID(value); err == nil {
			t.Errorf("ValidateUUID(%q) unexpectedly succeeded", value)
		}
	}
}
