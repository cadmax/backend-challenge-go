package application

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNewIDProducesCanonicalRandomUUIDs(t *testing.T) {
	seen := make(map[string]bool)
	for range 100 {
		value := NewID()
		parsed, err := uuid.Parse(value)
		if err != nil {
			t.Fatalf("generated ID is not a UUID: %v", err)
		}
		if len(value) != 36 || parsed.String() != value || parsed.Version() != 4 || parsed.Variant() != uuid.RFC4122 {
			t.Fatalf("generated ID is not a canonical UUID v4: %q", value)
		}
		if seen[value] {
			t.Fatalf("generated duplicate ID: %q", value)
		}
		seen[value] = true
	}
}

func TestUUIDInputKeepsTheHTTPContract(t *testing.T) {
	canonical := "123e4567-e89b-12d3-a456-426614174000"
	for _, test := range []struct {
		name  string
		value string
		valid bool
	}{
		{"canonical", canonical, true},
		{"uppercase", strings.ToUpper(canonical), true},
		{"nil UUID", uuid.Nil.String(), true},
		{"empty", "", false},
		{"invalid hex", "z23e4567-e89b-12d3-a456-426614174000", false},
		{"hyphens inside a hex group", "--3e4567-e89b-12d3-a456-426614174000", false},
		{"short", canonical[:35], false},
		{"raw hex", strings.ReplaceAll(canonical, "-", ""), false},
		{"URN", "urn:uuid:" + canonical, false},
		{"braces", "{" + canonical + "}", false},
		{"whitespace", " " + canonical, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validUUID(test.value); got != test.valid {
				t.Fatalf("validUUID(%q) = %t, want %t", test.value, got, test.valid)
			}
		})
	}
}
