package postgres_test

import (
	"testing"

	"pgtest-transient/pkg/postgres"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "public",
			expected: `"public"`,
		},
		{
			name:     "identifier with underscore",
			input:    "pgtest_table",
			expected: `"pgtest_table"`,
		},
		{
			name:     "identifier with quotes",
			input:    `schema"name`,
			expected: `"schema""name"`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
		{
			name:     "mixed case",
			input:    "PublicSchema",
			expected: `"PublicSchema"`,
		},
		{
			name:     "with spaces",
			input:    "schema name",
			expected: `"schema name"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := postgres.QuoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "simple qualified name",
			schema:   "public",
			table:    "pgtest_table",
			expected: `"public"."pgtest_table"`,
		},
		{
			name:     "with quotes in names",
			schema:   `schema"name`,
			table:    `table"name`,
			expected: `"schema""name"."table""name"`,
		},
		{
			name:     "mixed case",
			schema:   "PublicSchema",
			table:    "PgTest_Table",
			expected: `"PublicSchema"."PgTest_Table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := postgres.QuoteQualifiedName(tt.schema, tt.table)
			if result != tt.expected {
				t.Errorf("QuoteQualifiedName(%q, %q) = %q, want %q", tt.schema, tt.table, result, tt.expected)
			}
		})
	}
}
