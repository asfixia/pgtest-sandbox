package protocol_test

import (
	"testing"

	"pgtest-sandbox/pkg/protocol"
)

func TestExtractTestID(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		want    string
		wantErr bool
	}{
		{
			name: "valid test id",
			params: map[string]string{
				"application_name": "pgtest_abc123",
			},
			want:    "abc123",
			wantErr: false,
		},
		{
			name: "valid test id with underscore",
			params: map[string]string{
				"application_name": "pgtest_test_123",
			},
			want:    "test_123",
			wantErr: false,
		},
		{
			name: "missing application_name",
			params: map[string]string{
				"database": "mydb",
			},
			want:    "default",
			wantErr: false,
		},
		{
			name: "invalid format",
			params: map[string]string{
				"application_name": "invalid_format",
			},
			want:    "invalid_format",
			wantErr: false,
		},
		{
			name: "empty application_name",
			params: map[string]string{
				"application_name": "",
			},
			want:    "default",
			wantErr: false,
		},
		{
			name: "default application_name",
			params: map[string]string{
				"application_name": "default",
			},
			want:    "default",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.ExtractTestID(tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractTestID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractTestID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildStartupMessageForPostgres(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{
			name: "replace application_name",
			params: map[string]string{
				"application_name": "pgtest_abc123",
				"database":         "mydb",
			},
			want: "pgtest-proxy",
		},
		{
			name: "add application_name if missing",
			params: map[string]string{
				"database": "mydb",
			},
			want: "pgtest-proxy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protocol.BuildStartupMessageForPostgres(tt.params)
			if got["application_name"] != tt.want {
				t.Errorf("BuildStartupMessageForPostgres() application_name = %v, want %v", got["application_name"], tt.want)
			}
		})
	}
}
