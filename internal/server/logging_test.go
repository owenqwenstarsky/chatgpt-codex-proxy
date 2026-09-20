package server

import "testing"

func TestFormatPayloadForLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "JSON bytes",
			value: []byte("{\n  \"hello\": \"world\"\n}\n"),
			want:  "{\"hello\":\"\\u003credacted\\u003e\"}",
		},
		{
			name:  "map value",
			value: map[string]any{"stream": true, "model": "gpt-5.6-terra"},
			want:  "{\"model\":\"gpt-5.6-terra\",\"stream\":true}",
		},
		{
			name:  "sensitive payload fields",
			value: map[string]any{"model": "gpt-5.6-terra", "input": "private prompt"},
			want:  "{\"input\":\"\\u003credacted\\u003e\",\"model\":\"gpt-5.6-terra\"}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatPayloadForLog(tc.value); got != tc.want {
				t.Fatalf("formatPayloadForLog() = %q, want %q", got, tc.want)
			}
		})
	}
}
