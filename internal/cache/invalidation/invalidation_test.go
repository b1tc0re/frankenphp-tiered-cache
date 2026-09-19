package invalidation

import "testing"

func TestEventValidate(t *testing.T) {
	tests := []struct {
		name  string
		event Event
		valid bool
	}{
		{
			name:  "key invalidation",
			event: Event{Version: ProtocolVersion, Type: EventTypeInvalidate, Key: "key"},
			valid: true,
		},
		{
			name:  "flush",
			event: Event{Version: ProtocolVersion, Type: EventTypeFlush},
			valid: true,
		},
		{
			name:  "unsupported version",
			event: Event{Version: ProtocolVersion + 1, Type: EventTypeFlush},
		},
		{
			name:  "empty key",
			event: Event{Version: ProtocolVersion, Type: EventTypeInvalidate},
			valid: true,
		},
		{
			name:  "key on flush",
			event: Event{Version: ProtocolVersion, Type: EventTypeFlush, Key: "key"},
		},
		{
			name:  "unsupported operation",
			event: Event{Version: ProtocolVersion, Type: "unknown"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.event.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("Validate() error = %v, want valid=%t", err, test.valid)
			}
		})
	}
}
