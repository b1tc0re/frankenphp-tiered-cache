package redis

import "testing"

func TestInvalidationChannelForDBSeparatesRedisDatabases(t *testing.T) {
	channelDB0 := invalidationChannelForDB("test:", 0)
	channelDB1 := invalidationChannelForDB("test:", 1)

	if channelDB0 == channelDB1 {
		t.Fatalf("invalidation channels are equal across Redis DBs: %q", channelDB0)
	}
	if got, want := channelDB1, "test:__invalidation:db:1"; got != want {
		t.Fatalf("invalidationChannelForDB() = %q, want %q", got, want)
	}
}
