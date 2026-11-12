package config

import (
	"math"
	"reflect"
	"testing"
)

// TestSessionsPerOwner_Constants pins the documented values of the two
// MaxSessionsPerOwner constants.
func TestSessionsPerOwner_Constants(t *testing.T) {
	if DefaultMaxSessionsPerOwner != 128 {
		t.Errorf("DefaultMaxSessionsPerOwner = %d, want 128", DefaultMaxSessionsPerOwner)
	}
	if SessionsPerOwnerUnlimited != math.MaxUint64 {
		t.Errorf("SessionsPerOwnerUnlimited = %d, want math.MaxUint64", SessionsPerOwnerUnlimited)
	}
}

// TestSessionsPerOwner_SetDefaults checks that SetDefaults gives an unset
// MaxSessionsPerOwner the default, keeps an explicit cap or
// SessionsPerOwnerUnlimited, leaves a config Validate accepts, and is
// idempotent.
func TestSessionsPerOwner_SetDefaults(t *testing.T) {
	tests := []struct {
		name string
		set  uint64
		want uint64
	}{
		{"unset takes the default", 0, DefaultMaxSessionsPerOwner},
		{"explicit 1 is kept", 1, 1},
		{"explicit 7 is kept", 7, 7},
		{"above MaxTotalSessions is kept", DefaultMaxTotalSessions + 1, DefaultMaxTotalSessions + 1},
		{"unlimited is kept", SessionsPerOwnerUnlimited, math.MaxUint64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				ShardID:             1,
				ReplicaID:           1,
				MaxSessionsPerOwner: tt.set,
			}
			c.SetDefaults()
			if c.MaxSessionsPerOwner != tt.want {
				t.Fatalf("MaxSessionsPerOwner = %d, want %d", c.MaxSessionsPerOwner, tt.want)
			}
			if tt.set == DefaultMaxTotalSessions+1 && c.MaxSessionsPerOwner <= c.MaxTotalSessions {
				t.Fatalf("MaxSessionsPerOwner %d not above MaxTotalSessions %d",
					c.MaxSessionsPerOwner, c.MaxTotalSessions)
			}
			if err := c.Validate(); err != nil {
				t.Fatalf("Validate() after SetDefaults = %v, want nil", err)
			}
			first := c
			c.SetDefaults()
			if !reflect.DeepEqual(c, first) {
				t.Fatalf("second SetDefaults changed the config: %+v, want %+v", c, first)
			}
		})
	}
}
