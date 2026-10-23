package store

import (
	"errors"
	"maps"
	"testing"
)

func TestEncodeCounts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   map[string]int64
		want string
	}{
		{"nil is an empty object", nil, "{}"},
		{"empty is an empty object", map[string]int64{}, "{}"},
		{"one reason", map[string]int64{"empty_file": 1}, `{"empty_file":1}`},
		{"keys are sorted", map[string]int64{"not_an_email": 1, "empty_file": 2}, `{"empty_file":2,"not_an_email":1}`},
		{"large count", map[string]int64{"invalid_date": 1 << 40}, `{"invalid_date":1099511627776}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := encodeCounts(tt.in)
			if err != nil {
				t.Fatalf("encodeCounts(%v): %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("encodeCounts(%v) = %s, want %s", tt.in, got, tt.want)
			}
		})
	}
}

func TestDecodeCounts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    map[string]int64
		wantErr bool
	}{
		{"empty input", "", map[string]int64{}, false},
		{"empty object", "{}", map[string]int64{}, false},
		{"JSON null", "null", map[string]int64{}, false},
		{"counts", `{"empty_file": 2, "not_an_email": 1}`, map[string]int64{"empty_file": 2, "not_an_email": 1}, false},
		{"not an object", `[1]`, nil, true},
		{"non-integer count", `{"empty_file": "x"}`, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeCounts([]byte(tt.in))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("decodeCounts(%s) = %v, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeCounts(%s): %v", tt.in, err)
			}
			if got == nil {
				t.Fatalf("decodeCounts(%s) returned a nil map", tt.in)
			}
			if !maps.Equal(got, tt.want) {
				t.Errorf("decodeCounts(%s) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestCountsRoundTrip(t *testing.T) {
	t.Parallel()
	in := map[string]int64{"invalid_sender": 1557, "invalid_date": 4, "invalid_utf8": 90}
	enc, err := encodeCounts(in)
	if err != nil {
		t.Fatalf("encodeCounts: %v", err)
	}
	out, err := decodeCounts([]byte(enc))
	if err != nil {
		t.Fatalf("decodeCounts: %v", err)
	}
	if !maps.Equal(in, out) {
		t.Errorf("round trip = %v, want %v", out, in)
	}
}

func TestRunFinishedOnlyWhenCompleted(t *testing.T) {
	t.Parallel()
	for state, want := range map[RunState]bool{RunRunning: false, RunCompleted: true, RunFailed: false} {
		r := Run{State: state}
		if got := r.Finished(); got != want {
			t.Errorf("Run{State: %q}.Finished() = %v, want %v", state, got, want)
		}
	}
}

func TestFinishRunRejectsNonFinalState(t *testing.T) {
	t.Parallel()
	var p Postgres // validation happens before any database access
	for _, state := range []RunState{RunRunning, "", "idle", "COMPLETED"} {
		err := p.FinishRun(t.Context(), 1, state, Counters{}, nil)
		if !errors.Is(err, ErrInvalidRunState) {
			t.Errorf("FinishRun(state %q) error = %v, want ErrInvalidRunState", state, err)
		}
	}
}

func TestIngestLockKeyPgLocksParts(t *testing.T) {
	t.Parallel()
	// "enron" in ASCII; pg_locks shows the high and low 32 bits separately.
	if got, want := IngestLockKey>>32, int64(0x65); got != want {
		t.Errorf("classid part = %#x, want %#x", got, want)
	}
	if got, want := IngestLockKey&0xffffffff, int64(0x6E726F6E); got != want {
		t.Errorf("objid part = %#x, want %#x", got, want)
	}
}
