package scheduler_test

import (
	"testing"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
)

// TestBackoffDelayDoubles: the M6 exit-criterion shape — with a 1s base the
// delays run 1s → 2s → 4s (each plus jitter < base/4), capped at Cap.
func TestBackoffDelayDoubles(t *testing.T) {
	b := scheduler.Backoff{Base: time.Second, Cap: time.Minute}
	jitterMax := 250 * time.Millisecond // base/4

	wants := map[int]time.Duration{
		1: 1 * time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
	}
	for attempt, want := range wants {
		for i := 0; i < 50; i++ { // jitter is random: check bounds repeatedly
			got := b.Delay(attempt)
			if got < want || got >= want+jitterMax {
				t.Fatalf("Delay(%d) = %v, want in [%v, %v)", attempt, got, want, want+jitterMax)
			}
		}
	}
}

func TestBackoffDelayCapped(t *testing.T) {
	b := scheduler.Backoff{Base: time.Second, Cap: 5 * time.Second}
	for _, attempt := range []int{4, 10, 63, 64, 1000} { // 2^(n-1) would explode
		got := b.Delay(attempt)
		if got < 5*time.Second || got >= 5*time.Second+250*time.Millisecond {
			t.Fatalf("Delay(%d) = %v, want capped at ~5s", attempt, got)
		}
	}
}

func TestBackoffDefaults(t *testing.T) {
	var b scheduler.Backoff // zero value
	got := b.Delay(1)
	if got < time.Second || got >= 1250*time.Millisecond {
		t.Fatalf("zero-value Delay(1) = %v, want ~1s default", got)
	}
	if got := b.Delay(0); got < time.Second { // degenerate attempt treated as 1
		t.Fatalf("Delay(0) = %v, want >= base", got)
	}
}
