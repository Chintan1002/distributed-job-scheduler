package scheduler_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
)

// `make bench` — the M2 claim: firing due jobs from a timing wheel is O(1)
// per tick, while the M1 approach pays per tick for every pending job (a
// scan, or at best an indexed query round-trip to Postgres).
//
// Both in-memory benchmarks hold 100k pending jobs and measure ONE tick.

const (
	benchJobs    = 100_000
	benchHorizon = 1_000_000 // ticks the pending jobs are spread across
)

// BenchmarkWheelTick: one 100ms tick of a hierarchical timing wheel with
// 100k pending jobs. O(1) — cost does not grow with the number of jobs.
func BenchmarkWheelTick(b *testing.B) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tick := 100 * time.Millisecond
	w := scheduler.NewTimingWheel(tick, 64, 4, start)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < benchJobs; i++ {
		d := rng.Int63n(benchHorizon) + 1
		w.Add(wheelJob(itoa(int64(i))), start.Add(time.Duration(d)*tick))
	}

	b.ResetTimer()
	now := start
	for i := 0; i < b.N; i++ {
		now = now.Add(tick)
		w.Advance(now)
	}
}

// BenchmarkPollingScanTick: one tick of the M1 strategy's underlying work —
// compare every pending job's run_at against now. O(n) per tick. (In M1 the
// scan lives inside Postgres behind an index, plus a network round-trip;
// this in-memory version is the flattering lower bound for polling.)
func BenchmarkPollingScanTick(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	runAt := make([]int64, benchJobs)
	for i := range runAt {
		runAt[i] = rng.Int63n(benchHorizon) + 1
	}

	b.ResetTimer()
	due := 0
	for i := 0; i < b.N; i++ {
		now := int64(i)
		for _, at := range runAt {
			if at <= now {
				due++
			}
		}
	}
	_ = due
}

// BenchmarkPostgresPollTick: what one M1 poll actually cost — a ClaimDueJobs
// round-trip to Postgres (typical case: nothing due). Skips if Postgres is
// down.
func BenchmarkPostgresPollTick(b *testing.B) {
	st := testStore(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.ClaimDueJobs(ctx, 100); err != nil {
			b.Fatalf("claim due jobs: %v", err)
		}
	}
}
