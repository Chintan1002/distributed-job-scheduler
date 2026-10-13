package scheduler_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
)

// The wheel is driven by explicit times, so these tests are fully
// deterministic — no sleeping, no real clock. A small wheel (8 slots,
// 3 levels, 1s ticks) keeps cascade boundaries easy to hit:
// level 0 spans 8 ticks, level 1 spans 64, level 2 spans 512.

const (
	testTick   = time.Second
	testSize   = 8
	testLevels = 3
	testSpan   = 512 // testSize^testLevels
)

func wheelJob(id string) *model.Job {
	return &model.Job{ID: id, Name: "wheel-test-" + id}
}

// TestWheelFiresExactlyAtDueTick advances one tick at a time and checks each
// job fires exactly at its due tick — never early, never late — including
// delays that land on cascade boundaries and beyond the wheel's total span.
func TestWheelFiresExactlyAtDueTick(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := scheduler.NewTimingWheel(testTick, testSize, testLevels, start)

	delays := []int64{1, 2, 7, 8, 9, 15, 16, 63, 64, 65, 100, 511, 512, 600}
	for _, d := range delays {
		job := wheelJob(itoa(d))
		if due := w.Add(job, start.Add(time.Duration(d)*testTick)); due {
			t.Fatalf("delay %d reported as already due", d)
		}
	}
	if w.Len() != len(delays) {
		t.Fatalf("wheel has %d pending, want %d", w.Len(), len(delays))
	}

	firedAt := map[string]int64{}
	for tick := int64(1); tick <= 700; tick++ {
		for _, job := range w.Advance(start.Add(time.Duration(tick) * testTick)) {
			if prev, dup := firedAt[job.ID]; dup {
				t.Fatalf("job %s fired twice (ticks %d and %d)", job.ID, prev, tick)
			}
			firedAt[job.ID] = tick
		}
	}

	for _, d := range delays {
		got, ok := firedAt[itoa(d)]
		if !ok {
			t.Errorf("delay %d never fired", d)
			continue
		}
		if got != d {
			t.Errorf("delay %d fired at tick %d", d, got)
		}
	}
	if w.Len() != 0 {
		t.Fatalf("wheel still has %d pending after everything fired", w.Len())
	}
}

// TestWheelSubTickRoundsUp: a due time between tick boundaries must round UP —
// firing early is a correctness bug, firing one tick late is by design.
func TestWheelSubTickRoundsUp(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := scheduler.NewTimingWheel(testTick, testSize, testLevels, start)

	w.Add(wheelJob("half"), start.Add(2500*time.Millisecond)) // between ticks 2 and 3

	if fired := w.Advance(start.Add(2 * testTick)); len(fired) != 0 {
		t.Fatalf("job fired at tick 2, before its due time")
	}
	if fired := w.Advance(start.Add(3 * testTick)); len(fired) != 1 {
		t.Fatalf("job did not fire at tick 3")
	}
}

func TestWheelAlreadyDue(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := scheduler.NewTimingWheel(testTick, testSize, testLevels, start)

	if due := w.Add(wheelJob("now"), start); !due {
		t.Error("job due at start not reported as due")
	}
	if due := w.Add(wheelJob("past"), start.Add(-time.Hour)); !due {
		t.Error("job due in the past not reported as due")
	}
	if w.Len() != 0 {
		t.Errorf("already-due jobs were added to the wheel (len=%d)", w.Len())
	}
}

// TestWheelDedup: re-adding a pending job id is a no-op, so reloading an
// overlapping window from Postgres can't double-fire.
func TestWheelDedup(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := scheduler.NewTimingWheel(testTick, testSize, testLevels, start)

	job := wheelJob("dup")
	w.Add(job, start.Add(3*testTick))
	if due := w.Add(job, start.Add(5*testTick)); due {
		t.Fatal("duplicate add reported as due")
	}
	if w.Len() != 1 {
		t.Fatalf("wheel has %d pending, want 1", w.Len())
	}

	total := 0
	for tick := int64(1); tick <= 10; tick++ {
		total += len(w.Advance(start.Add(time.Duration(tick) * testTick)))
	}
	if total != 1 {
		t.Fatalf("job fired %d times, want 1", total)
	}
}

func TestWheelRemove(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := scheduler.NewTimingWheel(testTick, testSize, testLevels, start)

	w.Add(wheelJob("gone"), start.Add(3*testTick))
	if !w.Remove("gone") {
		t.Fatal("Remove returned false for a pending job")
	}
	if w.Remove("gone") {
		t.Fatal("Remove returned true for an already-removed job")
	}
	if fired := w.Advance(start.Add(10 * testTick)); len(fired) != 0 {
		t.Fatalf("removed job fired: %v", fired)
	}
}

// TestWheelBigJump: one Advance far into the future fires everything due,
// in order, exactly once (e.g. after the process was suspended).
func TestWheelBigJump(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := scheduler.NewTimingWheel(testTick, testSize, testLevels, start)

	delays := []int64{1, 8, 64, 200, 512}
	for _, d := range delays {
		w.Add(wheelJob(itoa(d)), start.Add(time.Duration(d)*testTick))
	}
	w.Add(wheelJob("future"), start.Add(650*testTick))

	fired := w.Advance(start.Add(600 * testTick))
	if len(fired) != len(delays) {
		t.Fatalf("big jump fired %d jobs, want %d", len(fired), len(delays))
	}
	for i, job := range fired {
		if job.ID != itoa(delays[i]) {
			t.Fatalf("fire order wrong at %d: got %s, want %s", i, job.ID, itoa(delays[i]))
		}
	}
	if w.Len() != 1 {
		t.Fatalf("wheel has %d pending, want 1 (the tick-650 job)", w.Len())
	}
}

// TestWheelRandomized: many random delays, advanced in random jumps — every
// job fires exactly once, within the first Advance whose target covers it.
func TestWheelRandomized(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := scheduler.NewTimingWheel(testTick, testSize, testLevels, start)
	rng := rand.New(rand.NewSource(42))

	const jobs = 300
	delay := map[string]int64{}
	for i := 0; i < jobs; i++ {
		d := rng.Int63n(1000) + 1
		id := itoa(int64(i))
		delay[id] = d
		w.Add(wheelJob(id), start.Add(time.Duration(d)*testTick))
	}

	fired := map[string]bool{}
	prev := int64(0)
	for tick := int64(0); tick < 1100; {
		tick += rng.Int63n(20) + 1
		for _, job := range w.Advance(start.Add(time.Duration(tick) * testTick)) {
			if fired[job.ID] {
				t.Fatalf("job %s fired twice", job.ID)
			}
			fired[job.ID] = true
			d := delay[job.ID]
			if d > tick {
				t.Fatalf("job %s fired early: due tick %d, fired by tick %d", job.ID, d, tick)
			}
			if d <= prev {
				t.Fatalf("job %s fired late: due tick %d, already advanced past %d", job.ID, d, prev)
			}
		}
		prev = tick
	}
	if len(fired) != jobs {
		t.Fatalf("%d of %d jobs fired", len(fired), jobs)
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
