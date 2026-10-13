package scheduler

import (
	"sync"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
)

// TimingWheel is a hierarchical timing wheel.
//
// Level 0 is a ring of `size` slots, each covering one tick. Level l covers
// size^l ticks per slot, like the second/minute/hour hands of a clock. A job
// due in d ticks goes in slot (due_tick / size^l) % size of the innermost
// level whose span contains d — O(1). Each tick advances the pointer and
// fires one level-0 bucket — O(1). When an outer wheel's slot comes current,
// its entries "cascade" down into finer wheels (the clock-hand carry).
//
// Time is measured in absolute ticks since `start`; a job's expireTick is
// rounded UP so nothing ever fires before its due time.
type TimingWheel struct {
	// The wheel is shared mutable state (tick loop + insert path), so it is
	// guarded by a mutex.
	mu sync.Mutex

	tick      time.Duration
	size      int64
	numLevels int
	start     time.Time
	cur       int64 // last processed absolute tick

	levels    [][][]*wheelEntry // levels[level][slot] = bucket of entries
	levelTick []int64           // levelTick[l] = size^l: ticks per slot at level l
	pending   map[string]*wheelEntry
}

type wheelEntry struct {
	job        *model.Job
	expireTick int64 // absolute tick at which the job is due
	removed    bool  // lazy deletion: skipped when its bucket is processed
}

// Wheel geometry defaults: 100ms × 64 slots × 4 levels spans ~19 days,
// far beyond the scheduler's lookahead window.
const (
	defaultWheelSize   = 64
	defaultWheelLevels = 4
)

func NewTimingWheel(tick time.Duration, size int64, numLevels int, start time.Time) *TimingWheel {
	w := &TimingWheel{
		tick:      tick,
		size:      size,
		numLevels: numLevels,
		start:     start.UTC(),
		levels:    make([][][]*wheelEntry, numLevels),
		levelTick: make([]int64, numLevels),
		pending:   make(map[string]*wheelEntry),
	}
	for l := 0; l < numLevels; l++ {
		w.levels[l] = make([][]*wheelEntry, size)
		if l == 0 {
			w.levelTick[l] = 1
		} else {
			w.levelTick[l] = w.levelTick[l-1] * size
		}
	}
	return w
}

// Add schedules a job to fire at `at`. It returns true when the job is
// already due, in which case it is NOT added and the caller should run it
// immediately. Adding a job id that is already pending is a no-op (dedup),
// so reloading the same window from Postgres is safe.
func (w *TimingWheel) Add(job *model.Job, at time.Time) (due bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.pending[job.ID]; ok {
		return false
	}
	// Round up to the next tick boundary so a job never fires early.
	d := at.Sub(w.start)
	exp := int64(0)
	if d > 0 {
		exp = int64((d + w.tick - 1) / w.tick)
	}
	if exp <= w.cur {
		return true
	}
	e := &wheelEntry{job: job, expireTick: exp}
	w.place(e)
	w.pending[job.ID] = e
	return false
}

// place buckets an entry into the innermost level whose span contains it.
// Slot index is the entry's due tick in that level's units, mod wheel size —
// the classic `(current + delay) mod size`, generalized per level.
func (w *TimingWheel) place(e *wheelEntry) {
	remaining := e.expireTick - w.cur
	for l := 0; l < w.numLevels; l++ {
		span := w.levelTick[l] * w.size // ticks covered by levels 0..l
		if remaining < span {
			slot := (e.expireTick / w.levelTick[l]) % w.size
			w.levels[l][slot] = append(w.levels[l][slot], e)
			return
		}
	}
	// Beyond the whole wheel's horizon: park in the outermost future slot.
	// When that slot cascades, the entry is re-placed by its true expireTick,
	// so it can only ever fire late-by-reparking, never early. In practice the
	// scheduler's lookahead window is far smaller than the wheel span.
	top := w.numLevels - 1
	slot := ((w.cur + w.levelTick[top]*w.size - 1) / w.levelTick[top]) % w.size
	w.levels[top][slot] = append(w.levels[top][slot], e)
}

// Advance moves the wheel forward to `now` and returns every job that came
// due, in fire order. Each elapsed tick costs O(1): one level-0 bucket, plus
// an outer-wheel cascade only at slot boundaries.
func (w *TimingWheel) Advance(now time.Time) []*model.Job {
	w.mu.Lock()
	defer w.mu.Unlock()

	target := int64(now.Sub(w.start) / w.tick) // floor: only completed ticks
	var fired []*model.Job
	for w.cur < target {
		w.cur++
		t := w.cur
		// Cascade outer wheels whose slot boundary we just crossed, outermost
		// first so re-placed entries land in inner levels processed below.
		for l := w.numLevels - 1; l >= 1; l-- {
			if t%w.levelTick[l] == 0 {
				slot := (t / w.levelTick[l]) % w.size
				fired = w.drainBucket(l, slot, t, fired)
			}
		}
		fired = w.drainBucket(0, t%w.size, t, fired)
	}
	return fired
}

// drainBucket empties one slot: due entries fire, future entries (cascades)
// are re-placed into finer levels, removed entries are dropped.
func (w *TimingWheel) drainBucket(level int, slot, t int64, fired []*model.Job) []*model.Job {
	bucket := w.levels[level][slot]
	w.levels[level][slot] = nil
	for _, e := range bucket {
		switch {
		case e.removed:
			// already deleted from pending by Remove
		case e.expireTick <= t:
			fired = append(fired, e.job)
			delete(w.pending, e.job.ID)
		default:
			w.place(e)
		}
	}
	return fired
}

// Remove cancels a pending job. Its entry is tombstoned and dropped when its
// bucket is next processed. Returns false if the job wasn't pending.
func (w *TimingWheel) Remove(jobID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.pending[jobID]
	if !ok {
		return false
	}
	e.removed = true
	delete(w.pending, jobID)
	return true
}

// Len reports how many jobs are pending in the wheel.
func (w *TimingWheel) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}
