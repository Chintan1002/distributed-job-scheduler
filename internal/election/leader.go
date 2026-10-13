// Package election implements leader election over a Redis lease.
// Many scheduler nodes run, but only the one holding the lease
// key fires jobs; the rest are hot standbys. Acquire is the atomic
// `SET key id NX EX ttl`; the leader renews every ttl/3 (the heartbeat), and
// if it dies the key expires on its own so a standby can take over.
//
// A lease alone still leaves the zombie-leader gap: a paused leader can wake
// up after losing the lease and keep writing. So every acquisition also
// mints a fencing token (M5) — a monotonic counter INCRed in
// Redis — and every fenced Postgres write carries it. The zombie's older
// token loses against the newer leadership's high-water mark.
//
// Tradeoff: the counter lives in Redis, so a Redis wipe would reset it (and
// with it the lease, so both sides reset together); production would back
// the counter with a durable sequence.
package election

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// renewDivisor: renew (and campaign) every ttl/3 — frequent enough to survive
// one missed beat, calm enough not to hammer Redis.
const renewDivisor = 3

type Elector struct {
	client *redis.Client
	key    string
	id     string
	ttl    time.Duration
	log    *slog.Logger
}

func New(ctx context.Context, redisURL, key, id string, ttl time.Duration, log *slog.Logger) (*Elector, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Elector{client: client, key: key, id: id, ttl: ttl, log: log}, nil
}

func (e *Elector) Close() error {
	return e.client.Close()
}

// TryAcquire makes one attempt to take the lease: SET NX EX — succeeds only
// if no one currently holds the key.
func (e *Elector) TryAcquire(ctx context.Context) (bool, error) {
	ok, err := e.client.SetNX(ctx, e.key, e.id, e.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("acquire lease: %w", err)
	}
	return ok, nil
}

// renewScript atomically extends the lease only if we still own it — a plain
// EXPIRE could extend a lease someone else has since acquired.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end`)

// Renew extends the lease. ok=false means we no longer own it.
func (e *Elector) Renew(ctx context.Context) (bool, error) {
	res, err := renewScript.Run(ctx, e.client, []string{e.key}, e.id, e.ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("renew lease: %w", err)
	}
	return res == 1, nil
}

// releaseScript deletes the lease only if we own it — a graceful handover so
// a standby doesn't have to wait out the TTL.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`)

func (e *Elector) Release(ctx context.Context) error {
	if err := releaseScript.Run(ctx, e.client, []string{e.key}, e.id).Err(); err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

// MintToken bumps and returns the fencing counter. Called once per acquired
// leadership; monotonic across all leaderships because INCR is atomic and
// the counter is never reset.
func (e *Elector) MintToken(ctx context.Context) (int64, error) {
	token, err := e.client.Incr(ctx, e.key+":token").Result()
	if err != nil {
		return 0, fmt.Errorf("mint fencing token: %w", err)
	}
	return token, nil
}

// Run campaigns until ctx is done: block until leadership is won, mint a
// fencing token, run `lead` with the token and a context that is cancelled
// the moment the lease is lost, release, campaign again. `lead` is expected
// to block until its context is done — gating the whole scheduler inside it
// enforces the invariant that jobs are only fired while holding the lease.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, fencingToken int64)) error {
	for {
		if err := e.campaign(ctx); err != nil {
			return err
		}
		token, err := e.MintToken(ctx)
		if err != nil {
			// Leadership without a token is unusable for fenced writes:
			// give the lease back and campaign again.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			e.log.Error("token mint failed, releasing leadership", "err", err)
			rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = e.Release(rctx)
			rcancel()
			continue
		}
		e.log.Info("became leader", "key", e.key, "id", e.id, "fencing_token", token)

		leaderCtx, cancel := context.WithCancel(ctx)
		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			e.renewLoop(leaderCtx)
			cancel() // lease lost (or ctx done): stop leading immediately
		}()

		lead(leaderCtx, token)
		cancel()
		<-renewDone

		// Graceful handover: delete the key (if still ours) so a standby
		// doesn't wait out the TTL. Fresh context — ctx may already be done.
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := e.Release(rctx); err != nil {
			e.log.Warn("release failed (lease will expire on its own)", "err", err)
		}
		rcancel()

		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.log.Info("no longer leader, standing by", "id", e.id)
	}
}

// campaign blocks until the lease is acquired or ctx is done. Redis errors
// are logged and retried — a standby must survive Redis blips.
func (e *Elector) campaign(ctx context.Context) error {
	interval := e.ttl / renewDivisor
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ok, err := e.TryAcquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			e.log.Error("acquire attempt failed", "err", err)
		} else if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// renewLoop heartbeats the lease and returns when leadership is lost. If
// Redis is unreachable we keep trying, but once a renewal hasn't been
// confirmed for ttl minus one beat we step down proactively — the key may be
// about to expire under us, and two active leaders is the one thing this
// package exists to prevent.
func (e *Elector) renewLoop(ctx context.Context) {
	interval := e.ttl / renewDivisor
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastConfirmed := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := e.Renew(ctx)
			switch {
			case err != nil:
				if ctx.Err() != nil {
					return
				}
				if time.Since(lastConfirmed) >= e.ttl-interval {
					e.log.Warn("stepping down: lease unconfirmed for too long", "err", err)
					return
				}
				e.log.Warn("renew failed, will retry", "err", err)
			case !ok:
				e.log.Warn("lease lost: another node owns it", "id", e.id)
				return
			default:
				lastConfirmed = time.Now()
			}
		}
	}
}
