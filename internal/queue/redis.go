// Package queue is the Redis-backed ready queue: the transient hand-off
// between the scheduler (push on due) and the workers (blocking pull).
// Redis is speed, Postgres is truth. A message here is
// only a pointer to a job; the job itself lives durably in Postgres.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Message is what the scheduler enqueues for workers: which job to run and
// under which fencing token (always 0 until M5 introduces real tokens).
type Message struct {
	JobID        string `json:"job_id"`
	FencingToken int64  `json:"fencing_token"`
}

type Queue struct {
	client *redis.Client
	key    string
}

func New(ctx context.Context, redisURL, key string) (*Queue, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Queue{client: client, key: key}, nil
}

func (q *Queue) Close() error {
	return q.client.Close()
}

// Push appends a message to the ready queue (LPUSH head; workers BRPOP the
// tail, so the list behaves FIFO).
func (q *Queue) Push(ctx context.Context, msg Message) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal queue message: %w", err)
	}
	if err := q.client.LPush(ctx, q.key, b).Err(); err != nil {
		return fmt.Errorf("lpush ready queue: %w", err)
	}
	return nil
}

// Pop blocks up to `timeout` for the next message. ok=false means the wait
// timed out with nothing available — callers just loop again, which is what
// makes worker shutdown responsive (each lap re-checks ctx).
func (q *Queue) Pop(ctx context.Context, timeout time.Duration) (msg Message, ok bool, err error) {
	res, err := q.client.BRPop(ctx, timeout, q.key).Result()
	if errors.Is(err, redis.Nil) {
		return Message{}, false, nil
	}
	if err != nil {
		return Message{}, false, fmt.Errorf("brpop ready queue: %w", err)
	}
	// BRPOP returns [key, value].
	if err := json.Unmarshal([]byte(res[1]), &msg); err != nil {
		return Message{}, false, fmt.Errorf("unmarshal queue message: %w", err)
	}
	return msg, true, nil
}

// Len reports the queue depth (for tests and, later, metrics).
func (q *Queue) Len(ctx context.Context) (int64, error) {
	n, err := q.client.LLen(ctx, q.key).Result()
	if err != nil {
		return 0, fmt.Errorf("llen ready queue: %w", err)
	}
	return n, nil
}
