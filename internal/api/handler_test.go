package api_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/api"
	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

// fakeStore is an in-memory api.Store so handler tests need no database.
type fakeStore struct {
	jobs map[string]*model.Job
}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: map[string]*model.Job{}}
}

func (f *fakeStore) CreateJob(_ context.Context, j *model.Job) error {
	f.jobs[j.ID] = j
	return nil
}

func (f *fakeStore) GetJob(_ context.Context, id string) (*model.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeStore) CancelJob(_ context.Context, id string) error {
	j, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	if j.Status != model.StatusScheduled {
		return store.ErrNotCancellable
	}
	j.Status = model.StatusCancelled
	return nil
}

func newTestServer(fs *fakeStore) http.Handler {
	log := slog.New(slog.DiscardHandler)
	return api.NewServer(fs, log).Handler()
}

func doRequest(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCreateGetCancelJob(t *testing.T) {
	fs := newFakeStore()
	h := newTestServer(fs)

	runAt := time.Now().UTC().Add(10 * time.Second).Format(time.RFC3339)
	body := `{"name":"nightly-report","schedule_type":"once","run_at":"` + runAt + `","payload":{"task":"generate_report"},"max_retries":3}`

	w := doRequest(t, h, http.MethodPost, "/jobs", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST /jobs = %d, want 201; body: %s", w.Code, w.Body)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || created.ID == "" {
		t.Fatalf("POST /jobs returned no id: %s", w.Body)
	}
	if got := fs.jobs[created.ID]; got == nil || got.Status != model.StatusScheduled {
		t.Fatalf("job not persisted as scheduled: %+v", fs.jobs[created.ID])
	}

	w = doRequest(t, h, http.MethodGet, "/jobs/"+created.ID, "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /jobs/{id} = %d, want 200; body: %s", w.Code, w.Body)
	}
	var got model.Job
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("GET /jobs/{id} bad JSON: %v", err)
	}
	if got.Name != "nightly-report" || got.Status != model.StatusScheduled {
		t.Fatalf("unexpected job: %+v", got)
	}

	w = doRequest(t, h, http.MethodDelete, "/jobs/"+created.ID, "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE /jobs/{id} = %d, want 204; body: %s", w.Code, w.Body)
	}
	if fs.jobs[created.ID].Status != model.StatusCancelled {
		t.Fatalf("job not cancelled: %+v", fs.jobs[created.ID])
	}

	// Cancelling twice conflicts: the job is no longer in a cancellable state.
	w = doRequest(t, h, http.MethodDelete, "/jobs/"+created.ID, "")
	if w.Code != http.StatusConflict {
		t.Fatalf("second DELETE = %d, want 409", w.Code)
	}
}

func TestCreateJobValidation(t *testing.T) {
	h := newTestServer(newFakeStore())
	runAt := time.Now().UTC().Add(10 * time.Second).Format(time.RFC3339)

	cases := []struct {
		name string
		body string
	}{
		{"invalid JSON", `{not json`},
		{"missing name", `{"schedule_type":"once","run_at":"` + runAt + `"}`},
		{"missing run_at", `{"name":"x","schedule_type":"once"}`},
		{"bad schedule_type", `{"name":"x","schedule_type":"weekly","run_at":"` + runAt + `"}`},
		{"cron not yet supported", `{"name":"x","schedule_type":"cron","cron_expr":"*/5 * * * *"}`},
		{"depends_on not yet supported", `{"name":"x","schedule_type":"once","run_at":"` + runAt + `","depends_on":["` + "1b4e28ba-2fa1-11d2-883f-0016d3cca427" + `"]}`},
		{"negative max_retries", `{"name":"x","schedule_type":"once","run_at":"` + runAt + `","max_retries":-1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := doRequest(t, h, http.MethodPost, "/jobs", tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("POST /jobs = %d, want 400; body: %s", w.Code, w.Body)
			}
		})
	}
}

func TestGetAndCancelUnknownJob(t *testing.T) {
	h := newTestServer(newFakeStore())

	w := doRequest(t, h, http.MethodGet, "/jobs/1b4e28ba-2fa1-11d2-883f-0016d3cca427", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET unknown job = %d, want 404", w.Code)
	}
	w = doRequest(t, h, http.MethodDelete, "/jobs/1b4e28ba-2fa1-11d2-883f-0016d3cca427", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("DELETE unknown job = %d, want 404", w.Code)
	}
	w = doRequest(t, h, http.MethodGet, "/jobs/not-a-uuid", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("GET bad uuid = %d, want 400", w.Code)
	}
}
