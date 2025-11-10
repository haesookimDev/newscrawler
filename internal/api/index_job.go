package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type indexEvent struct {
	Type       string    `json:"type"`
	Timestamp  time.Time `json:"timestamp"`
	SessionID  string    `json:"session_id"`
	Status     string    `json:"status"`
	Total      int64     `json:"total"`
	Processed  int64     `json:"processed"`
	CurrentURL string    `json:"current_url,omitempty"`
	Message    string    `json:"message,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type indexJob struct {
	sessionID string
	total     int64
	processed int64
	status    string
	message   string
	err       string
	current   string
	subs      map[chan indexEvent]struct{}
	finished  bool
	mu        sync.Mutex
}

func newIndexJob(sessionID string, total int64) *indexJob {
	return &indexJob{
		sessionID: sessionID,
		total:     total,
		status:    "running",
		subs:      make(map[chan indexEvent]struct{}),
	}
}

func (j *indexJob) snapshotLocked(eventType string) indexEvent {
	return indexEvent{
		Type:       eventType,
		Timestamp:  time.Now(),
		SessionID:  j.sessionID,
		Status:     j.status,
		Total:      j.total,
		Processed:  j.processed,
		CurrentURL: j.current,
		Message:    j.message,
		Error:      j.err,
	}
}

func (j *indexJob) snapshot(eventType string) indexEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.snapshotLocked(eventType)
}

func (j *indexJob) subscribe() (<-chan indexEvent, func()) {
	ch := make(chan indexEvent, 16)
	j.mu.Lock()
	finished := j.finished
	initial := j.snapshotLocked("index_snapshot")
	if !finished {
		j.subs[ch] = struct{}{}
	}
	j.mu.Unlock()
	ch <- initial
	if finished {
		close(ch)
		return ch, func() {}
	}
	cancel := func() {
		j.mu.Lock()
		if _, ok := j.subs[ch]; ok {
			delete(j.subs, ch)
			close(ch)
		}
		j.mu.Unlock()
	}
	return ch, cancel
}

func (j *indexJob) broadcast(eventType string) {
	j.mu.Lock()
	evt := j.snapshotLocked(eventType)
	for ch := range j.subs {
		select {
		case ch <- evt:
		default:
		}
	}
	shouldClose := eventType == "index_completed" || eventType == "index_failed"
	if shouldClose {
		for ch := range j.subs {
			close(ch)
		}
		j.subs = make(map[chan indexEvent]struct{})
	}
	j.mu.Unlock()
}

func (j *indexJob) progress(url string) {
	j.mu.Lock()
	j.processed++
	j.current = url
	j.message = ""
	j.mu.Unlock()
	j.broadcast("index_progress")
}

func (j *indexJob) progressWithMessage(url, message string) {
	j.mu.Lock()
	j.processed++
	j.current = url
	j.message = message
	j.mu.Unlock()
	j.broadcast("index_progress")
}

func (j *indexJob) complete() {
	j.mu.Lock()
	j.status = "completed"
	j.finished = true
	j.mu.Unlock()
	j.broadcast("index_completed")
}

func (j *indexJob) fail(err error) {
	j.mu.Lock()
	j.status = "failed"
	j.err = err.Error()
	j.finished = true
	j.mu.Unlock()
	j.broadcast("index_failed")
}

func (j *indexJob) isFinished() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.finished
}

func writeIndexEvent(w http.ResponseWriter, flusher http.Flusher, evt indexEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", evt.Type); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
