package solver

// earthly-specific: this is used to collect information related to "inconsistent graph state" errors

import (
	"fmt"
	"strings"
	"sync"
	"time"

	digest "github.com/opencontainers/go-digest"
)

var dgstTrackerInst = newDgstTracker()

type dgstTrackerItem struct {
	dgst   digest.Digest
	action string
	seen   time.Time
}

type dgstTracker struct {
	mu      sync.Mutex
	head    int
	records []dgstTrackerItem
}

func newDgstTracker() *dgstTracker {
	n := 10000
	return &dgstTracker{
		records: make([]dgstTrackerItem, n),
	}
}

func (d *dgstTracker) add(dgst digest.Digest, action string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.head++
	if d.head >= len(d.records) {
		d.head = 0
	}
	d.records[d.head].dgst = dgst
	d.records[d.head].action = action
	d.records[d.head].seen = time.Now()
}

// eachNewestFirst walks the ring from newest to oldest, calling fn for each
// populated record. Caller must hold d.mu.
func (d *dgstTracker) eachNewestFirst(fn func(dgstTrackerItem) bool) {
	for i := d.head; i >= 0; i-- {
		if d.records[i].seen.IsZero() {
			return
		}
		if !fn(d.records[i]) {
			return
		}
	}
	for i := len(d.records) - 1; i > d.head; i-- {
		if d.records[i].seen.IsZero() {
			return
		}
		if !fn(d.records[i]) {
			return
		}
	}
}

func (d *dgstTracker) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var sb strings.Builder
	d.eachNewestFirst(func(it dgstTrackerItem) bool {
		sb.WriteString(fmt.Sprintf("%s %s %s; ", it.dgst, it.action, it.seen))
		return true
	})
	return sb.String()
}

// historyFor returns up to max recorded actions for a single digest, newest
// first. Unlike String() it is bounded and digest-scoped, so it stays well
// under log-ingestion truncation limits and preserves the ordering that
// matters for diagnosing "inconsistent graph state" (e.g. a delete preceding
// a get-edge-not-found for the same digest).
func (d *dgstTracker) historyFor(dgst digest.Digest, max int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var sb strings.Builder
	shown, total := 0, 0
	d.eachNewestFirst(func(it dgstTrackerItem) bool {
		if it.dgst != dgst {
			return true
		}
		total++
		if shown < max {
			sb.WriteString(fmt.Sprintf("%s %s; ", it.action, it.seen))
			shown++
		}
		return true
	})
	if total > shown {
		sb.WriteString(fmt.Sprintf("(+%d older)", total-shown))
	}
	if total == 0 {
		return "(no prior records for digest)"
	}
	return sb.String()
}
