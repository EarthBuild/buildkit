package solver

// earthly-specific: this is used to collect information related to "inconsistent graph state" errors

import (
	"fmt"
	"strings"
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
	d.head++
	if d.head >= len(d.records) {
		d.head = 0
	}
	d.records[d.head].dgst = dgst
	d.records[d.head].action = action
	d.records[d.head].seen = time.Now()
}

func (d *dgstTracker) String() string {
	var sb strings.Builder

	for i := d.head; i >= 0; i-- {
		if d.records[i].seen.IsZero() {
			break
		}
		sb.WriteString(fmt.Sprintf("%s %s %s; ", d.records[i].dgst, d.records[i].action, d.records[i].seen))
	}
	for i := len(d.records) - 1; i > d.head; i-- {
		if d.records[i].seen.IsZero() {
			break
		}
		sb.WriteString(fmt.Sprintf("%s %s %s; ", d.records[i].dgst, d.records[i].action, d.records[i].seen))
	}
	return sb.String()
}
