package ingest

import (
	"maps"
	"sync"

	"github.com/hrhelena/elisa-task/internal/store"
)

// counters are the in-memory progress counters of one run, shared by the
// walker, the workers and the writer, and copied out for progress writes.
type counters struct {
	mu sync.Mutex
	c  store.Counters
}

func newCounters() *counters {
	return &counters{c: store.Counters{SkipReasons: map[string]int64{}, FieldErrors: map[string]int64{}}}
}

func (c *counters) fileSeen() {
	c.mu.Lock()
	c.c.FilesSeen++
	c.mu.Unlock()
}

func (c *counters) skipped(reason string) {
	c.mu.Lock()
	c.c.MessagesSkipped++
	c.c.SkipReasons[reason]++
	c.mu.Unlock()
}

func (c *counters) fieldError(issue string) {
	c.mu.Lock()
	c.c.FieldErrors[issue]++
	c.mu.Unlock()
}

// stored records a written batch: processed messages and newly inserted rows.
func (c *counters) stored(processed, inserted int64) {
	c.mu.Lock()
	c.c.MessagesProcessed += processed
	c.c.MessagesInserted += inserted
	c.mu.Unlock()
}

// snapshot returns a copy that is safe to use while the run goes on.
func (c *counters) snapshot() store.Counters {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.c
	s.SkipReasons = maps.Clone(c.c.SkipReasons)
	s.FieldErrors = maps.Clone(c.c.FieldErrors)
	return s
}

// Log sampling (ADR 0007): the first sampleFirst events per key and run are
// logged, then one in sampleEvery.
const (
	sampleFirst = 100
	sampleEvery = 1000
)

// sampler decides which per-file WARN logs are written in one run.
type sampler struct {
	mu   sync.Mutex
	seen map[string]int64
}

func newSampler() *sampler { return &sampler{seen: map[string]int64{}} }

// allow counts one event for key and reports whether to log it.
func (s *sampler) allow(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[key]++
	n := s.seen[key]
	return n <= sampleFirst || n%sampleEvery == 0
}
