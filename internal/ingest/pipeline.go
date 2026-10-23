package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/hrhelena/elisa-task/internal/mailparse"
	"github.com/hrhelena/elisa-task/internal/store"
)

// Skip reasons added by the engine, next to mailparse's own SkipReasons.
const (
	SkipOutsideMailbox = "outside_mailbox"  // regular file at the maildir root
	SkipNotRegularFile = "not_regular_file" // symlink, device, ...
	SkipParsePanic     = "parse_panic"      // the parser panicked on this file
	SkipParseError     = "parse_error"      // the parser broke its contract (not a *SkipError)
)

// job is one entry to parse.
type job struct {
	rel  string
	open OpenFunc
}

// pipeline is one run's walker -> workers -> batch writer pipeline.
type pipeline struct {
	s     *Service
	runID int64
	c     *counters
	log   *sampler
}

// run walks the source and stores every message. It returns nil when the
// whole dataset was processed, else the first fatal error (ctx canceled,
// dataset root lost, a batch that failed after its retries).
func (p *pipeline) run(ctx context.Context) error {
	cfg := p.s.cfg
	g, ctx := errgroup.WithContext(ctx)
	jobs := make(chan job, cfg.Workers*4)
	msgs := make(chan store.Message, cfg.BatchSize)

	g.Go(func() error {
		defer close(jobs)
		return p.walk(ctx, jobs)
	})
	var workers sync.WaitGroup
	for range cfg.Workers {
		workers.Add(1)
		g.Go(func() error {
			defer workers.Done()
			return p.parseLoop(ctx, jobs, msgs)
		})
	}
	g.Go(func() error {
		workers.Wait()
		close(msgs)
		return nil
	})
	g.Go(func() error { return p.writeLoop(ctx, msgs) })
	return g.Wait()
}

func (p *pipeline) walk(ctx context.Context, jobs chan<- job) error {
	err := p.s.src.Walk(ctx, func(rel string, open OpenFunc) error {
		p.c.fileSeen()
		p.s.m.files.Inc()
		select {
		case jobs <- job{rel: rel, open: open}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("walking dataset: %w", err)
	}
	return err
}

func (p *pipeline) parseLoop(ctx context.Context, jobs <-chan job, msgs chan<- store.Message) error {
	for {
		var (
			j  job
			ok bool
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case j, ok = <-jobs:
			if !ok {
				return nil
			}
		}
		m, ok := p.process(ctx, j)
		if !ok {
			continue
		}
		select {
		case msgs <- m:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// process opens and parses one entry. It returns false if the entry is
// skipped; the skip is counted and logged. It never fails (ADR 0007).
func (p *pipeline) process(ctx context.Context, j job) (store.Message, bool) {
	rc, err := j.open()
	if err != nil {
		reason := string(mailparse.SkipReadError)
		if errors.Is(err, ErrNotRegularFile) {
			reason = SkipNotRegularFile
		}
		p.skip(ctx, j.rel, reason, err)
		return store.Message{}, false
	}
	defer func() { _ = rc.Close() }() // read-only

	mailbox, folder, ok := splitPath(j.rel)
	if !ok {
		p.skip(ctx, j.rel, SkipOutsideMailbox, nil)
		return store.Message{}, false
	}

	pm, err := safeParse(p.s.parse, rc)
	if err != nil {
		var se *mailparse.SkipError
		reason := SkipParseError
		switch {
		case errors.As(err, &se):
			reason = string(se.Reason)
		case errors.Is(err, errParsePanic):
			reason = SkipParsePanic
		}
		p.skip(ctx, j.rel, reason, err)
		return store.Message{}, false
	}

	p.fieldErrors(ctx, j.rel, pm)
	return toStoreMessage(j.rel, mailbox, folder, pm, p.runID), true
}

// fieldErrors counts each distinct issue of a message once (ticket #7:
// field_errors are numbers of messages, though mailparse may report an issue
// once per header, for example invalid_recipient for To and Cc).
func (p *pipeline) fieldErrors(ctx context.Context, rel string, m *mailparse.Message) {
	for i, is := range m.Issues {
		if slices.Contains(m.Issues[:i], is) {
			continue
		}
		p.c.fieldError(string(is))
		p.s.m.fieldErrors.WithLabelValues(string(is)).Inc()
		if p.log.allow("issue:" + string(is)) {
			p.s.logger.WarnContext(ctx, "message has a field error",
				slog.String("path", rel), slog.String("reason", string(is)))
		}
	}
}

func (p *pipeline) skip(ctx context.Context, rel, reason string, err error) {
	p.c.skipped(reason)
	p.s.m.skipped.WithLabelValues(reason).Inc()
	if p.log.allow("skip:" + reason) {
		attrs := []any{slog.String("path", rel), slog.String("reason", reason)}
		if err != nil {
			attrs = append(attrs, slog.Any("err", err))
		}
		p.s.logger.WarnContext(ctx, "skipping dataset entry", attrs...)
	}
}

// writeLoop batches messages and writes a batch when it is full, every
// FlushInterval, and when msgs is closed.
func (p *pipeline) writeLoop(ctx context.Context, msgs <-chan store.Message) error {
	size := p.s.cfg.BatchSize
	batch := make([]store.Message, 0, size)
	tick := time.NewTicker(p.s.cfg.FlushInterval)
	defer tick.Stop()
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := p.writeBatch(ctx, batch)
		batch = batch[:0] // InsertMessages does not keep the slice
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-msgs:
			if !ok {
				return flush()
			}
			batch = append(batch, m)
			if len(batch) >= size {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-tick.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// writeBatch inserts one batch, retrying a failed insert MaxRetries times
// with exponential backoff. Rows are keyed by source path, so a retry of a
// batch that was in fact committed inserts nothing twice.
func (p *pipeline) writeBatch(ctx context.Context, batch []store.Message) error {
	cfg := p.s.cfg
	for attempt := 0; ; attempt++ {
		start := time.Now()
		inserted, err := p.s.st.InsertMessages(ctx, batch)
		p.s.m.batchDuration.Observe(time.Since(start).Seconds())
		if err == nil {
			p.c.stored(int64(len(batch)), inserted)
			p.s.m.messages.Add(float64(len(batch)))
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt >= cfg.MaxRetries || errors.Is(err, store.ErrInvalidMessage) {
			return fmt.Errorf("batch insert of %d messages failed after %d attempts: %w", len(batch), attempt+1, err)
		}
		backoff := cfg.RetryBackoff << attempt
		p.s.logger.WarnContext(ctx, "batch insert failed, retrying",
			slog.Int("messages", len(batch)), slog.Int("attempt", attempt+1),
			slog.Duration("backoff", backoff), slog.Any("err", err))
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// splitPath maps "allen-p/_sent_mail/1." to mailbox "allen-p" and folder
// "_sent_mail". Nested folders are joined with "/"; a file directly in a
// mailbox has folder "". ok is false for an entry at the root.
func splitPath(rel string) (mailbox, folder string, ok bool) {
	mailbox, rest, ok := strings.Cut(rel, "/")
	if !ok || mailbox == "" || rest == "" {
		return "", "", false
	}
	if i := strings.LastIndexByte(rest, '/'); i >= 0 {
		folder = rest[:i]
	}
	return mailbox, folder, true
}

var errParsePanic = errors.New("parser panicked")

// parseFunc is mailparse.Parse; tests substitute it to inject panics.
type parseFunc func(io.Reader) (*mailparse.Message, error)

// safeParse runs parse and turns a panic into an error, so one bad file
// cannot take the process down (ticket #7).
func safeParse(parse parseFunc, r io.Reader) (m *mailparse.Message, err error) {
	defer func() {
		if v := recover(); v != nil {
			m, err = nil, fmt.Errorf("%w: %v", errParsePanic, v)
		}
	}()
	return parse(r)
}

// toStoreMessage maps a parsed message to its row. Empty optional strings
// become nil (NULL) and a zero date becomes a nil SentAt.
func toStoreMessage(rel, mailbox, folder string, m *mailparse.Message, runID int64) store.Message {
	msg := store.Message{
		SourcePath:    rel,
		Mailbox:       mailbox,
		Folder:        folder,
		MessageID:     optional(m.MessageID),
		DateRaw:       optional(m.DateRaw),
		FromAddr:      optional(m.From),
		FromRaw:       optional(m.FromRaw),
		XFrom:         optional(m.XFrom),
		Subject:       optional(m.Subject),
		To:            m.To,
		Cc:            m.Cc,
		Bcc:           m.Bcc,
		ContentSHA256: m.ContentSHA256[:],
		RunID:         runID,
	}
	if !m.Date.IsZero() {
		d := m.Date
		msg.SentAt = &d
	}
	return msg
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
