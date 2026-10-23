package main

import (
	"context"

	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/store"
)

// senderRanker is the store side of GET /top-senders (#4).
type senderRanker interface {
	TopSenders(ctx context.Context, limit int) ([]store.SenderCount, error)
}

// senderStats adapts the store ranking to httpapi.SenderStats, so that
// httpapi does not depend on the storage packages (ADR 0009).
type senderStats struct{ store senderRanker }

var _ httpapi.SenderStats = senderStats{}

func (s senderStats) TopSenders(ctx context.Context, limit int) ([]httpapi.SenderCount, error) {
	rows, err := s.store.TopSenders(ctx, limit)
	if err != nil {
		return nil, err //nolint:wrapcheck // the store error is already wrapped; the adapter adds nothing.
	}
	out := make([]httpapi.SenderCount, len(rows))
	for i, r := range rows {
		out[i] = httpapi.SenderCount{Email: r.Email, Count: r.Count}
	}
	return out, nil
}
