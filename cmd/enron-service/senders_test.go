package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/store"
)

type rankerFunc func(ctx context.Context, limit int) ([]store.SenderCount, error)

func (f rankerFunc) TopSenders(ctx context.Context, limit int) ([]store.SenderCount, error) {
	return f(ctx, limit)
}

func TestSenderStatsAdapter(t *testing.T) {
	t.Parallel()

	t.Run("converts rows and passes the limit", func(t *testing.T) {
		t.Parallel()
		var gotLimit int
		a := senderStats{rankerFunc(func(_ context.Context, limit int) ([]store.SenderCount, error) {
			gotLimit = limit
			return []store.SenderCount{{Email: "a@enron.com", Count: 3}, {Email: "b@enron.com", Count: 1}}, nil
		})}
		got, err := a.TopSenders(t.Context(), 7)
		want := []httpapi.SenderCount{{Email: "a@enron.com", Count: 3}, {Email: "b@enron.com", Count: 1}}
		if err != nil || !slices.Equal(got, want) || gotLimit != 7 {
			t.Errorf("TopSenders(7) = %v, %v (store limit %d), want %v, nil (7)", got, err, gotLimit, want)
		}
	})

	t.Run("empty store result is an empty, non-nil slice", func(t *testing.T) {
		t.Parallel()
		a := senderStats{rankerFunc(func(context.Context, int) ([]store.SenderCount, error) { return nil, nil })}
		if got, err := a.TopSenders(t.Context(), 10); err != nil || got == nil || len(got) != 0 {
			t.Errorf("TopSenders = %#v, %v, want empty non-nil slice", got, err)
		}
	})

	t.Run("store errors pass through", func(t *testing.T) {
		t.Parallel()
		a := senderStats{rankerFunc(func(context.Context, int) ([]store.SenderCount, error) {
			return nil, context.DeadlineExceeded
		})}
		if _, err := a.TopSenders(t.Context(), 10); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want context.DeadlineExceeded", err)
		}
	})
}
