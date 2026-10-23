package ingest

import "golang.org/x/sync/errgroup"

// NewGroup is a CI demo: it imports a module that go.mod lists as indirect.
func NewGroup() *errgroup.Group { return new(errgroup.Group) }
