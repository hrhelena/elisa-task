package ingest

import (
	"context"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkMaildirSourceFullCorpus walks a local copy of the full dataset and
// reads every file with NumCPU workers, like the pipeline but without parsing
// or the database. It isolates the cost of the walk and the file opens (#49:
// opening files through an os.Root). It is skipped unless ENRON_MAILDIR is
// set, for example:
//
//	ENRON_MAILDIR=$PWD/Dataset/maildir go test -run '^$' -bench FullCorpus -benchtime 3x ./internal/ingest
func BenchmarkMaildirSourceFullCorpus(b *testing.B) {
	root := os.Getenv("ENRON_MAILDIR")
	if root == "" {
		b.Skip("ENRON_MAILDIR not set")
	}
	src := NewMaildirSource(root)
	var files int64
	for b.Loop() {
		files = readAll(b, src)
	}
	b.ReportMetric(float64(files)*float64(b.N)/b.Elapsed().Seconds(), "files/s")
}

func readAll(b *testing.B, src Source) int64 {
	b.Helper()
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	jobs := make(chan OpenFunc, runtime.NumCPU()*4)
	var n atomic.Int64
	var wg sync.WaitGroup
	for range runtime.NumCPU() {
		wg.Go(func() {
			for open := range jobs {
				rc, err := open()
				if err != nil {
					b.Error(err)
					continue
				}
				_, _ = io.Copy(io.Discard, rc)
				_ = rc.Close()
				n.Add(1)
			}
		})
	}
	err := src.Walk(ctx, func(_ string, open OpenFunc) error {
		jobs <- open
		return nil
	})
	close(jobs)
	wg.Wait()
	if err != nil {
		b.Fatal(err)
	}
	return n.Load()
}
