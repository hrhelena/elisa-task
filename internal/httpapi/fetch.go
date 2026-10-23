package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/hrhelena/elisa-task/internal/fetch"
)

// Error codes of POST /fetch.
const (
	CodeDataDirNotWritable  = "data_dir_not_writable"
	CodeMaildirNotDirectory = "maildir_not_directory"
	CodeMaildirUnreadable   = "maildir_unreadable"
)

// Fetcher starts the dataset download (ADR 0004). Start must not block on the
// download; *fetch.Fetcher implements it.
type Fetcher interface {
	Start() (fetch.Progress, error)
}

// datasetJSON is the dataset progress as served by POST /fetch and by the
// dataset section of GET /status.
//
// The struct tags other than json only describe the OpenAPI schema (ADR 0016).
type datasetJSON struct {
	State           fetch.State `json:"state" required:"true" enum:"absent,downloading,extracting,ready,failed" description:"Dataset lifecycle state."`
	Path            string      `json:"path" required:"true" description:"Where the maildir is (or will be) on the server."`
	BytesDownloaded int64       `json:"bytes_downloaded" required:"true" minimum:"0" description:"Archive bytes downloaded so far."`
	BytesTotal      int64       `json:"bytes_total" required:"true" minimum:"-1" description:"Archive size from Content-Length; -1 until the server sends a length."`
	FilesExtracted  int64       `json:"files_extracted" required:"true" minimum:"0" description:"Files extracted from the archive so far."`
	Error           *string     `json:"error" required:"true" description:"Why the last fetch failed; null unless state is failed."`
	StartedAt       *time.Time  `json:"started_at" required:"true" description:"When the current or last fetch started; null if none has."`
	FinishedAt      *time.Time  `json:"finished_at" required:"true" description:"When the last fetch ended; null while running or if none has."`
}

func newDatasetJSON(p fetch.Progress) datasetJSON {
	d := datasetJSON{
		State:           p.State,
		Path:            p.Path,
		BytesDownloaded: p.BytesDownloaded,
		BytesTotal:      p.BytesTotal,
		FilesExtracted:  p.FilesExtracted,
		StartedAt:       p.StartedAt,
		FinishedAt:      p.FinishedAt,
	}
	if p.Error != "" {
		d.Error = &p.Error
	}
	return d
}

// fetchHandler serves POST /fetch: 200 when the dataset is ready, 202 with
// Location: /status when a fetch was started or is already running.
func fetchHandler(f Fetcher, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := f.Start()
		switch {
		case errors.Is(err, fetch.ErrClosed):
			// First: shutdown wins over anything it is joined with.
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the service is shutting down")
			return
		case errors.Is(err, fetch.ErrTransient):
			// Out of file descriptors or memory: nothing is wrong with the
			// maildir, and nothing was changed; retry (#49).
			logger.WarnContext(r.Context(), "checking the maildir failed temporarily",
				slog.Any("err", err), slog.String("request_id", RequestIDFromContext(r.Context())))
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, transientMessage)
			return
		case errors.Is(err, fetch.ErrDataDirNotWritable):
			writeError(w, http.StatusInternalServerError, CodeDataDirNotWritable,
				"the data directory is not writable; check DATA_DIR and the volume permissions")
			return
		case errors.Is(err, fetch.ErrMaildirNotDirectory):
			// A state conflict, like ADR 0011's dataset_missing: the caller
			// must remove the file before a fetch can succeed.
			writeError(w, http.StatusConflict, CodeMaildirNotDirectory,
				"$DATA_DIR/maildir exists but is not a directory (symlinks are not followed); remove it and retry")
			return
		case errors.Is(err, fetch.ErrMaildirUnreadable):
			writeError(w, http.StatusConflict, CodeMaildirUnreadable,
				"$DATA_DIR/maildir is a directory the service cannot read; fix its permissions or remove it and retry")
			return
		case err != nil:
			logger.ErrorContext(r.Context(), "starting fetch failed", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, CodeInternal, "starting the fetch failed")
			return
		}
		if p.State == fetch.StateReady {
			writeJSON(w, http.StatusOK, newDatasetJSON(p))
			return
		}
		w.Header().Set("Location", "/status")
		writeJSON(w, http.StatusAccepted, newDatasetJSON(p))
	})
}
