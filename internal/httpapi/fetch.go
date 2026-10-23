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
)

// Fetcher starts the dataset download (ADR 0004). Start must not block on the
// download; *fetch.Fetcher implements it.
type Fetcher interface {
	Start() (fetch.Progress, error)
}

// datasetJSON is the dataset progress as served by POST /fetch (and, with
// #8, by the dataset section of GET /status).
type datasetJSON struct {
	State           fetch.State `json:"state"`
	Path            string      `json:"path"`
	BytesDownloaded int64       `json:"bytes_downloaded"`
	BytesTotal      int64       `json:"bytes_total"`
	FilesExtracted  int64       `json:"files_extracted"`
	Error           *string     `json:"error"`
	StartedAt       *time.Time  `json:"started_at"`
	FinishedAt      *time.Time  `json:"finished_at"`
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
		case errors.Is(err, fetch.ErrDataDirNotWritable):
			writeError(w, http.StatusInternalServerError, CodeDataDirNotWritable,
				"the data directory is not writable; check DATA_DIR and the volume permissions")
			return
		case errors.Is(err, fetch.ErrMaildirNotDirectory):
			// A state conflict, like ADR 0011's dataset_missing: the caller
			// must remove the file before a fetch can succeed.
			writeError(w, http.StatusConflict, CodeMaildirNotDirectory,
				"$DATA_DIR/maildir exists but is not a directory; remove it and retry")
			return
		case errors.Is(err, fetch.ErrClosed):
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the service is shutting down")
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
