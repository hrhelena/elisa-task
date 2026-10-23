// Package fetch downloads the Enron dataset archive and extracts it into the
// data directory (ADR 0004).
//
// A Fetcher runs at most one fetch at a time in the background, under its own
// context rather than a request's. The archive is streamed through gzip and
// tar straight to disk (never buffered whole in memory) into a temporary
// directory $DATA_DIR/.fetch-tmp-*, which is renamed to $DATA_DIR/maildir only
// after the whole archive, including the gzip trailer, has been read. A crash
// therefore never leaves a partial maildir that looks complete. Only regular
// files and directories under the archive's maildir/ are extracted; absolute
// or escaping paths, links and special files are skipped and counted.
package fetch
