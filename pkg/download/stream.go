package download

import (
	"errors"

	"github.com/GopeedLab/gopeed/internal/fetcher"
)

// StreamReader reads one file of a task that may still be downloading; see
// Downloader.Stream.
type StreamReader = fetcher.StreamReader

// ErrStreamUnsupported is returned by Stream for a task whose protocol cannot
// read a file before it is complete, or that has not been started.
var ErrStreamUnsupported = errors.New("this task cannot be read while it downloads")

// Stream opens file, by its index in the task's resource, for reading while
// the task runs. Reads wait for bytes that have not arrived yet, and the task
// fetches the part being read ahead of the rest until the reader is closed.
func (d *Downloader) Stream(id string, file int) (StreamReader, error) {
	task := d.GetTask(id)
	if task == nil {
		return nil, ErrTaskNotFound
	}
	task.statusLock.Lock()
	f := task.fetcher
	task.statusLock.Unlock()
	s, ok := f.(fetcher.Streamer)
	if !ok {
		return nil, ErrStreamUnsupported
	}
	return s.Stream(file)
}
