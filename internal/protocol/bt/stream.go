package bt

import (
	"errors"
	"fmt"
	"sync"

	"github.com/GopeedLab/gopeed/internal/fetcher"
	"github.com/anacrolix/torrent"
)

const (
	// streamEdge is how much of each end of a file is fetched first while it
	// is streamed, since players read a container's index from the start or
	// the end of a file before they play anything.
	streamEdge = 8 << 20
	// streamReadahead is the least a reader asks for ahead of where it reads.
	// The library's own readahead grows from nothing with each contiguous
	// read, which leaves a player that has just seeked waiting piece by piece.
	streamReadahead = 16 << 20
)

var errTorrentNotReady = errors.New("the torrent's file list is not known yet")

// streams counts the open readers of each file of a fetcher, so the pieces at
// either end of a file are raised by the first reader and put back by the last.
type streams struct {
	mu    sync.Mutex
	files map[int]int
}

func (f *Fetcher) Stream(index int) (fetcher.StreamReader, error) {
	if !f.torrentReady.Load() {
		return nil, errTorrentNotReady
	}
	files := f.torrent.Files()
	if index < 0 || index >= len(files) {
		return nil, fmt.Errorf("the torrent has no file %d", index)
	}
	file := files[index]

	f.streams.mu.Lock()
	if f.streams.files == nil {
		f.streams.files = map[int]int{}
	}
	if f.streams.files[index] == 0 {
		f.setEdgePriority(file, torrent.PiecePriorityHigh)
	}
	f.streams.files[index]++
	f.streams.mu.Unlock()

	r := file.NewReader()
	r.SetResponsive()
	r.SetReadaheadFunc(func(c torrent.ReadaheadContext) int64 {
		return max(streamReadahead, c.CurrentPos-c.ContiguousReadStartPos)
	})
	return &streamReader{Reader: r, closed: func() { f.closeStream(index, file) }}, nil
}

func (f *Fetcher) closeStream(index int, file *torrent.File) {
	f.streams.mu.Lock()
	defer f.streams.mu.Unlock()
	f.streams.files[index]--
	if f.streams.files[index] > 0 {
		return
	}
	delete(f.streams.files, index)
	// Start gives every piece a priority of its own only when it fetches the
	// whole torrent; a selection leaves that to each file's priority.
	back := torrent.PiecePriorityNone
	if len(f.meta.Opts.SelectFiles) == len(f.torrent.Files()) {
		back = torrent.PiecePriorityNormal
	}
	f.setEdgePriority(file, back)
}

// setEdgePriority sets the priority of the pieces that hold the first and the
// last streamEdge bytes of file.
func (f *Fetcher) setEdgePriority(file *torrent.File, prio torrent.PiecePriority) {
	pieceLen := f.torrent.Info().PieceLength
	if pieceLen <= 0 || file.Length() == 0 || f.torrentDropCtx.Err() != nil {
		return
	}
	begin, end := file.BeginPieceIndex(), file.EndPieceIndex()
	edge := int((streamEdge + pieceLen - 1) / pieceLen)
	for i := begin; i < end; i++ {
		if i < begin+edge || i >= end-edge {
			f.torrent.Piece(i).SetPriority(prio)
		}
	}
}

type streamReader struct {
	torrent.Reader
	once   sync.Once
	closed func()
}

func (r *streamReader) Close() error {
	err := r.Reader.Close()
	r.once.Do(r.closed)
	return err
}
