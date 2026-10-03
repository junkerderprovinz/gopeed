package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/GopeedLab/gopeed/internal/fetcher"
	fhttp "github.com/GopeedLab/gopeed/pkg/protocol/http"
)

const (
	// streamPoll is how often a waiting read looks for its bytes again.
	streamPoll = 50 * time.Millisecond
	// A connection reaches a byte soon when it is at most streamNearSeconds
	// of its own speed, and never less than streamNear bytes, away.
	streamNear        = 1 << 20
	streamNearSeconds = 2
	// streamEdge is how much of the end of a file is fetched as soon as a
	// reader first reads, since many players read a container's index from
	// there before they play anything.
	streamEdge = 8 << 20
	// streamSplitGap keeps a split away from where a connection that is
	// still winding down may be writing.
	streamSplitGap = 64 << 10
)

var errStreamStopped = errors.New("the download is not running, so the bytes asked for will not arrive")

// Stream opens the file for reading while it downloads. A read that reaches
// bytes no connection has written waits for them, and moves a connection to
// them when none would get there soon (see focus).
func (f *Fetcher) Stream(index int) (fetcher.StreamReader, error) {
	if index != 0 {
		return nil, fmt.Errorf("an HTTP download has one file, not %d", index+1)
	}
	if f.meta.Res == nil || f.meta.Res.Size <= 0 {
		return nil, errors.New("the size of the file is not known, so it cannot be read before it is complete")
	}
	r := &streamReader{f: f}
	f.connMu.Lock()
	if f.readers == nil {
		f.readers = map[*streamReader]int64{}
	}
	f.readers[r] = 0
	f.connMu.Unlock()
	return r, nil
}

func (f *Fetcher) maxConnections() int {
	if extra, ok := f.meta.Opts.Extra.(*fhttp.OptsExtra); ok && extra.Connections > 0 {
		return extra.Connections
	}
	return 1
}

func (c *connection) writePos() int64 {
	return c.Chunk.Begin + c.Chunk.Downloaded
}

func (c *connection) active() bool {
	return c.running && !c.parked
}

// near is how far ahead of c a byte may be and still arrive soon.
func (c *connection) near() int64 {
	return max(streamNear, c.speed*streamNearSeconds)
}

// missingAtLocked returns the connection whose unwritten range holds pos, nil
// when pos has been written. Every byte of a ranged download belongs to the
// range of one connection until that connection has written it, so what is
// missing is exactly the part of each range past its connection's position.
func (f *Fetcher) missingAtLocked(pos int64) *connection {
	for _, c := range f.connections {
		if c.Chunk != nil && pos >= c.writePos() && pos <= c.Chunk.End {
			return c
		}
	}
	return nil
}

// availableLocked is how many bytes from pos on are written, up to want.
func (f *Fetcher) availableLocked(pos int64, want int) int64 {
	size := f.meta.Res.Size
	limit := min(size, pos+int64(want))
	switch {
	case f.getState() == stateDone:
	case !f.meta.Res.Range:
		if len(f.connections) == 0 {
			return 0
		}
		limit = min(limit, f.connections[0].Chunk.Downloaded)
	case len(f.connections) == 0:
		// What the resolve request fetched is written before the first
		// connection exists.
		limit = min(limit, f.resolveDataPos.Load())
	default:
		for _, c := range f.connections {
			if c.Chunk == nil || c.writePos() > c.Chunk.End {
				continue
			}
			if pos >= c.writePos() && pos <= c.Chunk.End {
				return 0
			}
			if c.writePos() > pos {
				limit = min(limit, c.writePos())
			}
		}
	}
	return max(0, limit-pos)
}

// servesReaderLocked reports whether c is writing where a reader reads, or
// just ahead of it.
func (f *Fetcher) servesReaderLocked(c *connection) bool {
	w, near := c.writePos(), c.near()
	for _, r := range f.readers {
		if r >= w-near && r <= w+near && r <= c.Chunk.End {
			return true
		}
	}
	return false
}

// focus makes a connection fetch pos unless one will reach it soon anyway.
// The number of connections fetching stays the same: the one taken for pos is
// the connection whose range holds pos, or else one no reader is waiting on,
// and the range it leaves behind waits for the first connection to be free.
func (f *Fetcher) focus(pos int64) {
	if !f.meta.Res.Range {
		return
	}
	if s := f.getState(); s != stateSlowStart && s != stateSteady {
		return
	}
	f.connMu.Lock()
	defer f.connMu.Unlock()
	if f.ctx == nil || f.ctx.Err() != nil {
		return
	}
	owner := f.missingAtLocked(pos)
	if owner == nil {
		return
	}
	if owner.active() && pos-owner.writePos() <= owner.near() {
		return
	}
	if owner.running && pos-owner.writePos() < streamSplitGap {
		return
	}
	var victim *connection
	if owner.active() && !f.servesReaderLocked(owner) {
		victim = owner
	} else {
		for _, c := range f.connections {
			if c != owner && c.active() && !f.servesReaderLocked(c) {
				victim = c
				break
			}
		}
	}
	if victim == nil {
		return
	}

	end := owner.Chunk.End
	owner.Chunk.End = pos - 1
	if owner.Chunk.remain() <= 0 && !owner.running {
		owner.parked = false
		owner.Completed = true
		owner.State = connCompleted
	}
	conn := &connection{
		ID:      len(f.connections),
		Role:    roleWorker,
		State:   connNotStarted,
		Chunk:   newChunk(pos, end),
		running: true,
	}
	conn.ctx, conn.cancel = context.WithCancel(f.ctx)
	f.connections = append(f.connections, conn)
	victim.parked = true
	victim.cancel()
	f.wg.Add(1)
	go f.runConnection(conn)
}

// adoptParkedLocked hands helper the unwritten rest of a parked connection's
// range.
func (f *Fetcher) adoptParkedLocked(helper *connection) bool {
	for _, p := range f.connections {
		if p == helper || !p.parked || p.running || p.Chunk.remain() <= 0 {
			continue
		}
		helper.Chunk.Begin = p.writePos()
		helper.Chunk.End = p.Chunk.End
		helper.Chunk.Downloaded = 0
		p.Chunk.End = helper.Chunk.Begin - 1
		p.parked = false
		p.Completed = true
		p.State = connCompleted
		return true
	}
	return false
}

// restartParked starts the parked connections whose ranges nobody adopted,
// up to the connection limit, and reports whether it started any. It runs once
// every connection has stopped, so they cannot be adopted any more.
func (f *Fetcher) restartParked() bool {
	f.connMu.Lock()
	defer f.connMu.Unlock()
	var started int
	for _, c := range f.connections {
		if started == f.maxConnections() {
			break
		}
		if !c.parked || c.running || c.Chunk.remain() <= 0 {
			continue
		}
		c.ctx, c.cancel = context.WithCancel(f.ctx)
		c.State = connNotStarted
		c.failed = false
		c.parked = false
		c.running = true
		f.wg.Add(1)
		go f.runConnection(c)
		started++
	}
	return started > 0
}

// streamReader reads the file of a running download from its own handle.
type streamReader struct {
	f    *Fetcher
	pos  int64
	file *os.File
	// read is set by the first read. A reader that is opened and closed
	// without one, to learn the size, leaves the download as it was.
	read bool
}

func (r *streamReader) Read(p []byte) (int, error) {
	return r.ReadContext(context.Background(), p)
}

func (r *streamReader) ReadContext(ctx context.Context, p []byte) (int, error) {
	f := r.f
	if r.pos >= f.meta.Res.Size {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	f.connMu.Lock()
	f.readers[r] = r.pos
	f.connMu.Unlock()
	if !r.read {
		r.read = true
		if size := f.meta.Res.Size; size > 2*streamEdge {
			f.focus(size - streamEdge)
		}
	}
	for {
		f.connMu.Lock()
		n := f.availableLocked(r.pos, len(p))
		f.connMu.Unlock()
		if n > 0 {
			return r.readAt(p[:n])
		}
		switch f.getState() {
		case stateResolving, stateResolved, stateSlowStart, stateSteady:
		default:
			return 0, errStreamStopped
		}
		f.focus(r.pos)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(streamPoll):
		}
	}
}

func (r *streamReader) readAt(p []byte) (int, error) {
	if r.file == nil {
		file, err := openShared(r.f.meta.SingleFilepath())
		if err != nil {
			return 0, err
		}
		r.file = file
	}
	n, err := r.file.ReadAt(p, r.pos)
	r.pos += int64(n)
	if n > 0 && err == io.EOF {
		err = nil
	}
	return n, err
}

func (r *streamReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.pos
	case io.SeekEnd:
		offset += r.f.meta.Res.Size
	default:
		return r.pos, errors.New("invalid whence")
	}
	if offset < 0 {
		return r.pos, errors.New("negative position")
	}
	r.pos = offset
	r.f.connMu.Lock()
	r.f.readers[r] = offset
	r.f.connMu.Unlock()
	return offset, nil
}

func (r *streamReader) Close() error {
	r.f.connMu.Lock()
	delete(r.f.readers, r)
	r.f.connMu.Unlock()
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
