package bt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GopeedLab/gopeed/pkg/base"
	"github.com/anacrolix/torrent"
)

func startOffline(t *testing.T) *Fetcher {
	f := buildConfigFetcherWith(config{DisableDHT: true, DisablePEX: true}).(*Fetcher)
	if err := f.Resolve(&base.Request{URL: "./testdata/test.torrent"}, &base.Options{Path: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func largestFile(f *Fetcher) (int, *torrent.File) {
	at := 0
	files := f.torrent.Files()
	for i, file := range files {
		if file.Length() > files[at].Length() {
			at = i
		}
	}
	return at, files[at]
}

// priorityOf is the piece's priority once the library knows whether it has
// the piece; until then it reports none for every piece.
func priorityOf(f *Fetcher, piece int) torrent.PiecePriority {
	deadline := time.Now().Add(5 * time.Second)
	for {
		state := f.torrent.Piece(piece).State()
		if state.Ok && !state.Checking || time.Now().After(deadline) {
			return state.Priority
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStream_RaisesBothEndsOfTheFileUntilTheLastReaderCloses(t *testing.T) {
	f := startOffline(t)
	index, file := largestFile(f)
	first, last := file.BeginPieceIndex(), file.EndPieceIndex()-1
	middle := (first + last) / 2
	if priorityOf(f, last) != torrent.PiecePriorityNormal {
		t.Fatalf("last piece before streaming: priority %v, want normal", priorityOf(f, last))
	}

	a, err := f.Stream(index)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.Stream(index)
	if err != nil {
		t.Fatal(err)
	}
	if got := priorityOf(f, last); got < torrent.PiecePriorityHigh {
		t.Fatalf("last piece while streaming: priority %v, want at least high", got)
	}
	if got := priorityOf(f, middle); got != torrent.PiecePriorityNormal {
		t.Fatalf("middle piece while streaming: priority %v, want normal", got)
	}

	a.Close()
	if got := priorityOf(f, last); got < torrent.PiecePriorityHigh {
		t.Fatalf("last piece with one reader left: priority %v, want at least high", got)
	}
	b.Close()
	if got := priorityOf(f, last); got != torrent.PiecePriorityNormal {
		t.Fatalf("last piece after the last reader closed: priority %v, want normal", got)
	}
	if got := priorityOf(f, first); got != torrent.PiecePriorityNormal {
		t.Fatalf("first piece after the last reader closed: priority %v, want normal", got)
	}
}

func TestStream_ReadWithoutPeersWaitsForItsContext(t *testing.T) {
	f := startOffline(t)
	index, _ := largestFile(f)
	r, err := f.Stream(index)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	n, err := r.ReadContext(ctx, make([]byte, 1024))
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read without peers: %d bytes, %v; want a deadline error", n, err)
	}
}

func TestStream_RefusesAFileTheTorrentDoesNotHave(t *testing.T) {
	f := startOffline(t)
	if _, err := f.Stream(len(f.torrent.Files())); err == nil {
		t.Fatal("streamed a file past the end of the torrent")
	}
}
