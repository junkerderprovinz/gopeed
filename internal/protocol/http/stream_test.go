package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	gohttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GopeedLab/gopeed/pkg/base"
	fhttp "github.com/GopeedLab/gopeed/pkg/protocol/http"
)

// slowWriter sends a response at about rate bytes a second.
type slowWriter struct {
	gohttp.ResponseWriter
	rate int
}

func (w slowWriter) Write(p []byte) (int, error) {
	const piece = 16 << 10
	var sent int
	for len(p) > 0 {
		n := min(piece, len(p))
		m, err := w.ResponseWriter.Write(p[:n])
		sent += m
		if err != nil {
			return sent, err
		}
		p = p[n:]
		time.Sleep(time.Duration(n) * time.Second / time.Duration(w.rate))
	}
	return sent, nil
}

// slowServer serves data with ranges, each response at rate bytes a second.
func slowServer(t *testing.T, data []byte, rate int) *httptest.Server {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		gohttp.ServeContent(slowWriter{w, rate}, r, "media.bin", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// refusingServer is slowServer answering 403 to every range that starts at or
// past refuseFrom.
func refusingServer(t *testing.T, data []byte, rate int, refuseFrom int64) *httptest.Server {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		var from int64
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &from); err == nil && from >= refuseFrom {
			w.WriteHeader(gohttp.StatusForbidden)
			return
		}
		gohttp.ServeContent(slowWriter{w, rate}, r, "media.bin", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + i/251)
	}
	return data
}

func startStreamFetcher(t *testing.T, srv *httptest.Server, connections int) *Fetcher {
	f := buildFetcher()
	opts := &base.Options{
		Name:  "media.bin",
		Path:  t.TempDir(),
		Extra: &fhttp.OptsExtra{Connections: connections},
	}
	if err := f.Resolve(&base.Request{URL: srv.URL + "/media.bin"}, opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Pause() })
	return f
}

func readAtCtx(ctx context.Context, r interface {
	io.Seeker
	ReadContext(context.Context, []byte) (int, error)
}, off int64, n int) ([]byte, error) {
	if _, err := r.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	out := make([]byte, 0, n)
	buf := make([]byte, n)
	for len(out) < n {
		m, err := r.ReadContext(ctx, buf[:n-len(out)])
		out = append(out, buf[:m]...)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func TestStream_ReadFarAheadArrivesBeforeTheRest(t *testing.T) {
	const size = 24 << 20
	data := testData(size)
	srv := slowServer(t, data, 2<<20)
	f := startStreamFetcher(t, srv, 2)
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}

	r, err := f.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// At 2 MB/s on each of two connections the whole file takes about six
	// seconds; the range read here would be reached last of all without a
	// connection moved to it.
	off := int64(size - 3<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	got, err := readAtCtx(ctx, r, off, 256<<10)
	if err != nil {
		t.Fatalf("read at %d: %v", off, err)
	}
	if !bytes.Equal(got, data[off:off+int64(len(got))]) {
		t.Fatal("read bytes that differ from the file")
	}

	if err := f.Wait(); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(filepath.Join(f.meta.Opts.Path, "media.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(written, data) {
		t.Fatal("the finished file differs from what the server sent")
	}
}

func TestStream_SequentialReadMatchesTheFile(t *testing.T) {
	const size = 4 << 20
	data := testData(size)
	srv := slowServer(t, data, 4<<20)
	f := startStreamFetcher(t, srv, 4)
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	r, err := f.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes that differ from the %d of the file", len(got), len(data))
	}
}

func TestStream_ReadWaitsUntilTheContextEnds(t *testing.T) {
	data := testData(1 << 20)
	srv := slowServer(t, data, 1<<20)
	f := startStreamFetcher(t, srv, 1)
	r, err := f.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	n, err := r.ReadContext(ctx, make([]byte, 1024))
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read of a download not started: %d bytes, %v; want a deadline error", n, err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("the read gave up before its context ended")
	}
}

func TestStream_ReadOfAPausedDownloadFails(t *testing.T) {
	data := testData(8 << 20)
	srv := slowServer(t, data, 1<<20)
	f := startStreamFetcher(t, srv, 1)
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	if err := f.Pause(); err != nil {
		t.Fatal(err)
	}
	r, err := f.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Seek(7<<20, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := r.ReadContext(ctx, make([]byte, 1024)); !errors.Is(err, errStreamStopped) {
		t.Fatalf("read of a paused download: %v, want %v", err, errStreamStopped)
	}
}

func TestStream_RefusesAFileOtherThanTheFirst(t *testing.T) {
	data := testData(1 << 20)
	f := startStreamFetcher(t, slowServer(t, data, 8<<20), 1)
	if _, err := f.Stream(1); err == nil {
		t.Fatal("an HTTP download was streamed as a second file")
	}
}

func TestStream_FinishedFileCanBeRenamedWhileAReaderHoldsIt(t *testing.T) {
	data := testData(1 << 20)
	f := startStreamFetcher(t, slowServer(t, data, 8<<20), 1)
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	r, err := f.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Read(make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}
	if err := f.Wait(); err != nil {
		t.Fatal(err)
	}

	from := filepath.Join(f.meta.Opts.Path, "media.bin")
	to := filepath.Join(f.meta.Opts.Path, "renamed.bin")
	if err := os.Rename(from, to); err != nil {
		t.Fatalf("rename with a reader open: %v", err)
	}
	got, err := readAtCtx(context.Background(), r, 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[1024:2048]) {
		t.Fatal("the reader read other bytes after the rename")
	}
}

// A reader waiting on a range the server refuses moves a connection there
// again and again, and each time the parked one is started once more after
// every connection has stopped. Only one goroutine may wait for that.
func TestStream_ReaderAtARefusedRangeKeepsTheDownloadRunning(t *testing.T) {
	const size = 2 << 20
	for round := range 8 {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			t.Parallel()
			data := testData(size)
			// The first connection takes the first half and leaves too little
			// to split again, while the second is refused its half.
			f := startStreamFetcher(t, refusingServer(t, data, 128<<10, size/2), 4)
			if err := f.Start(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for f.getState() != stateSteady {
				if time.Now().After(deadline) {
					t.Fatalf("the download never settled on its connections: state %v", f.getState())
				}
				time.Sleep(5 * time.Millisecond)
			}
			r, err := f.Stream(0)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := readAtCtx(ctx, r, size*3/4, 1024)
			if len(got) > 0 || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("read of a refused range: %d bytes, %v; want nothing until the deadline", len(got), err)
			}
			select {
			case err := <-f.doneCh:
				t.Fatalf("the download reported its end (%v) while its first connection was still fetching", err)
			default:
			}
		})
	}
}

// The library ends a download whose range was refused with 403 as done,
// leaving that range out. A reader open at the time must not read it.
func TestStream_ReaderStopsAtARangeThatNeverArrived(t *testing.T) {
	const size = 2 << 20
	data := testData(size)
	f := startStreamFetcher(t, refusingServer(t, data, 4<<20, size/2), 2)
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	r, err := f.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := f.Wait(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := readAtCtx(ctx, r, 0, size)
	if !errors.Is(err, errStreamStopped) {
		t.Fatalf("read across the refused range: %d bytes, %v; want %v", len(got), err, errStreamStopped)
	}
	if len(got) < size/4 || !bytes.Equal(got, data[:len(got)]) {
		t.Fatalf("read %d bytes before the refused range, or bytes the server did not send", len(got))
	}
}
