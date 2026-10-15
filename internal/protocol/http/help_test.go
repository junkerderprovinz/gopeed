package http

import (
	"bytes"
	gohttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GopeedLab/gopeed/internal/fetcher"
	"github.com/GopeedLab/gopeed/pkg/base"
	"github.com/GopeedLab/gopeed/pkg/protocol/http"
)

// A connection whose server has gone quiet keeps the speed it measured
// before, and that speed must not make it look nearly done.
func TestFetcher_ConnectionThatStoppedReceivingIsHelped(t *testing.T) {
	f := &Fetcher{meta: &fetcher.FetcherMeta{
		Req:  &base.Request{},
		Opts: &base.Options{Extra: &http.OptsExtra{Connections: 2}},
		Res:  &base.Resource{Range: true},
	}}
	helper := &connection{ID: 0, State: connDownloading, Chunk: newChunk(0, 1<<20-1)}
	helper.Chunk.Downloaded = 1 << 20
	quiet := &connection{ID: 1, State: connDownloading, Chunk: newChunk(1<<20, 11<<20-1), running: true}
	quiet.Chunk.Downloaded = 1 << 20
	quiet.Downloaded = 1 << 20
	quiet.speed = 10 << 20
	quiet.lastSpeedCheck = time.Now().Add(-10 * time.Second).UnixNano()
	quiet.lastSpeedDownload = quiet.Downloaded
	f.connections = []*connection{helper, quiet}

	if !f.helpOtherConnection(helper) {
		t.Fatal("the connection that receives nothing kept all of its range")
	}
}

// slowAfter paces a response, and once after has been closed, slows it to a
// trickle.
type slowAfter struct {
	gohttp.ResponseWriter
	pace  time.Duration
	after <-chan struct{}
}

func (w slowAfter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := min(len(p), 4<<10)
		select {
		case <-w.after:
			time.Sleep(20 * time.Millisecond)
		default:
			time.Sleep(w.pace * time.Duration(n) / (32 << 10))
		}
		if _, err := w.ResponseWriter.Write(p[:n]); err != nil {
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

// A connection that has finished its range comes back when another one slows
// down after it finished, instead of leaving the rest of that range to the
// slow one alone.
func TestFetcher_FinishedConnectionHelpsOneThatSlowsDownLater(t *testing.T) {
	data := randomData(t, 16<<20)
	firstDone := make(chan struct{})
	var once sync.Once
	var ranged, afterSlowdown atomic.Int32
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		if r.Header.Get("Range") == "" {
			gohttp.ServeContent(w, r, "slow.data", time.Time{}, bytes.NewReader(data))
			return
		}
		n := ranged.Add(1)
		select {
		case <-firstDone:
			afterSlowdown.Add(1)
		default:
		}
		switch n {
		case 1:
			defer once.Do(func() { close(firstDone) })
			gohttp.ServeContent(slowAfter{w, 8 * time.Millisecond, nil}, r, "slow.data", time.Time{}, bytes.NewReader(data))
		case 2:
			gohttp.ServeContent(slowAfter{w, 16 * time.Millisecond, firstDone}, r, "slow.data", time.Time{}, bytes.NewReader(data))
		default:
			gohttp.ServeContent(w, r, "slow.data", time.Time{}, bytes.NewReader(data))
		}
	}))
	defer srv.Close()

	got := fetchWithMirrors(t, 2, srv.URL+"/slow.data")
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
	if afterSlowdown.Load() == 0 {
		t.Fatal("no connection came back for the range of the one that slowed down")
	}
}
