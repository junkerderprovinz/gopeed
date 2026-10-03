package http

import (
	"bytes"
	"context"
	"crypto/rand"
	gohttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GopeedLab/gopeed/internal/fetcher"
	"github.com/GopeedLab/gopeed/pkg/base"
	"github.com/GopeedLab/gopeed/pkg/protocol/http"
)

// source serves data with ranges, unless refuse says a request is turned
// away, and counts the ranged requests it answered.
type source struct {
	data   []byte
	refuse func(r *gohttp.Request) int
	ranged atomic.Int32
}

func (s *source) ServeHTTP(w gohttp.ResponseWriter, r *gohttp.Request) {
	if s.refuse != nil {
		if code := s.refuse(r); code != 0 {
			w.WriteHeader(code)
			return
		}
	}
	if r.Header.Get("Range") != "" {
		s.ranged.Add(1)
	}
	gohttp.ServeContent(w, r, "mirror.data", time.Time{}, bytes.NewReader(s.data))
}

func randomData(t *testing.T, n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// fetchWithMirrors downloads primary with the mirrors beside it over conns
// connections and returns what landed on disk.
func fetchWithMirrors(t *testing.T, conns int, primary string, mirrors ...string) []byte {
	t.Helper()
	dir := t.TempDir()
	f := buildFetcher()
	err := f.Resolve(&base.Request{
		URL:   primary,
		Extra: &http.ReqExtra{Mirrors: mirrors},
	}, &base.Options{
		Name:  "mirror.data",
		Path:  dir,
		Extra: &http.OptsExtra{Connections: conns},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	if err := f.Wait(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "mirror.data"))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestFetcher_MirrorsShareTheDownload(t *testing.T) {
	data := randomData(t, 16<<20)
	a, b := &source{data: data}, &source{data: data}
	sa, sb := httptest.NewServer(a), httptest.NewServer(b)
	defer sa.Close()
	defer sb.Close()

	got := fetchWithMirrors(t, 6, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
	if b.ranged.Load() == 0 {
		t.Fatal("the mirror was never asked for a range")
	}
}

func TestFetcher_DeadMirrorLeavesTheRestToTheURL(t *testing.T) {
	data := randomData(t, 16<<20)
	a := &source{data: data}
	b := &source{data: data, refuse: func(*gohttp.Request) int { return gohttp.StatusNotFound }}
	sa, sb := httptest.NewServer(a), httptest.NewServer(b)
	defer sa.Close()
	defer sb.Close()

	got := fetchWithMirrors(t, 6, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
}

func TestFetcher_MirrorTakesOverWhenTheURLStops(t *testing.T) {
	data := randomData(t, 16<<20)
	var served atomic.Int32
	// The URL answers the resolve and two ranges, then nothing more.
	a := &source{data: data, refuse: func(r *gohttp.Request) int {
		if r.Header.Get("Range") != "" && served.Add(1) > 2 {
			return gohttp.StatusGone
		}
		return 0
	}}
	b := &source{data: data}
	sa, sb := httptest.NewServer(a), httptest.NewServer(b)
	defer sa.Close()
	defer sb.Close()

	got := fetchWithMirrors(t, 6, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
}

func TestFetcher_MirrorSendingTheWholeFileIsRefused(t *testing.T) {
	data := randomData(t, 16<<20)
	a := &source{data: data}
	// Ignores the range and sends everything with 200.
	whole := gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		r.Header.Del("Range")
		gohttp.ServeContent(w, r, "mirror.data", time.Time{}, bytes.NewReader(data))
	})
	sa, sb := httptest.NewServer(a), httptest.NewServer(whole)
	defer sa.Close()
	defer sb.Close()

	got := fetchWithMirrors(t, 6, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
}

// slowWriter paces a response, so the requests a source answers overlap.
type slowWriter struct{ gohttp.ResponseWriter }

func (w slowWriter) Write(p []byte) (int, error) {
	time.Sleep(time.Millisecond)
	return w.ResponseWriter.Write(p)
}

// A hoster may count an account's connections, so the share a source was
// given is all it gets, also when the other sources die.
func TestFetcher_DeadMirrorsLeaveTheURLItsShareOfConnections(t *testing.T) {
	data := randomData(t, 8<<20)
	var refused atomic.Bool
	var inFlight, peak atomic.Int32
	primary := gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		if r.Header.Get("Range") != "" && refused.Load() {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
		}
		gohttp.ServeContent(slowWriter{w}, r, "mirror.data", time.Time{}, bytes.NewReader(data))
	})
	gone := gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		refused.Store(true)
		w.WriteHeader(gohttp.StatusGone)
	})
	sa := httptest.NewServer(primary)
	defer sa.Close()
	var mirrors []string
	for range 3 {
		s := httptest.NewServer(gone)
		defer s.Close()
		mirrors = append(mirrors, s.URL+"/mirror.data")
	}

	got := fetchWithMirrors(t, 4, sa.URL+"/mirror.data", mirrors...)
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
	if p := peak.Load(); p > 1 {
		t.Fatalf("the URL served %d ranges at once, want its share of 1", p)
	}
}

// The URL that answered the resolve may still refuse every range. Its share
// then goes to the mirrors instead of the first one carrying the file alone.
func TestFetcher_URLRefusingRangesLeavesTheDownloadToAllMirrors(t *testing.T) {
	data := randomData(t, 8<<20)
	primary := gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		if r.Header.Get("Range") != "" {
			w.WriteHeader(gohttp.StatusGone)
			return
		}
		gohttp.ServeContent(slowWriter{w}, r, "mirror.data", time.Time{}, bytes.NewReader(data))
	})
	sa := httptest.NewServer(primary)
	defer sa.Close()
	var copies []*source
	var mirrors []string
	for range 2 {
		c := &source{data: data}
		s := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
			c.ServeHTTP(slowWriter{w}, r)
		}))
		defer s.Close()
		copies = append(copies, c)
		mirrors = append(mirrors, s.URL+"/mirror.data")
	}

	got := fetchWithMirrors(t, 4, sa.URL+"/mirror.data", mirrors...)
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
	for i, c := range copies {
		if c.ranged.Load() == 0 {
			t.Fatalf("mirror %d was never asked for a range", i+1)
		}
	}
}

func TestFetcher_ConnectionWaitingForASourceLeavesItsChunkToTheOthers(t *testing.T) {
	f := &Fetcher{meta: &fetcher.FetcherMeta{
		Req:  &base.Request{Extra: &http.ReqExtra{Mirrors: []string{"mirror"}}},
		Opts: &base.Options{Extra: &http.OptsExtra{Connections: 2}},
		Res:  &base.Resource{Range: true},
	}}
	helper := &connection{ID: 0, State: connDownloading, Chunk: newChunk(0, 1<<20-1)}
	helper.Chunk.Downloaded = 1 << 20
	// The mirror died, so the waiter is left with the URL, which holds its
	// share of one already. runConnection leaves it marked failed from the
	// retries on the mirror.
	waiter := &connection{ID: 1, State: connFailed, Chunk: newChunk(1<<20, 9<<20-1), source: 1, speed: 1 << 30}
	f.connections = []*connection{helper, waiter}
	f.deadSources = map[int]bool{1: true}
	f.holdSourceLocked(helper, 0)

	ctx, cancel := context.WithCancel(context.Background())
	waiter.ctx = ctx
	done := make(chan bool)
	go func() { done <- f.takeSource(waiter) }()
	for waiting := false; !waiting; {
		time.Sleep(time.Millisecond)
		f.connMu.Lock()
		waiting = f.sourceFreed != nil
		f.connMu.Unlock()
	}

	helped := f.helpOtherConnection(helper)
	cancel()
	if <-done {
		t.Fatal("the waiter got a place on a source that was full")
	}
	if !helped {
		t.Fatal("the chunk of the waiting connection was not shared out")
	}
}
