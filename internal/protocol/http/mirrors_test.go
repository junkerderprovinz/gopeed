package http

import (
	"bytes"
	"crypto/rand"
	gohttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

// fetchWithMirrors downloads primary with the mirrors beside it and returns
// what landed on disk.
func fetchWithMirrors(t *testing.T, primary string, mirrors ...string) []byte {
	t.Helper()
	dir := t.TempDir()
	f := buildFetcher()
	err := f.Resolve(&base.Request{
		URL:   primary,
		Extra: &http.ReqExtra{Mirrors: mirrors},
	}, &base.Options{
		Name:  "mirror.data",
		Path:  dir,
		Extra: &http.OptsExtra{Connections: 6},
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

	got := fetchWithMirrors(t, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
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

	got := fetchWithMirrors(t, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
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

	got := fetchWithMirrors(t, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
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

	got := fetchWithMirrors(t, sa.URL+"/mirror.data", sb.URL+"/mirror.data")
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from the source")
	}
}
