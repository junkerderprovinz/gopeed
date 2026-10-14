package bt

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/GopeedLab/gopeed/pkg/base"
	"github.com/GopeedLab/gopeed/pkg/netbind"
	"github.com/GopeedLab/gopeed/pkg/protocol/bt"
)

// portTracker is an HTTP tracker that keeps the port of every announce.
type portTracker struct {
	mu    sync.Mutex
	ports []int
}

func (pt *portTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	port, _ := strconv.Atoi(r.URL.Query().Get("port"))
	pt.mu.Lock()
	pt.ports = append(pt.ports, port)
	pt.mu.Unlock()
	w.Write([]byte("d8:intervali3600e5:peers0:e"))
}

func (pt *portTracker) seen() []int {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return slices.Clone(pt.ports)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !ok(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestANewListeningPortTakesEffectWhileTorrentsRun(t *testing.T) {
	if err := closeClient(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeClient() })
	pt := &portTracker{}
	srv := httptest.NewServer(pt)
	defer srv.Close()

	f := buildFetcher()
	err := f.Resolve(&base.Request{
		URL:   "./testdata/test.torrent",
		Extra: bt.ReqExtra{Trackers: []string{srv.URL + "/announce"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldPort := client.LocalPort()
	eventually(t, "the first announce", func() bool { return slices.Contains(pt.seen(), oldPort) })

	newPort, err := netbind.FreePort()
	if err != nil {
		t.Fatal(err)
	}
	running := client
	new(FetcherManager).ApplyConfig(mockConfig(config{ListenPort: newPort}))

	if client != running || len(client.Torrents()) == 0 {
		t.Fatal("the client was rebuilt or lost its torrent")
	}
	if got := client.LocalPort(); got != newPort {
		t.Fatalf("client listens on %d, want %d", got, newPort)
	}
	for _, l := range client.Listeners() {
		if got := boundPortOf(l.Addr()); got != newPort {
			t.Errorf("%s listener on %d, want %d", l.Addr().Network(), got, newPort)
		}
	}
	c, err := net.DialTimeout("tcp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(newPort)).String(), 5*time.Second)
	if err != nil {
		t.Fatalf("nothing accepts peers on the new port: %v", err)
	}
	c.Close()
	eventually(t, "an announce of the new port", func() bool { return slices.Contains(pt.seen(), newPort) })
}

func boundPortOf(a net.Addr) int {
	ap, _ := netip.ParseAddrPort(a.String())
	return int(ap.Port())
}
