package netbind

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent/tracker/udp"
)

// udpTracker answers every connect and announce on loopback and counts the
// announces.
func udpTracker(t *testing.T) (string, func() int) {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	var mu sync.Mutex
	announces := 0
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			var h udp.RequestHeader
			if udp.Read(bytes.NewReader(buf[:n]), &h) != nil {
				continue
			}
			var resp bytes.Buffer
			udp.Write(&resp, udp.ResponseHeader{Action: h.Action, TransactionId: h.TransactionId})
			switch h.Action {
			case udp.ActionConnect:
				udp.Write(&resp, udp.ConnectionResponse{ConnectionId: 7})
			case udp.ActionAnnounce:
				mu.Lock()
				announces++
				mu.Unlock()
				udp.Write(&resp, udp.AnnounceResponseHeader{Interval: 1800})
			default:
				continue
			}
			pc.WriteTo(resp.Bytes(), from)
		}
	}()
	return pc.LocalAddr().String(), func() int {
		mu.Lock()
		defer mu.Unlock()
		return announces
	}
}

func announce(cc *udp.ConnClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := cc.Announce(ctx, udp.AnnounceRequest{NumWant: -1}, udp.Options{})
	return err
}

func TestATrackerSocketOpensForAFamilyTheInterfaceLacks(t *testing.T) {
	lb := newLoopback(t)
	addr, announces := udpTracker(t)
	cc, err := udp.NewConnClient(udp.NewConnClientOpts{Network: "udp6", Host: "[::1]:1", ListenPacket: lb.ListenPacket})
	if err != nil {
		t.Fatalf("tracker socket for IPv6 on an interface with IPv4 only: %v", err)
	}
	defer cc.Close()
	if err := announce(cc); !errors.Is(err, ErrDown) {
		t.Fatalf("announce over IPv6 on an interface with IPv4 only: %v, want ErrDown", err)
	}

	cc4, err := udp.NewConnClient(udp.NewConnClientOpts{Network: "udp4", Host: addr, ListenPacket: lb.ListenPacket})
	if err != nil {
		t.Fatal(err)
	}
	defer cc4.Close()
	if err := announce(cc4); err != nil {
		t.Fatalf("announce over IPv4: %v", err)
	}
	if n := announces(); n != 1 {
		t.Fatalf("the tracker saw %d announces, want 1", n)
	}
}

func TestAUDPTrackerAnnouncesAgainAfterTheInterfaceChanges(t *testing.T) {
	lb := newLoopback(t)
	name := lb.State().Interface
	addr, announces := udpTracker(t)
	cc, err := udp.NewConnClient(udp.NewConnClientOpts{Network: "udp4", Host: addr, ListenPacket: lb.ListenPacket})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	if err := announce(cc); err != nil {
		t.Fatalf("first announce: %v", err)
	}

	lb.set(true)
	if err := announce(cc); !errors.Is(err, ErrDown) {
		t.Fatalf("announce with the interface missing: %v, want ErrDown", err)
	}
	lb.set(false)
	if err := announce(cc); err != nil {
		t.Fatalf("announce once the interface is back: %v", err)
	}

	lb.SetInterface("")
	if err := announce(cc); err != nil {
		t.Fatalf("announce after leaving the interface: %v", err)
	}
	lb.SetInterface(name)
	if err := announce(cc); err != nil {
		t.Fatalf("announce after binding to the interface again: %v", err)
	}
	if n := announces(); n != 4 {
		t.Fatalf("the tracker saw %d announces, want 4", n)
	}
}

func TestSetInterfaceNamesTheInterfaceOnlyWithItsLink(t *testing.T) {
	b := New()
	looking := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	b.lookup = func(string) link {
		once.Do(func() { close(looking) })
		<-release
		return link{}
	}
	t.Cleanup(func() { b.SetInterface("") })
	done := make(chan struct{})
	go func() {
		b.SetInterface("wg0")
		close(done)
	}()
	<-looking
	st := b.State()
	close(release)
	<-done
	if st.Interface == "wg0" && st.Up {
		t.Fatal("State() reported wg0 up before wg0 was looked at")
	}
	if st := b.State(); st.Interface != "wg0" || st.Up {
		t.Fatalf("State() = %+v, want wg0 down", st)
	}
}
