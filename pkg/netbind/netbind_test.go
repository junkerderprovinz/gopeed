package netbind

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

func TestLinkOfNeedsTheInterfaceUpAndAnAddressBeyondTheLink(t *testing.T) {
	ipnet := func(s string) net.Addr {
		ip, n, err := net.ParseCIDR(s)
		if err != nil {
			t.Fatal(err)
		}
		n.IP = ip
		return n
	}
	up := net.Interface{Index: 7, Name: "wg0", Flags: net.FlagUp}
	down := net.Interface{Index: 7, Name: "wg0"}
	cases := []struct {
		name   string
		ifc    net.Interface
		addrs  []net.Addr
		up     bool
		v4, v6 string
	}{
		{"up with both families", up, []net.Addr{ipnet("10.2.0.2/32"), ipnet("fd00::2/128")}, true, "10.2.0.2", "fd00::2"},
		{"down with an address", down, []net.Addr{ipnet("10.2.0.2/32")}, false, "10.2.0.2", ""},
		{"up with link-local only", up, []net.Addr{ipnet("fe80::1/64"), ipnet("169.254.3.4/16")}, false, "", ""},
		{"up without addresses", up, nil, false, "", ""},
		{"first of a family wins", up, []net.Addr{ipnet("fe80::1/64"), ipnet("10.0.0.1/8"), ipnet("10.0.0.2/8")}, true, "10.0.0.1", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := linkOf(c.ifc, c.addrs)
			if l.up != c.up {
				t.Errorf("up = %v, want %v", l.up, c.up)
			}
			if got := addrString(l.v4); got != c.v4 {
				t.Errorf("v4 = %q, want %q", got, c.v4)
			}
			if got := addrString(l.v6); got != c.v6 {
				t.Errorf("v6 = %q, want %q", got, c.v6)
			}
		})
	}
}

func addrString(a netip.Addr) string {
	if !a.IsValid() {
		return ""
	}
	return a.String()
}

func TestASocketBindsToItsFamilyOnlyWhileTheInterfaceIsUp(t *testing.T) {
	v4 := netip.MustParseAddr("10.2.0.2")
	l := link{up: true, v4: v4}
	if got := l.addr("wg0", "udp4"); got != v4 {
		t.Errorf("udp4 binds to %v, want %v", got, v4)
	}
	if got := l.addr("wg0", "tcp6"); got.IsValid() {
		t.Errorf("tcp6 binds to %v on an interface without IPv6, want nowhere", got)
	}
	if got := l.addr("wg0", "tcp"); got != v4 {
		t.Errorf("tcp binds to %v, want the IPv4 address", got)
	}
	l.up = false
	if got := l.addr("wg0", "udp4"); got.IsValid() {
		t.Errorf("udp4 binds to %v while the interface is down, want nowhere", got)
	}
	if got := (link{}).addr("", "udp6"); got != netip.IPv6Unspecified() {
		t.Errorf("without an interface udp6 binds to %v, want any", got)
	}
}

// loopback is a Binder bound to the system's loopback interface, whose
// lookup the test switches between the real interface and a missing one.
type loopback struct {
	*Binder
	mu   sync.Mutex
	gone bool
}

func newLoopback(t *testing.T) *loopback {
	t.Helper()
	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	var lo net.Interface
	for _, ifc := range ifs {
		if ifc.Flags&net.FlagLoopback != 0 && ifc.Flags&net.FlagUp != 0 {
			lo = ifc
			break
		}
	}
	if lo.Name == "" {
		t.Skip("no loopback interface")
	}
	real := link{up: true, index: lo.Index, v4: netip.MustParseAddr("127.0.0.1")}
	lb := &loopback{Binder: New()}
	lb.lookup = func(string) link {
		lb.mu.Lock()
		defer lb.mu.Unlock()
		if lb.gone {
			return link{}
		}
		return real
	}
	lb.SetInterface(lo.Name)
	t.Cleanup(func() { lb.SetInterface("") })
	return lb
}

func (lb *loopback) set(gone bool) {
	lb.mu.Lock()
	lb.gone = gone
	lb.mu.Unlock()
	lb.Refresh()
}

// server accepts on loopback and counts what it accepted.
func server(t *testing.T) (string, func() int) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var mu sync.Mutex
	n := 0
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			n++
			mu.Unlock()
			go func() {
				buf := make([]byte, 64)
				for {
					k, err := c.Read(buf)
					if err != nil {
						c.Close()
						return
					}
					c.Write(buf[:k])
				}
			}()
		}
	}()
	return ln.Addr().String(), func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}

func TestNothingIsDialledWhileTheInterfaceIsMissing(t *testing.T) {
	lb := newLoopback(t)
	addr, accepted := server(t)
	lb.set(true)
	if st := lb.State(); st.Up {
		t.Fatalf("State().Up with the interface missing")
	}
	for _, network := range []string{"tcp", "tcp4"} {
		if c, err := lb.DialContext(context.Background(), network, addr); !errors.Is(err, ErrDown) {
			if c != nil {
				c.Close()
			}
			t.Fatalf("dial %s with the interface missing: %v, want ErrDown", network, err)
		}
	}
	pc, err := lb.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("ListenPacket with the interface missing: %v", err)
	}
	defer pc.Close()
	if _, err := pc.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}); !errors.Is(err, ErrDown) {
		t.Fatalf("send with the interface missing: %v, want ErrDown", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := accepted(); n != 0 {
		t.Fatalf("the server saw %d connections while the interface was missing", n)
	}

	lb.set(false)
	c, err := lb.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("dial once the interface is back: %v", err)
	}
	defer c.Close()
	if got := c.LocalAddr().(*net.TCPAddr).IP.String(); got != "127.0.0.1" {
		t.Errorf("the connection left from %s, want the interface's address", got)
	}
}

func TestOpenConnectionsCloseWhenTheInterfaceGoes(t *testing.T) {
	lb := newLoopback(t)
	addr, _ := server(t)
	c, err := Dialer{Binder: lb.Binder, KeepAlive: -1, NoLinger: true}.DialContext(context.Background(), "tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := c.Read(buf); err != nil {
		t.Fatal(err)
	}

	lb.set(true)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(buf); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after the interface went: %v, want the connection closed", err)
	}
}

func TestAPacketSocketGoesQuietWhileDownAndComesBackOnItsPort(t *testing.T) {
	lb := newLoopback(t)
	pc, err := lb.PacketConn("udp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	port := pc.LocalAddr().(*net.UDPAddr).Port
	if port == 0 {
		t.Fatal("no port settled")
	}
	peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	got := make(chan string, 4)
	go func() {
		buf := make([]byte, 64)
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				close(got)
				return
			}
			got <- string(buf[:n])
		}
	}()
	to := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
	expect := func(want string) {
		t.Helper()
		select {
		case s, ok := <-got:
			if !ok {
				t.Fatalf("the reader stopped, want %q", want)
			}
			if s != want {
				t.Fatalf("read %q, want %q", s, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("nothing read, want %q", want)
		}
	}
	peer.WriteTo([]byte("one"), to)
	expect("one")

	lb.set(true)
	if _, err := pc.WriteTo([]byte("x"), peer.LocalAddr()); !errors.Is(err, ErrDown) {
		t.Fatalf("send while down: %v, want ErrDown", err)
	}
	if p := pc.LocalAddr().(*net.UDPAddr).Port; p != port {
		t.Fatalf("the port moved to %d while down, want %d", p, port)
	}
	peer.WriteTo([]byte("lost"), to)
	select {
	case s := <-got:
		t.Fatalf("read %q while the interface was missing", s)
	case <-time.After(100 * time.Millisecond):
	}

	lb.set(false)
	peer.WriteTo([]byte("two"), to)
	expect("two")
	if _, err := pc.WriteTo([]byte("back"), peer.LocalAddr()); err != nil {
		t.Fatalf("send once back: %v", err)
	}
}

func TestAListenerAcceptsAgainOnceTheInterfaceIsBack(t *testing.T) {
	lb := newLoopback(t)
	ln, err := lb.Listener("tcp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()
	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				close(accepted)
				return
			}
			accepted <- c
		}
	}()
	dial := func() error {
		c, err := net.DialTimeout("tcp4", addr, time.Second)
		if err == nil {
			c.Close()
		}
		return err
	}

	lb.set(true)
	if err := dial(); err == nil {
		t.Fatal("a connection reached the listener while the interface was missing")
	}
	lb.set(false)
	if err := dial(); err != nil {
		t.Fatalf("dial once the interface is back: %v", err)
	}
	select {
	case c := <-accepted:
		c.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("the listener accepted nothing once the interface was back")
	}
	ln.Close()
	if _, ok := <-accepted; ok {
		t.Fatal("Accept went on after Close")
	}
}

func TestStateSaysSinceWhenTheInterfaceIsDown(t *testing.T) {
	lb := newLoopback(t)
	before := time.Now()
	lb.set(true)
	st := lb.State()
	if st.Up || st.Since.Before(before) || len(st.Addrs) != 0 {
		t.Fatalf("State() = %+v, want down since the change and no address", st)
	}
	lb.SetInterface("")
	if st := lb.State(); !st.Up || st.Interface != "" {
		t.Fatalf("State() without an interface = %+v, want up", st)
	}
}

func TestInterfacesListLoopbackWithItsAddress(t *testing.T) {
	ifs, err := Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, ifc := range ifs {
		for _, a := range ifc.Addrs {
			if a == netip.MustParseAddr("127.0.0.1") {
				if !ifc.Up {
					t.Errorf("%s holds 127.0.0.1 but is listed down", ifc.Name)
				}
				return
			}
		}
	}
	t.Fatalf("no interface lists 127.0.0.1 among %+v", ifs)
}

func TestWatchHearsAChangeBeforeAndAfterTheConnectionsClose(t *testing.T) {
	lb := newLoopback(t)
	addr, _ := server(t)
	c, err := lb.DialContext(context.Background(), "tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	open := func() bool {
		c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		_, err := c.Read(make([]byte, 1))
		return !errors.Is(err, net.ErrClosed)
	}
	var heard []string
	lb.Watch(func(st State) {
		heard = append(heard, "before")
		if st.Up || !open() {
			t.Errorf("before: up = %v, connection open = %v, want the new state down and the old connection still open", st.Up, open())
		}
	}, func(st State) {
		heard = append(heard, "after")
		if open() {
			t.Error("after: the old connection is still open")
		}
	})
	lb.set(true)
	lb.Refresh()
	lb.Watch(nil, nil)
	if len(heard) != 2 || heard[0] != "before" || heard[1] != "after" {
		t.Fatalf("heard %v, want before and after, once each", heard)
	}
}
