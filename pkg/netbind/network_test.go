package netbind

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"
)

// loopNet is a Network that is loopback underneath, counting what goes
// through it and resolving only the names it was given.
type loopNet struct {
	mu      sync.Mutex
	up      bool
	stack   uint64
	dials   int
	listens int
	hosts   map[string][]netip.Addr
}

func (n *loopNet) Status() NetworkStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	return NetworkStatus{Up: n.up, Addrs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Stack: n.stack}
}

func (n *loopNet) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	n.mu.Lock()
	n.dials++
	n.mu.Unlock()
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

func (n *loopNet) ListenPacket(network string, at netip.AddrPort) (net.PacketConn, error) {
	n.mu.Lock()
	n.listens++
	n.mu.Unlock()
	return net.ListenPacket(network, at.String())
}

func (n *loopNet) Listen(network string, at netip.AddrPort) (net.Listener, error) {
	n.mu.Lock()
	n.listens++
	n.mu.Unlock()
	return net.Listen(network, at.String())
}

func (n *loopNet) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ips, ok := n.hosts[host]; ok {
		return ips, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

func (n *loopNet) set(f func(n *loopNet)) {
	n.mu.Lock()
	f(n)
	n.mu.Unlock()
}

func (n *loopNet) count() (dials, listens int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dials, n.listens
}

func bindToNetwork(t *testing.T, name string) (*Binder, *loopNet) {
	t.Helper()
	n := &loopNet{up: true, stack: 1, hosts: map[string][]netip.Addr{
		"tracker.tunnel.invalid": {netip.MustParseAddr("127.0.0.1")},
	}}
	if err := Register(name, n); err != nil {
		t.Fatal(err)
	}
	b := New()
	b.SetInterface(name)
	t.Cleanup(func() {
		b.SetInterface("")
		Register(name, nil)
	})
	return b, n
}

func TestABinderSetToANetworkDialsAndResolvesThroughIt(t *testing.T) {
	b, n := bindToNetwork(t, "tun-dial")
	addr, accepted := server(t)
	_, port, _ := net.SplitHostPort(addr)

	c, err := b.DialContext(context.Background(), "tcp", net.JoinHostPort("tracker.tunnel.invalid", port))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if dials, _ := n.count(); dials != 1 {
		t.Errorf("the network dialled %d times, want 1", dials)
	}
	waitFor(t, func() bool { return accepted() == 1 })

	ips, err := b.LookupHost(context.Background(), "tracker.tunnel.invalid")
	if err != nil || !slices.Equal(ips, []string{"127.0.0.1"}) {
		t.Errorf("LookupHost = %v, %v; want the network's answer", ips, err)
	}
	if st := b.State(); st.Interface != "tun-dial" || !st.Up {
		t.Errorf("state = %+v, want the network up", st)
	}
}

func TestNothingLeavesWhileTheNetworkIsDown(t *testing.T) {
	b, n := bindToNetwork(t, "tun-down")
	addr, _ := server(t)
	open, err := b.DialContext(context.Background(), "tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := b.PacketConn("udp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	n.set(func(n *loopNet) { n.up = false })
	b.Refresh()

	if _, err := b.DialContext(context.Background(), "tcp4", addr); !errors.Is(err, ErrDown) {
		t.Errorf("dial while down = %v, want ErrDown", err)
	}
	if dials, _ := n.count(); dials != 1 {
		t.Errorf("the network dialled %d times, want only the dial from before", dials)
	}
	if _, err := b.LookupHost(context.Background(), "tracker.tunnel.invalid"); !errors.Is(err, ErrDown) {
		t.Errorf("lookup while down = %v, want ErrDown", err)
	}
	if _, err := pc.WriteTo([]byte("x"), net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:9"))); !errors.Is(err, ErrDown) {
		t.Errorf("send while down = %v, want ErrDown", err)
	}
	open.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := open.Read(make([]byte, 1)); err == nil {
		t.Error("a connection from before is still open while the network is down")
	}
}

func TestSocketsMoveOntoANewStackOnTheirPort(t *testing.T) {
	b, n := bindToNetwork(t, "tun-stack")
	pc, err := b.PacketConn("udp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_, before := n.count()

	n.set(func(n *loopNet) { n.stack++ })
	b.Refresh()

	if _, after := n.count(); after != before+1 {
		t.Errorf("the network opened %d sockets for the new stack, want 1", after-before)
	}
	if got := pc.LocalAddr().(*net.UDPAddr).Port; got != port {
		t.Errorf("the socket moved to port %d, want it to keep %d", got, port)
	}
}

func TestSetPortMovesAListenerAndKeepsWhatItAccepted(t *testing.T) {
	b := New()
	ln, err := b.Listener("tcp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
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
	oldPort := ln.Addr().(*net.TCPAddr).Port
	kept, err := net.Dial("tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer kept.Close()
	echo(t, kept)

	newPort, err := FreePort()
	if err != nil {
		t.Fatal(err)
	}
	if err := SetPort(ln, newPort); err != nil {
		t.Fatal(err)
	}
	if got := ln.Addr().(*net.TCPAddr).Port; got != newPort {
		t.Fatalf("listener on %d after SetPort(%d)", got, newPort)
	}
	fresh, err := net.Dial("tcp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(newPort)).String())
	if err != nil {
		t.Fatalf("nothing accepts on the new port: %v", err)
	}
	defer fresh.Close()
	echo(t, fresh)
	echo(t, kept)
	if c, err := net.DialTimeout("tcp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(oldPort)).String(), time.Second); err == nil {
		c.Close()
		t.Error("the old port still accepts")
	}
}

func TestSetPortMovesAPacketSocketUnderAWaitingReader(t *testing.T) {
	b := New()
	pc, err := b.PacketConn("udp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	got := make(chan string, 1)
	go func() {
		buf := make([]byte, 16)
		n, _, err := pc.ReadFrom(buf)
		if err == nil {
			got <- string(buf[:n])
		}
	}()
	// Let the reader block on the old socket first.
	time.Sleep(50 * time.Millisecond)

	newPort, err := FreePort()
	if err != nil {
		t.Fatal(err)
	}
	if err := SetPort(pc, newPort); err != nil {
		t.Fatal(err)
	}
	if p := pc.LocalAddr().(*net.UDPAddr).Port; p != newPort {
		t.Fatalf("socket on %d after SetPort(%d)", p, newPort)
	}
	sender, err := net.Dial("udp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(newPort)).String())
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	sender.Write([]byte("moved"))
	select {
	case s := <-got:
		if s != "moved" {
			t.Errorf("read %q", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the reader did not follow the socket to its new port")
	}
}

func TestSetPortKeepsTheSocketWhenThePortIsTaken(t *testing.T) {
	b := New()
	pc, err := b.PacketConn("udp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	port := pc.LocalAddr().(*net.UDPAddr).Port
	taken, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	if err := SetPort(pc, taken.LocalAddr().(*net.UDPAddr).Port); err == nil {
		t.Fatal("SetPort onto a taken port succeeded")
	}
	if got := pc.LocalAddr().(*net.UDPAddr).Port; got != port {
		t.Errorf("socket on %d after a failed SetPort, want %d", got, port)
	}
}

func TestInterfacesListRegisteredNetworks(t *testing.T) {
	bindToNetwork(t, "tun-listed")
	ifs, err := Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(ifs, func(ifc Interface) bool { return ifc.Name == "tun-listed" })
	if i < 0 || !ifs[i].Up || len(ifs[i].Addrs) != 1 {
		t.Errorf("Interfaces() = %+v, want tun-listed up with its address", ifs)
	}
}

func echo(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := c.Read(buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo = %q, %v", buf, err)
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// crashingNet is a loopNet that closes every socket it opened when its stack
// goes, before anyone has told the Binder.
type crashingNet struct {
	*loopNet
	mu      sync.Mutex
	sockets []interface{ Close() error }
}

func (n *crashingNet) ListenPacket(network string, at netip.AddrPort) (net.PacketConn, error) {
	pc, err := n.loopNet.ListenPacket(network, at)
	if err == nil {
		n.mu.Lock()
		n.sockets = append(n.sockets, pc)
		n.mu.Unlock()
	}
	return pc, err
}

func (n *crashingNet) Listen(network string, at netip.AddrPort) (net.Listener, error) {
	ln, err := n.loopNet.Listen(network, at)
	if err == nil {
		n.mu.Lock()
		n.sockets = append(n.sockets, ln)
		n.mu.Unlock()
	}
	return ln, err
}

func (n *crashingNet) crash() {
	n.set(func(n *loopNet) { n.up = false; n.stack++ })
	n.mu.Lock()
	for _, s := range n.sockets {
		s.Close()
	}
	n.sockets = nil
	n.mu.Unlock()
}

func TestAReaderWaitsOutAStackThatClosedUnderIt(t *testing.T) {
	n := &crashingNet{loopNet: &loopNet{up: true, stack: 1}}
	if err := Register("tun-crash", n); err != nil {
		t.Fatal(err)
	}
	b := New()
	b.SetInterface("tun-crash")
	t.Cleanup(func() {
		b.SetInterface("")
		Register("tun-crash", nil)
	})
	pc, err := b.PacketConn("udp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	ln, err := b.Listener("tcp4", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan error, 2)
	go func() {
		buf := make([]byte, 8)
		_, _, err := pc.ReadFrom(buf)
		got <- err
	}()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
		}
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)

	n.crash()
	select {
	case err := <-got:
		t.Fatalf("a socket failed when its stack went: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	n.set(func(n *loopNet) { n.up = true })
	b.Refresh()
	port := pc.LocalAddr().(*net.UDPAddr).Port
	s, err := net.Dial("udp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)).String())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Write([]byte("back"))
	c, err := net.Dial("tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for range 2 {
		select {
		case err := <-got:
			if err != nil {
				t.Errorf("after the stack came back: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a socket did not come back with the stack")
		}
	}
}
