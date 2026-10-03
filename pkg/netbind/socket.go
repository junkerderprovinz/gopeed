package netbind

import (
	"context"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"
)

// bound is what a following socket currently sits on: the system socket, nil
// while there is nothing to bind to, and the address and interface it was
// bound for.
type bound[T interface{ Close() error }] struct {
	inner T
	open  bool
	at    netip.Addr
	index int
}

// follower holds the state a following socket shares between its packet and
// stream forms: the current system socket and a channel closed whenever that
// changes, which wakes a reader waiting for the interface to come back.
type follower[T interface{ Close() error }] struct {
	b       *Binder
	network string
	port    int
	listen  func(name string, l link, at netip.Addr, port int) (T, int, error)
	// waits makes a socket that cannot be bound wait without an interface
	// too, rather than fail to open.
	waits bool

	mu     sync.Mutex
	cur    bound[T]
	wake   chan struct{}
	closed bool
}

func (f *follower[T]) signal() {
	close(f.wake)
	f.wake = make(chan struct{})
}

// rebind moves the socket to where l puts its family, or closes it while that
// is nowhere. A socket already there stays as it is, and one whose bind failed
// is tried again.
func (f *follower[T]) rebind(name string, l link) {
	at := l.addr(name, f.network)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || (f.cur.open && f.cur.at == at && f.cur.index == l.index) {
		return
	}
	if f.cur.open {
		f.cur.inner.Close()
		f.cur = bound[T]{}
	}
	if at.IsValid() {
		if s, port, err := f.listen(name, l, at, f.port); err == nil {
			f.cur = bound[T]{inner: s, open: true, at: at, index: l.index}
			f.port = port
		}
	}
	f.signal()
}

// current is the system socket, whether there is one, and the channel that
// is closed when either changes.
func (f *follower[T]) current() (T, bool, <-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		var zero T
		return zero, false, nil, net.ErrClosed
	}
	return f.cur.inner, f.cur.open, f.wake, nil
}

// moved reports whether inner is no longer the socket in use, so that an
// error reading it came from a rebind rather than from the network.
func (f *follower[T]) moved(inner T) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed || !f.cur.open || any(f.cur.inner) != any(inner)
}

func (f *follower[T]) close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return net.ErrClosed
	}
	f.closed = true
	var err error
	if f.cur.open {
		err = f.cur.inner.Close()
		f.cur = bound[T]{}
	}
	f.signal()
	f.mu.Unlock()
	return err
}

// start binds the first system socket. Without an interface its error is the
// system's unless the socket waits, so a caller can tell an unsupported family
// as it would from net.Listen; with one, a socket that cannot be bound yet
// waits for the interface. A port of 0 is settled here either way, so a later
// rebind lands on the same port.
func (f *follower[T]) start() error {
	name, l, _ := f.b.now()
	f.wake = make(chan struct{})
	at := l.addr(name, f.network)
	if at.IsValid() {
		s, port, err := f.listen(name, l, at, f.port)
		if err == nil {
			f.cur = bound[T]{inner: s, open: true, at: at, index: l.index}
			f.port = port
		} else if name == "" && !f.waits {
			return err
		}
	}
	if f.port == 0 {
		port, err := FreePort()
		if err != nil {
			return err
		}
		f.port = port
	}
	f.b.add(f)
	return nil
}

// FreePort finds a port that TCP and UDP can both listen on. It asks on
// loopback, so the probe itself reaches no network. Windows reserves blocks
// of ports for one protocol only and hands out ports in order, so after the
// system's first pick the probe tries ports at random.
func FreePort() (int, error) {
	var err error
	port := 0
	for range 200 {
		var ln net.Listener
		if ln, err = net.Listen("tcp4", netip.AddrPortFrom(loopback4, uint16(port)).String()); err == nil {
			port = ln.Addr().(*net.TCPAddr).Port
			var pc net.PacketConn
			pc, err = net.ListenPacket("udp4", netip.AddrPortFrom(loopback4, uint16(port)).String())
			ln.Close()
			if err == nil {
				pc.Close()
				return port, nil
			}
		}
		port = 49152 + rand.IntN(65536-49152)
	}
	return 0, err
}

var loopback4 = netip.AddrFrom4([4]byte{127, 0, 0, 1})

func unspecified(network string, port int) netip.AddrPort {
	if network[len(network)-1] == '6' {
		return netip.AddrPortFrom(netip.IPv6Unspecified(), uint16(port))
	}
	return netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(port))
}

// PacketConn opens a UDP socket on port that follows the interface: bound to
// its address of network's family, moved when that address changes, and
// neither sending nor receiving while there is none. network is "udp4" or
// "udp6"; a port of 0 is picked once and kept.
func (b *Binder) PacketConn(network string, port int) (net.PacketConn, error) {
	return b.packetConn(network, port, false)
}

// ListenPacket opens a packet socket the way net.ListenPacket would, with only
// the port of address used, that follows the interface as PacketConn does. It
// opens even while there is nothing of network's family to bind to, and
// starts sending once there is.
func (b *Binder) ListenPacket(network, address string) (net.PacketConn, error) {
	port, err := portOf(address)
	if err != nil {
		return nil, err
	}
	return b.packetConn(network, port, true)
}

func (b *Binder) packetConn(network string, port int, waits bool) (net.PacketConn, error) {
	p := &packetConn{follower: follower[net.PacketConn]{b: b, network: network, port: port, waits: waits}}
	p.listen = func(name string, l link, at netip.Addr, port int) (net.PacketConn, int, error) {
		pc, err := listenPacket(name, l, network, at, port)
		if err != nil {
			return nil, 0, err
		}
		pc.SetReadDeadline(p.readDeadline)
		pc.SetWriteDeadline(p.writeDeadline)
		return pc, pc.LocalAddr().(*net.UDPAddr).Port, nil
	}
	if err := p.start(); err != nil {
		return nil, err
	}
	return p, nil
}

// packetConn keeps its deadlines under the follower's lock, so a socket bound
// later gets them too.
type packetConn struct {
	follower[net.PacketConn]
	readDeadline, writeDeadline time.Time
}

func (p *packetConn) ReadFrom(buf []byte) (int, net.Addr, error) {
	for {
		inner, ok, wake, err := p.current()
		if err != nil {
			return 0, nil, err
		}
		if !ok {
			if err := p.wait(wake); err != nil {
				return 0, nil, err
			}
			continue
		}
		n, addr, err := inner.ReadFrom(buf)
		if err != nil && p.moved(inner) {
			continue
		}
		return n, addr, err
	}
}

// wait blocks until the socket changes or the read deadline passes.
func (p *packetConn) wait(wake <-chan struct{}) error {
	p.mu.Lock()
	dl := p.readDeadline
	p.mu.Unlock()
	if dl.IsZero() {
		<-wake
		return nil
	}
	t := time.NewTimer(time.Until(dl))
	defer t.Stop()
	select {
	case <-wake:
		return nil
	case <-t.C:
		return os.ErrDeadlineExceeded
	}
}

func (p *packetConn) WriteTo(buf []byte, addr net.Addr) (int, error) {
	inner, ok, _, err := p.current()
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, ErrDown
	}
	return inner.WriteTo(buf, addr)
}

func (p *packetConn) Close() error {
	p.b.remove(&p.follower)
	return p.close()
}

func (p *packetConn) LocalAddr() net.Addr {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cur.open {
		return p.cur.inner.LocalAddr()
	}
	return net.UDPAddrFromAddrPort(unspecified(p.network, p.port))
}

func (p *packetConn) SetDeadline(t time.Time) error {
	p.SetReadDeadline(t)
	return p.SetWriteDeadline(t)
}

func (p *packetConn) SetReadDeadline(t time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readDeadline = t
	if p.cur.open {
		p.cur.inner.SetReadDeadline(t)
	}
	// A reader waiting for the interface looks at the new deadline.
	p.signal()
	return nil
}

func (p *packetConn) SetWriteDeadline(t time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writeDeadline = t
	if p.cur.open {
		p.cur.inner.SetWriteDeadline(t)
	}
	return nil
}

// Listener opens a TCP listener on port that follows the interface the way
// PacketConn does. Connections it accepted are closed when the interface
// changes.
func (b *Binder) Listener(network string, port int) (net.Listener, error) {
	ln := &listener{follower: follower[net.Listener]{b: b, network: network, port: port}}
	ln.listen = func(name string, l link, at netip.Addr, port int) (net.Listener, int, error) {
		s, err := listenConfig(name, l, network).Listen(context.Background(), network, netip.AddrPortFrom(at, uint16(port)).String())
		if err != nil {
			return nil, 0, err
		}
		return s, s.Addr().(*net.TCPAddr).Port, nil
	}
	if err := ln.start(); err != nil {
		return nil, err
	}
	return ln, nil
}

type listener struct {
	follower[net.Listener]
}

func (ln *listener) Accept() (net.Conn, error) {
	for {
		inner, ok, wake, err := ln.current()
		if err != nil {
			return nil, err
		}
		if !ok {
			<-wake
			continue
		}
		_, _, gen := ln.b.now()
		c, err := inner.Accept()
		if err != nil {
			if ln.moved(inner) {
				continue
			}
			return nil, err
		}
		w := &conn{Conn: c, b: ln.b}
		if ln.b.keep(w, gen) {
			return w, nil
		}
	}
}

func (ln *listener) Close() error {
	ln.b.remove(&ln.follower)
	return ln.close()
}

func (ln *listener) Addr() net.Addr {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.cur.open {
		return ln.cur.inner.Addr()
	}
	return net.TCPAddrFromAddrPort(unspecified(ln.network, ln.port))
}
