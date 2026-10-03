// Package netbind ties sockets to one network interface and cuts them off
// while that interface is missing or down, the way a VPN kill switch does.
package netbind

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"
)

// ErrDown is the error of a dial or a send while the bound interface is
// missing, down or has no address to send from.
var ErrDown = errors.New("the bound network interface is missing or down")

// PollInterval is how often a Binder looks at its interface.
const PollInterval = 2 * time.Second

// Default is the Binder gopeed's BitTorrent client uses.
var Default = New()

// State is how the bound interface stands.
type State struct {
	// Interface is the bound interface's name, empty when sockets may use any.
	Interface string
	// Up reports whether traffic may flow. It is always true without an
	// interface.
	Up bool
	// Addrs are the addresses sockets bind to, at most one of each family.
	Addrs []netip.Addr
	// Since is when Up last changed.
	Since time.Time
}

// Interface is one of the system's network interfaces as a Binder sees it.
type Interface struct {
	Name  string
	Up    bool
	Addrs []netip.Addr
}

// Interfaces lists the system's interfaces with the addresses a Binder would
// bind to on each.
func Interfaces() ([]Interface, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	out := make([]Interface, 0, len(ifs))
	for _, ifc := range ifs {
		addrs, _ := ifc.Addrs()
		l := linkOf(ifc, addrs)
		out = append(out, Interface{Name: ifc.Name, Up: l.up, Addrs: l.addrs()})
	}
	return out, nil
}

// link is what a Binder binds to: the interface's index and one address of
// each family. A missing family is the zero Addr.
type link struct {
	up     bool
	index  int
	v4, v6 netip.Addr
}

func (l link) addrs() []netip.Addr {
	var out []netip.Addr
	for _, a := range []netip.Addr{l.v4, l.v6} {
		if a.IsValid() {
			out = append(out, a)
		}
	}
	return out
}

// addr is the address a socket of network's family binds to. Without an
// interface it is the unspecified one; for a network of no family it is IPv4
// when the interface has an IPv4 address.
func (l link) addr(name, network string) netip.Addr {
	v6 := len(network) > 0 && network[len(network)-1] == '6'
	v4 := len(network) > 0 && network[len(network)-1] == '4'
	if name == "" {
		if v6 {
			return netip.IPv6Unspecified()
		}
		return netip.IPv4Unspecified()
	}
	switch {
	case !l.up:
		return netip.Addr{}
	case v6:
		return l.v6
	case v4 || l.v4.IsValid():
		return l.v4
	}
	return l.v6
}

// linkOf reads an interface the way a Binder binds to it. It is up when the
// system says so and it has an address; link-local addresses do not count,
// since nothing past the link answers them.
func linkOf(ifc net.Interface, addrs []net.Addr) link {
	l := link{index: ifc.Index}
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		addr, ok := netip.AddrFromSlice(ip)
		addr = addr.Unmap()
		if !ok || addr.IsLinkLocalUnicast() || addr.IsUnspecified() {
			continue
		}
		switch {
		case addr.Is4() && !l.v4.IsValid():
			l.v4 = addr
		case addr.Is6() && !l.v6.IsValid():
			l.v6 = addr
		}
	}
	l.up = ifc.Flags&net.FlagUp != 0 && (l.v4.IsValid() || l.v6.IsValid())
	return l
}

func lookupLink(name string) link {
	ifc, err := net.InterfaceByName(name)
	if err != nil {
		return link{}
	}
	addrs, err := ifc.Addrs()
	if err != nil {
		return link{}
	}
	return linkOf(*ifc, addrs)
}

// socket is a socket that follows the interface: rebind moves it to where l
// puts its family, or closes it while that is nowhere.
type socket interface {
	rebind(name string, l link)
}

// Binder hands out sockets bound to one interface. Without an interface it
// binds to any, which is what the standard library does.
type Binder struct {
	lookup func(name string) link
	// refreshMu keeps two refreshes from applying their looks out of order.
	refreshMu sync.Mutex

	mu    sync.Mutex
	name  string
	cur   link
	since time.Time
	// gen counts the changes, so a dial that began before one is not kept.
	gen     uint64
	sockets map[socket]struct{}
	conns   map[io.Closer]struct{}
	stop    chan struct{}
	// before and after are Watch's.
	before, after func(State)
}

// New returns a Binder that binds to any interface until SetInterface names
// one.
func New() *Binder {
	return &Binder{
		lookup:  lookupLink,
		cur:     link{up: true},
		since:   time.Now(),
		sockets: map[socket]struct{}{},
		conns:   map[io.Closer]struct{}{},
	}
}

// SetInterface binds every socket to the named interface, or to any with an
// empty name. Sockets already open move at once and connections already open
// are closed. While a name is set, the interface is looked at every
// PollInterval.
func (b *Binder) SetInterface(name string) {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	if name == b.name {
		b.mu.Unlock()
		return
	}
	if b.stop != nil {
		close(b.stop)
		b.stop = nil
	}
	if name != "" {
		b.stop = make(chan struct{})
		go b.poll(b.stop)
	}
	b.mu.Unlock()
	b.apply(name)
}

func (b *Binder) poll(stop chan struct{}) {
	tick := time.NewTicker(PollInterval)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			b.Refresh()
		}
	}
}

// Refresh looks at the interface now rather than at the next poll.
func (b *Binder) Refresh() {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	b.mu.Lock()
	name := b.name
	b.mu.Unlock()
	b.apply(name)
}

// apply looks name up and makes it the interface, so that nobody sees the
// name with another interface's link. It must be called with refreshMu held.
func (b *Binder) apply(name string) {
	l := link{up: true}
	if name != "" {
		l = b.lookup(name)
	}

	b.mu.Lock()
	changed := l != b.cur || name != b.name || b.gen == 0
	var drop []io.Closer
	if changed {
		if l.up != b.cur.up {
			b.since = time.Now()
		}
		b.name = name
		b.cur = l
		b.gen++
		for c := range b.conns {
			drop = append(drop, c)
		}
		clear(b.conns)
	}
	sockets := make([]socket, 0, len(b.sockets))
	for s := range b.sockets {
		sockets = append(sockets, s)
	}
	st := b.stateLocked()
	before, after := b.before, b.after
	b.mu.Unlock()

	if changed && before != nil {
		before(st)
	}
	// Also when nothing changed: a socket whose bind failed tries again.
	for _, s := range sockets {
		s.rebind(name, l)
	}
	for _, c := range drop {
		c.Close()
	}
	if changed && after != nil {
		after(st)
	}
}

// Watch has before called on every change of the interface while the
// connections open on the old one are still there, and after once they are
// closed and the sockets have moved. Both get the new state.
func (b *Binder) Watch(before, after func(State)) {
	b.mu.Lock()
	b.before, b.after = before, after
	b.mu.Unlock()
}

// State reports how the bound interface stands.
func (b *Binder) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked()
}

func (b *Binder) stateLocked() State {
	return State{Interface: b.name, Up: b.name == "" || b.cur.up, Addrs: b.cur.addrs(), Since: b.since}
}

// now is the interface's name, link and generation under one lock.
func (b *Binder) now() (string, link, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.name, b.cur, b.gen
}

// keep registers c to be closed on the next change, unless a change came
// after gen, in which case c is closed at once.
func (b *Binder) keep(c io.Closer, gen uint64) bool {
	b.mu.Lock()
	if b.gen != gen {
		b.mu.Unlock()
		c.Close()
		return false
	}
	b.conns[c] = struct{}{}
	b.mu.Unlock()
	return true
}

func (b *Binder) forget(c io.Closer) {
	b.mu.Lock()
	delete(b.conns, c)
	b.mu.Unlock()
}

func (b *Binder) add(s socket) {
	b.mu.Lock()
	b.sockets[s] = struct{}{}
	b.mu.Unlock()
}

func (b *Binder) remove(s socket) {
	b.mu.Lock()
	delete(b.sockets, s)
	b.mu.Unlock()
}

// conn is a connection closed when the interface changes.
type conn struct {
	net.Conn
	b    *Binder
	once sync.Once
}

func (c *conn) Close() error {
	c.once.Do(func() { c.b.forget(c) })
	return c.Conn.Close()
}
