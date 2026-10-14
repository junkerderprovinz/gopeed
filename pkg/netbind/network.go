package netbind

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"sync"
)

// Network is a network stack a Binder can hand sockets out of in place of the
// system's, such as a VPN tunnel that runs inside the process. Its sockets
// cannot reach any other network, so binding to it leaves nothing to leak.
type Network interface {
	// Status reports whether the network carries traffic now.
	Status() NetworkStatus
	// DialContext dials an address of literal IPs, never a host name.
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
	// ListenPacket opens a UDP socket at one of the network's addresses.
	ListenPacket(network string, at netip.AddrPort) (net.PacketConn, error)
	// Listen opens a TCP listener at one of the network's addresses.
	Listen(network string, at netip.AddrPort) (net.Listener, error)
	// LookupNetIP resolves host with the network's own name servers.
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// NetworkStatus is how a Network stands. Stack names the stack behind the
// network and changes whenever that is replaced, which moves every socket of
// a Binder onto the new one.
type NetworkStatus struct {
	Up    bool
	Addrs []netip.Addr
	Stack uint64
}

var (
	networksMu sync.Mutex
	networks   = map[string]Network{}
)

// Register makes n bindable by name the way a system interface is: a Binder
// set to name hands out n's sockets, and while n is down it hands out none.
// A registered name hides a system interface of the same name. Registering nil
// removes the name. n must be comparable, which a pointer is.
func Register(name string, n Network) error {
	if name == "" {
		return errors.New("netbind: a network needs a name")
	}
	if n != nil && !reflect.TypeOf(n).Comparable() {
		return fmt.Errorf("netbind: %T is not comparable", n)
	}
	networksMu.Lock()
	defer networksMu.Unlock()
	if n == nil {
		delete(networks, name)
	} else {
		networks[name] = n
	}
	return nil
}

func registered(name string) Network {
	networksMu.Lock()
	defer networksMu.Unlock()
	return networks[name]
}

func registeredInterfaces() []Interface {
	networksMu.Lock()
	names := make([]string, 0, len(networks))
	for name := range networks {
		names = append(names, name)
	}
	networksMu.Unlock()
	slices.Sort(names)
	out := make([]Interface, 0, len(names))
	for _, name := range names {
		if n := registered(name); n != nil {
			l := networkLink(n)
			out = append(out, Interface{Name: name, Up: l.up, Addrs: l.addrs()})
		}
	}
	return out
}

func networkLink(n Network) link {
	st := n.Status()
	l := link{net: n, stack: st.Stack}
	for _, a := range st.Addrs {
		a = a.Unmap()
		switch {
		case a.Is4() && !l.v4.IsValid():
			l.v4 = a
		case a.Is6() && !l.v6.IsValid():
			l.v6 = a
		}
	}
	l.up = st.Up && (l.v4.IsValid() || l.v6.IsValid())
	return l
}

// LookupHost resolves host where the Binder's sockets go: with the network's
// name servers while bound to a Network, and with the system's otherwise.
// While the network is down it fails with ErrDown rather than ask the system.
func (b *Binder) LookupHost(ctx context.Context, host string) ([]string, error) {
	_, l, _ := b.now()
	if l.net == nil {
		return net.DefaultResolver.LookupHost(ctx, host)
	}
	if !l.up {
		return nil, ErrDown
	}
	ips, err := l.net.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(ips))
	for i, ip := range ips {
		out[i] = ip.Unmap().String()
	}
	return out, nil
}

// LookupAddr is the system's reverse lookup, which a Network does not offer.
// It completes the resolver interface DNS caches take.
func (b *Binder) LookupAddr(ctx context.Context, addr string) ([]string, error) {
	if _, l, _ := b.now(); l.net != nil {
		return nil, &net.DNSError{Err: "no reverse lookups through a bound network", Name: addr, IsNotFound: true}
	}
	return net.DefaultResolver.LookupAddr(ctx, addr)
}

// SetPort moves a socket opened by PacketConn, ListenPacket or Listener to
// another port at the address it is bound to. Connections a listener accepted
// stay open. On error the socket stays where it was.
func SetPort(socket any, port int) error {
	switch s := socket.(type) {
	case *packetConn:
		return s.setPort(port)
	case *listener:
		return s.setPort(port)
	}
	return fmt.Errorf("netbind: %T is not a socket of a Binder", socket)
}
