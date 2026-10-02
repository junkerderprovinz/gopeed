package netbind

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"
)

// Dialer dials through a Binder.
type Dialer struct {
	Binder *Binder
	// KeepAlive is net.Dialer's: zero for the default, negative for none.
	KeepAlive time.Duration
	// NoLinger resets a TCP connection on close rather than draining it.
	NoLinger bool
}

// DialContext dials address from the interface's address, with the socket tied
// to the interface where the system allows it (see bindControl). It fails with
// ErrDown while the interface is down, before a name is even looked up, and
// the connection is closed when the interface changes.
func (d Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	b := d.Binder
	name, l, gen := b.now()
	if name != "" && !l.up {
		return nil, ErrDown
	}
	nd := net.Dialer{KeepAlive: d.KeepAlive}
	var c net.Conn
	var err error
	if name == "" {
		c, err = nd.DialContext(ctx, network, address)
	} else {
		c, err = d.dialFrom(ctx, &nd, name, l, network, address)
	}
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok && d.NoLinger {
		tc.SetLinger(0)
	}
	w := &conn{Conn: c, b: b}
	if !b.keep(w, gen) {
		return nil, ErrDown
	}
	return w, nil
}

// dialFrom tries each address of the host whose family the interface has an
// address of, from that address.
func (d Dialer) dialFrom(ctx context.Context, nd *net.Dialer, name string, l link, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	var ips []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		ips = []netip.Addr{ip.Unmap()}
	} else if ips, err = net.DefaultResolver.LookupNetIP(ctx, "ip", host); err != nil {
		return nil, err
	}
	err = fmt.Errorf("no address of %s can be reached from %s", host, name)
	for _, ip := range ips {
		ip = ip.Unmap()
		family := "4"
		if ip.Is6() {
			family = "6"
		}
		n := network
		if n == "tcp" || n == "udp" {
			n += family
		}
		local := l.addr(name, n)
		if !local.IsValid() || n[len(n)-1:] != family {
			continue
		}
		nd.LocalAddr = localAddr(n, local, 0)
		nd.Control = bindControl(name, l, n)
		c, dialErr := nd.DialContext(ctx, n, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return c, nil
		}
		err = dialErr
	}
	return nil, err
}

// DialContext is a Dialer's with no options set.
func (b *Binder) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return Dialer{Binder: b}.DialContext(ctx, network, address)
}

// ListenPacket opens a packet socket on the interface the way net.ListenPacket
// would, with only the port of address used. It fails with ErrDown while the
// interface is down, and the socket is closed when the interface changes.
func (b *Binder) ListenPacket(network, address string) (net.PacketConn, error) {
	port, err := portOf(address)
	if err != nil {
		return nil, err
	}
	name, l, gen := b.now()
	var pc net.PacketConn
	if name == "" {
		pc, err = net.ListenPacket(network, address)
	} else if at := l.addr(name, network); at.IsValid() {
		pc, err = listenPacket(name, l, network, at, port)
	} else {
		err = ErrDown
	}
	if err != nil {
		return nil, err
	}
	p := &packet{PacketConn: pc, b: b}
	if !b.keep(p, gen) {
		return nil, ErrDown
	}
	return p, nil
}

func portOf(address string) (int, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(port)
}

func localAddr(network string, ip netip.Addr, port int) net.Addr {
	ap := netip.AddrPortFrom(ip, uint16(port))
	if network[:3] == "udp" {
		return net.UDPAddrFromAddrPort(ap)
	}
	return net.TCPAddrFromAddrPort(ap)
}

func listenConfig(name string, l link, network string) *net.ListenConfig {
	return &net.ListenConfig{Control: bindControl(name, l, network)}
}

func listenPacket(name string, l link, network string, at netip.Addr, port int) (net.PacketConn, error) {
	return listenConfig(name, l, network).ListenPacket(context.Background(), network, netip.AddrPortFrom(at, uint16(port)).String())
}
