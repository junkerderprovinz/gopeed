package netbind

import (
	"encoding/binary"
	"syscall"
)

// IP_UNICAST_IF and IPV6_UNICAST_IF from ws2ipdef.h.
const (
	ipUnicastIf   = 31
	ipv6UnicastIf = 31
)

// bindControl names the interface outgoing unicast leaves through. Windows
// keeps the IPv4 index in network byte order and the IPv6 one in host order.
func bindControl(name string, l link, network string) func(string, string, syscall.RawConn) error {
	if name == "" {
		return nil
	}
	return func(_, _ string, c syscall.RawConn) error {
		var err error
		if ctlErr := c.Control(func(fd uintptr) {
			h := syscall.Handle(fd)
			if network[len(network)-1] == '6' {
				err = syscall.SetsockoptInt(h, syscall.IPPROTO_IPV6, ipv6UnicastIf, l.index)
				return
			}
			var be [4]byte
			binary.BigEndian.PutUint32(be[:], uint32(l.index))
			err = syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipUnicastIf, int(binary.NativeEndian.Uint32(be[:])))
		}); ctlErr != nil {
			return ctlErr
		}
		return err
	}
}
