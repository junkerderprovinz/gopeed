package netbind

import "syscall"

// IP_BOUND_IF and IPV6_BOUND_IF from <netinet/in.h>.
const (
	ipBoundIf   = 25
	ipv6BoundIf = 125
)

// bindControl ties a socket to the interface by its index, so the system
// sends from it only through that interface.
func bindControl(name string, l link, network string) func(string, string, syscall.RawConn) error {
	if name == "" {
		return nil
	}
	return func(_, _ string, c syscall.RawConn) error {
		var err error
		if ctlErr := c.Control(func(fd uintptr) {
			if network[len(network)-1] == '6' {
				err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6BoundIf, l.index)
			} else {
				err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipBoundIf, l.index)
			}
		}); ctlErr != nil {
			return ctlErr
		}
		return err
	}
}
