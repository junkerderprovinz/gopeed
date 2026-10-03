package netbind

import (
	"errors"
	"syscall"
)

// bindControl ties a socket to the interface with SO_BINDTODEVICE, so a route
// through another interface cannot carry it even while the address stays
// configured on an interface that is down. Before Linux 5.7 that needs
// CAP_NET_RAW; without it the socket keeps only its bound address.
func bindControl(name string, _ link, _ string) func(string, string, syscall.RawConn) error {
	if name == "" {
		return nil
	}
	return func(_, _ string, c syscall.RawConn) error {
		var err error
		if ctlErr := c.Control(func(fd uintptr) {
			err = syscall.SetsockoptString(int(fd), syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, name)
		}); ctlErr != nil {
			return ctlErr
		}
		if errors.Is(err, syscall.EPERM) {
			return nil
		}
		return err
	}
}
