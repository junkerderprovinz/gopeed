//go:build !linux && !darwin && !windows

package netbind

import "syscall"

// bindControl does nothing here: the socket keeps only its bound address.
func bindControl(string, link, string) func(string, string, syscall.RawConn) error {
	return nil
}
