//go:build !windows

package http

import "os"

func openShared(name string) (*os.File, error) {
	return os.Open(name)
}
