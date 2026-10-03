package bt

import (
	"context"
	"net"
	"time"

	"github.com/rs/dnscache"
)

// DnsCacheResolver resolves DNS requests for an HTTP client using an in-memory cache.
type DnsCacheResolver struct {
	RefreshTimeout time.Duration
	// Dial dials each resolved address in turn, a plain net.Dialer when nil.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)

	resolver dnscache.Resolver
}

func (r *DnsCacheResolver) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := r.resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	dial := r.Dial
	if dial == nil {
		var dialer net.Dialer
		dial = dialer.DialContext
	}
	var conn net.Conn
	for _, ip := range ips {
		conn, err = dial(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			break
		}
	}
	return conn, err
}

func (r *DnsCacheResolver) Run(ctx context.Context) {
	ticker := time.NewTicker(r.RefreshTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.resolver.Refresh(true)
		}
	}
}
