package bt

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GopeedLab/gopeed/pkg/netbind"
	"github.com/anacrolix/dht/v2"
	"github.com/anacrolix/torrent"
)

// binder hands out every socket of the client, see initClient.
var binder = netbind.Default

func init() {
	binder.Watch(saveSwarms, restoreSwarms)
}

// builtFor is the interface the client was built bound to, guarded by lock
// like client.
var builtFor string

// familiesOff reports the address families a client built on st leaves out:
// those the interface has no address of. While the interface is down nothing
// is known of its families, so the client keeps both and its sockets wait.
func familiesOff(st netbind.State) (v4, v6 bool) {
	if st.Interface == "" || !st.Up {
		return false, false
	}
	v4, v6 = true, true
	for _, a := range st.Addrs {
		if a.Is4() {
			v4 = false
		} else {
			v6 = false
		}
	}
	return v4, v6
}

// peerListener is a TCP listener for peers with the network it listens on.
type peerListener struct {
	net.Listener
	network string
}

// peerListeners are the client's TCP listeners, guarded by lock like client.
var peerListeners []peerListener

// listenPeers opens the TCP listeners for peers on port. A port of 0 is one
// free for UDP as well, where the client opens its own sockets on the same
// port. An IPv6 listener the system refuses is left out, as the client leaves
// out its own.
func listenPeers(port int) ([]peerListener, int, error) {
	if port == 0 {
		var err error
		if port, err = netbind.FreePort(); err != nil {
			return nil, 0, err
		}
	}
	var out []peerListener
	for _, network := range []string{"tcp4", "tcp6"} {
		l, err := binder.Listener(network, port)
		if err != nil {
			if network == "tcp6" && len(out) > 0 {
				continue
			}
			return nil, 0, err
		}
		port = l.Addr().(*net.TCPAddr).Port
		out = append(out, peerListener{Listener: l, network: network})
	}
	return out, port, nil
}

// closePeerListeners must be called with lock held.
func closePeerListeners() {
	for _, l := range peerListeners {
		l.Close()
	}
	peerListeners = nil
	peerPackets = nil
}

// While the interface is down every dial fails, and the client forgets a peer
// it failed to reach. So the peers each torrent knows are kept when the
// interface changes and handed back once it is up. The binder calls these
// with lock possibly held by whoever changed the interface, so they reach the
// client through liveClient rather than lock.
var (
	liveClient atomic.Pointer[torrent.Client]
	swarmLock  sync.Mutex
	swarms     map[*torrent.Torrent][]torrent.PeerInfo
)

func saveSwarms(netbind.State) {
	cl := liveClient.Load()
	swarmLock.Lock()
	defer swarmLock.Unlock()
	if cl == nil {
		swarms = nil
		return
	}
	if swarms == nil {
		swarms = map[*torrent.Torrent][]torrent.PeerInfo{}
	}
	for _, t := range cl.Torrents() {
		if _, ok := swarms[t]; !ok {
			swarms[t] = t.KnownSwarm()
		}
	}
}

func restoreSwarms(st netbind.State) {
	if !st.Up || liveClient.Load() == nil {
		return
	}
	swarmLock.Lock()
	saved := swarms
	swarms = nil
	swarmLock.Unlock()
	for t, peers := range saved {
		t.AddPeers(peers)
	}
}

// peerPackets are the client's UDP sockets for uTP and the DHT, guarded by
// lock like client. The client closes them itself.
var peerPackets []net.PacketConn

// movePeerPort moves the client's peer sockets to port while torrents run, so
// a port a VPN forwards takes effect without dropping a download, and has
// every torrent tell its trackers and the DHT. The client announces whatever
// port its listeners report, so nothing else needs to learn it. A socket that
// cannot move takes the others back, so the client never listens on two
// ports. It must be called with lock held.
func movePeerPort(port int) error {
	old := cfg.ListenPort
	sockets := make([]any, 0, len(peerListeners)+len(peerPackets))
	for _, l := range peerListeners {
		sockets = append(sockets, l.Listener)
	}
	for _, pc := range peerPackets {
		sockets = append(sockets, pc)
	}
	for i, s := range sockets {
		if err := netbind.SetPort(s, port); err != nil {
			for _, moved := range sockets[:i] {
				netbind.SetPort(moved, old)
			}
			return err
		}
	}
	cfg.ListenPort = port
	client.ReannounceToTrackers()
	for _, t := range client.Torrents() {
		for _, s := range client.DhtServers() {
			if done, stop, err := t.AnnounceToDht(s); err == nil {
				go func() {
					select {
					case <-done:
					case <-time.After(time.Minute):
					}
					stop()
				}()
			}
		}
	}
	return nil
}

// dhtStartingNodes resolves the DHT's bootstrap nodes with the binder, like
// every other name the client looks up.
func dhtStartingNodes(string) dht.StartingNodesGetter {
	return func() ([]dht.Addr, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var addrs []dht.Addr
		for _, hp := range dht.DefaultGlobalBootstrapHostPorts {
			host, port, err := net.SplitHostPort(hp)
			if err != nil {
				continue
			}
			p, err := strconv.Atoi(port)
			if err != nil {
				continue
			}
			ips, err := binder.LookupHost(ctx, host)
			if err != nil {
				continue
			}
			for _, ip := range ips {
				if a, err := netip.ParseAddr(ip); err == nil {
					addrs = append(addrs, dht.NewAddr(net.UDPAddrFromAddrPort(netip.AddrPortFrom(a.Unmap(), uint16(p)))))
				}
			}
		}
		if len(addrs) == 0 {
			return nil, errors.New("no DHT bootstrap node resolved")
		}
		return addrs, nil
	}
}
