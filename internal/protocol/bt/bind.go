package bt

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/GopeedLab/gopeed/pkg/netbind"
	"github.com/anacrolix/torrent"
)

// binder hands out every socket of the client, see initClient.
var binder = netbind.Default

func init() {
	binder.Watch(saveSwarms, restoreSwarms)
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
