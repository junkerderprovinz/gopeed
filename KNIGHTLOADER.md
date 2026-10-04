# The knightloader branch

This branch is the gopeed that [KnightLoader](https://github.com/junkerderprovinz/knightloader) builds against: the latest gopeed release plus the changes KnightLoader needs.

- An upload rate limit for BitTorrent that also applies to running torrents.
- Settings to turn off DHT and PEX.
- `ConfigurableFetcherManager.ApplyConfig`, so a protocol can pick up config changes without waiting for the next task.
- A fix for a panic when the BitTorrent client is closed right after it was built.
- Locking for the task list and for task status and progress, which the downloader's own goroutines read while other calls change them.
- Mirror URLs for an HTTP download (`ReqExtra.Mirrors`). A ranged download spreads its connections over the URL and its mirrors, and moves a connection to another source when its own refuses it or keeps failing. No source holds more than its even share of the connections, so the connections of a dead source wait rather than crowd onto the others.
- `Downloader.Stream`, which reads a file of a running task. A read waits for bytes that have not arrived, and the task fetches the part being read first: a torrent through a reader with readahead and both ends of the file raised, an HTTP download by moving one of its connections to the read position.
- Binding BitTorrent traffic to one network interface (`pkg/netbind`): peers, trackers and the DHT use only that interface, and nothing is sent or received while it is missing or down. Tracker sockets open and wait in that state rather than fail, and a panic of anacrolix/torrent while a torrent is added becomes the task's error.
- `Downloader.ResolveContext`: a resolve the caller can give up on. Once its context ends, a magnet still waiting for its file list leaves the BitTorrent client, and so does a resolved torrent that `Create` never took.
- The BitTorrent client gives its default storage an in-memory piece completion. The default would open a database in the working directory, and no torrent uses the default storage.
- Fixes to anacrolix/torrent, taken from a fork of it (see below), for the announces of a dropped torrent and of a closing client.
- Locking for an HTTP download's connection list, which saving the task reads while the download loop adds to it.

KnightLoader pins the branch with a `replace` directive in its `go.mod` that points at a commit here. Because this is the fork's default branch, Renovate in KnightLoader proposes the new head whenever the branch moves.

## The anacrolix/torrent fork

`go.mod` replaces anacrolix/torrent with the `knightloader` branch of [junkerderprovinz/torrent](https://github.com/junkerderprovinz/torrent/tree/knightloader): the anacrolix/torrent commit gopeed requires, plus these fixes.

- In upstream, a dropped torrent stops the announces of all the others. Its entry in the announcer stays after the last announce, with no time set for the next one. Such an entry counts as overdue, so it sorts first and leaves the announce timer with nothing to wait for, and no torrent announces again until another one is added. The fork removes the entry once nothing is left to send for it.
- If a torrent is dropped and added again before its stopped announce has gone out, upstream keeps the old announcer entry, which still points at the dropped torrent. It announces for that one until the garbage collector takes it, and then not at all. The fork hands the entry to the new torrent, which starts with a started announce.
- Upstream's `Client.Close` never closes the tracker clients, so the socket of every UDP tracker stays open, and with an interface set the binder keeps moving these sockets along with it. The fork's `Close` sends the stopped announces of the torrents it drops, waits up to three seconds for them, and then closes the tracker clients. Nothing goes out after `Close` returns, so the binder can let go of the interface right away.
- Upstream reads its map of tracker clients for an announce after releasing the client lock, while adding a tracker writes to that map under the lock. The fork looks the tracker client up before it lets go of the lock.

KnightLoader carries the same `replace` in both of its `go.mod` files, since Go ignores a `replace` in a dependency. When gopeed moves to a newer anacrolix/torrent, rebase the fork's branch onto that version, push it, and point the `replace` here and in KnightLoader at the new commit. Drop the `replace` once upstream has these fixes.

## Staying on the latest release

`.github/workflows/knightloader-sync.yml` runs every Monday and can also be started by hand. It looks up the newest gopeed release tag (`vX.Y.Z`; pre-releases are skipped). If the branch is still based on an older release, the job rebases the commits above that release onto the new tag and runs `go vet ./...` plus the tests in `internal/protocol/bt` and `pkg/netbind` (with `-race`) and `pkg/download`. It force-pushes the branch only when all of them pass.

When the rebase hits a conflict or a check fails, nothing is pushed. The workflow opens an issue named after the tag instead, or comments on it if one is already open. To fix it by hand, rebase onto the tag locally, resolve the conflict, run the same tests and push with `--force-with-lease`.

## Why there are no gopeed workflows here

The workflow token may not push a change to `.github/workflows`, and gopeed edits its own workflows in most releases. So this branch removes them, and the sync job keeps them out when a rebase brings them back.
