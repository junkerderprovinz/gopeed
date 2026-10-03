# The knightloader branch

This branch is the gopeed that [KnightLoader](https://github.com/junkerderprovinz/knightloader) builds against: the latest gopeed release plus the changes KnightLoader needs.

- An upload rate limit for BitTorrent that also applies to running torrents.
- Settings to turn off DHT and PEX.
- `ConfigurableFetcherManager.ApplyConfig`, so a protocol can pick up config changes without waiting for the next task.
- A fix for a panic when the BitTorrent client is closed right after it was built.
- Locking for the task list and for task status and progress, which the downloader's own goroutines read while other calls change them.
- Binding BitTorrent traffic to one network interface (`pkg/netbind`): peers, trackers and the DHT use only that interface, and nothing is sent or received while it is missing or down. Tracker sockets open and wait in that state rather than fail, and a panic of anacrolix/torrent while a torrent is added becomes the task's error.
- Two fixes to anacrolix/torrent, taken from a fork of it (see below): trackers keep announcing the other torrents after one is dropped, and closing the client closes its tracker sockets.

KnightLoader pins the branch with a `replace` directive in its `go.mod` that points at a commit here. Because this is the fork's default branch, Renovate in KnightLoader proposes the new head whenever the branch moves.

## The anacrolix/torrent fork

`go.mod` replaces anacrolix/torrent with the `knightloader` branch of [junkerderprovinz/torrent](https://github.com/junkerderprovinz/torrent/tree/knightloader): the anacrolix/torrent commit gopeed requires, plus two fixes.

- In upstream, a dropped torrent stops the announces of all the others. Its entry in the announcer stays after the last announce, with no time set for the next one. Such an entry counts as overdue, so it sorts first and leaves the announce timer with nothing to wait for, and no torrent announces again until another one is added. The fork removes the entry once nothing is left to send for it.
- Upstream's `Client.Close` never closes the tracker clients, so the socket of every UDP tracker stays open, and with an interface set the binder keeps moving these sockets along with it. The fork closes them with the client.

KnightLoader carries the same `replace` in both of its `go.mod` files, since Go ignores a `replace` in a dependency. When gopeed moves to a newer anacrolix/torrent, rebase the fork's branch onto that version, push it, and point the `replace` here and in KnightLoader at the new commit. Drop the `replace` once upstream has both fixes.

## Staying on the latest release

`.github/workflows/knightloader-sync.yml` runs every Monday and can also be started by hand. It looks up the newest gopeed release tag (`vX.Y.Z`; pre-releases are skipped). If the branch is still based on an older release, the job rebases the commits above that release onto the new tag and runs `go vet ./...` plus the tests in `internal/protocol/bt` and `pkg/netbind` (with `-race`) and `pkg/download`. It force-pushes the branch only when all of them pass.

When the rebase hits a conflict or a check fails, nothing is pushed. The workflow opens an issue named after the tag instead, or comments on it if one is already open. To fix it by hand, rebase onto the tag locally, resolve the conflict, run the same tests and push with `--force-with-lease`.

## Why there are no gopeed workflows here

The workflow token may not push a change to `.github/workflows`, and gopeed edits its own workflows in most releases. So this branch removes them, and the sync job keeps them out when a rebase brings them back.
