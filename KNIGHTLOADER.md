# The knightloader branch

This branch is the gopeed that [KnightLoader](https://github.com/junkerderprovinz/knightloader) builds against: the latest gopeed release plus the changes KnightLoader needs.

- An upload rate limit for BitTorrent that also applies to running torrents.
- Settings to turn off DHT and PEX.
- `ConfigurableFetcherManager.ApplyConfig`, so a protocol can pick up config changes without waiting for the next task.
- A fix for a panic when the BitTorrent client is closed right after it was built.
- Locking for the task list and for task status and progress, which the downloader's own goroutines read while other calls change them.
- Mirror URLs for an HTTP download (`ReqExtra.Mirrors`). A ranged download spreads its connections over the URL and its mirrors, and moves a connection to another source when its own refuses it or keeps failing. No source holds more than its even share of the connections, so the connections of a dead source wait rather than crowd onto the others.
- `Downloader.Stream`, which reads a file of a running task. A read waits for bytes that have not arrived, and the task fetches the part being read first: a torrent through a reader with readahead and both ends of the file raised, an HTTP download by moving one of its connections to the read position.

KnightLoader pins the branch with a `replace` directive in its `go.mod` that points at a commit here. Because this is the fork's default branch, Renovate in KnightLoader proposes the new head whenever the branch moves.

## Staying on the latest release

`.github/workflows/knightloader-sync.yml` runs every Monday and can also be started by hand. It looks up the newest gopeed release tag (`vX.Y.Z`; pre-releases are skipped). If the branch is still based on an older release, the job rebases the commits above that release onto the new tag and runs `go vet ./...` plus the tests in `internal/protocol/bt` (with `-race`) and `pkg/download`. It force-pushes the branch only when all of them pass.

When the rebase hits a conflict or a check fails, nothing is pushed. The workflow opens an issue named after the tag instead, or comments on it if one is already open. To fix it by hand, rebase onto the tag locally, resolve the conflict, run the same tests and push with `--force-with-lease`.

## Why there are no gopeed workflows here

The workflow token may not push a change to `.github/workflows`, and gopeed edits its own workflows in most releases. So this branch removes them, and the sync job keeps them out when a rebase brings them back.
