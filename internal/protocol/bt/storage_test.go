package bt

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// The storage creates a torrent's empty files as soon as the info arrives, and
// a magnet's info comes from the swarm.
func TestStorage_KeepsEveryFileInsideTheTorrentsFolder(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim.mkv")
	if err := os.WriteFile(victim, []byte("already here"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir:   dir,
		TorrentDirMaker: func(string, *metainfo.Info, metainfo.Hash) string { return dir },
		FilePathMaker:   confinedFilePath,
	})
	t.Cleanup(func() { s.Close() })

	for _, path := range [][]string{{"..", "victim.mkv"}, {"../victim.mkv"}, {`..\victim.mkv`}} {
		info := &metainfo.Info{Name: "Show", Files: []metainfo.FileInfo{{Path: []string{"ep01.mkv"}}, {Path: path}}}
		if _, err := s.OpenTorrent(context.Background(), info, metainfo.Hash{}); err == nil {
			t.Errorf("a torrent with the file %q was opened", path)
		}
		if b, err := os.ReadFile(victim); err != nil || string(b) != "already here" {
			t.Fatalf("after %q the file beside the torrent's folder holds %q, %v", path, b, err)
		}
	}

	info := &metainfo.Info{Name: "Show", Files: []metainfo.FileInfo{{Path: []string{"subs", "en.srt"}}}}
	if _, err := s.OpenTorrent(context.Background(), info, metainfo.Hash{}); err != nil {
		t.Fatalf("an ordinary torrent was refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "Show", "subs", "en.srt")); err != nil {
		t.Fatalf("its empty file is not where the torrent puts it: %v", err)
	}
}
