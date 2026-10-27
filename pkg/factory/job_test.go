package factory

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/logoscore/logos-golang-protocol/protocol"
)

func testJob(st *store) *Job {
	steps := []protocol.BuildStep{{ID: "compile", Label: "Compile"}, {ID: "link", Label: "Link"}}
	return newJob(testRequest(st), testSchema().Targets[0], steps, http.DefaultClient)
}

func TestUploadArtifactEdges(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	job := testJob(st)
	if err := job.UploadArtifact(ctx, "empty.bin", "", strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if body, _, length, ok := st.upload("/artifacts/" + job.Request.MinionID); !ok || len(body) != 0 || length != 0 || st.chunked {
		t.Fatalf("empty upload: ok=%v length=%d chunked=%v", ok, length, st.chunked)
	}
	if a := job.uploaded(); a == nil || a.Size != 0 || a.SHA256 != sha("") {
		t.Fatalf("empty artifact %+v", a)
	}

	job = testJob(st)
	if err := job.UploadArtifact(ctx, "../minion.json", "", strings.NewReader("x")); err == nil {
		t.Error("a path as the file name was accepted")
	}
	if _, _, _, ok := st.upload("/artifacts/" + job.Request.MinionID); ok || job.uploaded() != nil {
		t.Error("a refused artifact was uploaded")
	}

	job = testJob(st)
	job.Request.Artifact.MaxSize = 4
	var f *Failure
	if err := job.UploadArtifact(ctx, "big.bin", "", strings.NewReader("12345")); !errors.As(err, &f) || f.Code != "artifact_too_large" {
		t.Errorf("over max_size: %v", err)
	}

	st.mu.Lock()
	st.putStatus = http.StatusForbidden
	st.mu.Unlock()
	job = testJob(st)
	if err := job.UploadArtifact(ctx, "minion.json", "", strings.NewReader("x")); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("refused upload: %v", err)
	}
	if job.uploaded() != nil {
		t.Error("a refused upload was recorded")
	}
}

func TestDownloads(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	job := testJob(st)
	icon, _ := job.File("icon")

	if b, err := job.ReadFile(ctx, icon); err != nil || string(b) != iconBody {
		t.Fatalf("ReadFile = %q, %v", b, err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "app.ico")
	if err := job.Download(ctx, icon, path); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != iconBody {
		t.Fatalf("downloaded %q, %v", b, err)
	}

	for name, f := range map[string]protocol.BuildFile{
		"wrong hash": {Name: icon.Name, Size: icon.Size, SHA256: sha("other"), URL: icon.URL},
		"wrong size": {Name: icon.Name, Size: icon.Size + 1, SHA256: icon.SHA256, URL: icon.URL},
		"missing":    {Name: "gone.ico", Size: 1, SHA256: sha("x"), URL: st.URL + "/files/gone.ico"},
	} {
		if _, err := job.ReadFile(ctx, f); err == nil {
			t.Errorf("ReadFile, %s: accepted", name)
		}
		bad := filepath.Join(dir, "bad.ico")
		if err := job.Download(ctx, f, bad); err == nil {
			t.Errorf("Download, %s: accepted", name)
		}
		if _, err := os.Stat(bad); !os.IsNotExist(err) {
			t.Errorf("Download, %s: left a file behind", name)
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("leftover temporary files: %v", entries)
	}
	if _, ok := job.File("greeting"); ok {
		t.Error("File found a file for a string field")
	}
}

func TestStepCoalesces(t *testing.T) {
	job := testJob(newStore(t))
	var unknown []string
	job.unknownStep = func(id string) { unknown = append(unknown, id) }
	job.Step("compile", "first")
	job.Step("link", "second")
	job.Step("Not A Step", "dropped")
	job.Step("sign", "not planned: dropped")
	job.Step("sign", "dropped, told once")
	p := <-job.progress
	if p.Step != "link" || p.Message != "second" {
		t.Fatalf("pending progress %+v, want the latest planned one", p)
	}
	if strings.Join(unknown, ",") != "Not A Step,sign" {
		t.Fatalf("unknown steps told: %v", unknown)
	}
	job.close()
	job.Step("link", "late") // after the build: dropped, no panic
	if _, open := <-job.progress; open {
		t.Fatal("progress after close")
	}
}

func TestBuildLogTail(t *testing.T) {
	tb := &tailBuffer{max: 16}
	for range 10 {
		_, _ = tb.Write([]byte("0123456789"))
	}
	if s := tb.String(); s != "4567890123456789" {
		t.Fatalf("tail = %q", s)
	}

	tb = &tailBuffer{max: 16}
	_, _ = tb.Write([]byte("abc\xff\xfedef"))
	if s := tb.String(); !utf8.ValidString(s) || !strings.HasPrefix(s, "abc") || !strings.HasSuffix(s, "def") {
		t.Fatalf("invalid UTF-8 kept: %q", s)
	}

	// "é" is 2 bytes: after "a", an odd limit cuts through one.
	tb = &tailBuffer{max: 15}
	_, _ = tb.Write([]byte("a" + strings.Repeat("é", 20)))
	if s := tb.String(); s != strings.Repeat("é", 7) {
		t.Fatalf("tail %q, want whole characters and no replacement", s)
	}
	// A 4-byte character cut after its first byte leaves 3 stray bytes,
	// which one replacement character would not even make longer.
	tb = &tailBuffer{max: 15}
	_, _ = tb.Write([]byte("a" + strings.Repeat("😀", 10)))
	if s := tb.String(); s != strings.Repeat("😀", 3) {
		t.Fatalf("tail %q, want whole characters and no replacement", s)
	}
	tb = &tailBuffer{max: 15}
	for range 20 { // trimmed on String, not on Write
		_, _ = tb.Write([]byte("é"))
	}
	_, _ = tb.Write([]byte("a"))
	if s := tb.String(); s != strings.Repeat("é", 7)+"a" {
		t.Fatalf("tail %q", s)
	}

	job := testJob(newStore(t))
	job.Logf("line %d", 1)
	job.Logf("line %d\n", 2)
	_, _ = job.Log().Write([]byte("raw"))
	if s := job.logs.String(); s != "line 1\nline 2\nraw" {
		t.Fatalf("log = %q", s)
	}
}
