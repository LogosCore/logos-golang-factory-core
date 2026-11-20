package factory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/logoscore/logos-golang-protocol/protocol"
)

// transfers moves a factory's files to and from core over the links core
// gives (protocol.IsLink): a link that is a path is on core's base URL,
// core's own transfer endpoints; an absolute URL goes as it is (a
// factorytest.Store's). A build's Job and a composite's ComposeCall share
// them.
type transfers struct {
	http *http.Client
	core string // core's base URL: a link that is a path is on it
}

// errTooLarge is spool's: the body ran past its limit.
var errTooLarge = errors.New("too large")

// link is where l goes.
func (t transfers) link(l string) (string, error) {
	if !strings.HasPrefix(l, "/") {
		return l, nil
	}
	if t.core == "" {
		return "", errors.New("a link on core's address, and no core base URL (Config.CoreURL)")
	}
	return strings.TrimRight(t.core, "/") + l, nil
}

// fetch downloads f into w and checks its size and SHA-256.
func (t transfers) fetch(ctx context.Context, f protocol.BuildFile, w io.Writer) error {
	u, err := t.link(f.URL)
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	// Read to the size or failed already: closing it says nothing more.
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", f.Name, resp.Status)
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), io.LimitReader(resp.Body, f.Size+1))
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	if n != f.Size {
		return fmt.Errorf("download %s: got %d bytes, want %d", f.Name, n, f.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return fmt.Errorf("download %s: SHA-256 is %s, want %s", f.Name, got, f.SHA256)
	}
	return nil
}

// readFile downloads a small file into memory, checked.
func (t transfers) readFile(ctx context.Context, f protocol.BuildFile) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(int(min(f.Size, 1<<20)))
	if err := t.fetch(ctx, f, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// download saves a file to path, checked. Nothing is left at path when it
// fails.
func (t transfers) download(ctx context.Context, f protocol.BuildFile, path string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".download-*")
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // a no-op after the rename
	err = t.fetch(ctx, f, tmp)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// get downloads what l names, limit bytes at most.
func (t transfers) get(ctx context.Context, l string, limit int64) ([]byte, error) {
	u, err := t.link(l)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, err
	}
	// Read to the limit or failed already: closing it says nothing more.
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("core answered %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("larger than %d bytes", limit)
	}
	return b, nil
}

// put sends n bytes of body to l with header, as a PUT of a known length (a
// zero length as http.NoBody, which must not read as unknown). The caller
// reads and closes the response's body.
func (t transfers) put(ctx context.Context, l string, body io.Reader, n int64, header http.Header) (*http.Response, error) {
	u, err := t.link(l)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		body = http.NoBody
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = n
	for k, v := range header {
		req.Header[k] = v
	}
	return t.http.Do(req)
}

// refusal is a PUT core did not take: its status, and what its body says.
func refusal(resp *http.Response) error {
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("%s %s", resp.Status, strings.TrimSpace(string(detail)))
}

// spool reads body to its end into a temporary file, refusing more than
// limit bytes (errTooLarge; a negative limit: none), and returns the file at
// its start, its length and its SHA-256 in hex: an upload's length is known
// before it starts. The caller removes it (unspool).
func spool(body io.Reader, limit int64) (*os.File, int64, string, error) {
	f, err := os.CreateTemp("", "logos-spool-*")
	if err != nil {
		return nil, 0, "", err
	}
	h := sha256.New()
	r := body
	if limit >= 0 {
		r = io.LimitReader(body, limit+1)
	}
	n, err := io.Copy(io.MultiWriter(f, h), r)
	switch {
	case err != nil:
	case limit >= 0 && n > limit:
		err = errTooLarge
	default:
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		unspool(f)
		return nil, 0, "", err
	}
	return f, n, hex.EncodeToString(h.Sum(nil)), nil
}

// unspool closes and removes a spooled file: a scratch copy, so failing to
// can only leave a temporary file behind.
func unspool(f *os.File) {
	_ = f.Close()
	_ = os.Remove(f.Name())
}
