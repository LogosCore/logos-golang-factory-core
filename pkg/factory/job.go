package factory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Job is one build: what core asked for, and what a builder needs to carry it
// out. Its methods are safe for concurrent use.
//
// Key material, secret values and files are only for the build: never write
// them to the build log, to progress messages, or anywhere that outlives the
// build.
type Job struct {
	// Request is the factory.build request, checked against the schema.
	// Values holds every field of the target that has a value or a default;
	// a file field holds the file's id, and Files the file.
	Request protocol.BuildRequest
	// Target is the declared target being built.
	Target protocol.BuildTarget

	steps    []protocol.BuildStep
	http     *http.Client
	logs     *tailBuffer
	progress chan protocol.BuildProgress
	// unknownStep is told once of each step id reported outside the plan.
	unknownStep func(id string)

	mu       sync.Mutex
	closed   bool
	unknown  map[string]bool
	artifact *protocol.BuildArtifact
}

func newJob(req protocol.BuildRequest, target protocol.BuildTarget, steps []protocol.BuildStep, hc *http.Client) *Job {
	return &Job{
		Request:     req,
		Target:      target,
		steps:       steps,
		http:        hc,
		logs:        &tailBuffer{max: protocol.MaxBuildLogBytes},
		progress:    make(chan protocol.BuildProgress, 1),
		unknownStep: func(string) {},
	}
}

// Steps is the build's plan, as core got it with the acceptance.
func (j *Job) Steps() []protocol.BuildStep { return slices.Clone(j.steps) }

// Step reports to core that the build is on step id of its plan, with a
// message for the operator ("compiling 3 of 10", or "" for none); report a
// step again to change its message. Core takes the steps before it as done.
// It never blocks the build: progress is best effort, and a newer report
// replaces one not sent yet. A step that is not in the plan is not reported
// (and logged once).
func (j *Job) Step(id, message string) {
	p := protocol.BuildProgress{MinionID: j.Request.MinionID, Step: id, Message: clip(message)}
	planned := slices.ContainsFunc(j.steps, func(s protocol.BuildStep) bool { return s.ID == id })
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return
	}
	if !planned || p.Validate() != nil {
		if !j.unknown[id] {
			if j.unknown == nil {
				j.unknown = map[string]bool{}
			}
			j.unknown[id] = true
			j.unknownStep(id)
		}
		return
	}
	select {
	case j.progress <- p:
	default:
		// Replace the step still waiting.
		select {
		case <-j.progress:
		default:
		}
		j.progress <- p
	}
}

// Log is the build log. What is written to it is sent with the result; core
// keeps its last 64 KiB and shows it with the build, mainly when it fails.
func (j *Job) Log() io.Writer { return j.logs }

// Logf appends a line to the build log.
func (j *Job) Logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	_, _ = io.WriteString(j.logs, line)
}

// File returns the file a file field of the target names: the field's value
// is the file's id, the file is in Request.Files.
func (j *Job) File(key string) (protocol.BuildFile, bool) {
	id, ok := j.Request.Values[key].(string)
	if !ok {
		return protocol.BuildFile{}, false
	}
	f, ok := j.Request.Files[id]
	return f, ok
}

// ReadFile downloads f and checks its size and SHA-256. It is for small files
// such as icons; Download saves large ones to disk.
func (j *Job) ReadFile(ctx context.Context, f protocol.BuildFile) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(int(min(f.Size, 1<<20)))
	if err := j.fetch(ctx, f, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Download saves f to path, checking its size and SHA-256. Nothing is left at
// path when it fails.
func (j *Job) Download(ctx context.Context, f protocol.BuildFile, path string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".download-*")
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	defer os.Remove(tmp.Name()) // a no-op after the rename
	err = j.fetch(ctx, f, tmp)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// fetch downloads f from its presigned URL into w and checks it.
func (j *Job) fetch(ctx context.Context, f protocol.BuildFile, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URL, nil)
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	resp, err := j.http.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	defer resp.Body.Close()
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

// UploadArtifact uploads the build's artifact and records it for the result.
// It reads body to the end, spooling it to a temporary file so that the
// upload has a known length, refuses an artifact larger than the request's
// max_size, and computes the size and SHA-256 it reports. A second call
// replaces the first.
func (j *Job) UploadArtifact(ctx context.Context, filename, mediaType string, body io.Reader) error {
	spool, err := os.CreateTemp("", "logos-artifact-*")
	if err != nil {
		return fmt.Errorf("spool artifact: %w", err)
	}
	defer os.Remove(spool.Name())
	defer spool.Close()

	limit := j.Request.Artifact.MaxSize
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(spool, h), io.LimitReader(body, limit+1))
	if err != nil {
		return fmt.Errorf("spool artifact: %w", err)
	}
	if n > limit {
		return Fail("artifact_too_large", fmt.Sprintf("the artifact is larger than %d bytes", limit))
	}
	art := protocol.BuildArtifact{Filename: filename, Size: n, SHA256: hex.EncodeToString(h.Sum(nil)), MediaType: mediaType}
	check := protocol.BuildResult{MinionID: j.Request.MinionID, Status: protocol.BuildSucceeded, Artifact: &art}
	if err := check.Validate(); err != nil {
		return fmt.Errorf("upload artifact: %w", err)
	}

	var upload io.Reader = http.NoBody // a zero length must not read as unknown
	if n > 0 {
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("spool artifact: %w", err)
		}
		upload = spool
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, j.Request.Artifact.UploadURL, upload)
	if err != nil {
		return fmt.Errorf("upload artifact: %w", err)
	}
	req.ContentLength = n
	if mediaType != "" {
		req.Header.Set("Content-Type", mediaType)
	}
	resp, err := j.http.Do(req)
	if err != nil {
		return fmt.Errorf("upload artifact: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("upload artifact: %s %s", resp.Status, strings.TrimSpace(string(detail)))
	}

	j.mu.Lock()
	j.artifact = &art
	j.mu.Unlock()
	return nil
}

// uploaded returns the artifact recorded by UploadArtifact, if any.
func (j *Job) uploaded() *protocol.BuildArtifact {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.artifact
}

// close ends the job: later progress is dropped.
func (j *Job) close() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.closed {
		j.closed = true
		close(j.progress)
	}
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
	cut bool // earlier output was dropped
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	// Trim only once twice the size has piled up, so writes stay cheap.
	if len(t.buf) > 2*t.max {
		t.buf, t.cut = append(t.buf[:0], t.buf[len(t.buf)-t.max:]...), true
	}
	return len(p), nil
}

// String returns the kept output as valid UTF-8 of at most max bytes,
// starting at a character boundary.
func (t *tailBuffer) String() string {
	t.mu.Lock()
	b, cut := t.buf, t.cut
	if len(b) > t.max {
		b, cut = b[len(b)-t.max:], true
	}
	s := string(b)
	t.mu.Unlock()
	if cut {
		s = trimToCharacter(s)
	}
	s = strings.ToValidUTF8(s, "�")
	// Replacing invalid bytes can make it longer again.
	if len(s) > t.max {
		s = trimToCharacter(s[len(s)-t.max:])
	}
	return s
}

// trimToCharacter drops the rest of a character a cut went through.
func trimToCharacter(s string) string {
	for i := 0; i < utf8.UTFMax && len(s) > 0 && !utf8.RuneStart(s[0]); i++ {
		s = s[1:]
	}
	return s
}
