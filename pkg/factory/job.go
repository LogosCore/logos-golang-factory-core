package factory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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

	transfers
	steps    []protocol.BuildStep
	logs     *buildLog
	progress chan protocol.BuildProgress
	// unknownStep is told once of each step id reported outside the plan.
	unknownStep func(id string)

	mu       sync.Mutex
	closed   bool
	unknown  map[string]bool
	artifact *protocol.BuildArtifact
}

func newJob(req protocol.BuildRequest, target protocol.BuildTarget, steps []protocol.BuildStep, hc *http.Client, core string) *Job {
	return &Job{
		Request:     req,
		Target:      target,
		transfers:   transfers{http: hc, core: core},
		steps:       steps,
		logs:        newBuildLog(steps[0].ID),
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
// (and logged once). The build log's lines from then on are the step's.
func (j *Job) Step(id, message string) {
	p := protocol.BuildProgress{BuildID: j.Request.BuildID, Step: id, Message: clip(message)}
	planned := slices.ContainsFunc(j.steps, func(s protocol.BuildStep) bool { return s.ID == id })
	if planned {
		j.logs.setStep(id)
	}
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

// Log is the build log. Core gets it as it is written, line by line, each
// line with the step it was written in (lines before the first Step are the
// plan's first step's), and shows it by step while the build runs. Its last
// 64 KiB also go with the result, for a core that does not take the live
// log.
func (j *Job) Log() io.Writer { return j.logs }

// Logf appends a line to the build log.
func (j *Job) Logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	_, _ = io.WriteString(j.logs, line)
}

// HasCommand reports whether the minion is built with command id: the
// operator picked it, or the target requires it. The minion gets the
// request's commands and no others; a builder that cannot build one in fails
// the build.
func (j *Job) HasCommand(id string) bool { return slices.Contains(j.Request.Commands, id) }

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
	return j.readFile(ctx, f)
}

// Download saves f to path, checking its size and SHA-256. Nothing is left at
// path when it fails.
func (j *Job) Download(ctx context.Context, f protocol.BuildFile, path string) error {
	return j.download(ctx, f, path)
}

// UploadArtifact uploads the build's artifact and records it for the result.
// It reads body to the end, spooling it to a temporary file so that the
// upload has a known length, refuses an artifact larger than the request's
// max_size, and computes the size and SHA-256 it reports. A second call
// replaces the first.
func (j *Job) UploadArtifact(ctx context.Context, filename, mediaType string, body io.Reader) error {
	limit := j.Request.Artifact.MaxSize
	spooled, n, sum, err := spool(body, limit)
	switch {
	case errors.Is(err, errTooLarge):
		return Fail("artifact_too_large", fmt.Sprintf("the artifact is larger than %d bytes", limit))
	case err != nil:
		return fmt.Errorf("spool artifact: %w", err)
	}
	defer unspool(spooled)
	art := protocol.BuildArtifact{Filename: filename, Size: n, SHA256: sum, MediaType: mediaType}
	check := protocol.BuildResult{BuildID: j.Request.BuildID, Status: protocol.BuildSucceeded, Artifact: &art}
	if err := check.Validate(); err != nil {
		return fmt.Errorf("upload artifact: %w", err)
	}

	header := http.Header{}
	if mediaType != "" {
		header.Set("Content-Type", mediaType)
	}
	resp, err := j.put(ctx, j.Request.Artifact.UploadURL, spooled, n, header)
	if err != nil {
		return fmt.Errorf("upload artifact: %w", err)
	}
	// The status says how the upload went; closing the body says nothing.
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("upload artifact: %w", refusal(resp))
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

// close ends the job: later progress is dropped, and the log is closed.
func (j *Job) close() {
	j.logs.close()
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
