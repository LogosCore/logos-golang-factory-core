package factory

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

const (
	// logBatch is how long the log waits for more lines before it sends a
	// chunk that is not full.
	logBatch = 250 * time.Millisecond
	// logFlush bounds the wait, once the build is done, for its last lines
	// to reach core before the result goes.
	logFlush = 10 * time.Second
	// maxQueuedLog is how much of the log waits for core at most: when core
	// falls behind, the oldest lines go, counted as dropped.
	maxQueuedLog = 4 << 20
)

// buildLog is a build's output. It keeps the output's end for the result,
// and splits it into lines, each with the step it was written in and when,
// for the live log (factory.build.log) that the manager sends as it goes.
type buildLog struct {
	tail *tailBuffer
	now  func() time.Time
	// ready holds a token when lines are queued or the log is closed.
	ready chan struct{}

	mu      sync.Mutex
	step    string // the step lines are written in
	partial []byte // the start of a line not ended yet
	queue   []protocol.BuildLogLine
	queued  int   // bytes of text in queue
	dropped int64 // lines dropped since the last chunk
	closed  bool
	off     bool // core does not take the live log: only the tail is kept
}

// newBuildLog is the log of a build whose lines start in step.
func newBuildLog(step string) *buildLog {
	return &buildLog{
		tail:  &tailBuffer{max: protocol.MaxBuildLogBytes},
		now:   time.Now,
		ready: make(chan struct{}, 1),
		step:  step,
	}
}

// Write takes output as it comes: a line goes to the live log once it ends.
func (l *buildLog) Write(p []byte) (int, error) {
	_, _ = l.tail.Write(p)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.off {
		return len(p), nil
	}
	l.partial = append(l.partial, p...)
	for {
		i := bytes.IndexByte(l.partial, '\n')
		if i < 0 {
			break
		}
		l.addLocked(string(l.partial[:i]))
		l.partial = l.partial[i+1:]
	}
	// A line that does not end goes out in pieces of the longest a line
	// can be.
	if len(l.partial) > protocol.MaxBuildLogLineBytes {
		l.addLocked(string(l.partial))
		l.partial = nil
	}
	if len(l.partial) == 0 {
		l.partial = nil // let a long line's buffer go
	}
	return len(p), nil
}

// String is the output's end, for the result.
func (l *buildLog) String() string { return l.tail.String() }

// setStep makes later lines the step's; a line not ended yet stays the
// step it was started in.
func (l *buildLog) setStep(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.step != id {
		l.flushLocked()
		l.step = id
	}
}

// close ends the log: the line not ended yet goes out, and the sender
// finishes once the queue is empty.
func (l *buildLog) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.flushLocked()
		l.closed = true
		l.signal()
	}
}

// stop turns the live log off: what is queued is thrown away, and only the
// tail is kept from then on.
func (l *buildLog) stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.off, l.queue, l.queued, l.partial = true, nil, 0, nil
}

func (l *buildLog) flushLocked() {
	if len(l.partial) > 0 && !l.off {
		l.addLocked(string(l.partial))
	}
	l.partial = nil
}

// addLocked queues a line, in pieces when it is longer than a line can be.
func (l *buildLog) addLocked(text string) {
	text = strings.ToValidUTF8(strings.TrimSuffix(text, "\r"), "�")
	at := l.now()
	for first := true; first || text != ""; first = false {
		piece := text
		if len(piece) > protocol.MaxBuildLogLineBytes {
			cut := protocol.MaxBuildLogLineBytes
			for cut > 0 && !utf8.RuneStart(text[cut]) {
				cut--
			}
			piece = text[:cut]
		}
		text = text[len(piece):]
		l.queue = append(l.queue, protocol.BuildLogLine{Step: l.step, At: at, Text: piece})
		l.queued += len(piece)
	}
	for l.queued > maxQueuedLog {
		l.queued -= len(l.queue[0].Text)
		l.queue[0] = protocol.BuildLogLine{}
		l.queue = l.queue[1:]
		l.dropped++
	}
	l.signal()
}

func (l *buildLog) signal() {
	select {
	case l.ready <- struct{}{}:
	default:
	}
}

// full reports whether a whole chunk is queued, or the log is closed: there
// is no point waiting for more lines.
func (l *buildLog) full() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed || len(l.queue) >= protocol.MaxBuildLogChunkLines || l.queued >= protocol.MaxBuildLogChunkBytes
}

// take removes the next chunk's lines from the queue, with the count of the
// lines dropped before them; done is true once the log is closed and
// nothing is left.
func (l *buildLog) take() (lines []protocol.BuildLogLine, dropped int64, done bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	size := 0
	n := 0
	for n < len(l.queue) && n < protocol.MaxBuildLogChunkLines && size+len(l.queue[n].Text) <= protocol.MaxBuildLogChunkBytes {
		size += len(l.queue[n].Text)
		n++
	}
	lines = append([]protocol.BuildLogLine(nil), l.queue[:n]...)
	clear(l.queue[:n])
	l.queue, l.queued = l.queue[n:], l.queued-size
	if len(l.queue) == 0 {
		l.queue = nil
	}
	dropped, l.dropped = l.dropped, 0
	return lines, dropped, (l.closed || l.off) && len(l.queue) == 0
}

// sendLog sends a build's live log to core in chunks until the log closes
// and is sent, ctx ends, or core does not take it (the log is then stopped:
// the result carries its end).
func (m *manager) sendLog(ctx context.Context, b *build, l *buildLog) {
	var seq int64
	for {
		select {
		case <-l.ready:
		case <-ctx.Done():
			return
		}
		if !l.full() {
			select {
			case <-time.After(m.logBatch):
			case <-ctx.Done():
				return
			}
		}
		for {
			lines, dropped, done := l.take()
			if len(lines) == 0 && dropped == 0 {
				if done {
					return
				}
				break
			}
			seq++
			c := protocol.BuildLogChunk{BuildID: b.id, Seq: seq, Lines: lines, Dropped: dropped}
			if !m.sendChunk(ctx, b, c) {
				l.stop()
				return
			}
		}
	}
}

// sendChunk sends one chunk until core takes it. False when the log is to
// stop: core refused it (no longer waits for the build, which then stops,
// or does not take the live log), the build was cancelled, or ctx ended.
func (m *manager) sendChunk(ctx context.Context, b *build, c protocol.BuildLogChunk) bool {
	wait := m.retryMin
	for {
		m.mu.Lock()
		stopped := b.state == cancelled
		m.mu.Unlock()
		if stopped {
			return false
		}
		cctx, cancel := context.WithTimeout(ctx, progressTimeout)
		err := m.core.Call(cctx, protocol.TypeFactoryBuildLog, c, nil)
		cancel()
		var refused *coreerrors.Error
		switch {
		case err == nil:
			return true
		case errors.As(err, &refused) && refused.Code == protocol.CodeNotFound:
			m.mu.Lock()
			if b.state == running {
				b.state = cancelled
				b.cancel()
				m.logger().Info("factory: core no longer waits for the build; stopping it", "build_id", b.id)
			}
			m.mu.Unlock()
			return false
		case errors.As(err, &refused):
			m.logger().Info("factory: core does not take the live build log; it goes with the result",
				"build_id", b.id, "code", refused.Code, "reason", refused.Message)
			return false
		}
		m.logger().Debug("factory: build log not delivered; retrying", "build_id", b.id, "seq", c.Seq, "err", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
		wait = min(wait*2, m.retryMax)
	}
}
