package factorytest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Core is core's end of the factory link, for a factory's codec tests: an
// HTTP server that takes the link at protocol.FactoryLinkPath from the
// instance it expects (403 for another, and while refusing), and sends it
// calls. Run the factory, or codeclink.Run, against URL.
type Core struct {
	srv      *httptest.Server
	instance string
	next     atomic.Uint64
	refuse   atomic.Bool

	mu      sync.Mutex
	ws      *websocket.Conn
	up      chan struct{}
	links   int
	waiting map[uint64]chan protocol.FactoryAnswerFrame
}

// NewCore starts a Core that takes the link of instance, closed when the
// test ends.
func NewCore(t testing.TB, instance string) *Core {
	t.Helper()
	c := &Core{instance: instance, up: make(chan struct{}), waiting: map[uint64]chan protocol.FactoryAnswerFrame{}}
	c.srv = httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(func() {
		c.Drop()
		c.srv.Close()
	})
	return c
}

// URL is core's base URL.
func (c *Core) URL() string { return c.srv.URL }

// Refuse makes the endpoint answer 403, as core does for an instance not
// registered yet, while on is true.
func (c *Core) Refuse(on bool) { c.refuse.Store(on) }

// Links is how many times the factory opened its link.
func (c *Core) Links() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.links
}

// WaitLinked waits until the factory's link is open.
func (c *Core) WaitLinked(t testing.TB, within time.Duration) {
	t.Helper()
	c.mu.Lock()
	up := c.up
	c.mu.Unlock()
	select {
	case <-up:
	case <-time.After(within):
		t.Fatal("the factory did not open its link")
	}
}

// Drop closes the open link, as core does when it stops; the factory opens
// it again.
func (c *Core) Drop() {
	c.mu.Lock()
	ws := c.ws
	c.mu.Unlock()
	if ws != nil {
		_ = ws.Close(websocket.StatusGoingAway, "")
	}
}

func (c *Core) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path != protocol.FactoryLinkPath:
		http.NotFound(w, r)
		return
	case r.Header.Get(protocol.HeaderFactoryInstance) == "":
		http.Error(w, "no instance", http.StatusBadRequest)
		return
	case c.refuse.Load() || r.Header.Get(protocol.HeaderFactoryInstance) != c.instance:
		http.Error(w, "factory not registered", http.StatusForbidden)
		return
	}
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ws.SetReadLimit(protocol.MaxFactoryFrameSize + 64)
	c.mu.Lock()
	c.ws = ws
	c.links++
	close(c.up)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.ws == ws {
			c.ws = nil
			c.up = make(chan struct{})
		}
		for req, ch := range c.waiting {
			close(ch)
			delete(c.waiting, req)
		}
		c.mu.Unlock()
		_ = ws.Close(websocket.StatusNormalClosure, "")
	}()
	for {
		_, b, err := ws.Read(r.Context())
		if err != nil {
			return
		}
		a, err := protocol.DecodeFactoryAnswer(b)
		if err != nil {
			return
		}
		c.mu.Lock()
		ch, ok := c.waiting[a.Request]
		delete(c.waiting, a.Request)
		c.mu.Unlock()
		if ok {
			ch <- a
		}
	}
}

// ErrNoLink: the factory has no link open.
var ErrNoLink = errors.New("factorytest: the factory has no link open")

// Call sends call over the link, its request id and deadline (timeout) set
// here, and waits for the answer, checked against it as core checks it.
func (c *Core) Call(ctx context.Context, call protocol.FactoryCall, timeout time.Duration) (protocol.FactoryAnswerFrame, error) {
	call.Request = c.next.Add(1)
	call.Deadline = uint64(max(timeout/time.Millisecond, 1))
	b, err := protocol.EncodeFactoryCall(call)
	if err != nil {
		return protocol.FactoryAnswerFrame{}, err
	}
	ch := make(chan protocol.FactoryAnswerFrame, 1)
	c.mu.Lock()
	ws := c.ws
	if ws != nil {
		c.waiting[call.Request] = ch
	}
	c.mu.Unlock()
	if ws == nil {
		return protocol.FactoryAnswerFrame{}, ErrNoLink
	}
	defer func() {
		c.mu.Lock()
		delete(c.waiting, call.Request)
		c.mu.Unlock()
	}()
	if err := ws.Write(ctx, websocket.MessageBinary, b); err != nil {
		return protocol.FactoryAnswerFrame{}, fmt.Errorf("factorytest: %w", err)
	}
	select {
	case a, ok := <-ch:
		if !ok {
			return protocol.FactoryAnswerFrame{}, ErrNoLink
		}
		return a, a.ValidateFor(call)
	case <-time.After(timeout + time.Second):
		return protocol.FactoryAnswerFrame{}, errors.New("factorytest: no answer in time")
	case <-ctx.Done():
		return protocol.FactoryAnswerFrame{}, ctx.Err()
	}
}
