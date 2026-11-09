// Package codeclink keeps a factory instance's factory link to core open
// (docs: contracts/minion-messages/codecs/, The factory link), for a factory
// whose build schema names a suite or an envelope of its own
// (protocol.BuildSchema.Translates). Core calls it once per minion message
// each way, and for its suite's keys; the factory's Codec answers, and the
// link answers each request by its id, in any order. The link reopens
// whenever it drops, backing off; calls waiting on a link that dropped are
// lost, and core treats them as the factory not answering.
//
// factory.Run runs it for a factory that translates (factory.Config.Codec);
// a factory's tests call Answer with the calls core would send.
package codeclink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

const (
	// PingEvery is how often the factory pings core's end; a ping without
	// its pong within pingWait drops the link.
	PingEvery = 30 * time.Second
	pingWait  = 30 * time.Second
	// DefaultMaxCalls bounds the calls in progress at once.
	DefaultMaxCalls = 64
	// DefaultMinBackoff and DefaultMaxBackoff bound the wait before the
	// link opens again: it starts at the first and doubles up to the
	// second, and starts over once a link stayed up a minute.
	DefaultMinBackoff = time.Second
	DefaultMaxBackoff = 30 * time.Second
	stableAfter       = time.Minute
	writeWait         = 30 * time.Second
)

// Codec is what a factory that translates its minions' codec gives the
// link: the layers of its builds' codecs that are its own, run in order.
// Its methods run in their own goroutines, several at once, each within
// the call's deadline (the context ends at it).
type Codec interface {
	// Inbound reads a minion's message: it opens Data with the suite (one
	// of Keys) when the suite is the factory's, and decodes it with the
	// envelope when that is the factory's. It returns the message in CBOR
	// (the protocol's form), or, when only the suite is the factory's, the
	// plaintext as core's envelope wrote it; and which of Keys opened it.
	Inbound(ctx context.Context, in Inbound) (Opened, error)
	// Outbound writes core's message: it encodes Data (the message in
	// CBOR) with the envelope when that is the factory's, and seals the
	// result with Key when the suite is. What it returns is the payload,
	// or, when only the envelope is the factory's, the plaintext core
	// seals.
	Outbound(ctx context.Context, out Outbound) ([]byte, error)
	// Keys makes the suite's keys: for a build (its material, which core
	// sends in the build request), or for a stage (StageKey is the stage
	// item's key: Wrapped is the material wrapped for it, which core puts
	// in the stage_response). A suite without staging answers Refused.
	Keys(ctx context.Context, k KeysFor) (Keys, error)
}

// Inbound is core's call to read a minion's message.
type Inbound struct {
	// Build is the build the minion was made by; ID the id the message
	// came under (the build's, a temporary id or a session id).
	Build, ID [16]byte
	// Data is the payload as the channel gave it (Suite), or the
	// plaintext core opened with its own suite.
	Data []byte
	// Suite and Envelope say which layers are the factory's.
	Suite, Envelope bool
	// Keys are the materials the message may open with, in order: the
	// id's own, then a persisted session's replaced keys, newest first.
	// Only with Suite.
	Keys [][]byte
}

// Opened is what Inbound read: Data, and the index in Keys of the material
// that opened it (0 without Suite).
type Opened struct {
	Data []byte
	Key  int
}

// Outbound is core's call to write a message for a minion.
type Outbound struct {
	Build, ID [16]byte
	// Data is the message in CBOR (Envelope), or the plaintext core's
	// envelope wrote.
	Data            []byte
	Suite, Envelope bool
	// Key is the id's material, with Suite.
	Key []byte
}

// KeysFor is core's call for keys: a build's, or a stage's (StageKey set).
type KeysFor struct {
	Build    [16]byte
	Stage    bool
	StageKey []byte
}

// Keys are a suite's keys: Material, and for a stage Wrapped.
type Keys struct {
	Material []byte
	Wrapped  []byte
}

// Failure is a call the Codec could not do, of a kind core tells apart
// (protocol.CodecFailNotOpened, ...). Any other error is CodecFailOther.
type Failure struct {
	Kind int
	Msg  string
}

func (f *Failure) Error() string { return protocol.CodecFailName(f.Kind) + ": " + f.Msg }

// NotOpened: no key fits the message, or a check failed.
func NotOpened(format string, args ...any) error {
	return &Failure{Kind: protocol.CodecFailNotOpened, Msg: fmt.Sprintf(format, args...)}
}

// NotMessage: the message opened, and does not decode.
func NotMessage(format string, args ...any) error {
	return &Failure{Kind: protocol.CodecFailNotMessage, Msg: fmt.Sprintf(format, args...)}
}

// Refused: a stage key the suite does not take, or a suite without
// staging.
func Refused(format string, args ...any) error {
	return &Failure{Kind: protocol.CodecFailRefused, Msg: fmt.Sprintf(format, args...)}
}

// Answer runs call c with codec and returns the frame the link sends back:
// the result, or a failure (the codec's, or other for any error and for a
// result the call does not take). Run answers every call with it; a
// factory's tests call it with the calls core would send.
func Answer(ctx context.Context, codec Codec, c protocol.FactoryCall) protocol.FactoryAnswerFrame {
	a := protocol.FactoryAnswerFrame{Type: protocol.FactoryAnswer, Request: c.Request}
	var build, id [16]byte
	copy(build[:], c.Build)
	copy(id[:], c.ID)
	var err error
	switch c.Type {
	case protocol.FactoryInbound:
		var o Opened
		o, err = codec.Inbound(ctx, Inbound{Build: build, ID: id, Data: c.Data, Suite: c.Suite(), Envelope: c.Envelope(), Keys: c.Keys})
		if err == nil {
			a.Data = o.Data
			if c.Suite() {
				if o.Key < 0 {
					err = fmt.Errorf("opened with key %d", o.Key)
				}
				k := uint64(max(o.Key, 0))
				a.Opened = &k
			}
		}
	case protocol.FactoryOutbound:
		out := Outbound{Build: build, ID: id, Data: c.Data, Suite: c.Suite(), Envelope: c.Envelope()}
		if len(c.Keys) > 0 {
			out.Key = c.Keys[0]
		}
		a.Data, err = codec.Outbound(ctx, out)
	case protocol.FactoryKeys:
		var k Keys
		k, err = codec.Keys(ctx, KeysFor{Build: build, Stage: c.For == protocol.FactoryKeysForStage, StageKey: c.StageKey})
		a.Material, a.Wrapped = k.Material, k.Wrapped
	default:
		err = fmt.Errorf("a call of type %d", c.Type)
	}
	if err == nil {
		if err = a.ValidateFor(c); err != nil {
			err = fmt.Errorf("the codec's result: %w", err)
		}
	}
	if err != nil {
		return failed(c.Request, err)
	}
	return a
}

// failed is the answer saying err.
func failed(request uint64, err error) protocol.FactoryAnswerFrame {
	f := &Failure{Kind: protocol.CodecFailOther, Msg: err.Error()}
	if errors.As(err, &f) {
		f = &Failure{Kind: f.Kind, Msg: f.Msg}
	}
	switch f.Kind {
	case protocol.CodecFailNotOpened, protocol.CodecFailNotMessage, protocol.CodecFailRefused, protocol.CodecFailOther:
	default:
		f.Kind = protocol.CodecFailOther
	}
	if len(f.Msg) > protocol.MaxLinkFailureMsg {
		f.Msg = f.Msg[:protocol.MaxLinkFailureMsg]
	}
	f.Msg = strings.ToValidUTF8(f.Msg, "")
	return protocol.FactoryAnswerFrame{Type: protocol.FactoryAnswer, Request: request,
		Failure: &protocol.CodecFailure{Kind: f.Kind, Msg: f.Msg}}
}

// Options are how a factory keeps its link.
type Options struct {
	// BaseURL is core's base URL (the channels' sync call's):
	// http(s)://host[:port].
	BaseURL string
	// Instance is the factory's registered instance id.
	Instance string
	// Codec answers the calls; required.
	Codec Codec
	// HTTPClient makes the handshake; http.DefaultClient when nil.
	HTTPClient *http.Client
	// MaxCalls bounds the calls in progress at once (DefaultMaxCalls when
	// zero); one more is answered with a failure at once.
	MaxCalls int
	// MinBackoff and MaxBackoff bound the wait between attempts to open
	// the link; the defaults when zero.
	MinBackoff, MaxBackoff time.Duration
	// Logger logs the link's ups and downs; slog.Default when nil.
	Logger *slog.Logger
	// OnState is told whenever the link opens (true) or drops (false).
	OnState func(up bool)
}

// ErrRefused is a handshake core answered with a status: not registered
// yet (403), a bad header (400).
var ErrRefused = errors.New("core refused the factory link")

// Run keeps the link open until ctx ends, reopening it whenever it drops.
// Calls in progress when ctx ends are cancelled.
func Run(ctx context.Context, o Options) error {
	if o.Codec == nil {
		return errors.New("codeclink: no codec")
	}
	if o.Instance == "" {
		return errors.New("codeclink: no instance")
	}
	u, err := LinkURL(o.BaseURL)
	if err != nil {
		return err
	}
	if o.MaxCalls <= 0 {
		o.MaxCalls = DefaultMaxCalls
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = DefaultMinBackoff
	}
	if o.MaxBackoff < o.MinBackoff {
		o.MaxBackoff = max(DefaultMaxBackoff, o.MinBackoff)
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	wait := o.MinBackoff
	for {
		opened := time.Now()
		err := runOnce(ctx, u, o, log)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(opened) > stableAfter {
			wait = o.MinBackoff
		}
		level := slog.LevelWarn
		if errors.Is(err, ErrRefused) {
			// Expected until the instance is registered.
			level = slog.LevelDebug
		}
		log.Log(ctx, level, "factory link down, opening it again", "err", err, "in", wait.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		wait = min(wait*2, o.MaxBackoff)
	}
}

// runOnce opens the link and serves it until it drops.
func runOnce(ctx context.Context, u string, o Options, log *slog.Logger) error {
	h := http.Header{}
	h.Set(protocol.HeaderFactoryInstance, o.Instance)
	ws, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: o.HTTPClient, HTTPHeader: h})
	if resp != nil && resp.Body != nil {
		// The handshake's body says nothing the status does not.
		_ = resp.Body.Close()
	}
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return fmt.Errorf("%w: %d", ErrRefused, resp.StatusCode)
		}
		return fmt.Errorf("factory link: %w", err)
	}
	ws.SetReadLimit(protocol.MaxFactoryFrameSize + 64)
	log.Info("factory link up")
	if o.OnState != nil {
		o.OnState(true)
		defer o.OnState(false)
	}

	lctx, cancel := context.WithCancel(ctx)
	var calls sync.WaitGroup
	defer func() {
		cancel()
		calls.Wait()
		_ = ws.Close(websocket.StatusGoingAway, "")
	}()

	var writeMu sync.Mutex
	write := func(a protocol.FactoryAnswerFrame) error {
		b, err := protocol.EncodeFactoryAnswer(a)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		wctx, wcancel := context.WithTimeout(lctx, writeWait)
		defer wcancel()
		return ws.Write(wctx, websocket.MessageBinary, b)
	}
	pingErr := make(chan error, 1)
	go func() {
		t := time.NewTicker(PingEvery)
		defer t.Stop()
		for {
			select {
			case <-lctx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(lctx, pingWait)
				err := ws.Ping(pctx)
				pcancel()
				if err != nil {
					pingErr <- fmt.Errorf("ping: %w", err)
					cancel()
					return
				}
			}
		}
	}()

	slots := make(chan struct{}, o.MaxCalls)
	for {
		mt, b, err := ws.Read(lctx)
		if err != nil {
			select {
			case perr := <-pingErr:
				return perr
			default:
			}
			return fmt.Errorf("factory link: %w", err)
		}
		if mt != websocket.MessageBinary {
			return errors.New("factory link: a text frame from core")
		}
		c, err := protocol.DecodeFactoryCall(b)
		if err != nil {
			// A frame that does not read cannot be answered by its id.
			return err
		}
		select {
		case slots <- struct{}{}:
		default:
			if err := write(failed(c.Request, fmt.Errorf("the factory has %d calls in progress", o.MaxCalls))); err != nil {
				return err
			}
			continue
		}
		calls.Add(1)
		go func(c protocol.FactoryCall) {
			defer func() { <-slots; calls.Done() }()
			cctx, ccancel := context.WithTimeout(lctx, time.Duration(c.Deadline)*time.Millisecond)
			defer ccancel()
			if err := write(Answer(cctx, o.Codec, c)); err != nil && lctx.Err() == nil {
				log.Warn("factory link: answer not sent", "request", c.Request, "err", err)
			}
		}(c)
	}
}

// LinkURL is core's factory link endpoint under base, as a WebSocket URL.
func LinkURL(base string) (string, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("core base URL %q", base)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("core base URL %q: http or https", base)
	}
	u.Path = strings.TrimRight(u.Path, "/") + protocol.FactoryLinkPath
	return u.String(), nil
}
