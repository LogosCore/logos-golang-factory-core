package codeclink_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-factory-core/pkg/codeclink"
	"github.com/logoscore/logos-golang-factory-core/pkg/factorytest"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// toy is a codec for the tests: its suite seals as tag ‖ plaintext XOR
// the key (the tag the key's first byte, so the key that opens it is
// known); its envelope is the message's CBOR in hex. A message "slow"
// waits for its context; "boom" fails with a plain error.
type toy struct {
	mu      sync.Mutex
	running int
	most    int
	release chan struct{}
}

func xor(key, b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		out[i] = b[i] ^ key[i%len(key)]
	}
	return out
}

func (c *toy) enter() func() {
	c.mu.Lock()
	c.running++
	c.most = max(c.most, c.running)
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		c.running--
		c.mu.Unlock()
	}
}

func (c *toy) Inbound(ctx context.Context, in codeclink.Inbound) (codeclink.Opened, error) {
	defer c.enter()()
	switch string(in.Data) {
	case "slow":
		select {
		case <-ctx.Done():
		case <-c.release:
		}
		return codeclink.Opened{Data: []byte{0xa0}}, nil
	case "boom":
		return codeclink.Opened{}, errors.New("boom")
	}
	data, key := in.Data, 0
	if in.Suite {
		key = -1
		for i, k := range in.Keys {
			if len(data) > 0 && data[0] == k[0] {
				data, key = xor(k, data[1:]), i
				break
			}
		}
		if key < 0 {
			return codeclink.Opened{}, codeclink.NotOpened("no key fits")
		}
	}
	if in.Envelope {
		b, err := hex.DecodeString(string(data))
		if err != nil {
			return codeclink.Opened{}, codeclink.NotMessage("not hex: %v", err)
		}
		data = b
	}
	return codeclink.Opened{Data: data, Key: key}, nil
}

func (c *toy) Outbound(_ context.Context, out codeclink.Outbound) ([]byte, error) {
	data := out.Data
	if out.Envelope {
		data = []byte(hex.EncodeToString(data))
	}
	if out.Suite {
		data = append([]byte{out.Key[0]}, xor(out.Key, data)...)
	}
	return data, nil
}

func (c *toy) Keys(_ context.Context, k codeclink.KeysFor) (codeclink.Keys, error) {
	if k.Stage {
		if string(k.StageKey) == "rsa" {
			return codeclink.Keys{}, codeclink.Refused("the toy suite does not stage with %s", k.StageKey)
		}
		return codeclink.Keys{Material: []byte("stage"), Wrapped: append([]byte("wrapped:"), k.StageKey...)}, nil
	}
	if k.Build[0] == 0xee {
		// A stage's answer for a build: not what the call takes.
		return codeclink.Keys{Material: []byte("m"), Wrapped: []byte("w")}, nil
	}
	return codeclink.Keys{Material: []byte("build key")}, nil
}

var (
	build = [16]byte{1, 2, 3}
	id    = [16]byte{4, 5, 6}
)

func call(typ int, data string, layers []int, keys ...[]byte) protocol.FactoryCall {
	return protocol.FactoryCall{Type: typ, Request: 9, Build: build[:], ID: id[:], Data: []byte(data), Deadline: 1000,
		Layers: layers, Keys: keys}
}

var both = []int{protocol.CodecLayerSuite, protocol.CodecLayerEnvelope}

func TestAnswer(t *testing.T) {
	ctx, codec := context.Background(), &toy{}
	msg, _ := protocol.MarshalCBOR(protocol.MinionMessage{V: protocol.MinionMessageV3, ID: protocol.NewMessageID()})
	own, old := []byte("A key, its own"), []byte("B key, replaced")

	// Outbound through both layers, read back inbound with the old key
	// among the candidates: the message, and which key opened it.
	out := codeclink.Answer(ctx, codec, call(protocol.FactoryOutbound, string(msg), both, old))
	if out.Failure != nil || out.Request != 9 || out.Opened != nil {
		t.Fatalf("outbound %+v", out)
	}
	in := codeclink.Answer(ctx, codec, call(protocol.FactoryInbound, string(out.Data), both, own, old))
	if in.Failure != nil || !bytes.Equal(in.Data, msg) || in.Opened == nil || *in.Opened != 1 {
		t.Fatalf("inbound %+v", in)
	}
	// The envelope alone: no keys, no opened.
	env := []int{protocol.CodecLayerEnvelope}
	in = codeclink.Answer(ctx, codec, call(protocol.FactoryInbound, hex.EncodeToString(msg), env))
	if in.Failure != nil || !bytes.Equal(in.Data, msg) || in.Opened != nil {
		t.Fatalf("envelope alone %+v", in)
	}
	// Keys for a build and a stage.
	k := codeclink.Answer(ctx, codec, protocol.FactoryCall{Type: protocol.FactoryKeys, Request: 9, Build: build[:], Deadline: 1,
		For: protocol.FactoryKeysForBuild})
	if string(k.Material) != "build key" || k.Wrapped != nil {
		t.Fatalf("build keys %+v", k)
	}
	stage := protocol.FactoryCall{Type: protocol.FactoryKeys, Request: 9, Build: build[:], Deadline: 1,
		For: protocol.FactoryKeysForStage, StageKey: []byte("x25519")}
	if k := codeclink.Answer(ctx, codec, stage); string(k.Wrapped) != "wrapped:x25519" {
		t.Fatalf("stage keys %+v", k)
	}

	// Failures: the codec's kinds, other for any error and for a result
	// the call does not take.
	for name, tc := range map[string]struct {
		c    protocol.FactoryCall
		kind int
		msg  string
	}{
		"no key fits":   {call(protocol.FactoryInbound, "zzz", both, own), protocol.CodecFailNotOpened, "no key fits"},
		"not hex":       {call(protocol.FactoryInbound, "nothex", env), protocol.CodecFailNotMessage, "not hex"},
		"a plain error": {call(protocol.FactoryInbound, "boom", env), protocol.CodecFailOther, "boom"},
		"refused":       {protocol.FactoryCall{Type: protocol.FactoryKeys, Request: 9, Build: build[:], Deadline: 1, For: protocol.FactoryKeysForStage, StageKey: []byte("rsa")}, protocol.CodecFailRefused, "does not stage"},
		"wrapped for a build": {protocol.FactoryCall{Type: protocol.FactoryKeys, Request: 9, Build: append([]byte{0xee}, build[1:]...), Deadline: 1,
			For: protocol.FactoryKeysForBuild}, protocol.CodecFailOther, "not wrapped"},
	} {
		a := codeclink.Answer(ctx, codec, tc.c)
		if a.Failure == nil || a.Failure.Kind != tc.kind || !strings.Contains(a.Failure.Msg, tc.msg) || a.Data != nil || a.Validate() != nil {
			t.Errorf("%s: %+v", name, a)
		}
	}
	// A long message is cut to the protocol's limit, still UTF-8.
	long := codeclink.Answer(ctx, failing{strings.Repeat("é", 400)}, call(protocol.FactoryInbound, "x", env))
	if long.Failure == nil || len(long.Failure.Msg) > protocol.MaxLinkFailureMsg || long.Validate() != nil {
		t.Fatalf("a long failure: %d bytes", len(long.Failure.Msg))
	}
}

// failing fails every call with msg.
type failing struct{ msg string }

func (f failing) Inbound(context.Context, codeclink.Inbound) (codeclink.Opened, error) {
	return codeclink.Opened{}, errors.New(f.msg)
}
func (f failing) Outbound(context.Context, codeclink.Outbound) ([]byte, error) {
	return nil, errors.New(f.msg)
}
func (f failing) Keys(context.Context, codeclink.KeysFor) (codeclink.Keys, error) {
	return codeclink.Keys{}, errors.New(f.msg)
}

// run runs the link against core until the test ends.
func run(t *testing.T, core *factorytest.Core, codec codeclink.Codec, maxCalls int, up *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := codeclink.Run(ctx, codeclink.Options{BaseURL: core.URL(), Instance: "acme-1", Codec: codec, MaxCalls: maxCalls,
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
			OnState: func(on bool) {
				if up != nil && on {
					up.Add(1)
				}
			}})
		if err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// The link over a real WebSocket: refused until the instance is
// registered, then open; calls answered concurrently and in any order; open
// again after core drops it.
func TestRun(t *testing.T) {
	core := factorytest.NewCore(t, "acme-1")
	core.Refuse(true)
	codec := &toy{release: make(chan struct{})}
	var ups atomic.Int32
	run(t, core, codec, 0, &ups)
	time.Sleep(60 * time.Millisecond)
	if core.Links() != 0 {
		t.Fatal("linked while refused")
	}
	core.Refuse(false)
	core.WaitLinked(t, 2*time.Second)

	// A slow call does not hold the next: it is answered first.
	ctx := context.Background()
	env := []int{protocol.CodecLayerEnvelope}
	slow := make(chan error, 1)
	go func() {
		_, err := core.Call(ctx, call(protocol.FactoryInbound, "slow", env), 5*time.Second)
		slow <- err
	}()
	msg, _ := protocol.MarshalCBOR(protocol.MinionMessage{V: protocol.MinionMessageV3, ID: protocol.NewMessageID()})
	a, err := core.Call(ctx, call(protocol.FactoryOutbound, string(msg), env), time.Second)
	if err != nil || string(a.Data) != hex.EncodeToString(msg) {
		t.Fatalf("outbound %+v: %v", a, err)
	}
	select {
	case err := <-slow:
		t.Fatalf("the slow call answered first: %v", err)
	default:
	}
	close(codec.release)
	if err := <-slow; err != nil {
		t.Fatal(err)
	}
	// Past its deadline a call is answered all the same, by what the codec
	// did with a context that ended.
	codec.release = make(chan struct{})
	if a, err := core.Call(ctx, call(protocol.FactoryInbound, "slow", env), 50*time.Millisecond); err != nil || a.Failure != nil {
		t.Fatalf("deadline: %+v %v", a, err)
	}

	core.Drop()
	deadline := time.Now().Add(2 * time.Second)
	for core.Links() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the link did not open again")
		}
		time.Sleep(5 * time.Millisecond)
	}
	core.WaitLinked(t, 2*time.Second)
	if a, err := core.Call(ctx, call(protocol.FactoryInbound, hex.EncodeToString(msg), env), time.Second); err != nil || !bytes.Equal(a.Data, msg) {
		t.Fatalf("after a drop: %+v %v", a, err)
	}
	if ups.Load() < 2 {
		t.Fatalf("OnState told %d ups", ups.Load())
	}
}

// Past MaxCalls a call is answered at once with a failure.
func TestRunMaxCalls(t *testing.T) {
	core := factorytest.NewCore(t, "acme-1")
	codec := &toy{release: make(chan struct{})}
	run(t, core, codec, 1, nil)
	core.WaitLinked(t, 2*time.Second)
	ctx := context.Background()
	env := []int{protocol.CodecLayerEnvelope}
	held := make(chan error, 1)
	go func() {
		_, err := core.Call(ctx, call(protocol.FactoryInbound, "slow", env), 5*time.Second)
		held <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		codec.mu.Lock()
		n := codec.running
		codec.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first call never ran")
		}
		time.Sleep(5 * time.Millisecond)
	}
	a, err := core.Call(ctx, call(protocol.FactoryInbound, "slow", env), time.Second)
	if err != nil || a.Failure == nil || !strings.Contains(a.Failure.Msg, "1 calls in progress") {
		t.Fatalf("past the limit: %+v %v", a, err)
	}
	close(codec.release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
}

func TestLinkURL(t *testing.T) {
	for base, want := range map[string]string{
		"http://core:8080":        "ws://core:8080/api/factory/link",
		"https://core.example/x/": "wss://core.example/x/api/factory/link",
	} {
		if got, err := codeclink.LinkURL(base); err != nil || got != want {
			t.Errorf("%s: %s %v", base, got, err)
		}
	}
	for _, bad := range []string{"", "core:8080", "ftp://core"} {
		if _, err := codeclink.LinkURL(bad); err == nil {
			t.Errorf("%q taken", bad)
		}
	}
	if err := codeclink.Run(context.Background(), codeclink.Options{BaseURL: "http://core", Instance: "x"}); err == nil {
		t.Error("Run without a codec")
	}
}
