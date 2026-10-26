package factory

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TestIntegrationBuildOverAMQP runs a factory against a scripted core on a
// real RabbitMQ: registration with the build schema, factory.build on the
// rpc_queue, progress and the result on logos.core.rpc, and the deregister.
//
// Opt-in: INTEGRATION_AMQP_URL names a broker and a vhost no real core
// consumes, since the scripted core takes logos.core.rpc. With the dev stack:
//
//	docker exec logos-rabbitmq rabbitmqctl add_vhost logos-it
//	docker exec logos-rabbitmq rabbitmqctl set_permissions -p logos-it logos '.*' '.*' '.*'
//	INTEGRATION_AMQP_URL=amqp://logos:changeme_rabbitmq@localhost:5672/logos-it go test -run Integration ./...
func TestIntegrationBuildOverAMQP(t *testing.T) {
	url := os.Getenv("INTEGRATION_AMQP_URL")
	if url == "" {
		t.Skip("INTEGRATION_AMQP_URL is not set")
	}
	core := startScriptedCore(t, url)
	st := newStore(t)
	instance := "bob-it-" + strings.ToLower(protocol.NewULID())
	schema := testSchema()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			Config: module.Config{
				AMQPURL:  url,
				Identity: lifecycle.Identity{ModuleName: "bob-it", Instance: instance, Version: "test"},
				Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
			Build: &schema,
			Builder: BuilderFunc(func(ctx context.Context, job *Job) (Result, error) {
				job.Progress("render", "writing the minion", 50)
				return uploadAndReturn(Result{Capabilities: []string{"hello"}})(ctx, job)
			}),
		})
	}()

	var reg struct {
		ModuleType string               `json:"module_type"`
		RPCQueue   string               `json:"rpc_queue"`
		Build      protocol.BuildSchema `json:"build"`
	}
	core.await(t, "the register", core.registers, &reg)
	if reg.ModuleType != ModuleType || reg.RPCQueue != RPCQueue(instance) || protocol.ValidateBuildSchema(reg.Build) != nil ||
		reg.Build.Targets[0].ID != "dummy" {
		t.Fatalf("register %+v", reg)
	}

	req := testRequest(st)
	var accepted protocol.BuildReply
	if rep := core.call(t, reg.RPCQueue, protocol.TypeFactoryBuild, req); rep.Status != protocol.StatusOK ||
		json.Unmarshal(rep.Payload, &accepted) != nil || !accepted.Accepted {
		t.Fatalf("factory.build reply %+v (error %+v)", rep, rep.Error)
	}
	var result protocol.BuildResult
	core.await(t, "the result", core.results, &result)
	body, _, _, _ := st.upload("/artifacts/" + req.BuildID)
	if result.Status != protocol.BuildSucceeded || result.Artifact.SHA256 != sha(string(body)) || result.Capabilities[0] != "hello" {
		t.Fatalf("result %+v", result)
	}
	var progress protocol.BuildProgress
	core.await(t, "the progress", core.progress, &progress)
	if progress.BuildID != req.BuildID || progress.Step != "render" {
		t.Fatalf("progress %+v", progress)
	}

	// The factory has no management: management requests are refused.
	if rep := core.call(t, reg.RPCQueue, protocol.TypeMgmtStateGet, struct{}{}); rep.Error == nil || rep.Error.Code != protocol.CodeValidationFailed {
		t.Fatalf("mgmt.state.get on an unmanaged factory: %+v", rep)
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	core.await(t, "the deregister", core.deregisters, nil)
}

// scriptedCore answers the lifecycle and build calls a factory makes on
// logos.core.rpc, and calls the factory like core does.
type scriptedCore struct {
	srv, cli    *amqp.Channel
	replies     <-chan amqp.Delivery
	registers   chan json.RawMessage
	results     chan json.RawMessage
	progress    chan json.RawMessage
	deregisters chan json.RawMessage
}

func startScriptedCore(t *testing.T, url string) *scriptedCore {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("connect to %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	srv, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(srv.ExchangeDeclare(lifecycle.CoreRPCExchange, "direct", true, false, false, false, nil))
	// Exclusive: only this test's core serves the vhost's logos.core.rpc.
	q, err := srv.QueueDeclare(lifecycle.CoreRPCExchange, false, true, true, false, nil)
	must(err)
	must(srv.QueueBind(q.Name, lifecycle.CoreRPCRoutingKey, lifecycle.CoreRPCExchange, false, nil))
	requests, err := srv.Consume(q.Name, "", true, true, false, false, nil)
	must(err)
	cli, err := conn.Channel()
	must(err)
	replies, err := cli.Consume("amq.rabbitmq.reply-to", "", true, false, false, false, nil)
	must(err)

	c := &scriptedCore{srv: srv, cli: cli, replies: replies,
		registers: make(chan json.RawMessage, 8), results: make(chan json.RawMessage, 8),
		progress: make(chan json.RawMessage, 8), deregisters: make(chan json.RawMessage, 8)}
	go c.serve(requests)
	return c
}

func (c *scriptedCore) serve(requests <-chan amqp.Delivery) {
	source := protocol.Source{Service: protocol.ServiceCore, Instance: "core-it"}
	for d := range requests {
		var req protocol.Envelope
		if json.Unmarshal(d.Body, &req) != nil {
			continue
		}
		var payload struct {
			Instance string `json:"instance"`
			BuildID  string `json:"build_id"`
		}
		_ = json.Unmarshal(req.Payload, &payload)
		var answer any
		switch req.Type {
		case lifecycle.TypeRegister:
			c.registers <- req.Payload
			answer = map[string]any{"instance": payload.Instance, "registered": true,
				"heartbeat_interval_seconds": 1, "heartbeat_grace_misses": 3}
		case lifecycle.TypeHeartbeat:
			answer = map[string]any{"instance": payload.Instance, "ack": true}
		case lifecycle.TypeDeregister:
			c.deregisters <- req.Payload
			answer = map[string]any{"instance": payload.Instance, "deregistered": true}
		case protocol.TypeFactoryBuildProgress:
			c.progress <- req.Payload
			answer = protocol.BuildProgressReply{BuildID: payload.BuildID, Ack: true}
		case protocol.TypeFactoryBuildResult:
			c.results <- req.Payload
			answer = protocol.BuildResultReply{BuildID: payload.BuildID, Recorded: true}
		}
		rep := protocol.NewErrorReply(req, source, protocol.CodeValidationFailed, "unknown type "+req.Type)
		if answer != nil {
			rep, _ = protocol.NewReply(req, source, answer)
		}
		body, _ := json.Marshal(rep)
		_ = c.srv.PublishWithContext(context.Background(), "", d.ReplyTo, false, false, amqp.Publishing{
			ContentType: "application/json", CorrelationId: d.CorrelationId, Body: body,
		})
	}
}

// call sends one request to the factory's rpc_queue, as core does, and
// returns the reply.
func (c *scriptedCore) call(t *testing.T, queue, typ string, payload any) protocol.ReplyEnvelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewULID()
	env := protocol.Envelope{MessageID: id, CorrelationID: id, Type: typ, Version: protocol.EnvelopeVersionV1,
		Timestamp: protocol.NowTimestamp(), Source: protocol.Source{Service: protocol.ServiceCore, Instance: "core-it"}, Payload: raw}
	body, _ := json.Marshal(env)
	err = c.cli.PublishWithContext(context.Background(), "", queue, true, false, amqp.Publishing{
		ContentType: "application/json", Type: typ, CorrelationId: id,
		ReplyTo: "amq.rabbitmq.reply-to", Expiration: "10000", Body: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	timeout := time.After(10 * time.Second)
	for {
		select {
		case d := <-c.replies:
			if d.CorrelationId != id {
				continue
			}
			var rep protocol.ReplyEnvelope
			if err := json.Unmarshal(d.Body, &rep); err != nil {
				t.Fatal(err)
			}
			return rep
		case <-timeout:
			t.Fatalf("no reply to %s", typ)
		}
	}
}

// await takes the next payload from ch into out (when non-nil).
func (c *scriptedCore) await(t *testing.T, what string, ch <-chan json.RawMessage, out any) {
	t.Helper()
	select {
	case raw := <-ch:
		if out != nil {
			if err := json.Unmarshal(raw, out); err != nil {
				t.Fatalf("%s: %v", what, err)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}
