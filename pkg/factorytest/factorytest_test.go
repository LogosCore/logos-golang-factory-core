package factorytest

import (
	"bytes"
	"context"
	"testing"

	"github.com/logoscore/logos-golang-factory-core/pkg/factory"
	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

var schema = protocol.BuildSchema{
	SchemaVersion: protocol.BuildSchemaV3, Version: 1,
	Channels: []protocol.BuildChannel{{ModuleName: "http", Num: 1, Versions: []int{2}, Kinds: []protocol.BuildKind{{Kind: protocol.KindReverseCall}}}},
	Targets: []protocol.BuildTarget{{ID: "copy", Format: "bin", Platforms: []string{"any/any"},
		Fields: []protocol.FieldSpec{{Key: "input", Num: 1, Type: protocol.FieldFile, Required: true}}}},
}

// copier uploads its input file as the artifact.
var copier = factory.BuilderFunc(func(ctx context.Context, job *factory.Job) (factory.Result, error) {
	f, _ := job.File("input")
	b, err := job.ReadFile(ctx, f)
	if err != nil {
		return factory.Result{}, err
	}
	return factory.Result{}, job.UploadArtifact(ctx, "copy.bin", "", bytes.NewReader(b))
})

func request(input protocol.BuildFile) protocol.BuildRequest {
	return protocol.BuildRequest{
		BuildID: "5b0c4f6e-8a1d-4c2b-9e7f-3d2a1b0c9e8f", Target: "copy", Platform: "any/any", SchemaVersion: 1,
		Values: map[string]any{"input": input.ID},
		Connections: []protocol.BuildConnection{{Endpoint: protocol.ChannelEndpointExport{
			Format: protocol.ChannelEndpointExportV2, Name: "edge", Kind: protocol.KindReverseCall,
			Channel: protocol.ExportChannel{ModuleName: "http", SchemaVersion: 2},
		}}},
		Files: map[string]protocol.BuildFile{input.ID: input},
	}
}

func TestBuild(t *testing.T) {
	st := NewStore(t)
	input := st.File("in-1", "input.bin", "", []byte("payload"))
	res, artifact := Build(t, schema, copier, st, request(input))
	if res.Status != protocol.BuildSucceeded || string(artifact) != "payload" || res.Artifact.SHA256 != input.SHA256 {
		t.Fatalf("result %+v, artifact %q", res, artifact)
	}
}

func TestBuildFailure(t *testing.T) {
	st := NewStore(t)
	input := st.File("in-1", "input.bin", "", []byte("payload"))
	input.SHA256 = st.File("in-2", "other.bin", "", []byte("other")).SHA256 // the download will not check out
	res, artifact := Build(t, schema, copier, st, request(input))
	if res.Status != protocol.BuildFailed || res.Error.Code != factory.CodeBuildFailed || artifact != nil {
		t.Fatalf("result %+v", res)
	}
}

// stepper plans two steps and reports both.
type stepper struct{ factory.BuilderFunc }

func (stepper) Steps(protocol.BuildRequest) []protocol.BuildStep {
	return []protocol.BuildStep{{ID: "read", Label: "Read the input"}, {ID: "write", Label: "Write the copy"}}
}

func TestRunReportsThePlan(t *testing.T) {
	st := NewStore(t)
	input := st.File("in-1", "input.bin", "", []byte("payload"))
	b := stepper{factory.BuilderFunc(func(ctx context.Context, job *factory.Job) (factory.Result, error) {
		job.Step("read", "")
		job.Step("write", "copying 7 bytes")
		return copier(ctx, job)
	})}
	out := Run(t, schema, b, st, request(input))
	if out.Result.Status != protocol.BuildSucceeded || string(out.Artifact) != "payload" {
		t.Fatalf("result %+v", out.Result)
	}
	if len(out.Steps) != 2 || out.Steps[1].ID != "write" {
		t.Fatalf("plan %+v", out.Steps)
	}
	// Progress is best effort: the last report is there, earlier ones may
	// have been replaced.
	if n := len(out.Progress); n == 0 || out.Progress[n-1].Step != "write" || out.Progress[n-1].Message != "copying 7 bytes" {
		t.Fatalf("progress %+v", out.Progress)
	}
}

// relay runs its input as a file a subtask uploads: it reads the end's
// output, puts it as a file for the task, and runs upload with it.
var relay = factory.ComposerFunc(func(ctx context.Context, c factory.ComposeCall) (factory.Decision, error) {
	if c.Event == factory.Start {
		return factory.RunSubtasks(factory.Subtask{Command: "shell", Args: map[string]any{"command": "cat /etc/hostname"}}), nil
	}
	out, err := c.Out(ctx, c.End)
	if err != nil {
		return factory.Decision{}, err
	}
	f, err := c.PutFile(ctx, "hostname.txt", "text/plain", bytes.NewReader(out))
	if err != nil {
		return factory.Decision{}, err
	}
	return factory.RunSubtasks(factory.Subtask{Command: "upload", Args: map[string]any{"file": f.ID, "path": "/tmp/h"}}), nil
})

func TestCompose(t *testing.T) {
	st := NewStore(t)
	d, err := Compose(t, relay, st, protocol.ComposeRequest{Command: "relay"})
	if err != nil || len(d.Run) != 1 || d.Run[0].Command != "shell" {
		t.Fatalf("start: %+v, %v", d, err)
	}
	// The shell's output came by link; the file put for the task is the
	// store's, named by the upload.
	end := &protocol.ComposeEnd{TaskID: protocol.NewID(), Command: "shell", State: protocol.EndDone, OK: true,
		OutURL: st.Link([]byte("minion-host\n"))}
	d, err = Compose(t, relay, st, protocol.ComposeRequest{Command: "relay", Event: protocol.ComposeEnded, End: end})
	if err != nil || len(d.Run) != 1 {
		t.Fatalf("ended: %+v, %v", d, err)
	}
	id, _ := d.Run[0].Args["file"].(string)
	p, ok := st.Put(id)
	if !ok || string(p.Content) != "minion-host\n" || p.File.Name != "hostname.txt" || p.File.MediaType != "text/plain" {
		t.Fatalf("put %+v (%v)", p, ok)
	}
}

func TestComposeRefusal(t *testing.T) {
	st := NewStore(t)
	failing := factory.ComposerFunc(func(context.Context, factory.ComposeCall) (factory.Decision, error) {
		return factory.RunOwnOp(nil), nil
	})
	// A command with no op of its own has no own op to run: the decision is
	// one core would refuse.
	if _, err := Compose(t, failing, st, protocol.ComposeRequest{Command: "relay"}); coreerrors.Code(err) != factory.CodeInvalidDecision {
		t.Fatalf("err = %v, want %s", err, factory.CodeInvalidDecision)
	}
}
