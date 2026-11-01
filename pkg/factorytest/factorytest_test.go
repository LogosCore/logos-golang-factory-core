package factorytest

import (
	"bytes"
	"context"
	"testing"

	"github.com/logoscore/logos-golang-factory-core/pkg/factory"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

var schema = protocol.BuildSchema{
	SchemaVersion: protocol.BuildSchemaV2, Version: 1,
	Channels: []protocol.BuildChannel{{ChannelVersions: protocol.ChannelVersions{ModuleName: "http", Versions: []int{2}}}},
	Targets: []protocol.BuildTarget{{ID: "copy", Format: "bin", Platforms: []string{"any/any"},
		Fields: []protocol.FieldSpec{{Key: "input", Type: protocol.FieldFile, Required: true}}}},
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
			Format: protocol.ChannelEndpointExportV1, Name: "edge",
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
