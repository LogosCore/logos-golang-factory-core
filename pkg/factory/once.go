package factory

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/logoscore/logos-golang-protocol/protocol"
)

// BuildOnce runs one build in process, without core or a broker, the way Run
// serves a factory.build: req is checked against schema, the builder runs with
// a Job, and the result core would receive is returned. Files and the artifact
// move through req's URLs as in a real build (factorytest.Store serves them).
// It is for testing builders.
func BuildOnce(ctx context.Context, schema protocol.BuildSchema, b Builder, req protocol.BuildRequest, hc *http.Client) (protocol.BuildResult, error) {
	if err := protocol.ValidateBuildSchema(schema); err != nil {
		return protocol.BuildResult{}, fmt.Errorf("invalid build schema: %w", err)
	}
	if hc == nil {
		hc = &http.Client{}
	}
	core := &resultTrap{results: make(chan protocol.BuildResult, 1)}
	m := newManager(schema, b, core, hc, 1, func() *slog.Logger { return slog.New(slog.DiscardHandler) })
	defer m.close(0)
	raw, err := json.Marshal(req)
	if err != nil {
		return protocol.BuildResult{}, err
	}
	if _, err := m.handleBuild(ctx, raw); err != nil {
		return protocol.BuildResult{}, err
	}
	select {
	case r := <-core.results:
		return r, nil
	case <-ctx.Done():
		return protocol.BuildResult{}, ctx.Err()
	}
}

// resultTrap stands in for core: it takes progress and keeps the result.
type resultTrap struct {
	results chan protocol.BuildResult
}

func (c *resultTrap) Call(_ context.Context, typ string, payload, _ any) error {
	if r, ok := payload.(*protocol.BuildResult); ok && typ == protocol.TypeFactoryBuildResult {
		select {
		case c.results <- *r:
		default:
		}
	}
	return nil
}
