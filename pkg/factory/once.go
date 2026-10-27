package factory

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"

	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Once is what core would see of a build BuildOnce ran: the plan sent with
// the acceptance, the progress reports that reached it, and the result.
// Progress is best effort: a newer report replaces one not sent yet, so a
// step may be missing from Progress.
type Once struct {
	Steps    []protocol.BuildStep
	Progress []protocol.BuildProgress
	Result   protocol.BuildResult
}

// BuildOnce runs one build in process, without core or a broker, the way Run
// serves a factory.build: req is checked against schema, planned, the builder
// runs with a Job, and what core would receive is returned. Files and the
// artifact move through req's URLs as in a real build (factorytest.Store
// serves them). It is for testing builders.
func BuildOnce(ctx context.Context, schema protocol.BuildSchema, b Builder, req protocol.BuildRequest, hc *http.Client) (Once, error) {
	if err := protocol.ValidateBuildSchema(schema); err != nil {
		return Once{}, fmt.Errorf("invalid build schema: %w", err)
	}
	if hc == nil {
		hc = &http.Client{}
	}
	core := &resultTrap{results: make(chan protocol.BuildResult, 1)}
	m := newManager(schema, b, core, hc, 1, func() *slog.Logger { return slog.New(slog.DiscardHandler) })
	defer m.close(0)
	raw, err := json.Marshal(req)
	if err != nil {
		return Once{}, err
	}
	rep, err := m.handleBuild(ctx, raw)
	if err != nil {
		return Once{}, err
	}
	select {
	case r := <-core.results:
		// The last report may still be on its way: the build's goroutines
		// end right after the result.
		done := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
		}
		return Once{Steps: rep.(protocol.BuildReply).Steps, Progress: core.reports(), Result: r}, nil
	case <-ctx.Done():
		return Once{}, ctx.Err()
	}
}

// resultTrap stands in for core: it keeps the progress and the result.
type resultTrap struct {
	results  chan protocol.BuildResult
	mu       sync.Mutex
	progress []protocol.BuildProgress
}

func (c *resultTrap) Call(_ context.Context, typ string, payload, _ any) error {
	switch typ {
	case protocol.TypeFactoryBuildResult:
		if r, ok := payload.(*protocol.BuildResult); ok {
			select {
			case c.results <- *r:
			default:
			}
		}
	case protocol.TypeFactoryBuildProgress:
		if p, ok := payload.(protocol.BuildProgress); ok {
			c.mu.Lock()
			c.progress = append(c.progress, p)
			c.mu.Unlock()
		}
	}
	return nil
}

func (c *resultTrap) reports() []protocol.BuildProgress {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.progress)
}
