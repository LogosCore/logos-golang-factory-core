package factory

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

func TestRPCQueue(t *testing.T) {
	if got := RPCQueue("bob-1"); got != "logos.factory.rpc.bob-1" {
		t.Fatalf("RPCQueue = %q", got)
	}
}

func TestRunRefusesInvalidConfig(t *testing.T) {
	valid := func() Config {
		schema := testSchema()
		return Config{
			Config: module.Config{
				AMQPURL:  "amqp://guest:guest@127.0.0.1:1/",
				Identity: lifecycle.Identity{ModuleName: "bob", Instance: "bob-test"},
			},
			Build:   &schema,
			Builder: uploadAndReturn(Result{}),
		}
	}
	noop := func(context.Context, json.RawMessage) (any, error) { return nil, nil }
	for _, c := range []struct {
		want   string
		mutate func(c *Config)
	}{
		{"module type must be", func(c *Config) { c.Identity.ModuleType = protocol.ServiceChannel }},
		{"Build schema is required", func(c *Config) { c.Build = nil }},
		{"invalid build schema", func(c *Config) { c.Build.Targets = nil }},
		{"Builder is required", func(c *Config) { c.Builder = nil }},
		{"MaxConcurrentBuilds", func(c *Config) { c.MaxConcurrentBuilds = -1 }},
		{`must not take "factory.build"`, func(c *Config) {
			c.Handlers = map[string]mgmt.HandlerFunc{protocol.TypeFactoryBuild: noop}
		}},
		{`duplicate action "drain"`, func(c *Config) { // the builds section's
			own, err := mgmt.New(protocol.ManagementManifest{SchemaVersion: protocol.ManagementSchemaV1,
				Actions: []protocol.ActionSpec{{Name: ActionDrain, Risk: protocol.RiskHigh}}})
			if err != nil {
				t.Fatal(err)
			}
			c.Management = own
		}},
		{"AMQPURL is required", func(c *Config) { c.AMQPURL = "" }}, // module.Run's own check
	} {
		cfg := valid()
		c.mutate(&cfg)
		if err := Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("want an error containing %q, got %v", c.want, err)
		}
	}
}
