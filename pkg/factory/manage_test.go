package factory

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// managed serves m's builds section the way Run does, for a factory without
// management of its own.
func managed(t *testing.T, m *manager) *mgmt.Server {
	t.Helper()
	var mc module.Config
	if err := manage(&mc, m); err != nil {
		t.Fatalf("manage: %v", err)
	}
	return mc.Management
}

// mgmtRequest is core's management request of typ.
func mgmtRequest(t *testing.T, typ string, payload any) protocol.Envelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewULID()
	return protocol.Envelope{
		MessageID: id, CorrelationID: id, Type: typ, Version: protocol.EnvelopeVersionV1,
		Source: protocol.Source{Service: "core", Instance: "core-1"}, Payload: raw,
	}
}

// invoke runs a management action as core does and returns its result.
func invoke(t *testing.T, s *mgmt.Server, action string) map[string]any {
	t.Helper()
	rep := s.Handle(context.Background(), mgmtRequest(t, protocol.TypeMgmtActionInvoke,
		protocol.ActionInvokeRequest{InvocationID: protocol.NewULID(), Action: action, Actor: &protocol.Actor{Type: "user", ID: "u1"}}))
	if rep.Status != protocol.StatusOK {
		t.Fatalf("%s: %+v", action, rep.Error)
	}
	var out struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal(rep.Payload, &out); err != nil {
		t.Fatal(err)
	}
	return out.Result
}

// limits is m's build limit and running builds.
func limits(m *manager) (max, running int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.max, m.running
}

// configure applies core's config of the instance, as core does.
func configure(t *testing.T, s *mgmt.Server, revision int64, values map[string]any) protocol.ConfigApplyReply {
	t.Helper()
	return s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: revision, Values: values})
}

func TestBuildsSection(t *testing.T) {
	m := newTestManager(t, uploadAndReturn(Result{}), newFakeCore(), 3)
	man := managed(t, m).Manifest()
	if len(man.Properties) != 1 || len(man.Actions) != 2 || len(man.Status) != 9 {
		t.Fatalf("manifest = %+v", man)
	}
	p := man.Properties[0]
	if p.Key != PropertyMaxConcurrentBuilds || p.Default != int64(3) || p.Apply != protocol.ApplyLive || p.Max != maxBuildsCap {
		t.Errorf("max_concurrent_builds = %+v", p)
	}
	if man.Actions[0].Name != ActionDrain || man.Actions[1].Name != ActionResume {
		t.Errorf("actions = %+v", man.Actions)
	}

	// A factory configured above the cap stays valid.
	big := newTestManager(t, uploadAndReturn(Result{}), newFakeCore(), 100)
	if p := managed(t, big).Manifest().Properties[0]; p.Default != int64(100) || p.Max != 100 {
		t.Errorf("max_concurrent_builds = %+v", p)
	}
}

func TestBuildsSectionJoinsTheFactorysOwn(t *testing.T) {
	m := newTestManager(t, uploadAndReturn(Result{}), newFakeCore(), 1)
	own, err := mgmt.New(protocol.ManagementManifest{
		SchemaVersion: protocol.ManagementSchemaV1,
		Properties: []protocol.PropertySpec{{FieldSpec: protocol.FieldSpec{
			Key: "log_level", Type: protocol.FieldEnum, Default: "info", Options: []protocol.EnumOption{{Value: "info"}, {Value: "debug"}},
		}, Apply: protocol.ApplyLive}},
		Actions: []protocol.ActionSpec{{Name: "say_hello", Risk: protocol.RiskLow}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var levels []any
	own.OnApply(func(_ context.Context, live map[string]any) error {
		levels = append(levels, live["log_level"])
		return nil
	})
	mc := module.Config{Management: own}
	if err := manage(&mc, m); err != nil {
		t.Fatalf("manage: %v", err)
	}
	if mc.Management != own {
		t.Fatal("the factory's own management was replaced")
	}
	man := own.Manifest()
	if len(man.Properties) != 2 || len(man.Actions) != 3 || len(man.Status) != 9 {
		t.Fatalf("manifest = %+v", man)
	}
	configure(t, own, 1, map[string]any{"log_level": "debug", PropertyMaxConcurrentBuilds: 2})
	if max, _ := limits(m); len(levels) != 1 || levels[0] != "debug" || max != 2 {
		t.Errorf("own apply saw %v, max = %d", levels, max)
	}

	// The section's keys are taken.
	clash, err := mgmt.New(protocol.ManagementManifest{
		SchemaVersion: protocol.ManagementSchemaV1,
		Actions:       []protocol.ActionSpec{{Name: ActionDrain, Risk: protocol.RiskHigh}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manage(&module.Config{Management: clash}, m); err == nil || !strings.Contains(err.Error(), `"drain"`) {
		t.Fatalf("a factory's own drain: %v", err)
	}
}

func TestDrainAndResume(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	m := newTestManager(t, waitForCancel(nil), core, 2)
	s := managed(t, m)

	first := testRequest(st)
	if err := submit(t, m, first); err != nil {
		t.Fatal(err)
	}
	if got := invoke(t, s, ActionDrain); got["draining"] != true || got["was_draining"] != false || got["builds_running"] != float64(1) {
		t.Errorf("drain = %v", got)
	}
	err := submit(t, m, testRequest(st))
	if coreerrors.Code(err) != protocol.CodeBusy || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("a build while draining: %v", err)
	}
	// The same build is still the same build.
	if err := submit(t, m, first); err != nil {
		t.Fatalf("a repeated factory.build while draining: %v", err)
	}
	if st := m.status(); st["draining"] != true || st["builds_refused_total"] != int64(1) || st["builds_running"] != 1 {
		t.Errorf("status = %v", st)
	}

	// Draining again is pointless, and so is resuming twice.
	refused(t, s, ActionDrain, "Already draining")
	if got := invoke(t, s, ActionResume); got["draining"] != false || got["was_draining"] != true {
		t.Errorf("resume = %v", got)
	}
	refused(t, s, ActionResume, "Not draining")
	if err := submit(t, m, testRequest(st)); err != nil {
		t.Fatalf("a build after resume: %v", err)
	}
}

// refused invokes a management action that must be unavailable for reason.
func refused(t *testing.T, s *mgmt.Server, action, reason string) {
	t.Helper()
	rep := s.Handle(context.Background(), mgmtRequest(t, protocol.TypeMgmtActionInvoke,
		protocol.ActionInvokeRequest{InvocationID: protocol.NewULID(), Action: action}))
	if rep.Error == nil || rep.Error.Code != protocol.CodeActionUnavailable || rep.Error.Message != reason {
		t.Fatalf("%s: %+v, want %s %q", action, rep.Error, protocol.CodeActionUnavailable, reason)
	}
}

func TestMaxConcurrentBuildsIsLive(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	m := newTestManager(t, waitForCancel(nil), core, 1)
	s := managed(t, m)

	if rep := configure(t, s, 1, map[string]any{PropertyMaxConcurrentBuilds: 2}); len(rep.Rejected) != 0 {
		t.Fatalf("apply: %+v", rep)
	}
	for i := range 2 {
		if err := submit(t, m, testRequest(st)); err != nil {
			t.Fatalf("build %d of 2: %v", i+1, err)
		}
	}
	if err := submit(t, m, testRequest(st)); coreerrors.Code(err) != protocol.CodeBusy {
		t.Fatalf("a third build: %v", err)
	}

	// Back to the default: both keep running, no new one starts.
	configure(t, s, 2, nil)
	if max, running := limits(m); max != 1 || running != 2 {
		t.Fatalf("max = %d, running = %d", max, running)
	}
	if err := submit(t, m, testRequest(st)); coreerrors.Code(err) != protocol.CodeBusy {
		t.Fatalf("a build over the lowered limit: %v", err)
	}

	rep := configure(t, s, 3, map[string]any{PropertyMaxConcurrentBuilds: 0})
	if max, _ := limits(m); rep.Rejected[PropertyMaxConcurrentBuilds] == "" || max != 1 {
		t.Errorf("a limit of 0: %+v, max = %d", rep, max)
	}
}

func TestBuildCounters(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	started := make(chan struct{}, 1)
	fail := errors.New("no")
	m := newTestManager(t, BuilderFunc(func(ctx context.Context, job *Job) (Result, error) {
		switch job.Request.Values["greeting"] {
		case "fail":
			return Result{}, Fail("compile_failed", "the compiler said no")
		case "plain":
			return Result{}, fail
		case "wait":
			return waitForCancel(started)(ctx, job)
		}
		return uploadAndReturn(Result{})(ctx, job)
	}), core, 1)
	s := managed(t, m)
	if st := s.StatusSnapshot(); st["builds_accepted_total"] != int64(0) || st["draining"] != false {
		t.Fatalf("status before any build = %v", st)
	}
	if _, ok := s.StatusSnapshot()["last_build_failure"]; ok {
		t.Fatal("a last failure before any build")
	}

	run := func(greeting string) protocol.BuildRequest {
		t.Helper()
		req := testRequest(st)
		req.Values["greeting"] = greeting
		if err := submit(t, m, req); err != nil {
			t.Fatal(err)
		}
		return req
	}
	run("ok")
	waitFor(t, "the build", func() bool { return idle(m) })
	run("fail")
	waitFor(t, "the build", func() bool { return idle(m) })
	failedAt := time.Now()
	waiting := run("wait")
	<-started
	if err := submit(t, m, testRequest(st)); coreerrors.Code(err) != protocol.CodeBusy {
		t.Fatalf("a build with no room: %v", err)
	}
	cancelBuild(t, m, waiting.BuildID)
	waitFor(t, "the cancelled build", func() bool { return idle(m) })

	got := s.StatusSnapshot()
	want := map[string]any{
		"builds_running": 0, "builds_accepted_total": int64(3), "builds_succeeded_total": int64(1),
		"builds_failed_total": int64(1), "builds_cancelled_total": int64(1), "builds_refused_total": int64(1),
		"last_build_failure": "compile_failed: the compiler said no", "draining": false,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v (%T), want %v", k, got[k], got[k], v)
		}
	}
	at, err := protocol.ParseDatetime(got["last_build_failure_at"])
	if err != nil || at.Sub(failedAt).Abs() > 5*time.Second {
		t.Errorf("last_build_failure_at = %v (%v)", got["last_build_failure_at"], err)
	}

	// A failure without a code of the builder's own.
	run("plain")
	waitFor(t, "the build", func() bool { return idle(m) })
	if got := s.StatusSnapshot()["last_build_failure"]; got != "build_failed: no" {
		t.Errorf("last_build_failure = %v", got)
	}
}
