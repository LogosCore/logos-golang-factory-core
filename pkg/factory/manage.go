package factory

import (
	"context"
	"fmt"
	"time"

	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// The builds section of a factory's management, which Run adds to what the
// factory declares itself (mgmt.Server.Extend): the build limit, drain and
// resume, and how the builds went. A factory's own manifest cannot use its
// keys: the property and actions below, and the status values builds_running,
// builds_accepted_total, builds_succeeded_total, builds_failed_total,
// builds_cancelled_total, builds_refused_total, last_build_failure,
// last_build_failure_at and draining.
const (
	// PropertyMaxConcurrentBuilds is how many builds run at once, applied
	// live. It starts at Config.MaxConcurrentBuilds.
	PropertyMaxConcurrentBuilds = "max_concurrent_builds"
	// ActionDrain stops taking builds; running ones finish.
	ActionDrain = "drain"
	// ActionResume takes builds again.
	ActionResume = "resume"
)

// statusDraining is the status value drain and resume are available by.
const statusDraining = "draining"

// maxBuildsCap is the most max_concurrent_builds takes, unless
// Config.MaxConcurrentBuilds is higher.
const maxBuildsCap = 64

// buildStats is what the status values report.
type buildStats struct {
	accepted, succeeded, failed, cancelled, refused int64
	lastFailure                                     string // "code: message"
	lastFailureAt                                   time.Time
}

// manage adds m's builds section to the factory's management, or makes the
// management of just that section when the factory declares none.
func manage(mc *module.Config, m *manager) error {
	if mc.Management == nil {
		s, err := mgmt.New(protocol.ManagementManifest{SchemaVersion: protocol.ManagementSchemaV1})
		if err != nil {
			return err
		}
		mc.Management = s
	}
	return mc.Management.Extend(m.section())
}

// section is the builds section served by m.
func (m *manager) section() mgmt.Section {
	m.mu.Lock()
	limit := m.max
	m.mu.Unlock()
	return mgmt.Section{
		Properties: []protocol.PropertySpec{{FieldSpec: protocol.FieldSpec{
			Key: PropertyMaxConcurrentBuilds, Type: protocol.FieldInt, Label: "Max concurrent builds",
			Description: "Builds this instance runs at once. Core offers further builds to other instances of the factory, or keeps them queued. Lowering it lets running builds finish.",
			Group:       "Builds", Order: 1,
			Default: int64(limit), Min: 1, Max: max(maxBuildsCap, limit),
		}, Apply: protocol.ApplyLive}},
		Actions: []protocol.ActionSpec{
			{
				Name: ActionDrain, Label: "Drain", Risk: protocol.RiskHigh,
				Description: "Take no new builds until resumed: core offers them to other instances of the factory, or keeps them queued. Running builds finish. Shown in the Draining status value.",
				Confirm:     "This instance will take no new builds until it is resumed.",
				AvailableWhen: []protocol.ActionCondition{
					{Status: statusDraining, Op: protocol.OpEq, Value: false, Message: "Already draining"},
				},
			},
			{
				Name: ActionResume, Label: "Resume", Risk: protocol.RiskLow, Description: "Take builds again after a drain.",
				AvailableWhen: []protocol.ActionCondition{
					{Status: statusDraining, Op: protocol.OpEq, Value: true, Message: "Not draining"},
				},
			},
		},
		Status: []protocol.StatusSpec{
			{Key: "builds_running", Type: protocol.FieldInt, Label: "Builds running"},
			{Key: "builds_accepted_total", Type: protocol.FieldInt, Label: "Builds accepted"},
			{Key: "builds_succeeded_total", Type: protocol.FieldInt, Label: "Builds succeeded"},
			{Key: "builds_failed_total", Type: protocol.FieldInt, Label: "Builds failed"},
			{Key: "builds_cancelled_total", Type: protocol.FieldInt, Label: "Builds cancelled",
				Description: "Stopped because core cancelled them or no longer waited for them."},
			{Key: "builds_refused_total", Type: protocol.FieldInt, Label: "Builds refused",
				Description: "Offers turned down while busy, draining or stopping. Core offers them to another instance or again later."},
			{Key: "last_build_failure", Type: protocol.FieldString, Label: "Last build failure",
				Description: "Its error code and message."},
			{Key: "last_build_failure_at", Type: protocol.FieldDatetime, Label: "Last build failure at"},
			// Draining needs an operator's eye: the instance takes no builds.
			{Key: statusDraining, Type: protocol.FieldBool, Label: "Draining", Alert: true},
		},
		Apply:    m.apply,
		Handlers: map[string]mgmt.ActionFunc{ActionDrain: m.drain, ActionResume: m.resume},
		Values:   m.status,
	}
}

// apply sets the build limit from the live values. Running builds above a
// lowered limit finish; no new one starts until there is room.
func (m *manager) apply(ctx context.Context, live map[string]any) {
	n, ok := protocol.AsInt64(live[PropertyMaxConcurrentBuilds])
	if !ok {
		return
	}
	m.mu.Lock()
	prev, running := m.max, m.running
	m.max = int(n)
	m.mu.Unlock()
	if prev != int(n) {
		m.logger().Info("factory: max concurrent builds changed", "from", prev, "to", n,
			"builds_running", running, "by", actorName(mgmt.ActorFromContext(ctx)))
	}
}

func (m *manager) drain(_ context.Context, _ map[string]any, actor *protocol.Actor) (any, error) {
	return m.setDraining(true, actor), nil
}

func (m *manager) resume(_ context.Context, _ map[string]any, actor *protocol.Actor) (any, error) {
	return m.setDraining(false, actor), nil
}

func (m *manager) setDraining(on bool, actor *protocol.Actor) map[string]any {
	m.mu.Lock()
	was, running := m.draining, m.running
	m.draining = on
	m.mu.Unlock()
	if on {
		m.logger().Info("factory: drained: taking no new builds", "by", actorName(actor), "builds_running", running)
	} else {
		m.logger().Info("factory: resumed: taking builds", "by", actorName(actor))
	}
	return map[string]any{"draining": on, "was_draining": was, "builds_running": running}
}

// status is the section's status values.
func (m *manager) status() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{
		"builds_running":         m.running,
		"builds_accepted_total":  m.stats.accepted,
		"builds_succeeded_total": m.stats.succeeded,
		"builds_failed_total":    m.stats.failed,
		"builds_cancelled_total": m.stats.cancelled,
		"builds_refused_total":   m.stats.refused,
		statusDraining:           m.draining,
	}
	if m.stats.lastFailure != "" {
		out["last_build_failure"] = m.stats.lastFailure
		out["last_build_failure_at"] = m.stats.lastFailureAt.UTC().Format(time.RFC3339)
	}
	return out
}

// countLocked counts a build that ended with result.
func (m *manager) countLocked(result protocol.BuildResult) {
	if result.Status == protocol.BuildSucceeded {
		m.stats.succeeded++
		return
	}
	m.stats.failed++
	if e := result.Error; e != nil {
		m.stats.lastFailure, m.stats.lastFailureAt = e.Code+": "+e.Message, m.now()
	}
}

// actorName names who triggered a management change, for the log.
func actorName(a *protocol.Actor) string {
	switch {
	case a == nil:
		return "core"
	case a.Name != "":
		return fmt.Sprintf("%s %s (%s)", a.Type, a.Name, a.ID)
	default:
		return a.Type + " " + a.ID
	}
}
