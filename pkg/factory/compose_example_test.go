package factory_test

import (
	"cmp"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-factory-core/pkg/factory"
	"github.com/logoscore/logos-golang-factory-core/pkg/factorytest"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// archive is the Task Outsourcing page's example (docs:
// contracts/task-outsourcing.md, A factory's code), as it is there: it
// finishes without its own op for a folder that is empty or cannot be read;
// downloads the archive its own op made, giving up after 10 minutes; and
// removes it from the host whatever the download did, or when the task is
// stopped.
type archive struct{}

// state is what the task's decisions keep in its ctx.
type state struct {
	Archive string `json:"archive,omitempty"` // the archive its own op made
	Failed  string `json:"failed,omitempty"`  // why the download failed
}

func (archive) Compose(ctx context.Context, c factory.ComposeCall) (factory.Decision, error) {
	var st state
	if err := c.ReadCtx(&st); err != nil {
		return factory.Decision{}, err
	}
	switch c.Event {
	case factory.Start:
		return factory.RunSubtasks(factory.Subtask{Command: "ls", Args: map[string]any{"path": c.Args["path"]}}), nil
	case factory.Timer: // the download took too long: stop it (it ends cancelled)
		st.Failed = "the download took over 10 minutes"
		return factory.Cancel("fetch").Ctx(st), nil
	case factory.Stopped: // clean up the archive, unless rm removed it
		for _, e := range c.Ends {
			switch {
			case e.OwnOp && e.OK:
				st.Archive = archivePath(e.Data)
			case e.Command == "rm" && e.OK:
				st.Archive = ""
			}
		}
		if st.Archive == "" {
			return factory.Decision{}, nil // nothing to clean up
		}
		return factory.RunSubtasks(rm(st.Archive)), nil
	}
	e := c.End
	switch {
	case e.Command == "ls":
		if !e.OK { // no such folder, access denied
			return factory.Finish(false, e.Err), nil
		}
		if isEmpty(e.Data) { // a dir_listing with no entries
			return factory.Finish(true, "nothing to archive"), nil // its own op never runs
		}
		return factory.RunOwnOp(nil), nil // with the task's own args
	case e.OwnOp:
		if !e.OK {
			return factory.Finish(false, e.Err), nil
		}
		st.Archive = archivePath(e.Data) // its data, in the factory's own shape
		fetch := factory.Subtask{Command: "download", Args: map[string]any{"path": st.Archive}, Key: "fetch", As: "fetch the archive"}
		return factory.RunSubtasks(fetch).After(10 * time.Minute).Ctx(st), nil
	case e.Key == "fetch": // remove the archive, whatever the download did
		if !e.OK && st.Failed == "" {
			st.Failed = "the download failed: " + e.Err
		}
		return factory.RunSubtasks(rm(st.Archive)).Ctx(st), nil
	default: // rm ended
		switch {
		case st.Failed != "":
			return factory.Finish(false, st.Failed), nil
		case !e.OK:
			return factory.Finish(false, "downloaded, but the archive is still on the host: "+e.Err), nil
		}
		return factory.Finish(true, ""), nil
	}
}

func rm(path string) factory.Subtask {
	return factory.Subtask{Command: "rm", Args: map[string]any{"path": path}, As: "remove the archive"}
}

// isEmpty and archivePath are the example's own: whether a dir_listing's
// data has no entries, and the path in its own op's data.
func isEmpty(data json.RawMessage) bool {
	var listing struct {
		Entries []json.RawMessage `json:"entries"`
	}
	return json.Unmarshal(data, &listing) == nil && len(listing.Entries) == 0
}

func archivePath(data json.RawMessage) string {
	var out struct {
		Archive string `json:"archive"`
	}
	_ = json.Unmarshal(data, &out)
	return out.Archive
}

// The example, call by call, through the same checks as a factory's Run.
func TestComposeArchiveExample(t *testing.T) {
	const (
		task    = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
		subtask = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
		zip     = `C:\Users\alice\AppData\Local\Temp\docs.zip`
	)
	st := factorytest.NewStore(t)
	compose := func(r protocol.ComposeRequest) protocol.Decision {
		t.Helper()
		r.TaskID, r.Command, r.Args = task, "archive", map[string]any{"path": `C:\Users\alice\Documents`}
		if r.OwnOp == "" {
			r.OwnOp = protocol.OwnOpNotRun
		}
		if r.End != nil || len(r.Ends) > 0 {
			r.Event = cmp.Or(r.Event, protocol.ComposeEnded)
		}
		d, err := factorytest.Compose(t, archive{}, st, r)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	ended := func(command, key string, ok bool, data string) *protocol.ComposeEnd {
		e := &protocol.ComposeEnd{TaskID: subtask, Command: command, Key: key, State: protocol.EndDone, OK: ok}
		if !ok {
			e.Err = "not found"
		}
		if data != "" {
			e.Data = json.RawMessage(data)
		}
		return e
	}
	ownOp := &protocol.ComposeEnd{TaskID: task, OwnOp: true, Command: "archive", State: protocol.EndDone, OK: true,
		Data: json.RawMessage(`{"archive": "C:\\Users\\alice\\AppData\\Local\\Temp\\docs.zip"}`)}
	kept := func(d protocol.Decision) state {
		t.Helper()
		var s state
		if d.Ctx == nil || json.Unmarshal(*d.Ctx, &s) != nil {
			t.Fatalf("no ctx kept: %+v", d)
		}
		return s
	}
	ctxOf := func(s state) []byte { b, _ := json.Marshal(s); return b }

	if d := compose(protocol.ComposeRequest{}); len(d.Run) != 1 || d.Run[0].Command != "ls" || d.Run[0].Args["path"] != `C:\Users\alice\Documents` {
		t.Fatalf("start: %+v", d)
	}
	if d := compose(protocol.ComposeRequest{End: ended("ls", "", true, `{"entries": [{"name": "a.txt"}]}`)}); d.OwnOp == nil {
		t.Fatalf("a folder with files: %+v", d)
	}
	if d := compose(protocol.ComposeRequest{End: ended("ls", "", true, `{"entries": []}`)}); d.Finish == nil || !d.Finish.OK ||
		d.Finish.Message != "nothing to archive" {
		t.Fatalf("an empty folder: %+v", d)
	}
	if d := compose(protocol.ComposeRequest{End: ended("ls", "", false, "")}); d.Finish == nil || d.Finish.OK || d.Finish.Err != "not found" {
		t.Fatalf("no such folder: %+v", d)
	}
	d := compose(protocol.ComposeRequest{OwnOp: protocol.OwnOpEnded, End: ownOp})
	if len(d.Run) != 1 || d.Run[0].Key != "fetch" || d.Run[0].Args["path"] != zip || d.After != "10m0s" || kept(d).Archive != zip {
		t.Fatalf("its own op ended: %+v", d)
	}
	archived := ctxOf(state{Archive: zip})
	if d := compose(protocol.ComposeRequest{OwnOp: protocol.OwnOpEnded, Ctx: archived, End: ended("download", "fetch", true, "")}); len(d.Run) != 1 ||
		d.Run[0].Command != "rm" || d.Run[0].Args["path"] != zip {
		t.Fatalf("downloaded: %+v", d)
	}
	if d := compose(protocol.ComposeRequest{OwnOp: protocol.OwnOpEnded, Ctx: archived, End: ended("rm", "", true, "")}); d.Finish == nil || !d.Finish.OK {
		t.Fatalf("removed: %+v", d)
	}
	// The timer: the download is stopped, and the task fails once the
	// archive is removed.
	d = compose(protocol.ComposeRequest{Event: protocol.ComposeTimer, OwnOp: protocol.OwnOpEnded, Ctx: archived})
	if d.Cancel == nil || len(d.Cancel.Keys) != 1 || d.Cancel.Keys[0] != "fetch" || !strings.Contains(kept(d).Failed, "10 minutes") {
		t.Fatalf("the timer: %+v", d)
	}
	late := ctxOf(kept(d))
	stopped := ended("download", "fetch", false, "")
	stopped.State, stopped.Err = protocol.EndCancelled, ""
	if d := compose(protocol.ComposeRequest{OwnOp: protocol.OwnOpEnded, Ctx: late, End: stopped}); len(d.Run) != 1 || d.Run[0].Command != "rm" {
		t.Fatalf("the download stopped: %+v", d)
	}
	if d := compose(protocol.ComposeRequest{OwnOp: protocol.OwnOpEnded, Ctx: late, End: ended("rm", "", true, "")}); d.Finish == nil || d.Finish.OK ||
		d.Finish.Err != "the download took over 10 minutes" {
		t.Fatalf("removed after the timer: %+v", d)
	}
	// A stop: the archive its own op made, which the factory had not heard
	// of, is removed; with none, nothing is cleaned up.
	if d := compose(protocol.ComposeRequest{Event: protocol.ComposeStopped, OwnOp: protocol.OwnOpEnded,
		Ends: []protocol.ComposeEnd{*ownOp}}); len(d.Run) != 1 || d.Run[0].Args["path"] != zip {
		t.Fatalf("stopped: %+v", d)
	}
	if d := compose(protocol.ComposeRequest{Event: protocol.ComposeStopped}); len(d.Run) != 0 || d.Finish != nil {
		t.Fatalf("stopped before its own op: %+v", d)
	}
}
