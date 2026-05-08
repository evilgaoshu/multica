package agent

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestBuildDroidArgsBaseline(t *testing.T) {
	t.Parallel()

	args := buildDroidArgs(ExecOptions{}, slog.Default())
	want := []string{"exec", "-o", "stream-json", "--auto", "high"}
	if strings.Join(args, "\n") != strings.Join(want, "\n") {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
}

func TestBuildDroidArgsIncludesDaemonManagedFlags(t *testing.T) {
	t.Parallel()

	args := buildDroidArgs(ExecOptions{
		Cwd:             "/tmp/work",
		Model:           "custom:DeepSeek-V4-Pro-0",
		ResumeSessionID: "session-123",
	}, slog.Default())

	assertArgPair(t, args, "--cwd", "/tmp/work")
	assertArgPair(t, args, "-m", "custom:DeepSeek-V4-Pro-0")
	assertArgPair(t, args, "-s", "session-123")
}

func TestBuildDroidArgsFiltersBlockedCustomArgs(t *testing.T) {
	t.Parallel()

	args := buildDroidArgs(ExecOptions{
		Cwd:             "/real/work",
		ResumeSessionID: "real-session",
		CustomArgs: []string{
			"--output-format", "text",
			"-o=json",
			"--input-format", "stream-jsonrpc",
			"-s", "evil-session",
			"--session-id=evil-session-2",
			"--fork", "forked",
			"--cwd", "/evil/work",
			"-f", "prompt.md",
			"--file=other.md",
			"--auto", "low",
			"--skip-permissions-unsafe",
			"--mission",
			"--worker-model", "worker",
			"--worker-reasoning-effort", "high",
			"--validator-model", "validator",
			"--validator-reasoning-effort", "high",
			"-w", "feature-wt",
			"--worktree-dir", "/tmp/wt",
			"--tag", "keep-me",
		},
	}, slog.Default())

	for _, forbidden := range []string{
		"--output-format", "text", "-o=json", "--input-format", "stream-jsonrpc",
		"evil-session", "--session-id=evil-session-2", "--fork", "forked",
		"/evil/work", "-f", "--file=other.md", "low", "--skip-permissions-unsafe",
		"--mission", "worker", "validator", "feature-wt", "/tmp/wt",
	} {
		if containsArg(args, forbidden) {
			t.Fatalf("blocked arg/value %q leaked into args: %#v", forbidden, args)
		}
	}

	assertArgPair(t, args, "--cwd", "/real/work")
	assertArgPair(t, args, "-s", "real-session")
	assertArgPair(t, args, "--tag", "keep-me")
}

func TestDroidProcessEventsHappyPath(t *testing.T) {
	t.Parallel()

	b := &droidBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 10)
	lines := strings.Join([]string{
		`{"type":"system","subtype":"init","cwd":"/repo","session_id":"abc-123","tools":["Read","Execute"],"model":"claude-sonnet-4-6"}`,
		`{"type":"message","role":"user","id":"msg-1","text":"run ls command","timestamp":1762517060816,"session_id":"abc-123"}`,
		`{"type":"message","role":"assistant","id":"msg-2","text":"I'll run ls.","timestamp":1762517062000,"session_id":"abc-123"}`,
		`{"type":"tool_call","id":"call-1","messageId":"msg-2","toolId":"Execute","toolName":"Execute","parameters":{"command":"ls -la"},"timestamp":1762517062500,"session_id":"abc-123"}`,
		`{"type":"tool_result","id":"call-1","messageId":"msg-3","toolId":"Execute","isError":false,"value":"total 16\n","timestamp":1762517063000,"session_id":"abc-123"}`,
		`{"type":"completion","finalText":"The ls command has been executed successfully.","numTurns":1,"durationMs":3000,"session_id":"abc-123","timestamp":1762517064000,"usage":{"input_tokens":10,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":2}}`,
	}, "\n")

	result := b.processEvents(strings.NewReader(lines), ch)

	if result.status != "completed" {
		t.Fatalf("status = %q, want completed", result.status)
	}
	if result.sessionID != "abc-123" {
		t.Fatalf("sessionID = %q, want abc-123", result.sessionID)
	}
	if result.model != "claude-sonnet-4-6" {
		t.Fatalf("model = %q, want claude-sonnet-4-6", result.model)
	}
	if result.output != "The ls command has been executed successfully." {
		t.Fatalf("output = %q", result.output)
	}
	if result.usage.InputTokens != 10 || result.usage.OutputTokens != 3 || result.usage.CacheReadTokens != 4 || result.usage.CacheWriteTokens != 2 {
		t.Fatalf("usage = %+v", result.usage)
	}

	close(ch)
	var msgs []Message
	for msg := range ch {
		msgs = append(msgs, msg)
	}
	if len(msgs) != 4 {
		t.Fatalf("expected 4 streamed messages, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != MessageStatus || msgs[0].Status != "running" || msgs[0].SessionID != "abc-123" {
		t.Fatalf("unexpected status message: %+v", msgs[0])
	}
	if msgs[1].Type != MessageText || msgs[1].Content != "I'll run ls." {
		t.Fatalf("unexpected text message: %+v", msgs[1])
	}
	if msgs[2].Type != MessageToolUse || msgs[2].Tool != "Execute" || msgs[2].CallID != "call-1" {
		t.Fatalf("unexpected tool-use message: %+v", msgs[2])
	}
	if got := msgs[2].Input["command"]; got != "ls -la" {
		t.Fatalf("tool input command = %v", got)
	}
	if msgs[3].Type != MessageToolResult || msgs[3].Tool != "Execute" || msgs[3].Output != "total 16\n" {
		t.Fatalf("unexpected tool-result message: %+v", msgs[3])
	}
}

func TestDroidProcessEventsErrorMarksFailure(t *testing.T) {
	t.Parallel()

	b := &droidBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 10)
	result := b.processEvents(strings.NewReader(`{"type":"error","message":"model unavailable","session_id":"err-session"}`), ch)

	if result.status != "failed" {
		t.Fatalf("status = %q, want failed", result.status)
	}
	if result.errMsg != "model unavailable" {
		t.Fatalf("errMsg = %q", result.errMsg)
	}
	if result.sessionID != "err-session" {
		t.Fatalf("sessionID = %q", result.sessionID)
	}

	close(ch)
	msg := <-ch
	if msg.Type != MessageError || msg.Content != "model unavailable" {
		t.Fatalf("unexpected error message: %+v", msg)
	}
}

func TestDroidExecuteUsesFakeCLIAndWritesPromptToStdin(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}

	tempDir := t.TempDir()
	argsFile := filepath.Join(tempDir, "argv.txt")
	stdinFile := filepath.Join(tempDir, "stdin.txt")
	fakePath := filepath.Join(tempDir, "droid")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > \"$DROID_ARGS_FILE\"\n" +
		"cat > \"$DROID_STDIN_FILE\"\n" +
		"printf '%s\\n' '{\"type\":\"system\",\"session_id\":\"fake-session\"}'\n" +
		"printf '%s\\n' '{\"type\":\"message\",\"role\":\"assistant\",\"text\":\"streamed\",\"session_id\":\"fake-session\"}'\n" +
		"printf '%s\\n' '{\"type\":\"completion\",\"finalText\":\"final output\",\"session_id\":\"fake-session\"}'\n"
	writeTestExecutable(t, fakePath, []byte(script))

	backend := &droidBackend{cfg: Config{
		ExecutablePath: fakePath,
		Env: map[string]string{
			"DROID_ARGS_FILE":  argsFile,
			"DROID_STDIN_FILE": stdinFile,
		},
		Logger: slog.Default(),
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "hello droid", ExecOptions{
		Cwd:             tempDir,
		Model:           "custom:DeepSeek-V4-Pro-0",
		ResumeSessionID: "resume-me",
		CustomArgs:      []string{"--output-format", "text", "--tag", "smoke"},
		Timeout:         5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var streamed []Message
	for msg := range session.Messages {
		streamed = append(streamed, msg)
	}
	result := <-session.Result
	if result.Status != "completed" || result.Output != "final output" || result.SessionID != "fake-session" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(streamed) != 2 || streamed[1].Type != MessageText || streamed[1].Content != "streamed" {
		t.Fatalf("unexpected streamed messages: %+v", streamed)
	}

	stdinData, err := os.ReadFile(stdinFile)
	if err != nil {
		t.Fatalf("read stdin fixture: %v", err)
	}
	if string(stdinData) != "hello droid" {
		t.Fatalf("stdin = %q, want prompt", string(stdinData))
	}

	argsData, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read argv fixture: %v", err)
	}
	args := strings.Fields(string(argsData))
	assertArgPair(t, args, "-o", "stream-json")
	assertArgPair(t, args, "--auto", "high")
	assertArgPair(t, args, "--cwd", tempDir)
	assertArgPair(t, args, "-m", "custom:DeepSeek-V4-Pro-0")
	assertArgPair(t, args, "-s", "resume-me")
	assertArgPair(t, args, "--tag", "smoke")
	if containsArg(args, "text") {
		t.Fatalf("blocked custom output-format value leaked into argv: %#v", args)
	}
}

func assertArgPair(t *testing.T, args []string, flag, value string) {
	t.Helper()
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return
		}
	}
	t.Fatalf("missing arg pair %q %q in %#v", flag, value, args)
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}
