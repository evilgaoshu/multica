package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"time"
)

// droidBlockedArgs are flags hardcoded by the daemon that must not be
// overridden by user-configured custom_args.
var droidBlockedArgs = map[string]blockedArgMode{
	"-o":                           blockedWithValue,  // stream-json protocol
	"--output-format":              blockedWithValue,  // stream-json protocol
	"--input-format":               blockedWithValue,  // daemon uses stdin prompt, not JSON-RPC input
	"-s":                           blockedWithValue,  // daemon owns session resume
	"--session-id":                 blockedWithValue,  // daemon owns session resume
	"--fork":                       blockedWithValue,  // fork would replace the daemon-managed session lineage
	"--cwd":                        blockedWithValue,  // daemon owns the workdir
	"-f":                           blockedWithValue,  // daemon owns prompt delivery over stdin
	"--file":                       blockedWithValue,  // daemon owns prompt delivery over stdin
	"--auto":                       blockedWithValue,  // daemon chooses autonomy level for unattended runs
	"--skip-permissions-unsafe":    blockedStandalone, // mutually exclusive with --auto and unsafe outside isolation
	"--mission":                    blockedStandalone, // mission orchestration has different semantics
	"--worker-model":               blockedWithValue,
	"--worker-reasoning-effort":    blockedWithValue,
	"--validator-model":            blockedWithValue,
	"--validator-reasoning-effort": blockedWithValue,
	"-w":                           blockedWithValue, // prevent droid from moving execution to another worktree
	"--worktree":                   blockedWithValue,
	"--worktree-dir":               blockedWithValue,
}

// droidBackend implements Backend by spawning `droid exec -o stream-json`
// and parsing Factory's JSONL event stream from stdout.
type droidBackend struct {
	cfg Config
}

func (b *droidBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "droid"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("droid executable not found at %q: %w", execPath, err)
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 20 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)

	args := buildDroidArgs(opts, b.cfg.Logger)
	cmd := exec.CommandContext(runCtx, execPath, args...)
	hideAgentWindow(cmd)
	b.cfg.Logger.Info("agent command", "exec", execPath, "args", args)
	cmd.WaitDelay = 10 * time.Second
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	cmd.Env = buildEnv(b.cfg.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("droid stdout pipe: %w", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("droid stdin pipe: %w", err)
	}
	closeStdin := func() {
		if stdin != nil {
			_ = stdin.Close()
			stdin = nil
		}
	}
	stderrBuf := newStderrTail(newLogWriter(b.cfg.Logger, "[droid:stderr] "), agentStderrTailBytes)
	cmd.Stderr = stderrBuf

	if err := cmd.Start(); err != nil {
		closeStdin()
		cancel()
		return nil, fmt.Errorf("start droid: %w", err)
	}
	if err := writeDroidInput(stdin, prompt); err != nil {
		closeStdin()
		cancel()
		_ = cmd.Wait()
		return nil, errors.New(withAgentStderr(fmt.Sprintf("write droid input: %v", err), "droid", stderrBuf.Tail()))
	}
	closeStdin()

	b.cfg.Logger.Info("droid started", "pid", cmd.Process.Pid, "cwd", opts.Cwd, "model", opts.Model)

	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)

	go func() {
		<-runCtx.Done()
		_ = stdout.Close()
	}()

	go func() {
		defer cancel()
		defer close(msgCh)
		defer close(resCh)

		startTime := time.Now()
		scanResult := b.processEvents(stdout, msgCh)

		exitErr := cmd.Wait()
		duration := time.Since(startTime)

		if runCtx.Err() == context.DeadlineExceeded {
			scanResult.status = "timeout"
			scanResult.errMsg = fmt.Sprintf("droid timed out after %s", timeout)
		} else if runCtx.Err() == context.Canceled {
			scanResult.status = "aborted"
			scanResult.errMsg = "execution cancelled"
		} else if exitErr != nil && scanResult.status == "completed" {
			scanResult.status = "failed"
			scanResult.errMsg = fmt.Sprintf("droid exited with error: %v", exitErr)
		}
		if scanResult.errMsg != "" {
			scanResult.errMsg = withAgentStderr(scanResult.errMsg, "droid", stderrBuf.Tail())
		}

		b.cfg.Logger.Info("droid finished", "pid", cmd.Process.Pid, "status", scanResult.status, "duration", duration.Round(time.Millisecond).String())

		var usage map[string]TokenUsage
		u := scanResult.usage
		if u.InputTokens > 0 || u.OutputTokens > 0 || u.CacheReadTokens > 0 || u.CacheWriteTokens > 0 {
			model := opts.Model
			if model == "" {
				model = scanResult.model
			}
			if model == "" {
				model = "unknown"
			}
			usage = map[string]TokenUsage{model: u}
		}

		sessionID := resolveSessionID(opts.ResumeSessionID, scanResult.sessionID, scanResult.status == "failed")
		resCh <- Result{
			Status:     scanResult.status,
			Output:     scanResult.output,
			Error:      scanResult.errMsg,
			DurationMs: duration.Milliseconds(),
			SessionID:  sessionID,
			Usage:      usage,
		}
	}()

	return &Session{Messages: msgCh, Result: resCh}, nil
}

func writeDroidInput(stdin io.Writer, prompt string) error {
	_, err := io.WriteString(stdin, prompt)
	return err
}

func buildDroidArgs(opts ExecOptions, logger *slog.Logger) []string {
	args := []string{"exec", "-o", "stream-json", "--auto", "high"}
	if opts.Cwd != "" {
		args = append(args, "--cwd", opts.Cwd)
	}
	if opts.Model != "" {
		args = append(args, "-m", opts.Model)
	}
	if opts.SystemPrompt != "" {
		args = append(args, "--append-system-prompt", opts.SystemPrompt)
	}
	if opts.MaxTurns > 0 {
		logger.Warn("droid does not support --max-turns; ignoring", "maxTurns", opts.MaxTurns)
	}
	if opts.ResumeSessionID != "" {
		args = append(args, "-s", opts.ResumeSessionID)
	}
	args = append(args, filterCustomArgs(opts.CustomArgs, droidBlockedArgs, logger)...)
	return args
}

func (b *droidBackend) processEvents(r io.Reader, ch chan<- Message) eventResult {
	var output strings.Builder
	var sessionID string
	var model string
	var usage TokenUsage
	finalStatus := "completed"
	var finalError string

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var event droidEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}
		if event.SessionID != "" {
			sessionID = event.SessionID
		}
		if event.Model != "" {
			model = event.Model
		}
		usage = addDroidUsage(usage, event.Usage)

		switch event.Type {
		case "system":
			trySend(ch, Message{Type: MessageStatus, Status: "running", SessionID: sessionID})
		case "message":
			b.handleDroidMessage(event, ch, &output)
		case "tool_call":
			b.handleDroidToolCall(event, ch)
		case "tool_result":
			b.handleDroidToolResult(event, ch)
		case "completion":
			if event.FinalText != "" {
				output.Reset()
				output.WriteString(event.FinalText)
			}
			if event.IsError {
				finalStatus = "failed"
				finalError = event.FinalText
				if finalError == "" {
					finalError = "droid completed with an error"
				}
				trySend(ch, Message{Type: MessageError, Content: finalError})
			}
		case "error":
			errText := droidErrorMessage(event)
			trySend(ch, Message{Type: MessageError, Content: errText})
			if finalStatus == "completed" {
				finalStatus = "failed"
				finalError = errText
			}
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		b.cfg.Logger.Warn("droid stdout scanner error", "error", scanErr)
		if finalStatus == "completed" {
			finalStatus = "failed"
			finalError = fmt.Sprintf("stdout read error: %v", scanErr)
		}
	}

	return eventResult{
		status:    finalStatus,
		errMsg:    finalError,
		output:    output.String(),
		sessionID: sessionID,
		model:     model,
		usage:     usage,
	}
}

func (b *droidBackend) handleDroidMessage(event droidEvent, ch chan<- Message, output *strings.Builder) {
	if event.Text == "" || event.Role == "user" {
		return
	}
	output.WriteString(event.Text)
	trySend(ch, Message{Type: MessageText, Content: event.Text})
}

func (b *droidBackend) handleDroidToolCall(event droidEvent, ch chan<- Message) {
	var input map[string]any
	if len(event.Parameters) > 0 {
		_ = json.Unmarshal(event.Parameters, &input)
	}
	trySend(ch, Message{
		Type:   MessageToolUse,
		Tool:   droidToolName(event),
		CallID: event.ID,
		Input:  input,
	})
}

func (b *droidBackend) handleDroidToolResult(event droidEvent, ch chan<- Message) {
	trySend(ch, Message{
		Type:   MessageToolResult,
		Tool:   droidToolName(event),
		CallID: event.ID,
		Output: decodeDroidValue(event.Value),
	})
}

func droidToolName(event droidEvent) string {
	if event.ToolName != "" {
		return event.ToolName
	}
	return event.ToolID
}

type droidEvent struct {
	Type       string          `json:"type"`
	Subtype    string          `json:"subtype,omitempty"`
	Role       string          `json:"role,omitempty"`
	ID         string          `json:"id,omitempty"`
	MessageID  string          `json:"messageId,omitempty"`
	Text       string          `json:"text,omitempty"`
	ToolID     string          `json:"toolId,omitempty"`
	ToolName   string          `json:"toolName,omitempty"`
	Parameters json.RawMessage `json:"parameters,omitempty"`
	Value      json.RawMessage `json:"value,omitempty"`
	FinalText  string          `json:"finalText,omitempty"`
	SessionID  string          `json:"session_id,omitempty"`
	Model      string          `json:"model,omitempty"`
	IsError    bool            `json:"isError,omitempty"`
	Message    json.RawMessage `json:"message,omitempty"`
	Error      json.RawMessage `json:"error,omitempty"`
	Usage      json.RawMessage `json:"usage,omitempty"`
}

func droidErrorMessage(event droidEvent) string {
	for _, raw := range []json.RawMessage{event.Message, event.Error} {
		if len(raw) == 0 {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil && s != "" {
			return s
		}
		var obj struct {
			Message string `json:"message"`
			Name    string `json:"name"`
		}
		if err := json.Unmarshal(raw, &obj); err == nil {
			if obj.Message != "" && obj.Name != "" {
				return obj.Name + ": " + obj.Message
			}
			if obj.Message != "" {
				return obj.Message
			}
			if obj.Name != "" {
				return obj.Name
			}
		}
		text := strings.TrimSpace(string(raw))
		if text != "" {
			return strings.Trim(text, `"`)
		}
	}
	return "droid emitted an error event"
}

func decodeDroidValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

func addDroidUsage(total TokenUsage, raw json.RawMessage) TokenUsage {
	if len(raw) == 0 {
		return total
	}
	var u struct {
		InputTokens          int64 `json:"inputTokens"`
		InputTokensSnake     int64 `json:"input_tokens"`
		Input                int64 `json:"input"`
		OutputTokens         int64 `json:"outputTokens"`
		OutputTokensSnake    int64 `json:"output_tokens"`
		Output               int64 `json:"output"`
		CacheReadTokens      int64 `json:"cacheReadTokens"`
		CacheReadTokensSnake int64 `json:"cache_read_tokens"`
		CacheReadInputTokens int64 `json:"cache_read_input_tokens"`
		CacheRead            int64 `json:"cacheRead"`
		CacheWriteTokens     int64 `json:"cacheWriteTokens"`
		CacheWriteTokensSnek int64 `json:"cache_write_tokens"`
		CacheCreationTokens  int64 `json:"cache_creation_input_tokens"`
		CacheWrite           int64 `json:"cacheWrite"`
	}
	if err := json.Unmarshal(raw, &u); err != nil {
		return total
	}
	total.InputTokens += firstNonZero(u.InputTokens, u.InputTokensSnake, u.Input)
	total.OutputTokens += firstNonZero(u.OutputTokens, u.OutputTokensSnake, u.Output)
	total.CacheReadTokens += firstNonZero(u.CacheReadTokens, u.CacheReadTokensSnake, u.CacheReadInputTokens, u.CacheRead)
	total.CacheWriteTokens += firstNonZero(u.CacheWriteTokens, u.CacheWriteTokensSnek, u.CacheCreationTokens, u.CacheWrite)
	return total
}

func firstNonZero(values ...int64) int64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}
