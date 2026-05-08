# Droid Provider Spec

## Goal

Add Factory Droid (`droid exec`) as a Multica agent provider with the same daemon execution, streaming, session resume, model selection, and context injection behavior as existing CLI-backed providers.

## Implementation

- Backend: `server/pkg/agent/droid.go`
- Invocation: `droid exec -o stream-json --auto high`
- Prompt delivery: stdin
- Workdir: `--cwd <workdir>` plus process `Dir`
- Model: `-m <model>` when `agent.model` or `MULTICA_DROID_MODEL` is configured
- Resume: `-s <session_id>` from the task prior session
- Stream parser: JSONL `system`, `message`, `tool_call`, `tool_result`, `completion`, and `error` events
- Context file: `AGENTS.md`
- Skill directory: `.factory/skills/<skill>/SKILL.md`
- Daemon discovery: `MULTICA_DROID_PATH` defaulting to `droid`, with optional `MULTICA_DROID_MODEL`

## Guardrails

The daemon filters custom args that would override protocol, session, prompt, workdir, or mission orchestration state: `-o`, `--output-format`, `--input-format`, `-s`, `--session-id`, `--fork`, `--cwd`, `-f`, `--file`, `--auto`, `--skip-permissions-unsafe`, `--mission`, worker/validator mission flags, and Droid worktree flags.

`MULTICA_DROID_ARGS` is intentionally not added; per-agent `custom_args` remains the extension point.

## Validation

- Unit: `cd server && go test ./pkg/agent ./internal/daemon ./internal/daemon/execenv`
- Fake CLI: argv construction, blocked args, stdin prompt delivery, JSONL parsing, final output, and session id handling
- Local smoke: call installed `droid` directly with the configured local credentials/API setup; `FACTORY_API_KEY` is not required by this spec because the owner confirmed another API setup is already configured
- Platform smoke: verify provider registration, task execution, stream messages, session resume, and custom arg filtering
