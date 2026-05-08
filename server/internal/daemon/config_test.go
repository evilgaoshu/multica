package daemon

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestPatternsFromEnv_DefaultsWhenUnset(t *testing.T) {
	t.Setenv("MULTICA_GC_ARTIFACT_PATTERNS", "")
	defaults := []string{"node_modules", ".next", ".turbo"}
	got := patternsFromEnv("MULTICA_GC_ARTIFACT_PATTERNS", defaults)
	if !reflect.DeepEqual(got, defaults) {
		t.Fatalf("expected defaults %v, got %v", defaults, got)
	}
	// Ensure callers get a copy, not a shared backing array.
	got[0] = "mutated"
	if defaults[0] == "mutated" {
		t.Fatal("patternsFromEnv must not return a slice aliased with defaults")
	}
}

func TestPatternsFromEnv_DropsSeparatorBearingEntries(t *testing.T) {
	t.Setenv("MULTICA_GC_ARTIFACT_PATTERNS", "node_modules, .next ,foo/bar, ../etc, ,target")
	got := patternsFromEnv("MULTICA_GC_ARTIFACT_PATTERNS", nil)
	want := []string{"node_modules", ".next", "target"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestLoadConfigDetectsDroidFromEnvPath(t *testing.T) {
	fakeDroid := writeFakeDaemonCLI(t, t.TempDir(), "droid")

	for _, name := range []string{
		"MULTICA_CLAUDE_PATH",
		"MULTICA_CODEX_PATH",
		"MULTICA_OPENCODE_PATH",
		"MULTICA_OPENCLAW_PATH",
		"MULTICA_HERMES_PATH",
		"MULTICA_GEMINI_PATH",
		"MULTICA_PI_PATH",
		"MULTICA_CURSOR_PATH",
		"MULTICA_COPILOT_PATH",
		"MULTICA_KIMI_PATH",
		"MULTICA_KIRO_PATH",
	} {
		t.Setenv(name, filepath.Join(t.TempDir(), "missing"))
	}
	t.Setenv("MULTICA_DROID_PATH", fakeDroid)
	t.Setenv("MULTICA_DROID_MODEL", "custom:DeepSeek-V4-Pro-0")

	cfg, err := LoadConfig(Overrides{
		DaemonID:       "test-daemon",
		WorkspacesRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	entry, ok := cfg.Agents["droid"]
	if !ok {
		t.Fatalf("expected droid agent to be detected, got %#v", cfg.Agents)
	}
	if entry.Path != fakeDroid {
		t.Fatalf("droid path = %q, want %q", entry.Path, fakeDroid)
	}
	if entry.Model != "custom:DeepSeek-V4-Pro-0" {
		t.Fatalf("droid model = %q", entry.Model)
	}
}

func writeFakeDaemonCLI(t *testing.T, dir, name string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(dir, name)
	data := []byte("#!/bin/sh\nexit 0\n")
	if runtime.GOOS == "windows" {
		data = []byte("")
	}
	if err := os.WriteFile(path, data, 0o755); err != nil {
		t.Fatalf("write fake CLI: %v", err)
	}
	return path
}
