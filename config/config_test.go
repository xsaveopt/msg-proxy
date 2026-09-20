package config

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func setEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	base := map[string]string{
		"TELEGRAM_APP_ID":      "12345",
		"TELEGRAM_APP_HASH":    "deadbeef",
		"CLIENT_TOKEN":         "client-token",
		"SERVER_TOKEN":         "server-token",
		"CHAT_ID":              "-1001234567890",
		"SOCKS5_ADDR":          "",
		"SESSION_IDLE_TIMEOUT": "",
		"LOG_LEVEL":            "",
	}
	for k, v := range overrides {
		base[k] = v
	}
	for k, v := range base {
		t.Setenv(k, v)
	}
}

func TestLoadClientDefaults(t *testing.T) {
	setEnv(t, nil)

	cfg, err := Load("client")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AppID != 12345 {
		t.Errorf("AppID: got %d, want 12345", cfg.AppID)
	}
	if cfg.AppHash != "deadbeef" {
		t.Errorf("AppHash: got %q, want %q", cfg.AppHash, "deadbeef")
	}
	if cfg.BotToken != "client-token" {
		t.Errorf("BotToken: got %q, want %q", cfg.BotToken, "client-token")
	}
	if cfg.ChatID != -1001234567890 {
		t.Errorf("ChatID: got %d, want -1001234567890", cfg.ChatID)
	}
	if cfg.Socks5Addr != "127.0.0.1:1080" {
		t.Errorf("Socks5Addr: got %q, want %q", cfg.Socks5Addr, "127.0.0.1:1080")
	}
	if cfg.SessionIdleTimeout != 60*time.Second {
		t.Errorf("SessionIdleTimeout: got %v, want 60s", cfg.SessionIdleTimeout)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel: got %v, want %v", cfg.LogLevel, slog.LevelInfo)
	}
}

func TestLoadServerUsesServerToken(t *testing.T) {
	setEnv(t, nil)

	cfg, err := Load("server")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BotToken != "server-token" {
		t.Errorf("BotToken: got %q, want %q", cfg.BotToken, "server-token")
	}
}

func TestLoadOptionalOverrides(t *testing.T) {
	setEnv(t, map[string]string{
		"SOCKS5_ADDR":          "0.0.0.0:9050",
		"SESSION_IDLE_TIMEOUT": "90s",
		"LOG_LEVEL":            "debug",
	})

	cfg, err := Load("client")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Socks5Addr != "0.0.0.0:9050" {
		t.Errorf("Socks5Addr: got %q, want %q", cfg.Socks5Addr, "0.0.0.0:9050")
	}
	if cfg.SessionIdleTimeout != 90*time.Second {
		t.Errorf("SessionIdleTimeout: got %v, want 90s", cfg.SessionIdleTimeout)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel: got %v, want %v", cfg.LogLevel, slog.LevelDebug)
	}
}

func TestLoadLogLevels(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"INFO":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			setEnv(t, map[string]string{"LOG_LEVEL": in})
			cfg, err := Load("client")
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.LogLevel != want {
				t.Errorf("LogLevel: got %v, want %v", cfg.LogLevel, want)
			}
		})
	}
}

func TestLoadErrors(t *testing.T) {
	cases := []struct {
		name      string
		role      string
		overrides map[string]string
		wantErr   string
	}{
		{"missing app id", "client", map[string]string{"TELEGRAM_APP_ID": ""}, "TELEGRAM_APP_ID is required"},
		{"bad app id", "client", map[string]string{"TELEGRAM_APP_ID": "abc"}, "TELEGRAM_APP_ID must be an integer"},
		{"missing app hash", "client", map[string]string{"TELEGRAM_APP_HASH": ""}, "TELEGRAM_APP_HASH is required"},
		{"unknown role", "worker", nil, `unknown role "worker"`},
		{"missing client token", "client", map[string]string{"CLIENT_TOKEN": ""}, "CLIENT_TOKEN is required"},
		{"missing server token", "server", map[string]string{"SERVER_TOKEN": ""}, "SERVER_TOKEN is required"},
		{"missing chat id", "client", map[string]string{"CHAT_ID": ""}, "CHAT_ID is required"},
		{"bad chat id", "client", map[string]string{"CHAT_ID": "not-a-number"}, "CHAT_ID must be an integer"},
		{"bad idle timeout", "client", map[string]string{"SESSION_IDLE_TIMEOUT": "ages"}, "SESSION_IDLE_TIMEOUT must be a duration"},
		{"bad log level", "client", map[string]string{"LOG_LEVEL": "loud"}, "LOG_LEVEL invalid"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.overrides)
			cfg, err := Load(tc.role)
			if err == nil {
				t.Fatalf("expected error, got config %+v", cfg)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error: got %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestPrintHelp(t *testing.T) {
	client := captureStderr(t, func() { PrintHelp("client") })
	if !strings.Contains(client, "SOCKS5_ADDR") {
		t.Error("client help should mention SOCKS5_ADDR")
	}
	if !strings.Contains(client, "./client") {
		t.Error("client help should show the client example")
	}

	server := captureStderr(t, func() { PrintHelp("server") })
	if strings.Contains(server, "SOCKS5_ADDR") {
		t.Error("server help should not mention SOCKS5_ADDR")
	}
	if !strings.Contains(server, "./server") {
		t.Error("server help should show the server example")
	}
	for _, want := range []string{"TELEGRAM_APP_ID", "TELEGRAM_APP_HASH", "CHAT_ID", "LOG_LEVEL"} {
		if !strings.Contains(server, want) {
			t.Errorf("help should mention %s", want)
		}
	}
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	orig := os.Stderr
	os.Stderr = w

	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		_, _ = io.Copy(&sb, r)
		done <- sb.String()
	}()

	fn()

	_ = w.Close()
	os.Stderr = orig
	out := <-done
	_ = r.Close()
	return out
}
