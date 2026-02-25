package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

func PrintHelp(role string) {
	fmt.Fprintln(os.Stderr, `msg-proxy — tunnel TCP through Telegram messages

Required environment variables:
  TELEGRAM_APP_ID    Telegram application ID (from https://my.telegram.org)
  TELEGRAM_APP_HASH  Telegram application hash (from https://my.telegram.org)
  CLIENT_TOKEN       Telegram bot token for the client-side bot
  SERVER_TOKEN       Telegram bot token for the server-side bot
  CHAT_ID            Telegram channel ID that both bots share (e.g. -1001234567890)

Optional environment variables:`)

	if role == "client" {
		fmt.Fprintln(os.Stderr, `  SOCKS5_ADDR           Local SOCKS5 listen address (default: 127.0.0.1:1080)`)
	}

	fmt.Fprintln(os.Stderr, `  SESSION_IDLE_TIMEOUT  Idle session timeout, e.g. 90s (default: 60s)
  LOG_LEVEL             debug | info | warn | error (default: info)

Example:
  export CLIENT_TOKEN=123456:ABC...
  export SERVER_TOKEN=654321:XYZ...
  export CHAT_ID=-1001234567890`)

	if role == "client" {
		fmt.Fprintln(os.Stderr, `  ./client`)
	} else {
		fmt.Fprintln(os.Stderr, `  ./server`)
	}
}

type Config struct {
	AppID              int
	AppHash            string
	BotToken           string
	ChatID             int64
	Socks5Addr         string
	SessionIdleTimeout time.Duration
	LogLevel           slog.Level
}

func Load(role string) (*Config, error) {
	appIDStr := os.Getenv("TELEGRAM_APP_ID")
	if appIDStr == "" {
		return nil, fmt.Errorf("TELEGRAM_APP_ID is required")
	}
	appIDVal, err := strconv.Atoi(appIDStr)
	if err != nil {
		return nil, fmt.Errorf("TELEGRAM_APP_ID must be an integer: %w", err)
	}

	appHash := os.Getenv("TELEGRAM_APP_HASH")
	if appHash == "" {
		return nil, fmt.Errorf("TELEGRAM_APP_HASH is required")
	}

	var tokenEnv string
	switch role {
	case "client":
		tokenEnv = "CLIENT_TOKEN"
	case "server":
		tokenEnv = "SERVER_TOKEN"
	default:
		return nil, fmt.Errorf("unknown role %q", role)
	}
	botToken := os.Getenv(tokenEnv)
	if botToken == "" {
		return nil, fmt.Errorf("%s is required", tokenEnv)
	}

	chatIDStr := os.Getenv("CHAT_ID")
	if chatIDStr == "" {
		return nil, fmt.Errorf("CHAT_ID is required")
	}
	chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("CHAT_ID must be an integer: %w", err)
	}

	socks5Addr := os.Getenv("SOCKS5_ADDR")
	if socks5Addr == "" {
		socks5Addr = "127.0.0.1:1080"
	}

	idleTimeout := 60 * time.Second
	if s := os.Getenv("SESSION_IDLE_TIMEOUT"); s != "" {
		idleTimeout, err = time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("SESSION_IDLE_TIMEOUT must be a duration: %w", err)
		}
	}

	logLevel := slog.LevelInfo
	if s := os.Getenv("LOG_LEVEL"); s != "" {
		if err := logLevel.UnmarshalText([]byte(s)); err != nil {
			return nil, fmt.Errorf("LOG_LEVEL invalid: %w", err)
		}
	}

	return &Config{
		AppID:              appIDVal,
		AppHash:            appHash,
		BotToken:           botToken,
		ChatID:             chatID,
		Socks5Addr:         socks5Addr,
		SessionIdleTimeout: idleTimeout,
		LogLevel:           logLevel,
	}, nil
}
