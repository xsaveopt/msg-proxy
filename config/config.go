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
  BOT_A_TOKEN   Telegram bot token for the A bot (client→server traffic)
  BOT_B_TOKEN   Telegram bot token for the B bot (server→client traffic)
  CHAT_ID       Telegram chat ID that both bots share

Optional environment variables:`)

	if role == "client" {
		fmt.Fprintln(os.Stderr, `  SOCKS5_ADDR           Local SOCKS5 listen address (default: 127.0.0.1:1080)`)
	}

	fmt.Fprintln(os.Stderr, `  SESSION_IDLE_TIMEOUT  Idle session timeout, e.g. 90s (default: 60s)
  LOG_LEVEL             debug | info | warn | error (default: info)

Example:
  export BOT_A_TOKEN=123456:ABC...
  export BOT_B_TOKEN=654321:XYZ...
  export CHAT_ID=-1001234567890`)

	if role == "client" {
		fmt.Fprintln(os.Stderr, `  ./client`)
	} else {
		fmt.Fprintln(os.Stderr, `  ./server`)
	}
}

type Config struct {
	BotAToken          string
	BotBToken          string
	ChatID             int64
	Socks5Addr         string
	SessionIdleTimeout time.Duration
	LogLevel           slog.Level
}

func Load() (*Config, error) {
	botA := os.Getenv("BOT_A_TOKEN")
	if botA == "" {
		return nil, fmt.Errorf("BOT_A_TOKEN is required")
	}

	botB := os.Getenv("BOT_B_TOKEN")
	if botB == "" {
		return nil, fmt.Errorf("BOT_B_TOKEN is required")
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
		BotAToken:          botA,
		BotBToken:          botB,
		ChatID:             chatID,
		Socks5Addr:         socks5Addr,
		SessionIdleTimeout: idleTimeout,
		LogLevel:           logLevel,
	}, nil
}
