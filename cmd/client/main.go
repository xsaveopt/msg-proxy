package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"msg-proxy/config"
	"msg-proxy/internal/client"
	"msg-proxy/internal/stats"
	"msg-proxy/internal/transport"
)

func main() {
	cfg, err := config.Load("client")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n\n", err)
		config.PrintHelp("client")
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	bot, err := transport.NewBot(cfg.BotToken, cfg.AppID, cfg.AppHash, cfg.ChatID, logger)
	if err != nil {
		log.Fatalf("bot init: %v", err)
	}
	defer bot.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stats.Global.LogPeriodically(ctx, logger, 30*time.Second)

	logger.Info("client starting", "socks5", cfg.Socks5Addr)
	proxy := client.New(bot, logger)
	if err := proxy.Run(ctx, cfg.Socks5Addr, cfg.SessionIdleTimeout); err != nil {
		log.Fatalf("proxy: %v", err)
	}
}
