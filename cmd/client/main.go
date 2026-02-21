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
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n\n", err)
		config.PrintHelp("client")
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	botA, err := transport.NewBot(cfg.BotAToken, cfg.ChatID, logger)
	if err != nil {
		log.Fatalf("BotA init: %v", err)
	}
	defer botA.Stop()

	botB, err := transport.NewBot(cfg.BotBToken, cfg.ChatID, logger)
	if err != nil {
		log.Fatalf("BotB init: %v", err)
	}
	defer botB.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stats.Global.LogPeriodically(ctx, logger, 30*time.Second)

	logger.Info("client starting", "socks5", cfg.Socks5Addr)
	proxy := client.New(botA, botB, logger)
	if err := proxy.Run(ctx, cfg.Socks5Addr, cfg.SessionIdleTimeout); err != nil {
		log.Fatalf("proxy: %v", err)
	}
}
