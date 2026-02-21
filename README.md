# msg-proxy

Tunnel TCP traffic through Telegram messages. A local SOCKS5 proxy encodes your browser's traffic as Telegram messages, a remote server decodes them and makes the real connections.

## How it works

- Two Telegram bots in a shared group: Bot A carries client→server traffic, Bot B carries server→client
- Data is zstd-compressed and base64-encoded to fit in Telegram messages
- Each TCP connection becomes a session tracked by UUID

## Setup

### 1. Create two Telegram bots

Talk to [@BotFather](https://t.me/BotFather), create two bots, save both tokens.

### 2. Create a Telegram channel

Add both bots to a channel. Get the channel's chat ID (it's a negative integer like `-1001234567890`).
You can get it by adding [@userinfobot](https://t.me/userinfobot) to the channel temporarily.

### 3. Build

```bash
make build
# produces ./bin/server and ./bin/client
```

Requires Go 1.23+.

## Usage

**On the remote server** (the machine that makes the real internet connections):

```bash
BOT_A_TOKEN=<bot-a-token> \
BOT_B_TOKEN=<bot-b-token> \
CHAT_ID=<group-chat-id> \
./server
```

**On your local machine:**

```bash
BOT_A_TOKEN=<bot-a-token> \
BOT_B_TOKEN=<bot-b-token> \
CHAT_ID=<group-chat-id> \
./client
```

Then configure your browser to use SOCKS5 proxy at `127.0.0.1:1080`.

**Verify it's working:**

```bash
curl --socks5 127.0.0.1:1080 http://httpbin.org/ip
# should show the server's IP, not yours
```

## Configuration

| Env var                | Default          | Description                            |
| ---------------------- | ---------------- | -------------------------------------- |
| `BOT_A_TOKEN`          | required         | Client→server bot token                |
| `BOT_B_TOKEN`          | required         | Server→client bot token                |
| `CHAT_ID`              | required         | Shared group chat ID (negative int)    |
| `SOCKS5_ADDR`          | `127.0.0.1:1080` | Local SOCKS5 listen address            |
| `SESSION_IDLE_TIMEOUT` | `60s`            | Kill idle sessions after this duration |
| `LOG_LEVEL`            | `info`           | `debug`, `info`, `warn`, `error`       |

## Docker images

Pre-built images are published to the GitHub Container Registry on every push to `main` and on version tags

**Run the server via Docker:**

```bash
docker run --rm \
  -e BOT_A_TOKEN=<bot-a-token> \
  -e BOT_B_TOKEN=<bot-b-token> \
  -e CHAT_ID=<group-chat-id> \
  ghcr.io/<owner>/msg-proxy/server:latest
```

**Build images locally** (requires [ko](https://ko.build)):

```bash
make images          # loads into local Docker daemon
```

## Development

```bash
make test   # run tests
make lint   # go vet
make clean  # remove binaries
```
