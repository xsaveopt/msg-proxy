# msg-proxy

Tunnel TCP traffic through Telegram messages. A local SOCKS5 proxy encodes your browser's traffic as Telegram messages, a remote server decodes them and makes the real connections.

## How it works

- Two Telegram bots in a shared channel: the client bot carries client→server traffic, the server bot carries server→client
- Data is zstd-compressed and base64-encoded to fit in Telegram messages
- Each TCP connection becomes a session tracked by UUID
- Uses the Telegram MTProto protocol directly (not the Bot API) for lower latency

## Setup

### 1. Get Telegram API credentials

Go to [my.telegram.org](https://my.telegram.org), log in, click **API Development Tools**, and create an application. Save the `api_id` (an integer) and `api_hash` (a hex string). These are shared by both the client and server.

### 2. Create two Telegram bots

Talk to [@BotFather](https://t.me/BotFather), create two bots, save both tokens.

### 3. Create a Telegram channel and add both bots as admins

Create a channel and add both bots as administrators with permission to post messages. Get the channel's chat ID (a negative integer like `-1001234567890`). You can get it by forwarding a message from the channel to [@userinfobot](https://t.me/userinfobot).

### 4. Build

```bash
make build
# produces ./bin/server and ./bin/client
```

Requires Go 1.24+.

## Usage

**On the remote server** (the machine that makes the real internet connections):

```bash
TELEGRAM_APP_ID=<api-id> \
TELEGRAM_APP_HASH=<api-hash> \
SERVER_TOKEN=<server-bot-token> \
CHAT_ID=<channel-id> \
./bin/server
```

**On your local machine:**

```bash
TELEGRAM_APP_ID=<api-id> \
TELEGRAM_APP_HASH=<api-hash> \
CLIENT_TOKEN=<client-bot-token> \
CHAT_ID=<channel-id> \
./bin/client
```

Then configure your browser to use SOCKS5 proxy at `127.0.0.1:1080`.

**Verify it's working:**

```bash
curl --socks5 127.0.0.1:1080 http://httpbin.org/ip
# should show the server's IP, not yours
```

## Configuration

| Env var                | Default          | Description                                      |
| ---------------------- | ---------------- | ------------------------------------------------ |
| `TELEGRAM_APP_ID`      | required         | Integer app ID from my.telegram.org              |
| `TELEGRAM_APP_HASH`    | required         | Hex app hash from my.telegram.org                |
| `CLIENT_TOKEN`         | client only      | Token for the client-side bot                    |
| `SERVER_TOKEN`         | server only      | Token for the server-side bot                    |
| `CHAT_ID`              | required         | Channel ID (negative int, e.g. `-1001234567890`) |
| `SOCKS5_ADDR`          | `127.0.0.1:1080` | Local SOCKS5 listen address (client only)        |
| `SESSION_IDLE_TIMEOUT` | `60s`            | Kill idle sessions after this duration           |
| `LOG_LEVEL`            | `info`           | `debug`, `info`, `warn`, `error`                 |

## Docker images

Pre-built images are published to the GitHub Container Registry on every push to `main` and on version tags.

**Run the server via Docker:**

```bash
docker run --rm \
  -e TELEGRAM_APP_ID=<api-id> \
  -e TELEGRAM_APP_HASH=<api-hash> \
  -e SERVER_TOKEN=<server-bot-token> \
  -e CHAT_ID=<channel-id> \
  ghcr.io/<owner>/msg-proxy/server:latest
```

**Build images locally** (requires [ko](https://ko.build)):

```bash
make images          # loads into local Docker daemon
```

## Performance

Throughput is capped by [Telegram's bot rate limit](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this) of **20 messages per minute per bot per chat**. With the maximum safe payload size per message (~2900 bytes after zstd compression), the practical ceiling is around **850 bytes/second** downstream. Latency for connection establishment is ~150–200 ms (one Telegram round-trip for the handshake).

This makes msg-proxy suitable for low-bandwidth use cases (API calls, shell sessions, small file transfers) rather than streaming or large downloads.

## Development

```bash
make test   # run tests
make lint   # go vet
make clean  # remove binaries
```
