# msg-proxy

msg-proxy tunnels TCP traffic through a Telegram channel.
A client on your machine runs a local SOCKS5 proxy and posts each connection's traffic to the channel as messages, and a server elsewhere reads those messages, opens the real connections and posts the responses back.
Each side logs in as its own bot over Telegram's MTProto protocol, every TCP connection becomes a session with its own UUID, and the data travels as zstd-compressed, base64-encoded chunks inside JSON messages.

Each bot sends a data message at most once every three seconds, which stays within Telegram's limit of 20 messages per minute per bot per chat and carries up to 2900 bytes of traffic per message.
That puts throughput at roughly a kilobyte per second in each direction, enough for api calls, shell sessions and small files.

## Setup

Log in at [my.telegram.org](https://my.telegram.org), open API Development Tools and create an application.
Its api_id and api_hash are shared by the client and the server.

Create two bots with [@BotFather](https://t.me/BotFather), one for the client and one for the server, and keep both tokens.
Then create a channel and add both bots as administrators with permission to post messages.
The channel's chat id is a negative integer like -1001234567890, and forwarding a message from the channel to [@userinfobot](https://t.me/userinfobot) is one way to find it.

## Running

Building from source needs Go 1.26 or newer, and `make build` writes bin/server and bin/client.

Start the server on the machine that should make the real connections:

```sh
TELEGRAM_APP_ID={api-id} TELEGRAM_APP_HASH={api-hash} SERVER_TOKEN={server-bot-token} CHAT_ID={chat-id} ./bin/server
```

Then start the client on your own machine:

```sh
TELEGRAM_APP_ID={api-id} TELEGRAM_APP_HASH={api-hash} CLIENT_TOKEN={client-bot-token} CHAT_ID={chat-id} ./bin/client
```

Point your browser or any other SOCKS5-aware tool at the client's SOCKS5_ADDR.
A request through the proxy comes back with the server's ip:

```sh
curl --socks5 127.0.0.1:1080 https://httpbin.org/ip
```

## Configuration

| Variable               | Default          | Description                                           |
| ---------------------- | ---------------- | ----------------------------------------------------- |
| `TELEGRAM_APP_ID`      | required         | Integer app id from my.telegram.org                   |
| `TELEGRAM_APP_HASH`    | required         | App hash from my.telegram.org                         |
| `CLIENT_TOKEN`         | required, client | Token of the client bot                               |
| `SERVER_TOKEN`         | required, server | Token of the server bot                               |
| `CHAT_ID`              | required         | Chat id of the shared channel                         |
| `SOCKS5_ADDR`          | `127.0.0.1:1080` | Address the client's SOCKS5 proxy listens on          |
| `SESSION_IDLE_TIMEOUT` | `60s`            | Go duration after which an idle session is closed     |
| `LOG_LEVEL`            | `info`           | One of `debug`, `info`, `warn` or `error`             |

## Docker

Every release publishes ghcr.io/xsaveopt/msg-proxy/server and ghcr.io/xsaveopt/msg-proxy/client for linux/amd64.
A release like v1.2.3 is tagged 1.2.3, 1.2 and 1, and also latest unless it is a pre-release such as 1.2.3-rc1, while the dev tag is rebuilt from every commit to main.

```sh
docker run --rm \
  -e TELEGRAM_APP_ID={api-id} \
  -e TELEGRAM_APP_HASH={api-hash} \
  -e SERVER_TOKEN={server-bot-token} \
  -e CHAT_ID={chat-id} \
  ghcr.io/xsaveopt/msg-proxy/server:latest
```

With [ko](https://ko.build) installed, `make images` builds both images into the local Docker daemon.

## Development

`make test` runs the tests with the race detector, `make lint` runs go vet and `make clean` removes bin.
CI additionally runs golangci-lint with the settings in .golangci.yml.

## License

GPL-2.0, see LICENSE.
