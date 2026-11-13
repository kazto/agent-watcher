# agent-watcher

A small resident watcher for [agent-gateway](https://github.com/kazto/agent-gateway).
It holds a WebSocket to each watched gateway session and appends detection
events to a JSON Lines file — no polling, no cost while nothing happens.

Events detected per session:

- **idle** — live output stopped for `quiet_seconds` (the agent is waiting
  for input). A session that is already quiet when the watcher (re)connects
  reports idle once — the catch-up path for work that finished while the
  watcher was down.
- **exited** — the session process exited. Detected via the WebSocket status
  event and backed up by a REST reconcile every 30 s (a stop via the API does
  not always produce a status event).

## Quick start

```bash
cargo build --release

mkdir -p ~/.config/agent-watcher
cp watch.json.example ~/.config/agent-watcher/watch.json   # edit session ids
./target/release/agent-watcher \
  --gateway-url http://localhost:3000 \
  --dir ~/.config/agent-watcher
```

`watch.json` lists the sessions to watch; it is re-read every 15 s, so you
can add or remove sessions without restarting:

```json
{"sessions": [{"id": "<session id>", "name": "my-agent", "quiet_seconds": 120}]}
```

Detected events are appended to `events.jsonl` in the same directory:

```json
{"ts":"2026-11-13T05:47:47+00:00","session_id":"...","session_name":"my-agent","kind":"idle","screen_tail":"...last screen lines..."}
```

`screen_tail` is the session's last 28 screen lines at detection time.
`state.json` remembers which sessions already reported idle / exited, so a
watcher restart does not re-report old events.

## Configuration

Flags win over environment variables.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--gateway-url` | `AGENT_GATEWAY_URL` | `http://localhost:3000` | gateway base URL (http/https) |
| `--dir` | — | `.` | directory holding watch.json / state.json / events.jsonl |
| `--watch-file` `--state-file` `--events-file` | — | under `--dir` | individual file overrides |
| `--token` | `AGENT_GATEWAY_TOKEN` | — | bearer token, given directly |
| `--token-command` | `AGENT_GATEWAY_TOKEN_COMMAND` | — | shell command whose stdout is the token (re-run per connection) |
| `--token-file` | `AGENT_GATEWAY_TOKEN_FILE` | `~/.config/agent-gateway/token` | file containing the token |

`https_proxy` / `HTTPS_PROXY` (and `http_proxy` for ws) are honored for both
REST and WebSocket connections (HTTP CONNECT). TLS roots are taken from both
the Mozilla set and the OS certificate store, so private proxy CAs work.

## systemd

`systemd/agent-watcher.service` is a user-service template; adjust the paths
and the gateway URL, then:

```bash
cargo install --path .
cp systemd/agent-watcher.service ~/.config/systemd/user/
systemctl --user enable --now agent-watcher
```

## Packaging

```bash
./scripts/package.sh   # -> dist/agent-watcher-<version>-<target>.tar.gz
```

The tarball contains the binary, this README, the LICENSE, the systemd unit
template, and `watch.json.example`.

## License

MIT — see [LICENSE](LICENSE).
