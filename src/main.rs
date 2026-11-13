//! agent-watcher: resident WebSocket listener for agent-gateway sessions.
//!
//! Watches the sessions listed in watch.json. For each session it holds a
//! WebSocket to the gateway and appends detection events to events.jsonl:
//!   - "idle":   live output stopped for `quiet_seconds` (waiting for input)
//!   - "exited": the session process exited (status event, or REST reconcile)
//!
//! Configuration is via CLI flags or environment variables (flag wins):
//!   --gateway-url / AGENT_GATEWAY_URL   (default http://localhost:3000)
//!   --dir                               base dir for watch/state/events files
//!   --watch-file / --state-file / --events-file
//!   --token / AGENT_GATEWAY_TOKEN
//!   --token-command / AGENT_GATEWAY_TOKEN_COMMAND  (shell command printing the token)
//!   --token-file / AGENT_GATEWAY_TOKEN_FILE (default ~/.config/agent-gateway/token)
//!
//! The gateway URL may be http/https; the WebSocket URL is derived from it.
//! https_proxy / HTTPS_PROXY (and http_proxy for ws) are honored both for
//! REST (via reqwest) and for the WebSocket (HTTP CONNECT).

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;

const UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36";
const DEFAULT_QUIET: u64 = 120;

#[derive(Clone)]
struct Config {
    gateway_url: String,
    watch_file: PathBuf,
    state_file: PathBuf,
    events_file: PathBuf,
    token: Option<String>,
    token_command: Option<String>,
    token_file: Option<PathBuf>,
}

impl Config {
    fn ws_base(&self) -> String {
        let u = self.gateway_url.trim_end_matches('/');
        if let Some(rest) = u.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = u.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            u.to_string()
        }
    }
    async fn bearer(&self) -> Option<String> {
        if let Some(t) = &self.token {
            if !t.trim().is_empty() {
                return Some(t.trim().to_string());
            }
        }
        if let Some(cmd) = &self.token_command {
            let out = tokio::process::Command::new("sh")
                .arg("-c").arg(cmd)
                .stdin(Stdio::null()).output().await.ok()?;
            let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
        if let Some(p) = &self.token_file {
            if let Ok(s) = tokio::fs::read_to_string(p).await {
                let s = s.trim().to_string();
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Deserialize)]
struct WatchSession {
    id: String,
    name: Option<String>,
    quiet_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WatchFile {
    sessions: Vec<WatchSession>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SessionState {
    #[serde(default)]
    idle_reported: bool,
    #[serde(default)]
    gone: bool,
}

type SharedState = Arc<Mutex<HashMap<String, SessionState>>>;

fn log(msg: &str) {
    println!("[watcher] {msg}");
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
}

async fn save_state(cfg: &Config, state: &SharedState) {
    let data = {
        let st = state.lock().await;
        serde_json::to_string_pretty(&*st).unwrap_or_else(|_| "{}".into())
    };
    let tmp = cfg.state_file.with_extension("json.tmp");
    if tokio::fs::write(&tmp, data).await.is_ok() {
        let _ = tokio::fs::rename(&tmp, &cfg.state_file).await;
    }
}

async fn rest_get(cfg: &Config, client: &reqwest::Client, path: &str) -> Result<Value, String> {
    let url = format!("{}{}", cfg.gateway_url.trim_end_matches('/'), path);
    let mut req = client.get(url).header("User-Agent", UA);
    if let Some(t) = cfg.bearer().await {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json::<Value>().await.map_err(|e| e.to_string())
}

async fn screen_tail(cfg: &Config, client: &reqwest::Client, agent_id: &str) -> String {
    match rest_get(cfg, client, &format!("/agents/{agent_id}/screen")).await {
        Ok(body) => {
            let text = body.get("screen").and_then(|v| v.as_str()).unwrap_or("");
            let lines: Vec<&str> = text.lines().collect();
            let start = lines.len().saturating_sub(28);
            lines[start..].join("\n")
        }
        Err(e) => format!("<screen unavailable: {e}>"),
    }
}

async fn emit(cfg: &Config, client: &reqwest::Client, session: &WatchSession, kind: &str) {
    let tail = if kind == "idle" || kind == "exited" {
        screen_tail(cfg, client, &session.id).await
    } else {
        String::new()
    };
    let evt = json!({
        "ts": now_iso(),
        "session_id": session.id,
        "session_name": session.name.clone().unwrap_or_else(|| session.id.clone()),
        "kind": kind,
        "screen_tail": tail,
    });
    let line = serde_json::to_string(&evt).unwrap_or_default();
    if let Ok(mut f) = tokio::fs::OpenOptions::new()
        .create(true).append(true).open(&cfg.events_file).await
    {
        let _ = f.write_all(line.as_bytes()).await;
        let _ = f.write_all(b"\n").await;
    }
    log(&format!("event: {kind} session={}", session.name.clone().unwrap_or_else(|| session.id.clone())));
}

// ---------------- WebSocket connect (with HTTP CONNECT proxy support) -----

enum WsStream {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl tokio::io::AsyncRead for WsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            WsStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            WsStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for WsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            WsStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            WsStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            WsStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            WsStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            WsStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            WsStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

fn env_proxy(scheme: &str) -> Option<String> {
    let keys: &[&str] = if scheme == "wss" || scheme == "https" {
        &["https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]
    } else {
        &["http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"]
    };
    keys.iter().find_map(|k| env::var(k).ok().filter(|v| !v.is_empty()))
}

async fn tcp_to_target(host: &str, port: u16, scheme: &str) -> Result<TcpStream, String> {
    if let Some(proxy) = env_proxy(scheme) {
        let purl = url::Url::parse(&proxy).map_err(|e| e.to_string())?;
        let phost = purl.host_str().ok_or("proxy host missing")?.to_string();
        let pport = purl.port().unwrap_or(8080);
        let mut stream = TcpStream::connect((phost.as_str(), pport))
            .await.map_err(|e| e.to_string())?;
        let req = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: keep-alive\r\n\r\n");
        stream.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?;
        // read headers until CRLFCRLF (CONNECT responses have no body)
        let mut buf = Vec::with_capacity(1024);
        let mut byte = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") && buf.len() < 16384 {
            let n = stream.read(&mut byte).await.map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("proxy closed during CONNECT".into());
            }
            buf.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&buf).to_string();
        if !head.contains(" 200") {
            return Err(format!("proxy CONNECT failed: {}", head.lines().next().unwrap_or("")));
        }
        Ok(stream)
    } else {
        TcpStream::connect((host, port)).await.map_err(|e| e.to_string())
    }
}

async fn connect_ws(
    cfg: &Config,
    session_id: &str,
) -> Result<tokio_tungstenite::WebSocketStream<WsStream>, String> {
    let ws_url = format!("{}/agents/{}/ws", cfg.ws_base(), session_id);
    let parsed = url::Url::parse(&ws_url).map_err(|e| e.to_string())?;
    let scheme = parsed.scheme().to_string();
    let host = parsed.host_str().ok_or("host missing")?.to_string();
    let port = parsed.port().unwrap_or(if scheme == "wss" { 443 } else { 80 });
    let tcp = tcp_to_target(&host, port, &scheme).await?;
    let stream = if scheme == "wss" {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        // Also trust the OS store (e.g. corporate / local MITM proxy CAs).
        let native = rustls_native_certs::load_native_certs();
        roots.add_parsable_certificates(native.certs);
        let tls_cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(tls_cfg));
        let server_name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|e| e.to_string())?;
        let tls = connector.connect(server_name, tcp).await.map_err(|e| e.to_string())?;
        WsStream::Tls(tls)
    } else {
        WsStream::Plain(tcp)
    };
    // Manual handshake: tungstenite's stock verifier rejects Cloudflare's
    // "Connection: upgrade, close" (a token list), which the Python watcher
    // accepted. Verify status 101 + Sec-WebSocket-Accept ourselves, with a
    // token-aware Connection check, then hand the stream to tungstenite.
    let path_and_query = parsed
        .path()
        .to_string()
        + parsed.query().map(|q| format!("?{q}")).as_deref().unwrap_or("");
    let mut key_bytes = [0u8; 16];
    {
        use std::io::Read as _;
        let mut f = std::fs::File::open("/dev/urandom").map_err(|e| e.to_string())?;
        f.read_exact(&mut key_bytes).map_err(|e| e.to_string())?;
    }
    use base64::Engine as _;
    let key = base64::engine::general_purpose::STANDARD.encode(key_bytes);
    let mut req = format!(
        "GET {path_and_query} HTTP/1.1\r\nHost: {host_header}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: {UA}\r\n",
        host_header = if parsed.port().is_some() {
            format!("{host}:{port}")
        } else {
            host.clone()
        }
    );
    if let Some(t) = cfg.bearer().await {
        req += &format!("Authorization: Bearer {t}\r\n");
    }
    req += "\r\n";
    let mut stream = stream;
    {
        use tokio::io::AsyncWriteExt as _;
        match &mut stream {
            WsStream::Plain(s) => s.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?,
            WsStream::Tls(s) => s.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?,
        }
    }
    // read response headers byte-by-byte (must not over-read into frames)
    let mut head_bytes: Vec<u8> = Vec::with_capacity(2048);
    {
        use tokio::io::AsyncReadExt as _;
        let mut byte = [0u8; 1];
        while !head_bytes.ends_with(b"\r\n\r\n") && head_bytes.len() < 32768 {
            let n = match &mut stream {
                WsStream::Plain(s) => s.read(&mut byte).await.map_err(|e| e.to_string())?,
                WsStream::Tls(s) => s.read(&mut byte).await.map_err(|e| e.to_string())?,
            };
            if n == 0 {
                return Err("connection closed during WS handshake".into());
            }
            head_bytes.push(byte[0]);
        }
    }
    let head = String::from_utf8_lossy(&head_bytes).to_string();
    let mut lines = head.lines();
    let status_line = lines.next().unwrap_or("");
    if !status_line.contains(" 101") {
        return Err(format!("WS handshake failed: {status_line}"));
    }
    let header_has_token = |name: &str, token: &str| -> bool {
        head.lines().skip(1).any(|l| {
            let mut it = l.splitn(2, ':');
            it.next().map(|n| n.trim().eq_ignore_ascii_case(name)).unwrap_or(false)
                && it.next().map(|v| {
                    v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token))
                }).unwrap_or(false)
        })
    };
    if !header_has_token("Upgrade", "websocket") || !header_has_token("Connection", "upgrade") {
        return Err("WS handshake: missing Upgrade/Connection tokens".into());
    }
    let accept_expected = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(key.as_bytes());
        h.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        base64::engine::general_purpose::STANDARD.encode(h.finalize())
    };
    let accept_ok = head.lines().skip(1).any(|l| {
        let mut it = l.splitn(2, ':');
        it.next().map(|n| n.trim().eq_ignore_ascii_case("Sec-WebSocket-Accept")).unwrap_or(false)
            && it.next().map(|v| v.trim() == accept_expected).unwrap_or(false)
    });
    if !accept_ok {
        return Err("WS handshake: Sec-WebSocket-Accept mismatch".into());
    }
    let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        stream,
        tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    Ok(ws)
}


// ---------------- Session watcher ------------------------------------------

struct SessionWatcher {
    cfg: Config,
    client: reqwest::Client,
    session: WatchSession,
    state: SharedState,
}

impl SessionWatcher {
    fn quiet(&self) -> u64 {
        self.session.quiet_seconds.unwrap_or(DEFAULT_QUIET)
    }
    async fn is_gone(&self) -> bool {
        self.state.lock().await.get(&self.session.id).map(|s| s.gone).unwrap_or(false)
    }
    async fn mark_gone(&self) {
        {
            let mut st = self.state.lock().await;
            st.entry(self.session.id.clone()).or_default().gone = true;
        }
        save_state(&self.cfg, &self.state).await;
    }
    async fn reconcile_status(&self) -> Option<String> {
        // Some("exited") / Some("missing") / Some("running") / None on error
        match rest_get(&self.cfg, &self.client, "/agents?status=all").await {
            Ok(Value::Array(agents)) => {
                match agents.iter().find(|a| a.get("id").and_then(|v| v.as_str()) == Some(&self.session.id)) {
                    None => Some("missing".into()),
                    Some(a) => Some(a.get("status").and_then(|v| v.as_str()).unwrap_or("running").to_string()),
                }
            }
            _ => None,
        }
    }

    async fn run(&self) {
        let mut backoff = 5u64;
        loop {
            if self.is_gone().await {
                return;
            }
            match self.connect_once().await {
                Ok(true) => return, // finished (exited)
                Ok(false) => {
                    backoff = 5;
                }
                Err(e) => {
                    log(&format!("session {}: connection error: {e}; retry in {backoff}s", self.session.id));
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                }
            }
        }
    }

    /// Ok(true) = watcher finished (session exited). Ok(false) = reconnect.
    async fn connect_once(&self) -> Result<bool, String> {
        // Reconcile first: does the session still exist / has it exited?
        if let Some(status) = self.reconcile_status().await {
            if status == "exited" {
                if !self.is_gone().await {
                    self.mark_gone().await;
                    emit(&self.cfg, &self.client, &self.session, "exited").await;
                }
                return Ok(true);
            }
        }
        log(&format!("session {}: connecting WS", self.session.id));
        let ws = connect_ws(&self.cfg, &self.session.id).await?;
        log(&format!("session {}: WS connected", self.session.id));
        self.pump(ws).await
    }

    async fn pump(
        &self,
        mut ws: tokio_tungstenite::WebSocketStream<WsStream>,
    ) -> Result<bool, String> {
        let sid = self.session.id.clone();
        let mut live = false;
        let connected_at = Instant::now();
        let mut last_io = connected_at;
        let mut last_reconcile = connected_at;
        let mut idle_reported = self
            .state.lock().await.get(&sid).map(|s| s.idle_reported).unwrap_or(false);
        loop {
            // Some(item) = stream produced something (None item = stream closed),
            // outer None = 5s tick with no message.
            let msg: Option<Option<Result<tungstenite::Message, tungstenite::Error>>> =
                tokio::select! {
                    m = ws.next() => Some(m),
                    _ = tokio::time::sleep(Duration::from_secs(5)) => None,
                };
            if let Some(m) = msg {
                match m {
                    None => return Ok(false), // stream closed: reconnect
                    Some(Ok(tungstenite::Message::Text(txt))) => {
                        if let Ok(v) = serde_json::from_str::<Value>(txt.as_str()) {
                            match v.get("type").and_then(|t| t.as_str()) {
                                Some("status") => {
                                    live = true;
                                    if v.get("status").and_then(|s| s.as_str()) == Some("exited") {
                                        if !self.is_gone().await {
                                            self.mark_gone().await;
                                            emit(&self.cfg, &self.client, &self.session, "exited").await;
                                        }
                                        return Ok(true);
                                    }
                                }
                                Some("io") if live => {
                                    last_io = Instant::now();
                                    if idle_reported {
                                        idle_reported = false;
                                        {
                                            let mut st = self.state.lock().await;
                                            st.entry(sid.clone()).or_default().idle_reported = false;
                                        }
                                        save_state(&self.cfg, &self.state).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.to_string()),
                }
            }
            // quiet check
            if live && !idle_reported && !self.is_gone().await
                && last_io.elapsed() >= Duration::from_secs(self.quiet())
            {
                idle_reported = true;
                {
                    let mut st = self.state.lock().await;
                    st.entry(sid.clone()).or_default().idle_reported = true;
                }
                save_state(&self.cfg, &self.state).await;
                emit(&self.cfg, &self.client, &self.session, "idle").await;
            }
            // periodic REST reconcile (WS status alone missed stop-via-API)
            if last_reconcile.elapsed() >= Duration::from_secs(30) {
                last_reconcile = Instant::now();
                if let Some(status) = self.reconcile_status().await {
                    if status == "exited" || status == "missing" {
                        if !self.is_gone().await {
                            self.mark_gone().await;
                            emit(&self.cfg, &self.client, &self.session, "exited").await;
                        }
                        return Ok(true);
                    }
                }
            }
        }
    }
}

// ---------------- main ------------------------------------------------------

fn parse_config() -> Config {
    let args: Vec<String> = env::args().collect();
    let get = |flag: &str| -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    };
    let dir = get("--dir").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    let env_or = |k: &str| env::var(k).ok().filter(|v| !v.is_empty());
    Config {
        gateway_url: get("--gateway-url")
            .or_else(|| env_or("AGENT_GATEWAY_URL"))
            .unwrap_or_else(|| "http://localhost:3000".into()),
        watch_file: get("--watch-file").map(PathBuf::from).unwrap_or_else(|| dir.join("watch.json")),
        state_file: get("--state-file").map(PathBuf::from).unwrap_or_else(|| dir.join("state.json")),
        events_file: get("--events-file").map(PathBuf::from).unwrap_or_else(|| dir.join("events.jsonl")),
        token: get("--token").or_else(|| env_or("AGENT_GATEWAY_TOKEN")),
        token_command: get("--token-command").or_else(|| env_or("AGENT_GATEWAY_TOKEN_COMMAND")),
        token_file: get("--token-file").map(PathBuf::from)
            .or_else(|| env_or("AGENT_GATEWAY_TOKEN_FILE").map(PathBuf::from))
            .or_else(|| dirs_home().map(|h| h.join(".config/agent-gateway/token"))),
    }
}

fn dirs_home() -> Option<PathBuf> {
    env::var("HOME").ok().map(PathBuf::from)
}

#[tokio::main]
async fn main() {
    let cfg = parse_config();
    let client = reqwest::Client::builder()
        .build()
        .expect("reqwest client");
    let state: SharedState = Arc::new(Mutex::new(
        tokio::fs::read_to_string(&cfg.state_file)
            .await
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
    ));
    log("watcher starting");
    let mut tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    loop {
        let watch: WatchFile = tokio::fs::read_to_string(&cfg.watch_file)
            .await
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(WatchFile { sessions: vec![] });
        let wanted: HashMap<String, WatchSession> = watch
            .sessions.into_iter().filter(|s| !s.id.is_empty())
            .map(|s| (s.id.clone(), s)).collect();
        for sid in tasks.keys().cloned().collect::<Vec<_>>() {
            if !wanted.contains_key(&sid) {
                tasks.remove(&sid).map(|t| t.abort());
                log(&format!("session {sid}: removed from watch list"));
            }
        }
        for (sid, session) in &wanted {
            let gone = state.lock().await.get(sid).map(|s| s.gone).unwrap_or(false);
            if gone {
                continue;
            }
            let needs = match tasks.get(sid) {
                None => true,
                Some(t) => t.is_finished(),
            };
            if needs {
                let watcher = SessionWatcher {
                    cfg: cfg.clone(),
                    client: client.clone(),
                    session: session.clone(),
                    state: state.clone(),
                };
                tasks.insert(sid.clone(), tokio::spawn(async move { watcher.run().await }));
            }
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}
