//! # solari-sdk
//!
//! Rust binding for the Solari sandbox SDK (core surface). Talks the same wire
//! protocol as the reference `@solarisdk/core` TypeScript package:
//!
//! - **REST** to the gateway for session lifecycle (create/connect/get/kill) and
//!   the one-shot `/exec` fast path.
//! - **Control WebSocket** (newline-delimited JSON-RPC) for everything a live
//!   session does: `commands`, `files`, `code.run`, and the client-side `git`
//!   namespace.
//!
//! ```no_run
//! use solari::{Client, ClientOptions, CreateOptions, RunOptions};
//!
//! # async fn ex() -> Result<(), solari::SolariError> {
//! let client = Client::new(ClientOptions::new("slr_live_…", "https://gw.example.com"))?;
//! let sbx = client.create(CreateOptions { template: Some("base".into()), ..Default::default() }).await?;
//! let out = sbx.commands().run("echo", RunOptions::new().args(["hello"])).await?;
//! println!("exit={} stdout={}", out.exit_code, out.stdout);
//! let status = sbx.git().status(Some("/repo")).await?;
//! println!("branch={} clean={}", status.branch, status.clean);
//! # Ok(()) }
//! ```

mod channel;
mod client;
mod code;
mod commands;
mod error;
mod files;
mod git;
mod http;
mod types;

pub use channel::{AsyncFrame, ControlChannel};
pub use client::{Client, ClientOptions, CreateOptions, Sandbox};
pub use code::{Code, RunCodeOptions};
pub use commands::{CommandHandle, Commands, OutputCallback, RunOptions};
pub use error::SolariError;
pub use files::Files;
pub use git::{
    Git, GitCloneOptions, GitCommitOptions, GitLogOptions, GitRemoteOptions, GitRunner,
};
pub use http::{new_idempotency_key, HttpOptions, HttpTransport};
pub use types::{
    Chart, ChartAxis, ChartType, CodeResultItem, CommandResult, CreateSandboxRequest,
    CreateSandboxResponse, FsEntry, FsStat, GatewayErrorBody, GitBranch, GitCommit, GitStatus,
    Lifecycle, ResumeResponse, RunCodeResult, SandboxView, VolumeAttachment,
};

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// commands().run happy path over an in-process mock control-WS that scripts
    /// start-reply → cmd.data → cmd.exit.
    #[tokio::test]
    async fn commands_run_over_mock_ws() {
        // Mock control-WS server.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Read the cmd.start request frame.
            let msg = ws.next().await.unwrap().unwrap();
            let text = msg.into_text().unwrap();
            let req: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
            assert_eq!(req.get("method").unwrap(), "cmd.start");
            let id = req.get("id").unwrap().as_str().unwrap().to_string();
            let hello = base64::engine::general_purpose::STANDARD.encode("hello\n");
            let world = base64::engine::general_purpose::STANDARD.encode("oops");
            // start reply, then data (stdout+stderr), then exit.
            let frames = vec![
                format!("{{\"id\":\"{id}\",\"ok\":true,\"result\":{{\"cmdId\":\"c1\"}}}}"),
                format!("{{\"type\":\"cmd.data\",\"cmdId\":\"c1\",\"stream\":\"stdout\",\"base64\":\"{hello}\"}}"),
                format!("{{\"type\":\"cmd.data\",\"cmdId\":\"c1\",\"stream\":\"stderr\",\"base64\":\"{world}\"}}"),
                "{\"type\":\"cmd.exit\",\"cmdId\":\"c1\",\"exitCode\":3}".to_string(),
            ];
            for f in frames {
                ws.send(tokio_tungstenite::tungstenite::Message::Text(
                    format!("{f}\n").into(),
                ))
                .await
                .unwrap();
            }
            // keep the socket open briefly so frames flush.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        // Build a sandbox pointed at the mock WS. base_url is unused because we
        // pre-connect the channel (fast path is skipped when connected).
        let resp = CreateSandboxResponse {
            sandbox_id: "sb".into(),
            kind: "sandbox".into(),
            control_url: format!("ws://127.0.0.1:{ws_port}/control/sb"),
            expires_at: "".into(),
            stream_url: None,
        };
        let sbx = Sandbox::from_response(
            Arc::new(HttpTransport::new(HttpOptions::new("k", "http://127.0.0.1:1")).unwrap()),
            resp,
            Some(5000),
        );
        sbx.connect().await.unwrap();

        let out = sbx.commands().run("echo", RunOptions::new().args(["hello"])).await.unwrap();
        assert_eq!(out.exit_code, 3);
        assert_eq!(out.stdout, "hello\n");
        assert_eq!(out.stderr, "oops");
    }

    /// files().list / files().stat must parse the guest wire shape exactly:
    /// the directory flag is "dir" (not "isDir") and the stat timestamp is
    /// "modTimeMs". Regression guard for the wire-field bug.
    #[tokio::test]
    async fn files_list_and_stat_parse_wire_shape() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Answer each RPC by echoing its id and switching on its method.
            for _ in 0..2 {
                let msg = ws.next().await.unwrap().unwrap();
                let text = msg.into_text().unwrap();
                let req: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
                let id = req.get("id").unwrap().as_str().unwrap().to_string();
                let method = req.get("method").unwrap().as_str().unwrap();
                let result = match method {
                    "fs.list" => "{\"entries\":[{\"name\":\"a.txt\",\"dir\":false,\"size\":12},{\"name\":\"sub\",\"dir\":true,\"size\":4096}]}".to_string(),
                    "fs.stat" => "{\"name\":\"a.txt\",\"dir\":false,\"size\":12,\"mode\":420,\"modTimeMs\":1720000000000}".to_string(),
                    _ => "{}".to_string(),
                };
                ws.send(tokio_tungstenite::tungstenite::Message::Text(
                    format!("{{\"id\":\"{id}\",\"ok\":true,\"result\":{result}}}\n").into(),
                ))
                .await
                .unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let resp = CreateSandboxResponse {
            sandbox_id: "sb".into(),
            kind: "sandbox".into(),
            control_url: format!("ws://127.0.0.1:{ws_port}/control/sb"),
            expires_at: "".into(),
            stream_url: None,
        };
        let sbx = Sandbox::from_response(
            Arc::new(HttpTransport::new(HttpOptions::new("k", "http://127.0.0.1:1")).unwrap()),
            resp,
            Some(5000),
        );
        sbx.connect().await.unwrap();

        let entries = sbx.files().list("/work").await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "a.txt");
        assert!(!entries[0].dir);
        assert_eq!(entries[0].size, 12);
        assert_eq!(entries[1].name, "sub");
        assert!(entries[1].dir);
        assert_eq!(entries[1].size, 4096);

        let st = sbx.files().stat("/work/a.txt").await.unwrap();
        assert_eq!(st.name, "a.txt");
        assert!(!st.dir);
        assert_eq!(st.size, 12);
        assert_eq!(st.mode, 420);
        assert_eq!(st.mod_time_ms, 1720000000000);
    }

    /// REST create request shape: method/path, headers (Authorization,
    /// Idempotency-Key, Accept, Content-Type), and body (template + kind).
    #[tokio::test]
    async fn create_rest_request_shape() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Read until end of headers, then read the Content-Length body.
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            loop {
                let n = sock.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                    let clen = content_length(&head).unwrap_or(0);
                    let body_start = pos + 4;
                    while buf.len() < body_start + clen {
                        let n = sock.read(&mut tmp).await.unwrap();
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    *cap.lock().unwrap() = Some(String::from_utf8_lossy(&buf).to_string());
                    break;
                }
            }
            let body = r#"{"sandboxId":"sbx_1","kind":"sandbox","controlUrl":"ws://x/control/sbx_1","expiresAt":"2026-01-01T00:00:00Z"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });

        let client =
            Client::new(ClientOptions::new("secret-key", format!("http://127.0.0.1:{port}"))).unwrap();
        let sbx = client
            .create(CreateOptions {
                template: Some("base".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(sbx.id(), "sbx_1");
        assert_eq!(sbx.control_url(), "ws://x/control/sbx_1");

        let raw = captured.lock().unwrap().clone().unwrap();
        let (head, body) = raw.split_once("\r\n\r\n").unwrap();
        // Request line + headers.
        assert!(head.starts_with("POST /sandboxes "), "head: {head}");
        let lower = head.to_lowercase();
        assert!(lower.contains("authorization: bearer secret-key"), "head: {head}");
        assert!(lower.contains("accept: application/json"));
        assert!(lower.contains("content-type: application/json"));
        assert!(lower.contains("idempotency-key: "), "missing idempotency key: {head}");
        // Body.
        let json: serde_json::Value = serde_json::from_str(body.trim()).unwrap();
        assert_eq!(json.get("template").unwrap(), "base");
        assert_eq!(json.get("kind").unwrap(), "sandbox");
        // Unset fields are omitted.
        assert!(json.get("cpu").is_none());
        assert!(json.get("fromSnapshot").is_none());
        // A headless sandbox has no display: the gateway sends no streamUrl.
        assert_eq!(sbx.kind(), "sandbox");
        assert_eq!(sbx.stream_url(), None);
    }

    /// `create_desktop` sends `kind:"desktop"` and surfaces the returned
    /// streamUrl on the handle.
    #[tokio::test]
    async fn create_desktop_sends_kind_and_surfaces_stream_url() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            loop {
                let n = sock.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                    let clen = content_length(&head).unwrap_or(0);
                    let body_start = pos + 4;
                    while buf.len() < body_start + clen {
                        let n = sock.read(&mut tmp).await.unwrap();
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    *cap.lock().unwrap() = Some(String::from_utf8_lossy(&buf).to_string());
                    break;
                }
            }
            // Mirrors the gateway: streamUrl is present for kind:"desktop".
            let body = r#"{"sandboxId":"sbx_d","kind":"desktop","controlUrl":"ws://x/control/sbx_d","expiresAt":"2026-01-01T00:00:00Z","streamUrl":"ws://x/stream/sbx_d"}"#;
            let resp = format!(
                "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });

        let client =
            Client::new(ClientOptions::new("secret-key", format!("http://127.0.0.1:{port}"))).unwrap();
        let sbx = client
            .create_desktop(CreateOptions {
                template: Some("default".into()),
                cpu: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(sbx.id(), "sbx_d");
        assert_eq!(sbx.kind(), "desktop");
        assert_eq!(sbx.stream_url(), Some("ws://x/stream/sbx_d"));
        assert_eq!(sbx.control_url(), "ws://x/control/sbx_d");

        let raw = captured.lock().unwrap().clone().unwrap();
        let (head, body) = raw.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("POST /sandboxes "), "head: {head}");
        let json: serde_json::Value = serde_json::from_str(body.trim()).unwrap();
        // The whole point: the gateway keys desktop-ness off this field.
        assert_eq!(json.get("kind").unwrap(), "desktop");
        assert_eq!(json.get("template").unwrap(), "default");
        assert_eq!(json.get("cpu").unwrap(), 2);
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    fn content_length(head: &str) -> Option<usize> {
        for line in head.lines() {
            if let Some(v) = line.to_lowercase().strip_prefix("content-length:") {
                return v.trim().parse().ok();
            }
        }
        None
    }
}
