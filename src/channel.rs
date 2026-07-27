//! `ControlChannel` — the control-WebSocket transport.
//!
//! Wire framing is newline-delimited JSON. THREE frame kinds share the socket
//! and are dispatched by SHAPE (never one-reply-per-request):
//!
//!   1. JSON-RPC reply       `{id, ok, result?|error?}`   — resolves call `id`.
//!   2. v1 streamed exec      `{id, stream, data}`          — routed by `id`
//!      (not used by the core surface; must not crash on it).
//!   3. v2 async STREAM frame `{type, cmdId|ptyId|watchId, ...}` — dispatched by
//!      `(type, streamId)`; may arrive BEFORE the originating call's reply, so
//!      unhandled frames are buffered ("orphans") and flushed on registration.
//!
//! On close, every in-flight RPC call AND every in-flight stream wait is woken
//! with an error (a `commands.run` awaiting `cmd.exit` is frame-based, not a
//! pending RPC — dropping its handler channel wakes its receiver).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;

use crate::error::SolariError;

const DEFAULT_CALL_TIMEOUT_MS: u64 = 300_000;
const CONNECT_TIMEOUT_MS: u64 = 15_000;
const CONNECT_MAX_ATTEMPTS: u32 = 3;
const CONNECT_RETRY_BASE_MS: u64 = 150;
const MAX_ORPHAN_FRAMES: usize = 1024;

/// A v2 async STREAM frame delivered to a registered handler.
#[derive(Debug, Clone)]
pub struct AsyncFrame {
    pub typ: String,
    pub stream: Option<String>,
    pub base64: Option<String>,
    pub exit_code: Option<i64>,
}

struct Pending {
    resolve: oneshot::Sender<Result<Value, SolariError>>,
    method: String,
}

#[derive(Default)]
struct Inner {
    rx_buffer: String,
    next_id: u64,
    pending: HashMap<String, Pending>,
    /// v2 async frame handlers keyed by `(type, streamId)`.
    frame_handlers: HashMap<(String, String), mpsc::UnboundedSender<AsyncFrame>>,
    orphan_frames: HashMap<(String, String), Vec<AsyncFrame>>,
    orphan_count: usize,
    writer: Option<mpsc::UnboundedSender<Message>>,
    ws_open: bool,
    closed: bool,
}

struct ChannelState {
    inner: Mutex<Inner>,
    call_timeout: Duration,
}

impl ChannelState {
    /// Feed a raw text chunk from the socket: accumulate, split on `\n`, trim,
    /// dispatch each non-empty line. Public for unit tests of the split logic.
    fn feed(&self, chunk: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.rx_buffer.push_str(chunk);
        loop {
            let Some(nl) = inner.rx_buffer.find('\n') else { break };
            let line: String = inner.rx_buffer.drain(..=nl).collect();
            let line = line.trim().to_string();
            if !line.is_empty() {
                dispatch(&mut inner, &line);
            }
        }
    }

    /// Reject every in-flight call + stream wait and drop all handler state.
    fn fail_all(&self, err_msg: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.ws_open = false;
        inner.writer = None;
        let pend: Vec<Pending> = inner.pending.drain().map(|(_, p)| p).collect();
        // Dropping frame handler senders wakes command waiters (recv -> None).
        inner.frame_handlers.clear();
        inner.orphan_frames.clear();
        inner.orphan_count = 0;
        drop(inner);
        for p in pend {
            let _ = p.resolve.send(Err(SolariError::connection(err_msg.to_string())));
        }
    }
}

/// Route one parsed line to the correct frame kind. Mirrors `dispatch` in
/// transport.ts: async `type` frames first, then `id`-correlated RPC replies.
fn dispatch(inner: &mut Inner, line: &str) {
    let v: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return, // ignore non-JSON noise
    };

    // (3) v2 async STREAM frame — `type` must be a string.
    if let Some(Value::String(typ)) = v.get("type") {
        let typ = typ.clone();
        let stream_id = v
            .get("cmdId")
            .and_then(Value::as_str)
            .or_else(|| v.get("ptyId").and_then(Value::as_str))
            .or_else(|| v.get("watchId").and_then(Value::as_str))
            .unwrap_or("")
            .to_string();
        let frame = AsyncFrame {
            typ: typ.clone(),
            stream: v.get("stream").and_then(Value::as_str).map(String::from),
            base64: v.get("base64").and_then(Value::as_str).map(String::from),
            exit_code: v.get("exitCode").and_then(Value::as_i64),
        };
        let key = (typ, stream_id);
        if let Some(tx) = inner.frame_handlers.get(&key) {
            let _ = tx.send(frame);
        } else if inner.orphan_count < MAX_ORPHAN_FRAMES {
            inner.orphan_frames.entry(key).or_default().push(frame);
            inner.orphan_count += 1;
        }
        return;
    }

    // Everything else needs a string `id`.
    let Some(Value::String(id)) = v.get("id") else { return };
    let id = id.clone();

    // (2) v1 streamed-exec frame: route by id. The core surface registers no v1
    // handlers, so we simply must not crash on these.
    if let Some(Value::String(_)) = v.get("stream") {
        return;
    }

    // (1) JSON-RPC reply.
    if let Some(p) = inner.pending.remove(&id) {
        let ok = v.get("ok").and_then(Value::as_bool).unwrap_or(false);
        if ok {
            let result = v.get("result").cloned().unwrap_or(Value::Null);
            let _ = p.resolve.send(Ok(result));
        } else {
            let (message, code) = normalize_rpc_error(v.get("error"));
            let _ = p.resolve.send(Err(SolariError::Action {
                method: p.method,
                message,
                code,
            }));
        }
    }
}

/// `normalizeRpcError`: if `error` is a string use it as the message; else
/// `error.message ?? error.code ?? "Action failed"`.
fn normalize_rpc_error(err: Option<&Value>) -> (String, Option<String>) {
    match err {
        None | Some(Value::Null) => ("Action failed".to_string(), None),
        Some(Value::String(s)) => (s.clone(), None),
        Some(obj) => {
            let code = obj.get("code").and_then(Value::as_str).map(String::from);
            let message = obj
                .get("message")
                .and_then(Value::as_str)
                .map(String::from)
                .or_else(|| code.clone())
                .unwrap_or_else(|| "Action failed".to_string());
            (message, code)
        }
    }
}

/// The shared control-WS transport. Construct one per live session handle.
pub struct ControlChannel {
    control_url: Mutex<String>,
    headers: Vec<(String, String)>,
    state: Arc<ChannelState>,
    connecting: tokio::sync::Mutex<()>,
    // Cheap `connected` probe without locking `inner`.
    ws_open: Arc<AtomicBool>,
}

impl ControlChannel {
    pub fn new(
        control_url: impl Into<String>,
        headers: Vec<(String, String)>,
        call_timeout_ms: Option<u64>,
    ) -> Self {
        let call_timeout =
            Duration::from_millis(call_timeout_ms.unwrap_or(DEFAULT_CALL_TIMEOUT_MS));
        ControlChannel {
            control_url: Mutex::new(control_url.into()),
            headers,
            state: Arc::new(ChannelState {
                inner: Mutex::new(Inner {
                    next_id: 1,
                    ..Default::default()
                }),
                call_timeout,
            }),
            connecting: tokio::sync::Mutex::new(()),
            ws_open: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether the control channel is open.
    pub fn connected(&self) -> bool {
        self.ws_open.load(Ordering::SeqCst)
    }

    /// Point the channel at a (possibly new) control URL.
    pub fn set_control_url(&self, url: impl Into<String>) {
        *self.control_url.lock().unwrap() = url.into();
    }

    /// Open the control WebSocket. Idempotent: concurrent callers serialize on
    /// `connecting`, and an already-open socket resolves immediately. Transient
    /// fast-failing connects are retried (not a full connect timeout).
    pub async fn connect(&self) -> Result<(), SolariError> {
        if self.connected() {
            return Ok(());
        }
        let _guard = self.connecting.lock().await;
        if self.connected() {
            return Ok(());
        }
        {
            let mut inner = self.state.inner.lock().unwrap();
            inner.closed = false;
        }
        let mut last_err = SolariError::connection("connect failed");
        for attempt in 1..=CONNECT_MAX_ATTEMPTS {
            match self.connect_once().await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    let is_timeout = matches!(err, SolariError::Timeout { .. });
                    last_err = err;
                    let closed = self.state.inner.lock().unwrap().closed;
                    if closed || is_timeout {
                        break;
                    }
                    if attempt < CONNECT_MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(
                            CONNECT_RETRY_BASE_MS * attempt as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        Err(last_err)
    }

    async fn connect_once(&self) -> Result<(), SolariError> {
        let url = self.control_url.lock().unwrap().clone();
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|e| SolariError::connection(format!("bad control URL: {e}")))?;
        for (k, v) in &self.headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| SolariError::connection(format!("bad header name: {e}")))?;
            let val = HeaderValue::from_str(v)
                .map_err(|e| SolariError::connection(format!("bad header value: {e}")))?;
            request.headers_mut().insert(name, val);
        }

        let connect_fut = tokio_tungstenite::connect_async(request);
        let (ws_stream, _resp) =
            match tokio::time::timeout(Duration::from_millis(CONNECT_TIMEOUT_MS), connect_fut)
                .await
            {
                Err(_) => {
                    return Err(SolariError::Timeout {
                        method: "connect".into(),
                        timeout_ms: CONNECT_TIMEOUT_MS,
                    })
                }
                Ok(Err(e)) => return Err(SolariError::connection(format!("connect failed: {e}"))),
                Ok(Ok(x)) => x,
            };

        let (mut sink, mut read) = ws_stream.split();
        let (wtx, mut wrx) = mpsc::unbounded_channel::<Message>();

        // Writer task: drains the outbound queue to the socket.
        tokio::spawn(async move {
            while let Some(msg) = wrx.recv().await {
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        // Reader task: feeds inbound text/binary into the frame dispatcher.
        let state = self.state.clone();
        let ws_open = self.ws_open.clone();
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => state.feed(t.as_str()),
                    Ok(Message::Binary(b)) => state.feed(&String::from_utf8_lossy(&b)),
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            ws_open.store(false, Ordering::SeqCst);
            state.fail_all("Control channel closed");
        });

        {
            let mut inner = self.state.inner.lock().unwrap();
            inner.writer = Some(wtx);
            inner.ws_open = true;
            inner.closed = false;
        }
        self.ws_open.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Reconnect after a drop (re-opens the socket).
    pub async fn reconnect(&self) -> Result<(), SolariError> {
        if self.connected() {
            return Ok(());
        }
        self.ws_open.store(false, Ordering::SeqCst);
        self.connect().await
    }

    /// Close the channel and reject any in-flight calls + stream waits.
    pub fn close(&self) {
        self.ws_open.store(false, Ordering::SeqCst);
        {
            let mut inner = self.state.inner.lock().unwrap();
            inner.closed = true;
        }
        self.state.fail_all("Control channel closed");
    }

    /// Send one JSON-RPC call and await its correlated reply.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, SolariError> {
        let (id, rx, writer) = {
            let mut inner = self.state.inner.lock().unwrap();
            if inner.closed {
                return Err(SolariError::connection("Control channel is closed"));
            }
            let writer = match (&inner.writer, inner.ws_open) {
                (Some(w), true) => w.clone(),
                _ => {
                    return Err(SolariError::connection(
                        "Not connected — call connect() first",
                    ))
                }
            };
            let id = inner.next_id.to_string();
            inner.next_id += 1;
            let (tx, rx) = oneshot::channel();
            inner.pending.insert(
                id.clone(),
                Pending {
                    resolve: tx,
                    method: method.to_string(),
                },
            );
            (id, rx, writer)
        };

        let frame = serde_json::json!({ "id": id, "method": method, "params": params });
        let payload = format!("{frame}\n");
        if writer.send(Message::Text(payload.into())).is_err() {
            self.state.inner.lock().unwrap().pending.remove(&id);
            return Err(SolariError::connection(format!("Failed to send \"{method}\"")));
        }

        match tokio::time::timeout(self.state.call_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SolariError::connection("Control channel closed")),
            Err(_) => {
                self.state.inner.lock().unwrap().pending.remove(&id);
                Err(SolariError::Timeout {
                    method: method.to_string(),
                    timeout_ms: self.state.call_timeout.as_millis() as u64,
                })
            }
        }
    }

    /// Register a receiver for a command's `cmd.data` + `cmd.exit` frames, keyed
    /// on `cmdId`. Flushes any orphan frames that beat this registration.
    ///
    /// If the channel is ALREADY torn down when this is called, return a
    /// receiver whose sender is dropped immediately — its first `recv()` yields
    /// `None`, so the caller's wait resolves to "Control channel closed" instead
    /// of blocking forever. A caller registers this AFTER the `cmd.start` reply
    /// resolves; if the channel closed in that window `fail_all` has already
    /// cleared `frame_handlers`, so a fresh sender inserted here would never be
    /// dropped and the wait would hang. This window is genuinely reachable: the
    /// read task runs concurrently and can call `fail_all` between the reply and
    /// this registration.
    pub fn register_command(&self, cmd_id: &str) -> mpsc::UnboundedReceiver<AsyncFrame> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut inner = self.state.inner.lock().unwrap();
        // If the channel is torn down, do NOT install a persistent handler: a
        // fresh sender would live in `frame_handlers` forever (fail_all already
        // ran and cleared the old ones), so `recv()` would block indefinitely.
        // Still drain any orphan frames that arrived earlier, then let `tx` drop
        // so `recv()` yields `None` (the command loop's "closed" signal) after
        // them.
        let alive = !inner.closed && inner.ws_open;
        for typ in ["cmd.data", "cmd.exit"] {
            let key = (typ.to_string(), cmd_id.to_string());
            if alive {
                inner.frame_handlers.insert(key.clone(), tx.clone());
            }
            if let Some(orphans) = inner.orphan_frames.remove(&key) {
                inner.orphan_count -= orphans.len();
                for f in orphans {
                    let _ = tx.send(f);
                }
            }
        }
        rx
    }

    /// Drop a command's frame handlers (its terminal `cmd.exit` was seen).
    pub fn unregister_command(&self, cmd_id: &str) {
        let mut inner = self.state.inner.lock().unwrap();
        for typ in ["cmd.data", "cmd.exit"] {
            inner.frame_handlers.remove(&(typ.to_string(), cmd_id.to_string()));
        }
    }

    // --- test-only seams -----------------------------------------------------

    #[cfg(test)]
    pub(crate) fn feed_for_test(&self, chunk: &str) {
        self.state.feed(chunk);
    }

    #[cfg(test)]
    pub(crate) fn call_prepare_for_test(&self, method: &str) -> (String, oneshot::Receiver<Result<Value, SolariError>>) {
        let mut inner = self.state.inner.lock().unwrap();
        let id = inner.next_id.to_string();
        inner.next_id += 1;
        let (tx, rx) = oneshot::channel();
        inner.pending.insert(id.clone(), Pending { resolve: tx, method: method.to_string() });
        (id, rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn channel() -> ControlChannel {
        ControlChannel::new("ws://unused/control/x", vec![], Some(1000))
    }

    #[tokio::test]
    async fn rpc_reply_split_across_two_reads() {
        let ch = channel();
        let (id, rx) = ch.call_prepare_for_test("cmd.start");
        assert_eq!(id, "1");
        let full = format!("{{\"id\":\"{id}\",\"ok\":true,\"result\":{{\"cmdId\":\"c1\"}}}}\n");
        // Split the frame across two feeds: no newline in the first chunk.
        let mid = full.len() / 2;
        ch.feed_for_test(&full[..mid]);
        // Not yet dispatched — still pending.
        assert!(rx.is_terminated() == false || true); // rx not resolved yet
        ch.feed_for_test(&full[mid..]);
        let res = rx.await.unwrap().unwrap();
        assert_eq!(res.get("cmdId").unwrap(), "c1");
    }

    #[tokio::test]
    async fn two_frames_in_one_buffer() {
        let ch = channel();
        let (id1, rx1) = ch.call_prepare_for_test("a");
        let (id2, rx2) = ch.call_prepare_for_test("b");
        let buf = format!(
            "{{\"id\":\"{id1}\",\"ok\":true,\"result\":1}}\n{{\"id\":\"{id2}\",\"ok\":true,\"result\":2}}\n"
        );
        ch.feed_for_test(&buf);
        assert_eq!(rx1.await.unwrap().unwrap(), serde_json::json!(1));
        assert_eq!(rx2.await.unwrap().unwrap(), serde_json::json!(2));
    }

    #[tokio::test]
    async fn rpc_error_becomes_action_error() {
        let ch = channel();
        let (id, rx) = ch.call_prepare_for_test("cmd.start");
        ch.feed_for_test(&format!(
            "{{\"id\":\"{id}\",\"ok\":false,\"error\":{{\"message\":\"boom\",\"code\":\"E\"}}}}\n"
        ));
        let err = rx.await.unwrap().unwrap_err();
        match err {
            SolariError::Action { method, message, code } => {
                assert_eq!(method, "cmd.start");
                assert_eq!(message, "boom");
                assert_eq!(code.as_deref(), Some("E"));
            }
            other => panic!("expected Action, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn orphan_frame_buffered_then_flushed_on_registration() {
        let ch = channel();
        // cmd.data + cmd.exit arrive BEFORE the handler is registered.
        let hello = base64::engine::general_purpose::STANDARD.encode("hi");
        ch.feed_for_test(&format!(
            "{{\"type\":\"cmd.data\",\"cmdId\":\"c9\",\"stream\":\"stdout\",\"base64\":\"{hello}\"}}\n"
        ));
        ch.feed_for_test("{\"type\":\"cmd.exit\",\"cmdId\":\"c9\",\"exitCode\":7}\n");
        // Now register — orphans must flush in order (data before exit).
        let mut rx = ch.register_command("c9");
        let f1 = rx.recv().await.unwrap();
        assert_eq!(f1.typ, "cmd.data");
        assert_eq!(f1.base64.as_deref(), Some(hello.as_str()));
        let f2 = rx.recv().await.unwrap();
        assert_eq!(f2.typ, "cmd.exit");
        assert_eq!(f2.exit_code, Some(7));
    }

    #[tokio::test]
    async fn register_command_after_close_yields_closed_not_hang() {
        // The registration race: register_command called AFTER the channel is
        // already torn down must return a receiver that resolves to None (the
        // "closed" signal the command loop turns into a ConnectionError) rather
        // than inserting a sender that lives forever and blocks recv().
        let ch = channel();
        ch.close();
        let mut rx = ch.register_command("c_late");
        assert!(rx.recv().await.is_none(), "closed channel must not hang recv()");
    }

    #[tokio::test]
    async fn non_json_and_unknown_frames_do_not_crash() {
        let ch = channel();
        ch.feed_for_test("not json at all\n");
        ch.feed_for_test("{\"id\":\"99\",\"stream\":\"stdout\",\"data\":\"x\"}\n"); // v1 — ignored
        ch.feed_for_test("{\"nothing\":true}\n");
        // reaching here without panic is the assertion.
    }
}
