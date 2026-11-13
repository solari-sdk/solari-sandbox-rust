//! Concurrency, file-transfer and cancellation behaviour, against an in-process control-WS that answers every call.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

use crate::client::Sandbox;
use crate::commands::RunOptions;
use crate::http::{HttpOptions, HttpTransport};
use crate::error::SolariError;
use crate::types::CreateSandboxResponse;

type Calls = Arc<Mutex<Vec<(String, Value)>>>;

/// What the gateway sends back for one call: the frames to write (a reply carries `$ID` for the call's id).
type Handler = Arc<dyn Fn(&str, &Value) -> Vec<Value> + Send + Sync>;

struct Gateway {
    port: u16,
    calls: Calls,
    /// Notified when a client socket ends.
    closed: Arc<Notify>,
}

fn reply(result: Value) -> Value {
    json!({"id": "$ID", "ok": true, "result": result})
}

fn fail(msg: &str) -> Value {
    json!({"id": "$ID", "ok": false, "error": msg})
}

async fn gateway(handler: Handler) -> Gateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(Notify::new());
    let (calls2, closed2) = (calls.clone(), closed.clone());
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (handler, calls, closed) = (handler.clone(), calls2.clone(), closed2.clone());
            tokio::spawn(async move {
                let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                let (mut sink, mut read) = ws.split();
                while let Some(Ok(msg)) = read.next().await {
                    let Message::Text(t) = msg else { continue };
                    for line in t.as_str().lines().filter(|l| !l.trim().is_empty()) {
                        let Ok(req) = serde_json::from_str::<Value>(line) else {
                            continue;
                        };
                        let method = req["method"].as_str().unwrap_or("").to_string();
                        let id = req["id"].as_str().unwrap_or("").to_string();
                        calls
                            .lock()
                            .unwrap()
                            .push((method.clone(), req["params"].clone()));
                        for mut frame in handler(&method, &req["params"]) {
                            if frame["id"] == "$ID" {
                                frame["id"] = Value::String(id.clone());
                            }
                            let _ = sink.send(Message::Text(format!("{frame}\n").into())).await;
                        }
                    }
                }
                closed.notify_one();
            });
        }
    });
    Gateway {
        port,
        calls,
        closed,
    }
}

fn sandbox(gw: &Gateway) -> Sandbox {
    sandbox_with_call_timeout(gw, 2000)
}

/// A sandbox whose per-call timeout is `call_timeout_ms`. Tests that move multi-MiB payloads through the fake gateway need
/// more than the default 2 s: base64 and JSON of a 4 MiB chunk in a debug build on a loaded machine takes longer than that.
fn sandbox_with_call_timeout(gw: &Gateway, call_timeout_ms: u64) -> Sandbox {
    let resp = CreateSandboxResponse {
        sandbox_id: "sb".into(),
        kind: "sandbox".into(),
        control_url: format!("ws://127.0.0.1:{}/control/sb", gw.port),
        expires_at: "".into(),
        stream_url: None,
        recording_url: None,
    };
    Sandbox::from_response(
        Arc::new(HttpTransport::new(HttpOptions::new("k", "http://127.0.0.1:1")).unwrap()),
        resp,
        Some(call_timeout_ms),
    )
    .unwrap()
}

fn methods(calls: &Calls) -> Vec<String> {
    calls
        .lock()
        .unwrap()
        .iter()
        .map(|(m, _)| m.clone())
        .collect()
}

fn b64(s: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(s)
}

// ---- Sandbox has a Drop ------------------------------------------------------------------------

/// Dropping a `Sandbox` left its socket and tasks running: the reader task kept the state alive, the state kept the writer's
/// sender alive, and the writer task waited on that sender.
#[tokio::test]
async fn dropping_the_sandbox_closes_its_socket() {
    let gw = gateway(Arc::new(|_, _| vec![])).await;
    let sbx = sandbox(&gw);
    sbx.connect().await.unwrap();
    drop(sbx);
    tokio::time::timeout(Duration::from_secs(2), gw.closed.notified())
        .await
        .expect("the socket stayed open after the Sandbox was dropped");
}

// ---- background: true delivers callbacks ------------------------------------------------------------

#[tokio::test]
async fn background_commands_still_deliver_output_to_callbacks() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![
            reply(json!({"cmdId": "c1"})),
            json!({"type": "cmd.data", "cmdId": "c1", "stream": "stdout", "base64": b64(b"tick")}),
            json!({"type": "cmd.exit", "cmdId": "c1", "exitCode": 0}),
        ],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let mut opts = RunOptions::new();
    opts.background = true;
    opts.on_stdout = Some(Arc::new(move |s: &str| {
        let _ = tx.send(s.to_string());
    }));
    let r = sbx.commands().run("tail", opts).await.unwrap();
    assert_eq!(r.exit_code, 0);
    let got = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("no callback for a background command");
    assert_eq!(got.as_deref(), Some("tick"));
}

// ---- stdin while another task waits ---------------------------------------------------------------------

/// `commands().start` callbacks used to run only inside `wait()`: a caller that started a command, kept the handle and never
/// awaited it saw no output at all.
#[tokio::test]
async fn start_callbacks_fire_without_wait_being_polled() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![
            reply(json!({"cmdId": "c1"})),
            json!({"type": "cmd.data", "cmdId": "c1", "stream": "stdout", "base64": b64(b"hello")}),
            json!({"type": "cmd.data", "cmdId": "c1", "stream": "stderr", "base64": b64(b"warn")}),
        ],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    let seen = Arc::new(std::sync::Mutex::new((String::new(), String::new())));
    let (so, se) = (seen.clone(), seen.clone());
    let mut opts = RunOptions::new();
    opts.on_stdout = Some(Arc::new(move |t| so.lock().unwrap().0.push_str(t)));
    opts.on_stderr = Some(Arc::new(move |t| se.lock().unwrap().1.push_str(t)));
    let handle = sbx.commands().start("sh", opts).await.unwrap();
    // Nobody awaits `handle.wait()`: the callbacks must still run.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while seen.lock().unwrap().clone() != ("hello".to_string(), "warn".to_string()) {
        assert!(tokio::time::Instant::now() < deadline, "callbacks never fired: {:?}", seen.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(handle);
}

/// Rust's `stdout` is text, so a command that prints binary data lost its bytes. `stdout_bytes` keeps them exactly.
#[tokio::test]
async fn stdout_bytes_keep_binary_output_exactly() {
    let raw: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x00, 0xff, 0xfe, 0x1a];
    let raw2 = raw.clone();
    let gw = gateway(Arc::new(move |method, _| match method {
        "cmd.start" => vec![
            reply(json!({"cmdId": "c1"})),
            json!({"type": "cmd.data", "cmdId": "c1", "stream": "stdout", "base64": b64(&raw2)}),
            json!({"type": "cmd.exit", "cmdId": "c1", "exitCode": 0}),
        ],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    sbx.connect().await.unwrap();
    let r = sbx.commands().run("cat", RunOptions::new().args(["img.png"])).await.unwrap();
    assert_eq!(r.stdout_bytes, raw, "the exact bytes");
    assert!(r.stdout.contains('\u{FFFD}'), "the text form replaces the invalid bytes: {:?}", r.stdout);
    assert_eq!(r.stderr_bytes, Vec::<u8>::new());
}

/// stderr gets the same exact bytes over the live connection, kept apart from stdout.
#[tokio::test]
async fn stderr_bytes_keep_binary_output_exactly_and_apart_from_stdout() {
    let err: Vec<u8> = vec![0xfe, 0x00, 0xff, b'w'];
    let err2 = err.clone();
    let gw = gateway(Arc::new(move |method, _| match method {
        "cmd.start" => vec![
            reply(json!({"cmdId": "c1"})),
            json!({"type": "cmd.data", "cmdId": "c1", "stream": "stdout", "base64": b64(b"out")}),
            json!({"type": "cmd.data", "cmdId": "c1", "stream": "stderr", "base64": b64(&err2)}),
            json!({"type": "cmd.exit", "cmdId": "c1", "exitCode": 2}),
        ],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    sbx.connect().await.unwrap();
    let r = sbx.commands().run("x", RunOptions::new()).await.unwrap();
    assert_eq!(r.stderr_bytes, err);
    assert_eq!(r.stdout_bytes, b"out");
}

/// `wait(self)` consumes the handle, so a command could not be written to while it was being waited on.
#[tokio::test]
async fn stdin_can_be_written_while_another_task_waits() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![reply(json!({"cmdId": "c1"}))],
        "cmd.stdin" => vec![
            reply(json!({"ok": true})),
            json!({"type": "cmd.data", "cmdId": "c1", "stream": "stdout", "base64": b64(b"got it")}),
            json!({"type": "cmd.exit", "cmdId": "c1", "exitCode": 0}),
        ],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    let handle = sbx
        .commands()
        .start("cat", RunOptions::new())
        .await
        .unwrap();
    let io = handle.io();
    let waiter = tokio::spawn(handle.wait());
    io.stdin("yes\n").await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((r.exit_code, r.stdout.as_str()), (0, "got it"));
}

// ---- files -------------------------------------------------------------------------------------------------

#[tokio::test]
async fn upload_sends_four_mib_chunks_truncating_only_the_first() {
    let gw = gateway(Arc::new(|_, _| vec![reply(json!({"ok": true}))])).await;
    let sbx = sandbox_with_call_timeout(&gw, 60_000);
    let data = vec![b'x'; 9 << 20];
    sbx.files().upload("/f", &data).await.unwrap();
    let calls = gw.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 3);
    for (i, (offset, truncate)) in [(0u64, true), (4 << 20, false), (8 << 20, false)]
        .iter()
        .enumerate()
    {
        assert_eq!(calls[i].0, "fs.upload");
        assert_eq!(calls[i].1["offset"].as_u64(), Some(*offset), "call {i}");
        assert_eq!(
            calls[i].1["truncate"].as_bool(),
            Some(*truncate),
            "call {i}"
        );
    }
}

#[tokio::test]
async fn upload_of_an_empty_file_is_one_zero_length_call() {
    let gw = gateway(Arc::new(|_, _| vec![reply(json!({"ok": true}))])).await;
    let sbx = sandbox(&gw);
    sbx.files().upload("/e", Vec::<u8>::new()).await.unwrap();
    let calls = gw.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1["base64"], "");
    assert_eq!(calls[0].1["truncate"], true);
}

#[tokio::test]
async fn read_loops_over_chunks_until_eof() {
    let content: Vec<u8> = b"abc".iter().cycle().take(9 << 20).copied().collect();
    let c2 = content.clone();
    let gw = gateway(Arc::new(move |_, p| {
        let off = p["offset"].as_u64().unwrap() as usize;
        let len = p["length"].as_u64().unwrap() as usize;
        let end = (off + len).min(c2.len());
        vec![reply(json!({"base64": b64(&c2[off..end]), "eof": end == c2.len(), "size": c2.len(), "nextOffset": end}))]
    }))
    .await;
    let sbx = sandbox_with_call_timeout(&gw, 60_000);
    let got = sbx.files().read("/big").await.unwrap();
    assert!(got == content);
    assert_eq!(methods(&gw.calls), vec!["fs.download"; 3]);
}

#[tokio::test]
async fn read_falls_back_to_fs_read_on_an_older_guest() {
    let gw = gateway(Arc::new(|method, _| match method {
        "fs.download" => vec![fail("unknown method: fs.download")],
        _ => vec![reply(json!({"base64": b64(b"hi")}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    assert_eq!(sbx.files().read("/f").await.unwrap(), b"hi");
    assert_eq!(methods(&gw.calls), vec!["fs.download", "fs.read"]);
}

#[tokio::test]
async fn write_refuses_more_than_32_mib_before_sending() {
    let gw = gateway(Arc::new(|_, _| vec![reply(json!({"ok": true}))])).await;
    let sbx = sandbox(&gw);
    let err = sbx
        .files()
        .write("/f", vec![0u8; (32 << 20) + 1], None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("file too large for a single call (33554433 bytes, max 33554432): use files.upload()/files.download() or uploadUrl()"),
        "{err}"
    );
    assert!(gw.calls.lock().unwrap().is_empty(), "nothing may be sent");
    sbx.files()
        .write("/f", vec![0u8; 1024], None)
        .await
        .unwrap(); // a small write still goes through
}

// ---- cancellation ---------------------------------------------------------------------------------------------

/// Dropping a `commands().run` future left the guest process running.
#[tokio::test]
async fn a_dropped_run_stops_the_guest_process() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![reply(json!({"cmdId": "c1"}))],
        _ => vec![reply(json!({"ok": true}))], // the command never exits
    }))
    .await;
    let sbx = sandbox(&gw);
    sbx.connect().await.unwrap(); // an unconnected sandbox runs one-shot over REST, which has nothing to cancel here
    let commands = sbx.commands();
    let run = commands.run("sleep", RunOptions::new().args(["60"]));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), run)
            .await
            .is_err(),
        "the run should still be going"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    let calls = gw.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 3, "{:?}", methods(&gw.calls));
    assert_eq!(
        (calls[1].0.as_str(), calls[1].1["signal"].as_i64()),
        ("cmd.kill", Some(15))
    );
    assert_eq!(calls[2].0, "cmd.stdin");
    assert_eq!(calls[2].1["eof"], true);
    assert_eq!(calls[2].1["base64"], "");
    tokio::time::sleep(Duration::from_millis(2300)).await;
    let calls = gw.calls.lock().unwrap().clone();
    let last = calls.last().unwrap();
    assert_eq!(
        (last.0.as_str(), last.1["signal"].as_i64()),
        ("cmd.kill", Some(9)),
        "no SIGKILL after the grace: {:?}",
        methods(&gw.calls)
    );
}

/// Retest 2026-11-10: dropping a run and closing the sandbox at once left the process running, because the cancel was sent
/// from a spawned task that ran after the channel had closed. The SIGTERM and the closed stdin are queued before the drop
/// returns, and `close()` still flushes them.
#[tokio::test]
async fn a_dropped_run_followed_at_once_by_close_still_stops_the_process() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![reply(json!({"cmdId": "c1"}))],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    sbx.connect().await.unwrap();
    let commands = sbx.commands();
    let run = commands.run("sleep", RunOptions::new().args(["60"]));
    assert!(tokio::time::timeout(Duration::from_millis(150), run).await.is_err(), "the run should still be going");
    sbx.close(); // straight after the drop, before any spawned task could run
    tokio::time::sleep(Duration::from_millis(400)).await;
    let calls = gw.calls.lock().unwrap().clone();
    let names: Vec<_> = calls.iter().map(|c| c.0.clone()).collect();
    assert!(
        calls.iter().any(|c| c.0 == "cmd.kill" && c.1["signal"].as_i64() == Some(15)),
        "no SIGTERM reached the guest after drop + close: {names:?}"
    );
    assert!(
        calls.iter().any(|c| c.0 == "cmd.stdin" && c.1["eof"] == true),
        "stdin was not closed after drop + close: {names:?}"
    );
}

#[tokio::test]
async fn a_finished_run_sends_no_cancel_calls() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![
            reply(json!({"cmdId": "c1"})),
            json!({"type": "cmd.exit", "cmdId": "c1", "exitCode": 0}),
        ],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    sbx.connect().await.unwrap();
    sbx.commands().run("true", RunOptions::new()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(methods(&gw.calls), vec!["cmd.start"]);
}

#[tokio::test]
async fn a_dropped_code_run_interrupts_the_kernel() {
    let gw = gateway(Arc::new(|method, _| match method {
        "code.run" => vec![], // never answers
        "code.interrupt" => vec![fail("unknown method: code.interrupt")], // today's guest: ignored
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    let code = sbx.code();
    let run = code.run(
        "while True: pass",
        crate::code::RunCodeOptions {
            context_id: Some("k1".into()),
            ..Default::default()
        },
    );
    assert!(tokio::time::timeout(Duration::from_millis(150), run)
        .await
        .is_err());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let calls = gw.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2, "{:?}", methods(&gw.calls));
    assert_eq!(calls[1].0, "code.interrupt");
    assert_eq!(calls[1].1["contextId"], "k1");
    // Today's guests do not know code.interrupt: that must not tear the channel down or surface anywhere.
    assert!(
        sbx.connected(),
        "the unknown-method reply tore the channel down"
    );
    sbx.files()
        .mkdir("/still-works")
        .await
        .expect("the channel is unusable after a cancelled code.run");
}

#[tokio::test]
async fn a_finished_code_run_sends_no_interrupt() {
    let gw = gateway(Arc::new(|method, _| match method {
        "code.run" => vec![reply(
            json!({"results": [{"type": "stdout", "text": "ok"}]}),
        )],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let sbx = sandbox(&gw);
    let code = sbx.code();
    let out = code
        .run(
            "print('ok')",
            crate::code::RunCodeOptions {
                context_id: Some("k1".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(out.results.len(), 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        methods(&gw.calls),
        vec!["code.run"],
        "a code run that finished must not be interrupted"
    );
}

// ---- commands.run on a connected shared sandbox -------------------------------------------------------

/// A one-route HTTP gateway: every request is answered with a finished command; returns the port and the request counter.
async fn exec_http_gateway() -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits2 = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let hits = hits2.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                loop {
                    let Ok(n) = s.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    if buf[..n].windows(5).any(|w| w == b"/exec") {
                        hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    let body = r#"{"exitCode":0,"stdout":"hi from rest","stderr":""}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    if s.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (port, hits)
}

fn sandbox_with_http(gw: &Gateway, http_port: u16) -> Sandbox {
    let resp = CreateSandboxResponse {
        sandbox_id: "sb".into(),
        kind: "sandbox".into(),
        control_url: format!("ws://127.0.0.1:{}/control/sb", gw.port),
        expires_at: "".into(),
        stream_url: None,
        recording_url: None,
    };
    Sandbox::from_response(
        Arc::new(
            HttpTransport::new(HttpOptions::new(
                "k",
                format!("http://127.0.0.1:{http_port}"),
            ))
            .unwrap(),
        ),
        resp,
        Some(2000),
    )
    .unwrap()
}

fn shared_refusal() -> Value {
    json!({"id": "$ID", "ok": false, "error": {"code": "NotAvailableOnSharedIsolation", "message": "cmd.start is not available on a shared context: it operates on the whole VM, not this context"}})
}

/// A shared sandbox refuses cmd.start but runs commands over REST: run() on a CONNECTED shared sandbox used to return the
/// refusal. It falls back to REST, and remembers the sandbox is shared so later runs do not try the stream again.
#[tokio::test]
async fn run_on_a_connected_shared_sandbox_falls_back_to_rest() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![shared_refusal()],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let (http_port, hits) = exec_http_gateway().await;
    let sbx = sandbox_with_http(&gw, http_port);
    sbx.connect().await.unwrap();
    for i in 0..3 {
        let r = sbx
            .commands()
            .run("sh", RunOptions::new().args(["-c", "echo hi"]))
            .await
            .unwrap_or_else(|e| panic!("run {i}: {e}"));
        assert_eq!((r.exit_code, r.stdout.as_str()), (0, "hi from rest"));
    }
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 3);
    let starts = methods(&gw.calls)
        .iter()
        .filter(|m| *m == "cmd.start")
        .count();
    assert_eq!(starts, 1, "the refusal is remembered: cmd.start sent once");
}

/// A run that needs a stream (an output callback) cannot be served by REST: the refusal is reported.
#[tokio::test]
async fn a_streaming_run_on_a_shared_sandbox_reports_the_refusal() {
    let gw = gateway(Arc::new(|method, _| match method {
        "cmd.start" => vec![shared_refusal()],
        _ => vec![reply(json!({"ok": true}))],
    }))
    .await;
    let (http_port, hits) = exec_http_gateway().await;
    let sbx = sandbox_with_http(&gw, http_port);
    sbx.connect().await.unwrap();
    let mut opts = RunOptions::new().args(["-c", "echo hi"]);
    opts.on_stdout = Some(Arc::new(|_: &str| {}));
    let err = sbx.commands().run("sh", opts).await.unwrap_err();
    assert!(crate::error::is_shared_refusal(&err), "got {err}");
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
}

// ---- timeout_ms above the 600000 cap is refused before any transport ------------------------------------------------

fn start_ok() -> Handler {
    Arc::new(|method, _| match method {
        "cmd.start" => vec![reply(json!({"cmdId": "c1"})), json!({"type": "cmd.exit", "cmdId": "c1", "exitCode": 0})],
        _ => vec![reply(json!({"ok": true}))],
    })
}

fn assert_timeout_refused(r: Result<crate::types::CommandResult, SolariError>) {
    match r {
        Err(SolariError::Validation { status, message, code }) => {
            assert_eq!(status, 400);
            assert_eq!(message, "timeoutMs must be at most 600000");
            assert_eq!(code.as_deref(), Some("ValidationError"));
        }
        other => panic!("expected SolariError::Validation, got {other:?}"),
    }
}

/// With `env` set the command goes over the live connection, which used to accept any deadline (`/exec` answered 400).
#[tokio::test]
async fn run_refuses_a_timeout_over_the_cap_on_the_live_connection() {
    let gw = gateway(start_ok()).await;
    let sbx = sandbox(&gw);
    let mut opts = RunOptions::new();
    opts.timeout_ms = Some(700_000);
    opts.env = Some([("A".to_string(), "1".to_string())].into_iter().collect());
    assert_timeout_refused(sbx.commands().run("true", opts).await);
    assert!(methods(&gw.calls).is_empty(), "nothing may reach the gateway: {:?}", methods(&gw.calls));
}

/// A plain run takes `/exec` first: 600001 is refused before that request too (no HTTP, no fallback to cmd.start).
#[tokio::test]
async fn run_refuses_one_ms_over_the_cap_on_the_plain_path() {
    let gw = gateway(start_ok()).await;
    let sbx = sandbox(&gw);
    let mut opts = RunOptions::new();
    opts.timeout_ms = Some(600_001);
    assert_timeout_refused(sbx.commands().run("true", opts).await);
    assert!(methods(&gw.calls).is_empty(), "nothing may reach the gateway: {:?}", methods(&gw.calls));
}

/// The cap itself is allowed, and passed through as the command's deadline.
#[tokio::test]
async fn run_accepts_a_timeout_at_the_cap() {
    let gw = gateway(start_ok()).await;
    let sbx = sandbox(&gw);
    let mut opts = RunOptions::new();
    opts.timeout_ms = Some(600_000);
    opts.env = Some([("A".to_string(), "1".to_string())].into_iter().collect());
    let r = sbx.commands().run("true", opts).await.unwrap();
    assert_eq!(r.exit_code, 0);
    assert_eq!(methods(&gw.calls), vec!["cmd.start".to_string()]);
}
