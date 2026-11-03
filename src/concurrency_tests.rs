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
        Some(2000),
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
    let sbx = sandbox(&gw);
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
    let sbx = sandbox(&gw);
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
        "code.run" => vec![],                              // never answers
        _ => vec![fail("unknown method: code.interrupt")], // an older guest: ignored
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
}
