//! Idle close of the control socket (retest C-17). An open control socket counts as activity at the gateway, so one left open
//! keeps an idle sandbox from pausing or expiring on time. The channel closes the socket after `idle_close_ms` with nothing using
//! it (no call in flight, no running command) and reopens it by itself on the next call. Mock gateway: a loopback WebSocket
//! server on its own thread that answers every request with an ok reply and counts connections and client-side closes.
use futures_util::{SinkExt, StreamExt};
use solari::ControlChannel;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Gateway {
    port: u16,
    opened: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
}

/// `exit_on_connect`: send a `cmd.exit` for c1 as soon as a client connects (so it is buffered before any handler exists).
fn gateway(exit_on_connect: bool) -> Gateway {
    let (opened, closed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (tx, rx) = std::sync::mpsc::channel();
    let (o2, c2) = (opened.clone(), closed.clone());
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(l.local_addr().unwrap().port()).unwrap();
            loop {
                let Ok((sock, _)) = l.accept().await else { return };
                let (o3, c3) = (o2.clone(), c2.clone());
                tokio::spawn(async move {
                    let Ok(mut ws) = tokio_tungstenite::accept_async(sock).await else { return };
                    let nth = o3.fetch_add(1, Ordering::SeqCst) + 1;
                    if exit_on_connect && nth == 1 {
                        let _ = ws
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                "{\"type\":\"cmd.exit\",\"cmdId\":\"c1\",\"exitCode\":0}\n".into(),
                            ))
                            .await;
                    }
                    let mut counted = false;
                    while let Some(Ok(m)) = ws.next().await {
                        if let tokio_tungstenite::tungstenite::Message::Close(_) = &m {
                            // The client closed: count it now, and answer late, so the client's reader of that socket ends
                            // well after the idle close (it must not read that end as a drop of a socket opened since).
                            c3.fetch_add(1, Ordering::SeqCst);
                            counted = true;
                            tokio::time::sleep(Duration::from_millis(400)).await;
                        }
                        if let tokio_tungstenite::tungstenite::Message::Text(t) = m {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(t.trim()) {
                                if v.get("method").and_then(|m| m.as_str()) == Some("fs.slow") {
                                    tokio::time::sleep(Duration::from_millis(700)).await;
                                }
                                if let Some(id) = v.get("id").and_then(|i| i.as_str()) {
                                    let reply = format!("{{\"id\":\"{id}\",\"ok\":true,\"result\":{{}}}}\n");
                                    let _ = ws.send(tokio_tungstenite::tungstenite::Message::Text(reply.into())).await;
                                }
                            }
                        }
                    }
                    if !counted {
                        c3.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });
    });
    Gateway { port: rx.recv().unwrap(), opened, closed }
}

async fn becomes(f: impl Fn() -> bool) -> bool {
    becomes_within(Duration::from_secs(3), f).await
}

async fn becomes_within(d: Duration, f: impl Fn() -> bool) -> bool {
    let end = Instant::now() + d;
    while Instant::now() < end {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    f()
}

fn channel(g: &Gateway) -> ControlChannel {
    let ch = ControlChannel::new(format!("ws://127.0.0.1:{}/control/x", g.port), vec![], None);
    ch.set_idle_close_ms(200);
    ch
}

#[tokio::test]
async fn an_idle_socket_is_closed_and_the_next_call_reopens_it() {
    let g = gateway(false);
    let ch = channel(&g);
    ch.connect().await.unwrap();
    ch.call("fs.stat", serde_json::json!({"path": "/"})).await.unwrap();
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 1).await, "the idle socket was never closed");
    assert!(!ch.connected());
    ch.call("fs.stat", serde_json::json!({"path": "/"})).await.expect("a call after the idle close reopens the socket");
    assert_eq!(g.opened.load(Ordering::SeqCst), 2, "reopened lazily");
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 2).await, "the reopened socket idle-closes again");
}

#[tokio::test]
async fn a_running_command_keeps_the_socket_open_and_it_closes_after_the_command_ends() {
    let g = gateway(false);
    let ch = channel(&g);
    ch.connect().await.unwrap();
    let _rx = ch.register_command("c1");
    tokio::time::sleep(Duration::from_millis(700)).await; // several idle windows
    assert_eq!(g.closed.load(Ordering::SeqCst), 0, "the socket was closed while a command was running");
    assert!(ch.connected());
    ch.unregister_command("c1"); // its cmd.exit was seen
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 1).await, "the socket did not close after the command ended");
}

#[tokio::test]
async fn a_command_that_exited_before_its_handler_existed_does_not_keep_the_socket_open() {
    let g = gateway(true); // the cmd.exit arrives at connect, before register_command: it is buffered as an orphan
    let ch = channel(&g);
    ch.connect().await.unwrap();
    ch.call("fs.stat", serde_json::json!({"path": "/"})).await.unwrap(); // the exit frame has been read by now
    let mut rx = ch.register_command("c1");
    let exit = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(f) = rx.recv().await {
            if f.typ == "cmd.exit" {
                return true;
            }
        }
        false
    })
    .await
    .unwrap();
    assert!(exit, "the buffered exit was not replayed");
    ch.unregister_command("c1");
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 1).await, "the socket stayed open after a fast command");
}

// --- defaults, options and the hard cases --------------------------------------------------------------------------------------

#[tokio::test]
async fn the_default_idle_close_is_three_seconds_and_a_channel_with_no_call_closes_on_its_own() {
    let g = gateway(false);
    let ch = ControlChannel::new(format!("ws://127.0.0.1:{}/control/x", g.port), vec![], None); // no setter: the default
    ch.connect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert_eq!(g.closed.load(Ordering::SeqCst), 0, "closed before the 3 s default");
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 1).await, "a connected channel with no call never closed");
}

#[tokio::test]
async fn zero_disables_the_idle_close() {
    let g = gateway(false);
    let ch = ControlChannel::new(format!("ws://127.0.0.1:{}/control/x", g.port), vec![], None);
    ch.set_idle_close_ms(0);
    ch.connect().await.unwrap();
    ch.call("fs.stat", serde_json::json!({"path": "/"})).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(g.closed.load(Ordering::SeqCst), 0);
    assert!(ch.connected());
}

#[tokio::test]
async fn a_call_inside_the_window_pushes_the_close_back() {
    let g = gateway(false);
    let ch = ControlChannel::new(format!("ws://127.0.0.1:{}/control/x", g.port), vec![], None);
    ch.set_idle_close_ms(600);
    ch.connect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    ch.call("fs.stat", serde_json::json!({"path": "/"})).await.unwrap(); // at 400 ms: the close was due at 600 ms
    tokio::time::sleep(Duration::from_millis(400)).await; // 800 ms: past the original deadline, inside the new one
    assert_eq!(g.closed.load(Ordering::SeqCst), 0, "the call did not push the close back");
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 1).await, "and it still closes after the last call");
}

#[tokio::test]
async fn a_slow_call_in_flight_keeps_the_socket_and_an_idle_close_is_not_a_drop_for_what_follows() {
    let g = gateway(false);
    let ch = ControlChannel::new(format!("ws://127.0.0.1:{}/control/x", g.port), vec![], None);
    ch.set_idle_close_ms(200);
    ch.connect().await.unwrap();
    // the reply comes after the idle window: the call still succeeds on the same socket
    ch.call("fs.slow", serde_json::json!({})).await.expect("a call in flight lost its socket to the idle close");
    assert_eq!(g.opened.load(Ordering::SeqCst), 1, "the slow call must not have been reopened");
    assert!(ch.connected(), "the idle timer closed the channel while a call was in flight");
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 1).await);
    // reopened by a slow call: the OLD socket's end (the close handshake) lands while this call is pending, and must not fail it
    ch.call("fs.slow", serde_json::json!({})).await.expect("the old socket's end was read as a drop and failed the new call");
    assert_eq!(g.opened.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn client_create_applies_idle_close_ms() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let g = gateway(false);
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http.local_addr().unwrap().port();
    let body = format!(
        "{{\"sandboxId\":\"sb\",\"kind\":\"sandbox\",\"controlUrl\":\"ws://127.0.0.1:{}/control/sb\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}}",
        g.port
    );
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = http.accept().await else { return };
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    let mut o = solari::ClientOptions::new("slr_test_x", format!("http://127.0.0.1:{http_port}"));
    o.idle_close_ms = Some(200);
    let sb = solari::Client::new(o).unwrap().create(solari::CreateOptions::default()).await.expect("create");
    sb.connect().await.unwrap();
    assert!(
        becomes_within(Duration::from_millis(1500), || g.closed.load(Ordering::SeqCst) == 1).await,
        "ClientOptions::idle_close_ms was not applied (the 3 s default would not have closed yet)"
    );
}

#[tokio::test]
async fn close_flushed_leaves_the_channel_closed() {
    let g = gateway(false);
    let ch = ControlChannel::new(format!("ws://127.0.0.1:{}/control/x", g.port), vec![], None);
    ch.connect().await.unwrap();
    ch.close_flushed(Duration::from_secs(2)).await;
    assert!(!ch.connected());
    let err = ch.call("fs.stat", serde_json::json!({"path": "/"})).await.unwrap_err();
    assert!(err.to_string().contains("closed"), "a call after close_flushed should say the channel is closed: {err}");
}

#[tokio::test]
async fn what_the_channel_keeps_for_a_later_handler_survives_an_idle_close() {
    let g = gateway(true); // the first connection sends a cmd.exit for c1 at once: buffered, no handler yet
    let ch = channel(&g);
    ch.connect().await.unwrap();
    assert!(becomes(|| g.closed.load(Ordering::SeqCst) == 1).await);
    tokio::time::sleep(Duration::from_millis(700)).await; // the old socket's reader has ended by now
    ch.call("fs.stat", serde_json::json!({"path": "/"})).await.unwrap(); // reopens (the second connection sends nothing)
    let mut rx = ch.register_command("c1");
    let got = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
    assert!(matches!(got, Ok(Some(ref f)) if f.typ == "cmd.exit"), "the buffered frame was dropped by the idle close (read as a drop)");
}
