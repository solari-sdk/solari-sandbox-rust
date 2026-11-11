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
                    o3.fetch_add(1, Ordering::SeqCst);
                    if exit_on_connect {
                        let _ = ws
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                "{\"type\":\"cmd.exit\",\"cmdId\":\"c1\",\"exitCode\":0}\n".into(),
                            ))
                            .await;
                    }
                    while let Some(Ok(m)) = ws.next().await {
                        if let tokio_tungstenite::tungstenite::Message::Text(t) = m {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(t.trim()) {
                                if let Some(id) = v.get("id").and_then(|i| i.as_str()) {
                                    let reply = format!("{{\"id\":\"{id}\",\"ok\":true,\"result\":{{}}}}\n");
                                    let _ = ws.send(tokio_tungstenite::tungstenite::Message::Text(reply.into())).await;
                                }
                            }
                        }
                    }
                    c3.fetch_add(1, Ordering::SeqCst);
                });
            }
        });
    });
    Gateway { port: rx.recv().unwrap(), opened, closed }
}

async fn becomes(f: impl Fn() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(3);
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
