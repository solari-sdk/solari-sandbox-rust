//! A frame queued with `send_now` and then `close()` (what dropping a `run()` and closing the sandbox do) must reach the
//! gateway, also when the program ends right after `close()`. Mock gateway: a loopback WebSocket server on its own thread
//! and runtime, so it is independent of the client's runtime.
use futures_util::StreamExt;
use solari::ControlChannel;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn mock_gateway() -> (u16, Arc<Mutex<Vec<String>>>) {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = std::sync::mpsc::channel();
    let s2 = seen.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(l.local_addr().unwrap().port()).unwrap();
            loop {
                let Ok((sock, _)) = l.accept().await else { return };
                let s3 = s2.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = tokio_tungstenite::accept_async(sock).await else { return };
                    while let Some(Ok(m)) = ws.next().await {
                        if let tokio_tungstenite::tungstenite::Message::Text(t) = m {
                            s3.lock().unwrap().push(t.to_string());
                        }
                    }
                });
            }
        });
    });
    (rx.recv().unwrap(), seen)
}

fn saw_kill(seen: &Arc<Mutex<Vec<String>>>) -> bool {
    let end = Instant::now() + Duration::from_secs(3);
    while Instant::now() < end {
        if seen.lock().unwrap().iter().any(|f| f.contains("cmd.kill")) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

#[test]
fn queued_kill_reaches_the_gateway_when_the_program_awaits_close_flushed() {
    let (port, seen) = mock_gateway();
    {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let ch = ControlChannel::new(format!("ws://127.0.0.1:{port}/control/x"), vec![], None);
            ch.connect().await.unwrap();
            assert!(ch.send_now("cmd.kill", serde_json::json!({ "cmdId": "c1", "signal": 15 })));
            ch.close_flushed(Duration::from_secs(2)).await;
        });
        // the program ends here: the runtime (and every task spawned on it) is dropped. Plain `close()` here loses the frame.
    }
    assert!(saw_kill(&seen), "the cmd.kill queued before close() never reached the gateway: {:?}", seen.lock().unwrap());
}

/// `Lifecycle` has a `Default`, so a caller sets only what it needs (`..Default::default()`); it is the gateway's default, pause.
#[test]
fn lifecycle_has_a_default_that_pauses() {
    let l = solari::Lifecycle { park_after_ms: Some(1000), ..Default::default() };
    assert_eq!(serde_json::to_value(&l).unwrap(), serde_json::json!({ "onTimeout": "pause", "parkAfterMs": 1000 }));
    // alone: every other knob is unset and so absent from the wire
    assert_eq!(serde_json::to_value(solari::Lifecycle::default()).unwrap(), serde_json::json!({ "onTimeout": "pause" }));
}
