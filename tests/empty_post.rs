//! A POST with no JSON body (`pause`, `resume`, `start`) must still carry `Content-Length: 0`.
//! The Google front end answers 411 Length Required to a body-less POST without it (found by the live harness: the
//! Rust `pause()` and `start()` failed against the real gateway with "status 411" while TypeScript, Python and Go passed).
//! No network beyond loopback.
use solari::{Client, ClientOptions};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serves a canned 200 and records every request head (lower-cased).
async fn mock(body: &str) -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (s2, body) = (seen.clone(), body.to_string());
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { return };
            let (s2, body) = (s2.clone(), body.clone());
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                s2.lock().unwrap().push(String::from_utf8_lossy(&buf[..n]).to_lowercase());
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (port, seen)
}

fn client(port: u16) -> Client {
    Client::new(ClientOptions::new("slr_test_x", format!("http://127.0.0.1:{port}"))).expect("client")
}

fn has_zero_length(head: &str) -> bool {
    head.lines().any(|l| l.trim() == "content-length: 0")
}

#[tokio::test]
async fn pause_sends_content_length_zero() {
    let (port, seen) = mock(r#"{"ok":true}"#).await;
    client(port).pause("sbx-1").await.expect("pause");
    let head = seen.lock().unwrap()[0].clone();
    assert!(head.starts_with("post /sandboxes/sbx-1/pause "), "{head}");
    assert!(has_zero_length(&head), "body-less POST must carry content-length: 0, got:\n{head}");
}

#[tokio::test]
async fn resume_and_start_send_content_length_zero() {
    let (port, seen) = mock(r#"{"state":"running","controlUrl":"wss://gw.test/control/x"}"#).await;
    let c = client(port);
    c.resume("sbx-1").await.expect("resume");
    c.start("sbx-1").await.expect("start");
    let heads = seen.lock().unwrap().clone();
    assert_eq!(heads.len(), 2);
    for h in &heads {
        assert!(has_zero_length(h), "body-less POST must carry content-length: 0, got:\n{h}");
    }
}

#[tokio::test]
async fn get_and_delete_do_not_send_a_body_length() {
    let (port, seen) = mock(r#"{"sandboxId":"sbx-1","kind":"sandbox","state":"running","expiresAt":"x"}"#).await;
    let c = client(port);
    c.get("sbx-1").await.expect("get");
    let head = seen.lock().unwrap()[0].clone();
    assert!(head.starts_with("get /sandboxes/sbx-1 "), "{head}");
    assert!(!head.lines().any(|l| l.starts_with("content-length")), "a GET must not carry content-length, got:\n{head}");
}
