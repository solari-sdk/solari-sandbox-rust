//! A host that cuts a pause short leaves the sandbox running and the gateway answers a retryable 503 PauseRetryable. POST /pause
//! carried no idempotency key, so the transport never retried it and the caller saw the 503. No network beyond loopback.
use solari::{Client, ClientOptions};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Answers the first `fail` requests with a 503 PauseRetryable, then 200; records every request head (lower-cased).
async fn mock(fail: usize) -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let s2 = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { return };
            let s2 = s2.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let count = {
                    let mut g = s2.lock().unwrap();
                    g.push(String::from_utf8_lossy(&buf[..n]).to_lowercase());
                    g.len()
                };
                let (status, body) = if count <= fail {
                    ("503 Service Unavailable", r#"{"error":"Sandbox pause did not finish; it is still running","code":"PauseRetryable","retryable":true}"#)
                } else {
                    ("200 OK", r#"{"state":"paused"}"#)
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (port, seen)
}

fn key_of(head: &str) -> Option<String> {
    head.lines().find_map(|l| l.strip_prefix("idempotency-key: ").map(|k| k.trim().to_string()))
}

#[tokio::test]
async fn pause_retries_pause_retryable_with_one_key() {
    let (port, seen) = mock(2).await;
    let c = Client::new(ClientOptions::new("slr_test_x", format!("http://127.0.0.1:{port}"))).expect("client");
    c.pause("sbx-1").await.expect("pause is retried until it succeeds");
    let heads = seen.lock().unwrap().clone();
    assert_eq!(heads.len(), 3, "two 503s then the 200");
    let k0 = key_of(&heads[0]).expect("an Idempotency-Key on the first attempt");
    assert!(heads.iter().all(|h| key_of(h).as_deref() == Some(k0.as_str())), "one key on every attempt: {heads:?}");
}
