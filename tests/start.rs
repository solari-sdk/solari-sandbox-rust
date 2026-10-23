//! `Client::start` against a mock gateway: the fresh control URL, the idempotent
//! already-running answer, and the refusal codes. No network beyond loopback.
use solari::{Client, ClientOptions, SolariError};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serves one canned status/body to every request and records the request line.
async fn mock(status: u16, body: &str) -> (u16, Arc<Mutex<Vec<String>>>) {
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
                let head = String::from_utf8_lossy(&buf[..n]).to_string();
                let line = head.lines().next().unwrap_or("").to_string();
                s2.lock().unwrap().push(line);
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
    let o = ClientOptions::new("slr_test_x", format!("http://127.0.0.1:{port}"));
    Client::new(o).expect("client")
}

#[tokio::test]
async fn start_returns_the_fresh_control_url() {
    let (port, seen) =
        mock(200, r#"{"state":"running","controlUrl":"wss://gw.test/control/fresh"}"#).await;
    let url = client(port).start("pool-a:vm-7").await.expect("start");
    assert_eq!(url, "wss://gw.test/control/fresh");
    assert_eq!(seen.lock().unwrap()[0], "POST /sandboxes/pool-a%3Avm-7/start HTTP/1.1");
}

#[tokio::test]
async fn start_already_running_still_returns_the_url() {
    let (port, _) = mock(
        200,
        r#"{"state":"running","controlUrl":"wss://gw.test/control/cur","alreadyRunning":true}"#,
    )
    .await;
    assert_eq!(client(port).start("sbx1").await.unwrap(), "wss://gw.test/control/cur");
}

#[tokio::test]
async fn start_derives_the_url_when_omitted() {
    let (port, _) = mock(200, r#"{"state":"running"}"#).await;
    let url = client(port).start("sbx1").await.unwrap();
    assert!(url.starts_with("ws") && url.ends_with("/control/sbx1"), "{url}");
}

#[tokio::test]
async fn start_surfaces_each_409_code() {
    for code in [
        "NotStopped",
        "StartInProgress",
        "StoppedDiskMissing",
        "StoppedDiskUnsafe",
        "GoldenMismatch",
        "DiskPlatformMismatch",
        "DiskShaMismatch",
        "DiskExceedsSpec",
        "DiskOwnerMismatch",
        "HostCapabilityMissing",
        "SandboxStopped",
    ] {
        let body = format!(r#"{{"code":"{code}","error":"refused","state":"paused","retryable":false}}"#);
        let (port, _) = mock(409, &body).await;
        match client(port).start("sbx1").await {
            Err(SolariError::Gateway { status: 409, code: Some(c), .. }) => assert_eq!(c, code),
            other => panic!("{code}: unexpected {other:?}"),
        }
    }
}

#[tokio::test]
async fn start_maps_429_503_and_500() {
    let (p, _) = mock(429, r#"{"error":"cap"}"#).await;
    assert!(matches!(client(p).start("s").await, Err(SolariError::ConcurrencyLimit { .. })));
    let (p, _) = mock(503, r#"{"code":"NoCapacity","error":"none","retryable":true}"#).await;
    assert!(matches!(client(p).start("s").await, Err(SolariError::NoCapacity { .. })));
    let (p, _) = mock(500, r#"{"code":"StartFailed","error":"x","retryable":false}"#).await;
    match client(p).start("s").await {
        Err(SolariError::Gateway { status: 500, code: Some(c), .. }) => assert_eq!(c, "StartFailed"),
        other => panic!("unexpected {other:?}"),
    }
}
