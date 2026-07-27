//! LIVE integration suite — Rust, against staging.
//!
//! ```text
//! cd sdk/rust && SOLARI_LIVE=1 SOLARI_API_KEY=… cargo test --test live -- --nocapture
//! SOLARI_BASE_URL=…   # override target (default: staging)
//! ```
//!
//! Gated on `SOLARI_LIVE` so it never runs by accident, but it always COMPILES
//! (`cargo test --no-run`) so the suite can't rot silently.
//!
//! Scope: the Rust SDK is CONTROL-PLANE ONLY. There is no screenshot/mouse/
//! keyboard surface to test — a desktop here is a VM whose `kind()` is
//! `"desktop"` and which carries a `stream_url()`. This suite covers what
//! exists: create/connect, kind, stream_url, commands, files, code.run, git,
//! and lifecycle.
//!
//! Creates exactly ONE VM per test and kills it in a teardown that runs even if
//! the body panics (the body is driven on a `tokio::spawn` task, so a panic
//! surfaces as a `JoinError` instead of skipping cleanup). A leaked staging VM
//! is real money and real pool pressure.
//!
//! Each SDK call is logged as a transcript:   `-> call`   `<- result`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use solari::{
    Client, ClientOptions, CreateOptions, GitCommitOptions, GitLogOptions, RunCodeOptions,
    RunOptions, Sandbox, SolariError,
};

const DEFAULT_BASE_URL: &str = "https://api-sta.getsolari.com";

/// A failed expectation. Returned (never panicked) so teardown always runs.
macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) { return Err(format!($($arg)*)); }
    };
}

/// `(api_key, base_url)` for the live run, or `None` when not enabled.
fn live_target() -> Option<(String, String)> {
    if std::env::var("SOLARI_LIVE").ok().filter(|v| !v.is_empty()).is_none() {
        eprintln!("skipping: set SOLARI_LIVE=1 (and SOLARI_API_KEY) to run live tests");
        return None;
    }
    let key = std::env::var("SOLARI_API_KEY").unwrap_or_default();
    assert!(!key.is_empty(), "SOLARI_LIVE=1 but SOLARI_API_KEY is empty");
    let base = std::env::var("SOLARI_BASE_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
    Some((key, base))
}

/// Log `-> call`.
fn call(what: &str) {
    println!("-> {what}");
}

/// Log `<- result` (truncated).
fn result(v: impl std::fmt::Debug) {
    let mut s = format!("{v:?}");
    if s.chars().count() > 220 {
        s = s.chars().take(220).collect::<String>() + "…";
    }
    println!("<- {s}");
}

/// Is this create failure a transient capacity/concurrency condition?
///
/// Matched on the TYPED error, not on message text: the gateway's 503 body is
/// "No sandbox host available", which contains neither "concurrent" nor
/// "capacity" — a substring matcher (as the TS suite uses) silently fails to
/// retry the single most common staging failure, warm-pool exhaustion.
///
/// * 429 `ConcurrencyLimit` — the org's slots are held (by other agents/tests)
/// * 503 `NoCapacity`       — no host free yet; the pool is refilling
fn transient_create(e: &SolariError) -> bool {
    matches!(
        e,
        SolariError::NoCapacity { .. } | SolariError::ConcurrencyLimit { .. }
    )
}

/// Create a VM, retrying 25× / 6s on transient capacity/concurrency errors
/// (mirrors the TS suite's retry budget). Any other error is a real bug and
/// fails immediately.
async fn create_with_retry(client: &Client, desktop: bool) -> Result<Sandbox, String> {
    let mut meta = HashMap::new();
    meta.insert("suite".to_string(), "live-rust".to_string());
    for i in 0..25 {
        // No template: the gateway defaults kind:"desktop" to the desktop
        // golden and kind:"sandbox" to the base image. Exercise that default.
        let opts = CreateOptions {
            metadata: Some(meta.clone()),
            template: if desktop { None } else { Some("base".into()) },
            ..Default::default()
        };
        call(if desktop { "client.create_desktop({})" } else { "client.create({template:\"base\"})" });
        match if desktop { client.create_desktop(opts).await } else { client.create(opts).await } {
            Ok(sbx) => {
                println!("<- id={} kind={} stream_url={:?}", sbx.id(), sbx.kind(), sbx.stream_url());
                return Ok(sbx);
            }
            Err(e) => {
                if !transient_create(&e) {
                    return Err(format!("create failed (not a capacity/concurrency error): {e}"));
                }
                println!("<- transient ({e}); retry {}/25 in 6s", i + 1);
                tokio::time::sleep(Duration::from_secs(6)).await;
            }
        }
    }
    Err("create: exhausted retries".to_string())
}

/// Run `body`, then ALWAYS kill the VM, then report. A panic inside `body` is
/// captured by the join handle, so teardown is never skipped.
async fn with_teardown<F>(client: Arc<Client>, sbx: Arc<Sandbox>, body: F)
where
    F: std::future::Future<Output = Result<(), String>> + Send + 'static,
{
    let outcome = tokio::spawn(body).await;

    let id = sbx.id().to_string();
    println!("-> client.kill({id})  [teardown]");
    match client.kill(&id).await {
        Ok(()) => println!("<- ok (VM destroyed)"),
        Err(e) => println!("<- TEARDOWN FAILED — VM {id} may be leaked: {e}"),
    }

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(msg)) => panic!("{msg}"),
        Err(join) => panic!("test body panicked: {join}"),
    }
}

/// Drives the whole Rust control-plane surface against ONE live desktop VM.
#[tokio::test]
async fn live_desktop() {
    let Some((key, base)) = live_target() else { return };
    let client = Arc::new(Client::new(ClientOptions::new(key, base)).expect("client"));

    let sbx = match create_with_retry(&client, true).await {
        Ok(s) => Arc::new(s),
        Err(e) => panic!("{e}"),
    };

    let c = client.clone();
    let s = sbx.clone();
    with_teardown(client.clone(), sbx.clone(), async move {
        // --- create contract -------------------------------------------------
        ensure!(!s.id().is_empty(), "empty sandbox id");
        ensure!(s.kind() == "desktop", "kind = {:?}, want \"desktop\"", s.kind());
        // stream_url is returned by CREATE for desktop kind only (router.ts:1356).
        let stream = s.stream_url().ok_or("desktop create returned no stream_url")?;
        ensure!(stream.contains("/stream/"), "stream_url = {stream:?}, want a /stream/ URL");
        ensure!(
            stream.starts_with("ws://") || stream.starts_with("wss://"),
            "stream_url = {stream:?}, want a ws(s) scheme"
        );
        ensure!(!s.control_url().is_empty(), "create returned no control_url");

        call("sbx.connect()");
        s.connect().await.map_err(|e| format!("connect: {e}"))?;
        println!("<- ok");
        ensure!(s.connected(), "connected() = false after connect()");

        // --- commands --------------------------------------------------------
        call("sbx.commands().run(\"uname\", args=[\"-s\"])");
        let r = s
            .commands()
            .run("uname", RunOptions::new().args(["-s"]))
            .await
            .map_err(|e| format!("uname: {e}"))?;
        result(&r);
        ensure!(r.exit_code == 0, "exit_code = {}, want 0", r.exit_code);
        // Real evidence: a Linux guest, not merely "didn't throw".
        ensure!(r.stdout.trim() == "Linux", "stdout = {:?}, want \"Linux\"", r.stdout.trim());

        call("sbx.commands().run(\"sh\", args=[\"-c\", \"exit 7\"])");
        let r = s
            .commands()
            .run("sh", RunOptions::new().args(["-c", "exit 7"]))
            .await
            .map_err(|e| format!("exit 7: {e}"))?;
        result(&r);
        ensure!(r.exit_code == 7, "exit_code = {}, want 7", r.exit_code);

        call("sbx.commands().run(\"sh\", args=[\"-c\", \"echo out; echo oops 1>&2\"])");
        let r = s
            .commands()
            .run("sh", RunOptions::new().args(["-c", "echo out; echo oops 1>&2"]))
            .await
            .map_err(|e| format!("stderr: {e}"))?;
        result(&r);
        ensure!(r.stderr.contains("oops"), "stderr = {:?}, want it to contain \"oops\"", r.stderr);
        // The streams must not be merged.
        ensure!(!r.stdout.contains("oops"), "stdout = {:?} leaked stderr", r.stdout);
        ensure!(r.stdout.contains("out"), "stdout = {:?}, want it to contain \"out\"", r.stdout);

        println!("-> sbx.commands().run(\"sh\", [\"-c\",\"echo tokN\"])  x20");
        for i in 0..20 {
            let r = s
                .commands()
                .run("sh", RunOptions::new().args(["-c", &format!("echo tok{i}")]))
                .await
                .map_err(|e| format!("run {i}: {e}"))?;
            ensure!(r.exit_code == 0, "run {i}: exit_code = {}", r.exit_code);
            ensure!(r.stdout.trim() == format!("tok{i}"), "run {i}: stdout = {:?}", r.stdout);
        }
        println!("<- 20/20 stdout captured, no dropped frames");

        // commands().start streams output and reports the exit code.
        call("sbx.commands().start(\"sh\", [\"-c\", \"echo streamed; exit 3\"])");
        let h = s
            .commands()
            .start("sh", RunOptions::new().args(["-c", "echo streamed; exit 3"]))
            .await
            .map_err(|e| format!("start: {e}"))?;
        let r = h.wait().await.map_err(|e| format!("wait: {e}"))?;
        result(&r);
        ensure!(r.exit_code == 3, "started exit_code = {}, want 3", r.exit_code);
        ensure!(
            r.stdout.contains("streamed"),
            "started stdout = {:?}, want it to contain \"streamed\"",
            r.stdout
        );

        // --- files -----------------------------------------------------------
        call("sbx.files().write(\"/tmp/rs-live.txt\", \"hello\")");
        s.files()
            .write("/tmp/rs-live.txt", "hello", None)
            .await
            .map_err(|e| format!("write: {e}"))?;
        println!("<- ok");

        call("sbx.files().read_text(\"/tmp/rs-live.txt\")");
        let txt = s.files().read_text("/tmp/rs-live.txt").await.map_err(|e| format!("read_text: {e}"))?;
        result(&txt);
        ensure!(txt == "hello", "read_text = {txt:?}, want \"hello\"");

        call("sbx.files().list(\"/tmp\")");
        let entries = s.files().list("/tmp").await.map_err(|e| format!("list: {e}"))?;
        println!("<- {} entries", entries.len());
        let found = entries
            .iter()
            .find(|e| e.name == "rs-live.txt")
            .ok_or("rs-live.txt not in /tmp listing")?;
        // Wire-shape evidence: `dir` + a real size, not just presence.
        ensure!(!found.dir, "rs-live.txt reported as a directory");
        ensure!(found.size == 5, "list size = {}, want 5", found.size);

        call("sbx.files().stat(\"/tmp/rs-live.txt\")");
        let st = s.files().stat("/tmp/rs-live.txt").await.map_err(|e| format!("stat: {e}"))?;
        result(&st);
        ensure!(st.size == 5, "stat size = {}, want 5", st.size);
        ensure!(!st.dir, "stat says dir");
        // mod_time_ms is unix-millis — guards the "modTimeMs" wire-field bug.
        ensure!(st.mod_time_ms > 1_600_000_000_000, "stat mod_time_ms = {}, want unix-millis", st.mod_time_ms);

        call("sbx.files().mkdir(\"/tmp/rs-sub\")");
        s.files().mkdir("/tmp/rs-sub").await.map_err(|e| format!("mkdir: {e}"))?;
        println!("<- ok");
        call("sbx.files().rename(\"/tmp/rs-live.txt\", \"/tmp/rs-sub/b.txt\")");
        s.files()
            .rename("/tmp/rs-live.txt", "/tmp/rs-sub/b.txt")
            .await
            .map_err(|e| format!("rename: {e}"))?;
        println!("<- ok");
        let txt = s.files().read_text("/tmp/rs-sub/b.txt").await.map_err(|e| format!("read after rename: {e}"))?;
        ensure!(txt == "hello", "content after rename = {txt:?}, want \"hello\"");
        call("sbx.files().remove(\"/tmp/rs-sub\", recursive=true)");
        s.files().remove("/tmp/rs-sub", true).await.map_err(|e| format!("remove: {e}"))?;
        println!("<- ok");
        let entries = s.files().list("/tmp").await.map_err(|e| format!("list: {e}"))?;
        ensure!(
            !entries.iter().any(|e| e.name == "rs-sub"),
            "rs-sub still present after recursive remove"
        );

        // Binary round-trip preserves every byte.
        let want: Vec<u8> = vec![0, 1, 2, 250, 255];
        call("sbx.files().write(\"/tmp/rs-bin\", <5 bytes>)");
        s.files().write("/tmp/rs-bin", &want, None).await.map_err(|e| format!("write bin: {e}"))?;
        println!("<- ok");
        call("sbx.files().read(\"/tmp/rs-bin\")");
        let got = s.files().read("/tmp/rs-bin").await.map_err(|e| format!("read bin: {e}"))?;
        result(&got);
        ensure!(got == want, "binary round-trip: got {got:?}, want {want:?}");

        // --- code.run --------------------------------------------------------
        call("sbx.code().run(\"print(2**10)\")");
        let rc = s
            .code()
            .run("print(2**10)", RunCodeOptions::default())
            .await
            .map_err(|e| format!("code.run: {e}"))?;
        let text: String = rc.results.iter().filter_map(|i| i.text.clone()).collect();
        println!("<- text={text:?} charts={}", rc.charts.len());
        ensure!(text.contains("1024"), "code.run text = {text:?}, want it to contain \"1024\"");

        // --- git -------------------------------------------------------------
        const REPO: &str = "/tmp/rs-repo";
        call("sbx.commands().run(\"sh\", [\"-c\", \"… git init /tmp/rs-repo\"])");
        let r = s
            .commands()
            .run(
                "sh",
                RunOptions::new().args([
                    "-c",
                    &format!(
                        "rm -rf {REPO} && mkdir -p {REPO} && cd {REPO} && git init -q \
                         && git config user.email live@solari.test && git config user.name live"
                    ),
                ]),
            )
            .await
            .map_err(|e| format!("git init: {e}"))?;
        ensure!(r.exit_code == 0, "git init exit = {} stderr={:?}", r.exit_code, r.stderr);
        println!("<- ok");

        s.files()
            .write(&format!("{REPO}/f.txt"), "v1", None)
            .await
            .map_err(|e| format!("write repo file: {e}"))?;

        call("sbx.git().status(\"/tmp/rs-repo\")");
        let st = s.git().status(Some(REPO)).await.map_err(|e| format!("git status: {e}"))?;
        result(&st);
        // A fresh repo with one new file: untracked, not clean.
        ensure!(!st.clean, "status.clean = true, want false (f.txt is untracked)");
        ensure!(
            st.untracked.iter().any(|u| u == "f.txt"),
            "status.untracked = {:?}, want it to contain f.txt",
            st.untracked
        );

        call("sbx.git().add([\"f.txt\"], \"/tmp/rs-repo\")");
        s.git()
            .add(&["f.txt".to_string()], Some(REPO))
            .await
            .map_err(|e| format!("git add: {e}"))?;
        println!("<- ok");

        call("sbx.git().commit(\"live commit\")");
        let hash = s
            .git()
            .commit(
                "live commit",
                GitCommitOptions {
                    cwd: Some(REPO.into()),
                    author: Some("live".into()),
                    email: Some("live@solari.test".into()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| format!("git commit: {e}"))?;
        result(&hash);
        // Real evidence: a git hash is hex and >= 7 chars.
        ensure!(hash.len() >= 7, "commit hash = {hash:?}, want a real hash");
        ensure!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "commit hash = {hash:?}, want hex"
        );

        call("sbx.git().status(\"/tmp/rs-repo\")  (post-commit)");
        let st2 = s.git().status(Some(REPO)).await.map_err(|e| format!("git status 2: {e}"))?;
        result(&st2);
        ensure!(st2.clean, "status.clean = false after commit, want true ({st2:?})");

        call("sbx.git().log({cwd, max_count: 10})");
        let log = s
            .git()
            .log(GitLogOptions { cwd: Some(REPO.into()), max_count: Some(10) })
            .await
            .map_err(|e| format!("git log: {e}"))?;
        result(&log);
        ensure!(log.len() == 1, "log has {} commits, want 1", log.len());
        ensure!(log[0].message == "live commit", "log[0].message = {:?}", log[0].message);
        ensure!(log[0].hash == hash, "log[0].hash = {:?}, want {hash:?}", log[0].hash);

        call("sbx.git().branches(\"/tmp/rs-repo\")");
        let branches = s.git().branches(Some(REPO)).await.map_err(|e| format!("git branches: {e}"))?;
        result(&branches);
        let current = branches.iter().find(|b| b.current).ok_or("no current branch")?;
        ensure!(current.name == st2.branch, "current branch {:?} != status branch {:?}", current.name, st2.branch);

        call("sbx.git().checkout(\"feature\", cwd, create=true)");
        s.git().checkout("feature", Some(REPO), true).await.map_err(|e| format!("git checkout: {e}"))?;
        println!("<- ok");
        let st3 = s.git().status(Some(REPO)).await.map_err(|e| format!("git status 3: {e}"))?;
        ensure!(st3.branch == "feature", "branch after checkout = {:?}, want \"feature\"", st3.branch);

        // --- query / re-attach ------------------------------------------------
        call("client.get(id)");
        let view = c.get(s.id()).await.map_err(|e| format!("get: {e}"))?;
        result(&view);
        ensure!(view.sandbox_id == s.id(), "sandboxId = {:?}, want {:?}", view.sandbox_id, s.id());
        ensure!(view.kind == "desktop", "kind = {:?}, want desktop", view.kind);
        ensure!(view.state == "running", "state = {:?}, want running", view.state);
        // By design (toSandboxView, router.ts:2501) the view carries NO
        // controlUrl/streamUrl — connect() derives them. Assert the design.
        ensure!(
            view.control_url.is_none(),
            "SandboxView.control_url = {:?}, want None (view serializer omits it)",
            view.control_url
        );

        call("client.connect(id)");
        let re = c.connect(s.id()).await.map_err(|e| format!("connect: {e}"))?;
        println!("<- id={} kind={} stream_url={:?}", re.id(), re.kind(), re.stream_url());
        ensure!(re.id() == s.id(), "reattached id mismatch");
        ensure!(re.kind() == "desktop", "reattached kind = {:?}", re.kind());
        // GET carries no streamUrl, so connect() DERIVES it (desktops only).
        let derived = re.stream_url().ok_or("connect derived no stream_url for a desktop")?;
        ensure!(
            derived.ends_with(&format!("/stream/{}", re.id())),
            "derived stream_url = {derived:?}, want it to end with /stream/{}",
            re.id()
        );
        ensure!(
            derived.starts_with("ws://") || derived.starts_with("wss://"),
            "derived stream_url = {derived:?}, want a ws(s) scheme"
        );
        // The re-attached handle is live, not just a struct.
        re.connect().await.map_err(|e| format!("reattached connect: {e}"))?;
        call("reattached.commands().run(\"sh\", [\"-c\",\"echo reattached\"])");
        let r = re
            .commands()
            .run("sh", RunOptions::new().args(["-c", "echo reattached"]))
            .await
            .map_err(|e| format!("reattached run: {e}"))?;
        result(&r);
        ensure!(r.stdout.contains("reattached"), "stdout = {:?}", r.stdout);
        re.close();

        Ok(())
    })
    .await;
}

/// The other half of the stream_url design: a headless sandbox has no display,
/// so the gateway omits streamUrl and `connect()` derives none.
#[tokio::test]
async fn live_sandbox_has_no_stream_url() {
    let Some((key, base)) = live_target() else { return };
    let client = Arc::new(Client::new(ClientOptions::new(key, base)).expect("client"));

    let sbx = match create_with_retry(&client, false).await {
        Ok(s) => Arc::new(s),
        Err(e) => panic!("{e}"),
    };

    let c = client.clone();
    let s = sbx.clone();
    with_teardown(client.clone(), sbx.clone(), async move {
        ensure!(s.kind() == "sandbox", "kind = {:?}, want \"sandbox\"", s.kind());
        // The whole point: no display => no stream URL on create...
        ensure!(
            s.stream_url().is_none(),
            "headless sandbox create returned stream_url {:?}, want none",
            s.stream_url()
        );

        // ...and none derived on connect either.
        call("client.connect(id)");
        let re = c.connect(s.id()).await.map_err(|e| format!("connect: {e}"))?;
        println!("<- kind={} stream_url={:?}", re.kind(), re.stream_url());
        ensure!(
            re.stream_url().is_none(),
            "connect derived stream_url {:?} for a headless sandbox, want none",
            re.stream_url()
        );
        re.close();

        // It is a real, working sandbox all the same.
        call("sbx.connect()");
        s.connect().await.map_err(|e| format!("connect: {e}"))?;
        println!("<- ok");
        call("sbx.commands().run(\"uname\", [\"-s\"])");
        let r = s
            .commands()
            .run("uname", RunOptions::new().args(["-s"]))
            .await
            .map_err(|e| format!("uname: {e}"))?;
        result(&r);
        ensure!(r.stdout.trim() == "Linux", "stdout = {:?}, want \"Linux\"", r.stdout.trim());

        Ok(())
    })
    .await;
}
