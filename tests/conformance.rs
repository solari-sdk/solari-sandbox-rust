//! Conformance runner — Rust side.
//!
//! Runs every row of `tests/conformance/spec/{wire,errors,retry}.json` (the Solari SDK conformance suite, pinned in
//! `tests/conformance/SOURCE.txt`) against THIS SDK through the shared mock gateway, `node tests/conformance/mock-server.mjs`.
//! The rows are the same ones the TypeScript runner and the other bindings run, so a rule one binding got wrong is a red row here.
//!
//!  * An op this binding does not have is listed in `tests/conformance/unsupported.json` with the reason; an unlisted gap fails.
//!  * A row this binding currently fails because the BINDING is wrong is listed in `tests/conformance/knownGaps.json`; the runner
//!    asserts it STILL fails, so a gap cannot decay into a skip and closing it without deleting the entry fails too.
//!  * Rows run concurrently on a small pool of mock processes (the retry rows wait out the SDK's real backoff).
//!
//! Needs `node` on PATH (the mock and `dump-rows.mjs` are JavaScript).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use solari::{Client, ClientOptions, CreateOptions, Lifecycle, RunOptions, SolariError, VolumeAttachment};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/conformance")
}

// --- the mock gateway as a child process ------------------------------------

struct Mock {
    child: Child,
    url: String,
    http: reqwest::Client,
}

impl Mock {
    async fn start() -> Mock {
        let mut child = Command::new("node")
            .arg(dir().join("mock-server.mjs"))
            .args(["--port", "0"])
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("cannot start node (is it on PATH?)");
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).await.unwrap();
        let url = serde_json::from_str::<Value>(&line).unwrap()["url"].as_str().unwrap().to_string();
        Mock { child, url, http: reqwest::Client::new() }
    }
    async fn post(&self, path: &str, body: Value) -> Value {
        self.http.post(format!("{}{}", self.url, path)).json(&body).send().await.unwrap().json().await.unwrap()
    }
    async fn log(&self) -> Value {
        self.http.get(format!("{}/__log", self.url)).send().await.unwrap().json().await.unwrap()
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

// --- comparing: expected is a deep SUBSET of actual --------------------------

fn subset_mismatch(exp: &Value, act: &Value, path: &str) -> Option<String> {
    match exp {
        Value::Object(m) => {
            let a = act.as_object().ok_or_else(|| format!("{path}: expected object, got {act}"));
            let a = match a {
                Ok(a) => a,
                Err(e) => return Some(e),
            };
            for (k, v) in m {
                match a.get(k) {
                    None => return Some(format!("{path}.{k}: missing (expected {v})")),
                    Some(av) => {
                        if let Some(e) = subset_mismatch(v, av, &format!("{path}.{k}")) {
                            return Some(e);
                        }
                    }
                }
            }
            None
        }
        Value::Array(xs) => match act.as_array() {
            Some(ys) if ys.len() == xs.len() => xs.iter().zip(ys).enumerate().find_map(|(i, (x, y))| subset_mismatch(x, y, &format!("{path}[{i}]"))),
            _ => Some(format!("{path}: expected array of {}, got {act}", xs.len())),
        },
        _ => (exp != act).then(|| format!("{path}: expected {exp}, got {act}")),
    }
}

// --- the adapter: abstract op -> this SDK; its errors -> {type,status,code} --

fn normalize_error(e: &SolariError) -> Value {
    let (ty, status, code) = match e {
        SolariError::Auth { status, code, .. } => ("auth", Some(*status), code.clone()),
        SolariError::Plan { status, code, .. } => ("plan", Some(*status), code.clone()),
        SolariError::ConcurrencyLimit { status, code, .. } => ("concurrency_limit", Some(*status), code.clone()),
        SolariError::NoCapacity { status, code, .. } => ("no_capacity", Some(*status), code.clone()),
        SolariError::SandboxNotStopped { status, code, .. } => ("not_stopped", Some(*status), code.clone()),
        SolariError::Gateway { status, code, .. } => ("gateway", Some(*status), code.clone()),
        SolariError::ExecInterrupted { status, code, .. } => ("gateway", Some(*status), code.clone()),
        // The gateway answers a bad value 400 ValidationError; the SDK raises the same class before any request.
        SolariError::Validation { .. } => ("validation", Some(400), Some("ValidationError".to_string())),
        SolariError::Action { code, .. } => ("action", None, code.clone()),
        SolariError::Connection { .. } => ("connection", None, None),
        _ => ("other", None, None),
    };
    let mut out = json!({ "type": ty, "message": e.to_string() });
    if let Some(s) = status {
        out["status"] = json!(s);
    }
    if let Some(c) = code {
        out["code"] = json!(c);
    }
    if matches!(e, SolariError::ExecInterrupted { .. }) {
        out["retryable"] = json!(false); // by definition: it may have partly run
    }
    out
}

fn create_options(i: &Value) -> CreateOptions {
    let s = |k: &str| i.get(k).and_then(Value::as_str).map(String::from);
    let u = |k: &str| i.get(k).and_then(Value::as_u64);
    let map = |k: &str| {
        i.get(k).and_then(Value::as_object).map(|m| m.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string())).collect())
    };
    CreateOptions {
        template: s("template"),
        cpu: u("cpu").map(|v| v as u32),
        mem_mb: u("memMb").map(|v| v as u32),
        disk_gb: u("diskGb").map(|v| v as u32),
        envs: map("envs"),
        metadata: map("metadata"),
        timeout_ms: u("timeoutMs"),
        ttl_seconds: u("ttlSeconds"),
        from_snapshot: s("fromSnapshot"),
        isolation: s("isolation"),
        resolution: s("resolution"),
        record: i.get("record").and_then(Value::as_bool),
        lifecycle: i.get("lifecycle").map(|l| Lifecycle {
            on_timeout: l["onTimeout"].as_str().unwrap_or("").to_string(),
            auto_resume: l.get("autoResume").and_then(Value::as_bool),
            park_after_ms: l.get("parkAfterMs").and_then(Value::as_u64),
            park_max_ms: l.get("parkMaxMs").and_then(Value::as_u64),
            hibernate_after_parked_ms: l.get("hibernateAfterParkedMs").and_then(Value::as_u64),
            checkpoint_every_ms: l.get("checkpointEveryMs").and_then(Value::as_u64),
        }),
        volumes: i.get("volumes").and_then(Value::as_array).map(|vs| {
            vs.iter().map(|v| VolumeAttachment { volume_id: v["volumeId"].as_str().unwrap().into(), path: v["path"].as_str().unwrap().into() }).collect()
        }),
    }
}

fn command_view(r: &solari::CommandResult) -> Value {
    {
        use base64::Engine;
        json!({ "exitCode": r.exit_code, "stdout": r.stdout, "stderr": r.stderr, "timedOut": r.timed_out, "oomKilled": r.oom_killed,
                "stdoutBase64": base64::engine::general_purpose::STANDARD.encode(&r.stdout_bytes) })
    }
}

fn handle_view(s: &solari::Sandbox) -> Value {
    let mut v = json!({ "sandboxId": s.id(), "kind": s.kind() });
    if let Some(u) = s.stream_url() {
        v["streamUrl"] = json!(u);
    }
    if let Some(u) = s.recording_url() {
        v["recordingUrl"] = json!(u);
    }
    v
}

const SUPPORTED: [&str; 9] =
    ["commands.run", "sandbox.create", "sandbox.createDesktop", "sandbox.get", "sandbox.pause", "sandbox.resume", "sandbox.start", "sandbox.kill", "sandbox.connect"];

/// Run one op against `base_url`. Returns `{result}` or `{error}`; never panics on an SDK error. A handle op first creates a
/// sandbox (the row's mock lists that exchange).
async fn run_op(op: &str, input: &Value, base_url: &str) -> Value {
    let r: Result<Value, SolariError> = async {
        let c = Client::new(ClientOptions::new("conformance-test-key", base_url))?;
        match op {
            "sandbox.create" => Ok(handle_view(&c.create(create_options(input)).await?)),
            "sandbox.createDesktop" => Ok(handle_view(&c.create_desktop(create_options(input)).await?)),
            "sandbox.connect" => Ok(handle_view(&c.connect(input["id"].as_str().unwrap()).await?)),
            "sandbox.get" => {
                let v = c.get(input["id"].as_str().unwrap()).await?;
                let mut r = json!({ "sandboxId": v.sandbox_id, "kind": v.kind, "state": v.state });
                if let Some(u) = v.control_url {
                    r["controlUrl"] = json!(u);
                }
                Ok(r)
            }
            "commands.run" => {
                let s = c.create(CreateOptions { template: Some("base".into()), ..Default::default() }).await?;
                let mut o = RunOptions::new();
                if let Some(a) = input.get("args").and_then(Value::as_array) {
                    o = o.args(a.iter().map(|x| x.as_str().unwrap().to_string()));
                }
                match input.get("timeoutMs").and_then(Value::as_i64) {
                    Some(t) if t >= 0 => o.timeout_ms = Some(t as u64),
                    // timeout_ms is an unsigned integer: a negative value cannot be written, so there is nothing to reject.
                    Some(_) => return Err(SolariError::Other("negative timeoutMs cannot be expressed".into())),
                    None => {}
                }
                if let Some(u) = input.get("user").and_then(Value::as_str) {
                    o.user = Some(u.to_string());
                }
                // via "ws": a non-empty env forces the live control connection; via "rest": nothing does (no channel, no env).
                if input.get("via").and_then(Value::as_str) == Some("ws") {
                    o.env = Some(std::collections::HashMap::from([("SOLARI_CONFORMANCE".to_string(), "1".to_string())]));
                }
                let r = s.commands().run(input["cmd"].as_str().unwrap(), o).await?;
                Ok(command_view(&r))
            }
            _ => {
                let mut s = c.create(CreateOptions { template: Some("base".into()), ..Default::default() }).await?;
                match op {
                    "sandbox.pause" => s.pause().await?,
                    "sandbox.resume" => s.resume().await?,
                    "sandbox.start" => s.start().await?,
                    "sandbox.kill" => s.kill().await?,
                    other => panic!("unmapped op {other}"),
                }
                Ok(json!({}))
            }
        }
    }
    .await;
    match r {
        Ok(v) => json!({ "result": v }),
        Err(e) => json!({ "error": normalize_error(&e) }),
    }
}

/// Run one row on a mock; returns `None` when it passes, else why it fails.
async fn check_row(mock: &Mock, row: &Value) -> Option<String> {
    mock.post("/__reset", json!({})).await;
    let script = row.get("mock").cloned().unwrap_or(json!([])).to_string().replace("$MOCK_WS", &mock.url.replacen("http", "ws", 1)).replace("$MOCK_URL", &mock.url);
    let strict = row.get("expect").and_then(|e| e.get("wsCalls")).is_some();
    let armed = mock.post("/__arm", json!({ "mock": serde_json::from_str::<Value>(&script).unwrap(), "ws": row.get("ws").cloned().unwrap_or(json!([])), "strict": strict })).await;
    if armed["ok"] != json!(true) {
        return Some(format!("could not arm the mock: {armed}"));
    }
    let out = run_op(row["op"].as_str().unwrap(), row.get("input").unwrap_or(&json!({})), &mock.url).await;
    let log = mock.log().await;
    let requests = log["requests"].as_array().unwrap().iter().filter(|r| r.get("upgrade").is_none()).count();
    if !log["mismatches"].as_array().unwrap().is_empty() {
        let why: Vec<String> = log["mismatches"].as_array().unwrap().iter().map(|m| format!("{}: {}", m["request"], m["reason"])).collect();
        return Some(format!("request did not fit the script: {}", why.join("; ")));
    }
    let exp = row.get("expect").cloned().unwrap_or(json!({}));
    if let Some(e) = exp.get("error") {
        match out.get("error") {
            None => return Some(format!("expected error {e}, got result {}", out["result"])),
            Some(a) => {
                if let Some(m) = subset_mismatch(e, a, "error") {
                    return Some(format!("{m} (message: {})", a["message"]));
                }
            }
        }
    } else {
        if let Some(e) = out.get("error") {
            return Some(format!("expected success, got error {e}"));
        }
        if let Some(r) = exp.get("result") {
            if let Some(m) = subset_mismatch(r, &out["result"], "result") {
                return Some(m);
            }
        }
    }
    if let Some(n) = exp.get("requests").and_then(Value::as_u64) {
        if requests as u64 != n {
            return Some(format!("expected {n} request(s), made {requests}"));
        }
    }
    if let Some(n) = exp.get("wsCalls").and_then(Value::as_u64) {
        let made = log["wsCalls"].as_array().map(|a| a.len()).unwrap_or(0) as u64;
        if made != n {
            return Some(format!("expected {n} control call(s), made {made}"));
        }
    }
    let remaining = log["remaining"].as_u64().unwrap();
    (remaining != 0).then(|| format!("{remaining} scripted exchange(s) never happened"))
}

fn read_json(name: &str) -> Value {
    serde_json::from_str(&std::fs::read_to_string(dir().join(name)).unwrap_or_else(|_| panic!("cannot read {name}"))).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_rows_run_through_the_shared_mock_gateway() {
    let dumped = std::process::Command::new("node").arg("dump-rows.mjs").current_dir(dir()).output().expect("cannot run node");
    assert!(dumped.status.success(), "dump-rows.mjs failed: {}", String::from_utf8_lossy(&dumped.stderr));
    let rows: Arc<Vec<Value>> = Arc::new(serde_json::from_slice(&dumped.stdout).unwrap());
    let unsupported: BTreeMap<String, String> = read_json("unsupported.json").as_array().unwrap().iter().map(|u| (u["op"].as_str().unwrap().into(), u["why"].as_str().unwrap().into())).collect();
    let gaps: BTreeMap<String, Value> = read_json("knownGaps.json").as_array().unwrap().iter().map(|g| (g["id"].as_str().unwrap().into(), g.clone())).collect();

    let next = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let (rows, next) = (rows.clone(), next.clone());
        workers.push(tokio::spawn(async move {
            let mock = Mock::start().await;
            let mut done: Vec<(usize, Option<String>)> = Vec::new();
            loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= rows.len() {
                    return done;
                }
                if SUPPORTED.contains(&rows[i]["op"].as_str().unwrap()) {
                    done.push((i, check_row(&mock, &rows[i]).await));
                }
            }
        }));
    }
    let mut results: BTreeMap<usize, Option<String>> = BTreeMap::new();
    for w in workers {
        results.extend(w.await.unwrap());
    }

    let (mut passed, mut gap_rows, mut skipped) = (0, 0, 0);
    let (mut keys, mut ops, mut problems) = (BTreeSet::new(), BTreeSet::new(), Vec::new());
    for (i, row) in rows.iter().enumerate() {
        let (key, op) = (row["key"].as_str().unwrap().to_string(), row["op"].as_str().unwrap().to_string());
        keys.insert(key.clone());
        ops.insert(op.clone());
        match results.get(&i) {
            None => {
                if !unsupported.contains_key(&op) {
                    problems.push(format!("{op} is not implemented by the Rust adapter and not listed in unsupported.json"));
                }
                skipped += 1;
            }
            Some(failure) if gaps.contains_key(&key) => {
                if failure.is_none() {
                    problems.push(format!("{key} is listed in knownGaps.json ({}) but now PASSES: delete the entry", gaps[&key]["why"]));
                }
                gap_rows += 1;
            }
            Some(failure) => match failure {
                None => passed += 1,
                Some(f) => problems.push(format!("{key}: {f}")),
            },
        }
    }
    for id in gaps.keys() {
        if !keys.contains(id) {
            problems.push(format!("knownGaps entry {id} matches no row"));
        }
    }
    for op in unsupported.keys() {
        if !ops.contains(op) {
            problems.push(format!("unsupported entry {op} matches no row"));
        }
    }
    println!(
        "conformance: {} rows, {passed} pass, {gap_rows} known gaps, {skipped} rows of {} unsupported ops; supported ops {} of {}",
        rows.len(),
        unsupported.len(),
        SUPPORTED.len(),
        ops.len()
    );
    assert!(problems.is_empty(), "{} problem(s):\n{}", problems.len(), problems.join("\n"));
}
