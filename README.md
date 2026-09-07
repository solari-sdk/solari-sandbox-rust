# solari-sdk (Rust)

Rust binding for the Solari sandbox SDK — the **core surface**: create / connect /
kill a sandbox, then drive it via `commands`, `files`, `code.run`, and a
client-side `git` namespace. It speaks the exact same wire protocol as the
reference `@solarisdk/core` TypeScript package (see [`PROTOCOL.md`](PROTOCOL.md)).

- **REST** to the gateway for session lifecycle + the one-shot `/exec` fast path.
- **Control WebSocket** (newline-delimited JSON-RPC) for everything a live
  session does.

Async on [`tokio`]; `reqwest` (rustls) for REST, `tokio-tungstenite` for the
control WS, `serde` for wire types, `thiserror` for the [`SolariError`] enum.

Not in this crate (follow-ups): pty, volumes, computer-use, viewer, snapshots,
templates.

## Install

```toml
[dependencies]
solari-sdk = { path = "sdk/rust" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The crate is `solari-sdk`; the root module is `solari`.

## Example: create → run a command → git

```rust
use solari::{Client, ClientOptions, CreateOptions, RunOptions};
use solari::{GitCloneOptions, GitCommitOptions};

#[tokio::main]
async fn main() -> Result<(), solari::SolariError> {
    // 1. Client + create a sandbox.
    let client = Client::new(ClientOptions::new(
        "slr_live_…",                 // API key (also authenticates the WS)
        "https://gw.example.com",     // gateway base URL
    ))?;

    let sbx = client
        .create(CreateOptions {
            template: Some("base".into()),
            ..Default::default()
        })
        .await?;

    // 2. Run a command. The first run rides the warm REST `/exec` fast path
    //    (skips the cold control-WS handshake); later runs stream over the WS.
    let out = sbx
        .commands()
        .run("echo", RunOptions::new().args(["hello world"]))
        .await?;
    println!("exit={} stdout={:?}", out.exit_code, out.stdout);

    // 3. Files.
    sbx.files().write("/tmp/hi.txt", "hi", None).await?;
    let text = sbx.files().read_text("/tmp/hi.txt").await?;
    println!("file: {text}");

    // 4. Git — client-side, composed over `git` invocations (no shell).
    sbx.git()
        .clone(
            "https://github.com/acme/repo.git",
            GitCloneOptions {
                path: Some("/repo".into()),
                username: Some("u".into()),
                password: Some("token".into()), // creds are URL-encoded, never persisted
                ..Default::default()
            },
        )
        .await?;

    let status = sbx.git().status(Some("/repo")).await?;
    println!("branch={} clean={}", status.branch, status.clean);

    sbx.git().add(&["README.md".into()], Some("/repo")).await?;
    let hash = sbx
        .git()
        .commit(
            "docs: update",
            GitCommitOptions {
                cwd: Some("/repo".into()),
                author: Some("Ada".into()),
                email: Some("ada@example.com".into()),
                ..Default::default()
            },
        )
        .await?;
    println!("committed {hash}");

    // 5. Run code in a stateful kernel (charts are flattened for you).
    let result = sbx
        .code()
        .run("print(6*7)", solari::RunCodeOptions::default())
        .await?;
    println!("{} result item(s), {} chart(s)", result.results.len(), result.charts.len());

    // 6. Done.
    sbx.kill().await?;
    Ok(())
}
```

## Errors

Every fallible method returns `Result<T, SolariError>`. Variants mirror the
reference hierarchy: `Auth` (401/403), `Plan` (402), `ConcurrencyLimit` (429,
not retryable), `NoCapacity` (503), `Gateway` (other non-2xx), `Action` (a
control-WS RPC replied `ok:false`), `Timeout`, `Connection`, and `Git`.

## Testing

Fully offline — the transports are mocked (in-process WS + a tiny TCP HTTP
server); no live gateway is needed.

```
cargo build
cargo test
```
