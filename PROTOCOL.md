> **Public copy.** This is the authoritative wire contract the Go/Rust/C++
> core-surface bindings are held to, published from the Solari monorepo (where
> paths like `sdk/packages/core` and `tests/sdk/ts-request-fixtures.json`
> refer to monorepo locations). The machine-readable half of the contract — the
> exact HTTP requests the reference TypeScript SDK emits — ships in this repo at
> `tests/ts-request-fixtures.json` and is exercised by `tests/contract_wire.rs`.

# Solari SDK wire protocol (core surface)

Single source of truth for the **core-surface** language bindings (Go, Rust, C++).
The reference implementations are the TypeScript `@solarisdk/core` package
(`sdk/packages/core/src/*.ts`) and the Python `solari_desktop` package
(`sdk/python/solari_desktop/*.py`). Ports MUST behave identically to those on the
wire; language idiom is free above the wire.

"Core surface" = **create / connect / get / pause / resume / kill** a sandbox, plus
the session namespaces **commands**, **files**, **code.run**, and **git**. (No pty,
computer-use, viewer, snapshots, templates, metrics, or volumes *CRUD* in the first
cut — those are follow-ups.)

**Attaching** a volume at create IS core (`volumes` below); **managing** volumes
(`POST/GET/DELETE /volumes`) is not, and is deliberately absent from Go/Rust/C++.
Attaching an id minted elsewhere (console, TS/Python SDK) is a complete workflow —
nothing becomes unreachable without the CRUD, which is what separates it from
`pause`/`resume`. Those two are core precisely because `lifecycle.onTimeout:"pause"`
without `resume` strands a VM you can no longer reach.

**The set of ops above is enforced, not described**: `coreOps` in
`tests/sdk/ts-request-fixtures.json` lists them, and every binding's contract test
asserts its op table covers exactly that list. Adding one here means adding it to
the fixture, which fails all four non-reference suites until they implement it.

---

## 1. Transports

Two transports, exactly like the reference SDKs:

- **REST** to the gateway (`baseUrl`, e.g. `https://gw.example.com`) — session
  lifecycle (create/get/kill) + the one-shot exec fast path.
- **Control WebSocket** (`controlUrl`, `wss://…/control/<id>`) —
  newline-delimited JSON RPC for everything a live session does.

Both authenticate with the **same** API key via `Authorization: Bearer <apiKey>`.

### 1.1 REST

Base headers on every request:

```
Authorization: Bearer <apiKey>
Accept: application/json
Content-Type: application/json      (only when there is a body)
Idempotency-Key: <uuid-v4>          (on create; makes create retry-safe)
```

Requests that are idempotent (GET, DELETE, or any request carrying an
`Idempotency-Key`) retry on: network error, HTTP 5xx, HTTP 429-is-NOT-retryable
(see below), and JSON bodies with `"retryable": true`. Backoff is exponential
with jitter: `min(150 * 2^attempt, 8000) + rand(0..250)` ms, default max 5
retries. **429 is NOT retried** — it maps to a concurrency-limit error (the org
is at its session cap; retrying won't help).

Endpoints used by the core surface:

| Method & path | Body | Response |
|---|---|---|
| `POST /sandboxes` | `CreateSandboxRequest` | `CreateSandboxResponse` |
| `GET /sandboxes/{id}` | — | `SandboxView` |
| `DELETE /sandboxes/{id}` | — | `{ "ok": true }` (or empty) — idempotent |
| `POST /sandboxes/{id}/pause` | — | `{ "state": "paused" }` |
| `POST /sandboxes/{id}/resume` | — | `{ "state": "running", "controlUrl"? }` |
| `POST /sandboxes/{id}/exec` | `{cmd,args?,cwd?,timeoutMs?}` | `CommandResult` (one-shot fast path, optional) |

`{id}` MUST be escaped with JavaScript `encodeURIComponent` semantics, NOT RFC-3986
path escaping. Session ids are `<poolId>:<vmId>:<orgId>.<sig>` — all colons — and
RFC 3986 permits a bare `:` in a path segment, so Go's `url.PathEscape` left it as
`:` where every other binding sent `%3A`. Both route, but they are not the same
bytes, and the wire contract compares bytes.

`resume` returns the session on a FRESH slot: adopt the returned `controlUrl` (or
derive `<wsOrigin>/control/<encodeURIComponent(id)>` when absent) and reconnect —
the pre-pause control URL is stale.

`CreateSandboxRequest` (omit null/unset fields — never send an explicit `null`):
```json
{ "template": "base", "kind": "sandbox",
  "cpu": 2, "memMb": 2048, "diskGb": 10,
  "envs": {"K":"V"}, "metadata": {"k":"v"},
  "timeoutMs": 300000, "fromSnapshot": "snap_…",
  "lifecycle": { "onTimeout": "pause", "autoResume": true },
  "volumes": [ { "volumeId": "vol_…", "path": "/data" } ],
  "resolution": "1280x720", "record": true }
```
`kind` is `"sandbox"` (this surface) or `"desktop"`. Only `template` is commonly
set; everything else is optional.

`resolution` and `record` are **desktop-only**: a headless sandbox has no display,
and `record` on one is rejected (400 `RecordingRequiresDesktop`).

`record` and `lifecycle.autoResume` are booleans that must distinguish **unset**
(omit the key) from an explicit **`false`** (send `false`). In Go that means
`*bool`, not `bool` — `bool` + `omitempty` cannot express "explicitly off".

THIS LIST IS THE WHOLE LIST. Every field above is exercised by the maximal create
in `tests/sdk/ts-request-fixtures.json`, so a binding that silently drops one
fails its own contract test rather than shipping a create that quietly ignores it.

`CreateSandboxResponse`:
```json
{ "sandboxId": "sbx_…", "kind": "sandbox",
  "controlUrl": "wss://gw…/control/sbx_…", "expiresAt": "2026-01-01T00:00:00Z" }
```
`streamUrl` may also appear (desktop only) — ignore for sandbox core surface.

`SandboxView` (from GET; used by `connect`): at least
`{ "sandboxId", "kind", "state", "expiresAt" }`. When re-attaching by id, if the
view has no `controlUrl`, derive it: take `baseUrl`, swap the scheme
`http→ws`/`https→wss`, then append `/control/<url-encoded id>`.

### 1.2 Control WebSocket

- Open a WS to `controlUrl`. Send `Authorization: Bearer <apiKey>` as an upgrade
  **header** (all three target languages can set upgrade headers).
- **Framing: newline-delimited JSON.** Each frame is one JSON object followed by
  `\n`. Read side: accumulate bytes, split on `\n`, `trim` each line, ignore
  empty lines, `JSON.parse` each; ignore (do not crash on) non-JSON lines.
- Connect timeout: 15s. Connect retry: up to **3 attempts** on a *fast* failure
  (immediate error/close), backoff `150 * attempt` ms. Do **not** retry a full
  connect *timeout* (a hung dial just re-hangs). Rationale: right after a
  snapshot restore the guest briefly accepts only one vsock control connection,
  so the host can answer a concurrent `/control` upgrade with a transient
  502 `guest_unreachable`; the VM is live, so a couple of quick retries land on a
  ready guest.
- Default per-call RPC timeout: **300000 ms**.

---

## 2. Control-WS frame model

THREE frame kinds share the socket and are dispatched **by shape**, never by
assuming one-reply-per-request:

**(1) JSON-RPC request → reply.** Client sends:
```json
{"id":"1","method":"cmd.start","params":{…}}
```
`id` is an opaque **string**, allocated by a per-channel counter starting at
`"1"`. Server reply, correlated by `id`:
```json
{"id":"1","ok":true,"result":{…}}          // success
{"id":"1","ok":false,"error":{"code":"…","message":"…"}}   // failure (error may also be a plain string)
```
On success, resolve the pending call with `result`. On failure, raise an
**action error** carrying `method`, `message`, and optional `code`
(`normalizeRpcError`: if `error` is a string use it as the message; else
`error.message ?? error.code ?? "Action failed"`).

**(2) v1 streamed-exec frame** (legacy; still handle it): `{"id":"1","stream":"stdout"|"stderr","data":"…"}`.
Route to the per-`id` stream handler if one is registered; the call stays pending
until its terminal `{id,ok,…}` reply. Not required for the core surface if you
implement commands via v2 frames (below), but you MUST NOT crash on these.

**(3) v2 async STREAM frame** — NOT correlated to a request id; dispatched by
`type` + the stream's own id:
```json
{"type":"cmd.data","cmdId":"c1","stream":"stdout","base64":"…"}
{"type":"cmd.exit","cmdId":"c1","exitCode":0}
```
Route by `(type, streamId)` where `streamId = cmdId ?? ptyId ?? watchId`.

**Ordering hazard (critical):** the guest can emit `cmd.data`/`cmd.exit` frames
**before** the `cmd.start` reply arrives (output pumps start before the reply is
sent). So a frame can beat the caller's handler registration. The channel MUST
buffer async frames whose `(type,streamId)` handler is not yet registered
("orphan frames", bounded ~1024) and flush them to the handler the instant it
registers. Reference: `ControlChannel.onFrame` / `orphanFrames` in
`transport.ts`.

**Channel close:** on WS close, reject every in-flight RPC call **and** every
in-flight stream wait (a `commands.run` awaiting `cmd.exit` is NOT a pending RPC
— it's frame-based — so it must be woken by a separate "stream closer" registry
or it hangs forever). Drop all handler + orphan state.

---

## 3. RPC methods (core surface)

All are `channel.call(method, params)` → `result` unless noted.

### commands
- **`cmd.start`** `{cmd, args?, cwd?, env?, user?}` → `{cmdId}`. Starts a process,
  no shell — `cmd` is the binary, `args` its argv tail (this is what makes `git`
  and everything else injection-safe). After the reply, output arrives as async
  `cmd.data` frames (`{stream, base64}` — base64-decode to bytes → UTF-8 text)
  and terminates with `cmd.exit` `{exitCode}`.
- **`cmd.stdin`** `{cmdId, base64}` → `{ok:true}`.
- **`cmd.kill`** `{cmdId, signal?}` → `{ok:true}`.

**`commands.run(cmd, {args?, cwd?, env?, user?, background?, timeoutMs?, onStdout?, onStderr?})` → `CommandResult`:**
1. If a one-shot exec hook is available (REST `/exec`), the channel is **not**
   already connected, and there are no streaming callbacks / `env` / `user` /
   `background`: POST `/sandboxes/{id}/exec` with `{cmd,args,cwd,timeoutMs}` and
   return its `CommandResult`. This skips the cold WS handshake (the dominant
   create→run-one-command latency). If `/exec` is unavailable (older gateway:
   404/route-unavailable) fall back to the WS path; any **other** error
   propagates (never silently double-execute).
2. WS path: `cmd.start`, accumulate `cmd.data` by stream, resolve on `cmd.exit`.
   Deliver each chunk to `onStdout`/`onStderr` if given. `background:true` returns
   immediately with `{exitCode:0, stdout:"", stderr:""}` (caller drives via
   callbacks). Otherwise return `{exitCode, stdout, stderr}`.

`CommandResult = { exitCode: int, stdout: string, stderr: string }`.

### files (all `fs.*`)
- **`fs.read`** `{path}` → `{base64}` (decode to bytes). Provide a text helper.
- **`fs.write`** `{path, base64, mode?}` → `{ok:true}`. `mode` is an int (e.g. 0o644) or unset.
- **`fs.list`** `{path}` → `{entries: FsEntry[]}` where **`FsEntry = {name: string, dir: bool, size: number}`** (exact guest wire — the field is `dir`, NOT `isDir`; there is no `path`/`mode`/`modTime` on a list entry).
- **`fs.stat`** `{path}` → **`FsStat = {name: string, dir: bool, size: number, mode: number, modTimeMs: number}`** (field `dir` not `isDir`; `modTimeMs` = unix-millis, not `modTime`).
- **`fs.mkdir`** `{path}` → `{ok:true}` (creates parents).
- **`fs.remove`** `{path, recursive?}` → `{ok:true}`.
- **`fs.rename`** `{from, to}` → `{ok:true}`.

### code.run
- **`code.run`** `{code, language?, contextId?}` → `RunCodeResult`:
  ```
  RunCodeResult = { results: CodeResultItem[], error?, charts: Chart[] }
  CodeResultItem = { type: "stdout"|"stderr"|"result",
                     text?, png?, jpeg?, svg?, html?, latex?, json?, markdown?, chart? }
  ```
  `language` defaults server-side to python. `charts` is a client-side
  convenience: flatten every `results[i].chart` that is present into a top-level
  array (see `Chart` below). `error` is `{name?,message?,traceback?}` or a string.
  Stream `type:"stdout"/"stderr"` items with text to `onStdout`/`onStderr` if the
  caller supplied them.

`Chart` (structured matplotlib figure; ports parse it, they don't build it):
```
Chart     = { type, title?, xLabel?, yLabel?, x?: ChartAxis, y?: ChartAxis, elements?: any[] }
ChartType = "line"|"scatter"|"bar"|"pie"|"box_and_whisker"|"composite"|"unknown"
ChartAxis = { label?, ticks?: (number|string)[], scale? }
```
`elements` is intentionally loose (`any`/`unknown`/`json`) — do not strongly type
per-chart element payloads; keep the raw decoded JSON so new chart types don't
need an SDK bump.

### git (client-side; composes over `commands.run("git", …)`)
Git is NOT a server RPC. Each method shells out to the `git` binary via
`commands.run("git", {args:[…], cwd})` (no shell → injection-safe) and parses
stdout **client-side**. Port the exact arg construction + parsers from the
reference `git` namespace (`handle.ts` `git = {…}` / Python `_Git`). Methods:

- **`clone(url, {path?, branch?, depth?, username?, password?, cwd?})`** — build
  args `["clone", ("--depth",N)?, ("--branch",B)?, authUrl, path?]`. `authUrl`
  injects basic-auth into an `https://` URL: parse it, URL-encode username and
  password, set userinfo → `https://<enc-user>:<enc-pass>@host/…`; on any parse
  failure return the URL unchanged. Verify: `p@ss word` → `p%40ss%20word`.
- **`status(cwd?)` → `GitStatus`** — run `git status --porcelain=v1 --branch`.
  Parse: first `## …` line gives branch + ahead/behind. `## HEAD (no branch)` →
  `detached:true`, empty branch. `## No commits yet on <b>` → branch `<b>`.
  `## <branch>...<upstream> [ahead N, behind M]` → branch, ahead, behind (either
  bracket term may be absent). Remaining lines are `XY <path>`: `??` → untracked;
  else index (X) status non-space & non-`?` → staged; worktree (Y) status
  non-space → modified. A path can be BOTH staged and modified (e.g. `MM`).
  `clean = staged∪modified∪untracked all empty`.
  `GitStatus = {branch, detached, ahead, behind, staged[], modified[], untracked[], clean}`.
- **`add(paths[], cwd?)`** — no-op if `paths` empty; else
  `["add", "--", ...paths]` (the `--` guards paths that look like flags).
- **`commit(message, {cwd?, author?, email?, all?})` → `{hash}`** — args
  `[("-c","user.name=<author>")?, ("-c","user.email=<email>")?, "commit", "-m", message, ("-a")?]`,
  then `git rev-parse HEAD` (cwd) → trimmed `hash`.
- **`push({cwd?, remote?, branch?, username?, password?})`** and
  **`pull(...)`** — when creds given, use a one-off authed remote via
  `["-c", "url.<authUrl>.insteadOf=<plainUrl>", "push"/"pull", remote?, branch?]`
  so the token never persists in `.git/config`. Without creds, just
  `["push"/"pull", remote?, branch?]`.
- **`checkout(ref, {cwd?, create?})`** — `["checkout", ("-b")?, ref]`.
- **`branches(cwd?)` → `GitBranch[]`** — `git branch --format=%(HEAD)%1f%(refname:short)%1f%(objectname:short)`.
  Split each line on `\x1f`: field0 `*`→current, field1 name, field2 short commit.
  `GitBranch = {name, commit, current}`.
- **`log({cwd?, maxCount?})` → `GitCommit[]`** —
  `git log ("--max-count",N)? --format=%H%x1f%an%x1f%ae%x1f%aI%x1f%s`. Split lines
  on `\x1f` → `GitCommit = {hash, author, email, date, message}`.

Any git method whose underlying command exits non-zero MUST raise an error whose
message includes the subcommand, exit code, and trimmed stderr (fallback stdout).
Reference messages: `git <sub> failed (exit <n>): <detail>`.

---

## 4. Errors

Map gateway/RPC failures to a typed hierarchy (mirror `errors.ts`):

| Condition | Error type |
|---|---|
| HTTP 401/403 | `AuthError` |
| HTTP 402 / body `plan_*` | `PlanError` |
| HTTP 404 | `NotFound` / `GatewayError(404)` |
| HTTP 409 | concurrency/state conflict |
| HTTP 429 | `ConcurrencyLimitError` (org at session cap — NOT retryable) |
| body `no_capacity` / 503 | `NoCapacityError` (retryable) |
| other non-2xx | `GatewayError(status, message, body?)` |
| WS RPC `ok:false` | `ActionError(method, message, code?)` |
| connect/socket failure | `ConnectionError` |
| call/connect timeout | `TimeoutError(method|"connect", ms)` |

A `GatewayErrorBody` looks like `{error?|code?, message?, retryable?}`. Keep the
common base type (`SolariError`) so callers can catch broadly.

---

## 5. Idioms per language (above the wire)

- **Go** (`sdk/go`, module `go.getsolari.com/solari-sandbox`):
  context-first (`ctx context.Context` on every network method), errors as
  values (typed error structs + `errors.As`), `Client` → `Sandbox` handle with
  `Commands`, `Files`, `Code`, `Git` sub-structs. WS via `github.com/gorilla/websocket`.
- **Rust** (`sdk/rust`, crate `solari-sdk`): async/`tokio`, `reqwest` for REST,
  `tokio-tungstenite` for WS, `serde`/`serde_json` for types, `thiserror` for the
  error enum. `Client` → `Sandbox` with `.commands()`, `.files()`, `.code()`,
  `.git()`. Methods return `Result<T, SolariError>`.
- **C++** (`sdk/cpp`, C++17, CMake): `libcurl` for REST, **IXWebSocket** for the
  control WS (TLS via OpenSSL), **nlohmann/json** for JSON — both via CMake
  `FetchContent`. `solari::Client` → `solari::Sandbox` with `.commands`,
  `.files`, `.code`, `.git`. Blocking API (a background read thread feeds a
  frame dispatcher + condition-variable-gated pending map). Throw typed
  exceptions derived from `solari::Error`.

## 6. Definition of done (each language)

1. Compiles clean with the repo toolchain.
2. **Offline tests** covering: newline-JSON frame split incl. a partial/chunked
   read and an orphan-frame-before-handler flush; `commands.run` happy path over
   a mock WS (start → data → exit) → correct `{exitCode,stdout,stderr}`; the git
   parsers + arg construction (status ahead/behind + staged/modified/untracked,
   detached, no-commits, branches, log, commit hash resolution, clone auth-URL
   `p@ss word`→`p%40ss%20word`, add `--` separator + empty no-op, non-zero→error);
   `code.run` chart flattening (`results[].chart` → top-level `charts`); REST
   create request shape (headers incl. Idempotency-Key + body) against a mock
   HTTP server. Mock the transports exactly like the TS (`git.test.mjs`) and
   Python (`test_git.py`) suites do — **no live gateway**.
3. A short `README.md` with a create→run-a-command→git example.
4. Do NOT publish anything (crates.io / pkg registries) — build + test only.
