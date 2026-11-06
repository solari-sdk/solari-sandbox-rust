# conformance

One set of cases that every Solari SDK language runs, so the bindings stay consistent with each other and with the gateway
instead of each growing its own idea of "correct". This directory is the **core layer**: what the SDK sends and how it maps
what the gateway answers (request bytes, headers, retry behaviour, error classes, response fields).
`../compat-harness/` is the layer on top (key and URL guards, core error to vendor class) and `../../live-harness/` is the
nightly live runner; all three share one case convention so nothing is written twice.

Why it exists: the earlier cross-language checks (`contract.json` method names, `tests/sdk/ts-request-fixtures.json`) compared
SDKs with the TypeScript one and could not see a rule that every SDK got wrong or a case with no fixture (status 403, 429 with
`retryable`, a dropped response field, a 5xx marked `retryable:false`). Cases here are written from the gateway's behaviour
(`sdk/PROTOCOL.md`, `gateway/openapi.yaml`), not generated from any SDK.

## Layout

```
spec/*.json                 case tables (data); spec/gateway-responses.json = real gateway error responses by id
lib/spec.mjs                loader + validator (also loads ../compat-harness/spec/conformance.json unchanged), $ref resolver
lib/mock-server.mjs         scripted gateway (dependency-free); mock-server.mjs = same thing as its own process
schema/row.schema.json      documentation of a row (the executable validator is lib/spec.mjs)
test/                       self-tests of the lib and the mock
```

`ts/` is the TypeScript runner (the reference): `adapter.mjs` maps ops to the SDK and its errors to the neutral
`{type,status,code,retryable}`, `runner.test.mjs` runs every row of `spec/{wire,errors,retry}.json` through the mock,
`unsupported.json` / `knownGaps.json` are the per-language lists described below. Other languages add their own directory.

`py/` is the Python runner (`adapter.py`, `test_runner.py`, `unsupported.json`, `knownGaps.json`): it starts `mock-server.mjs` as a process per row,
runs every row against `solari_sandbox` / `solari_desktop` / `solari_core`, and holds the same still-fails rule. `py/test_live_smoke.py` is the
one live-staging row (create, get, kill, gone), marker `live`, off by default. Run: `python -m pytest -c conformance/py/pytest.ini conformance/py`
(from `sdk/`, after `pip install httpx pytest pytest-asyncio pydantic -e python-core -e python-sandbox -e python-desktop`).

## Case format

A spec file is an object. Every array-valued key is a **named group** of **rows** (a `{ "rules": [...] }` value works too; a
list of plain strings is a prose rule list and is kept as metadata). A row:

```jsonc
{
  "id": "create-sandbox-full",                 // required, unique within its group
  "op": "sandbox.create",                      // abstract operation; the language adapter maps it to a call
  "input": { "template": "base", "cpu": 2 },   // language-neutral, camelCase
  "mock": [                                    // optional: the gateway script this case runs against
    { "expect": { "method": "POST", "path": "/sandboxes",
                  "headers": { "authorization": "Bearer <present>", "idempotency-key": "<uuid>" },
                  "body": { "template": "base", "cpu": 2 } },
      "reply":  { "status": 201, "body": { "sandboxId": "sbx_1", "controlUrl": "wss://gw/control/sbx_1" } } }
  ],
  "expect": { "result": { "sandboxId": "sbx_1" }, "requests": 1 },   // or { "error": { "type", "status", "code", "retryable" } }
  "live": false                                // true = stateless row that may also run on staging
}
```

* `reply` can be `"gateway:<id>"`, a row of `spec/gateway-responses.json` (`notEntitled-403`, `tooManyExecutions-429`, ...); `times`
  repeats an exchange (retry tables). `{ "$ref": "gateway:<id>" }` anywhere resolves to the plain `{ status, code?, retryable? }`
  object, so other layers can point at the same responses.
* Matchers (headers, body leaves, `expect.query` values): `<any> <present> <absent> <string> <number> <uuid>`;
  `<capture:NAME>` / `<same:NAME>` check a value is identical across exchanges (one `Idempotency-Key` on every retry).
  Expected bodies are a deep **subset** (extra fields are fine); `body: null` means "no body"; `expect.query` (absent or `null` = the request must carry NO query string; `"<any>"` accepts any) is a subset of the
  **decoded** query, so each language's encoding of a filter never matters. `reply: { "drop": true }` closes the socket with no
  answer (transport failure).
* Unknown fields are tolerated with a warning: each layer adds its own (compat rows carry `env`, `options`, `exceptions`, ...).
* The mock answers a request that does not fit with **599** and records the reason (`mock.mismatches()`), so a wrong request is a
  specific failure and not a hang. `mock.remaining()` tells a case that made fewer calls than scripted.

## Mock server

In process (TypeScript): `const mock = await startMockServer(); mock.arm(row.mock); ...; mock.log(); mock.mismatches();`.
Other languages start it as a process, `node conformance/mock-server.mjs --port 8099` (prints `{url,port}`), and drive it over HTTP:
`POST /__arm {"mock":[...]}`, `POST /__reset`, `GET /__log -> {requests, mismatches, remaining}`.

## Runner contract

* An adapter maps `op` to the SDK call and the SDK's result/error to `{ result }` / `{ error: { type, status, code, retryable } }`.
* An op a language does not have is listed in that language's `unsupported.json` with a reason; CI fails on an unlisted gap.
* A case a binding currently fails because the binding is wrong (not the case) is listed in `knownGaps.json` with the owner and what
  closes it, and the runner asserts it **still fails**; closing the gap without deleting the entry fails too. A gap cannot decay
  into a skip. (Same pattern as `tests/sdk/gateway-field-coverage.ts`.)
* Live use goes through `live-harness`: a `conformance-<lang>` driver runs the `live: true` rows against staging and reports in the
  harness' own format. A live runner reads `LH_BASE_URL LH_API_KEY LH_RUN_ID LH_DRIVER LH_LEDGER LH_OUT`, appends
  `{"kind","id","driver"}` to `LH_LEDGER` **before** using any id, creates with metadata `{lh_run, lh_driver}`, and writes
  `{"driver","sdk","steps":[{"name","status":"pass|fail|unsupported|skipped","ms","detail"}]}` to `LH_OUT`.
  Own ids only, staging only, never delete by tag.

## Run

```sh
# from sdk/, after `npm run build`
node --test conformance/test/*.test.mjs        # lib + mock self-tests
node --test conformance/ts/runner.test.mjs     # the TypeScript runner (the 503 rows retry to exhaustion: ~15 s)
```

Neutral error types in `expect.error.type`: `auth`, `plan`, `concurrency_limit`, `no_capacity`, `not_stopped`, `gateway`
(an adapter maps its own classes). `$MOCK_WS` / `$MOCK_URL` in a mock are replaced with the mock's origin.
