// A scripted gateway for conformance cases. Dependency-free (node:http).
//
//   const mock = await startMockServer();
//   mock.arm(case.mock);                       // ordered exchanges the code under test must make
//   ... point the SDK at mock.url, run the op ...
//   mock.log()         -> requests it saw       mock.mismatches() -> requests that did not fit
//   mock.remaining()   -> exchanges not yet used (a case that expects 3 calls and made 2 leaves 1)
//
// Each incoming request consumes the NEXT armed exchange. If it does not match (method, path, headers, body) or
// nothing is left, the mock answers 599 with the reason and records a mismatch, so a wrong request is a loud,
// specific failure instead of a hang or a silent pass.
//
// Out-of-process runners (Python/Go/Rust/C++) drive the same server over HTTP: POST /__arm {mock}, POST /__reset,
// GET /__log -> { requests, mismatches, remaining }. Paths under /__ are reserved.
//
// Matchers in expected headers / body leaves / query values:
//   "<any>"  "<string>"  "<number>"  "<uuid>"  "<present>"  "<absent>" (header or key must not be there)
//   "<capture:NAME>"  records the value;  "<same:NAME>"  must equal the value captured earlier in this script
//   (e.g. one Idempotency-Key across every retry attempt).
// Expected body is a deep SUBSET of the real body (extra fields are fine); `null` means "no body".
// `expect.query` is a subset of the DECODED query (repeated keys become arrays), so encodings never matter.
// A reply of { drop: true } closes the socket without answering (transport failure).
import { createServer } from "node:http";
import { createHash } from "node:crypto";
import { resolveRefs } from "./spec.mjs";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MATCHERS = {
  "<any>": () => true,
  "<absent>": (v) => v === undefined,
  "<present>": (v) => v !== undefined && v !== null,
  "<string>": (v) => typeof v === "string",
  "<number>": (v) => typeof v === "number",
  "<uuid>": (v) => typeof v === "string" && UUID.test(v),
};

/** Why `actual` does not satisfy `expected` (deep subset), or null when it does. */
export function subsetMismatch(expected, actual, path = "$", captures = new Map()) {
  if (typeof expected === "string") {
    const cap = /^<(capture|same):([\w-]+)>$/.exec(expected);
    if (cap) {
      if (actual === undefined) return `${path}: expected a value to ${cap[1]} as ${cap[2]}, got none`;
      if (cap[1] === "capture") { captures.set(cap[2], actual); return null; }
      if (!captures.has(cap[2])) return `${path}: <same:${cap[2]}> used before <capture:${cap[2]}>`;
      return Object.is(captures.get(cap[2]), actual) ? null : `${path}: expected the same value as captured "${cap[2]}" (${JSON.stringify(captures.get(cap[2]))}), got ${JSON.stringify(actual)}`;
    }
    if (MATCHERS[expected]) return MATCHERS[expected](actual) ? null : `${path}: expected ${expected}, got ${JSON.stringify(actual)}`;
  }
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual) || actual.length !== expected.length) return `${path}: expected array of ${expected.length}, got ${JSON.stringify(actual)}`;
    for (let i = 0; i < expected.length; i++) {
      const m = subsetMismatch(expected[i], actual[i], `${path}[${i}]`, captures);
      if (m) return m;
    }
    return null;
  }
  if (expected && typeof expected === "object") {
    if (!actual || typeof actual !== "object" || Array.isArray(actual)) return `${path}: expected object, got ${JSON.stringify(actual)}`;
    for (const [k, v] of Object.entries(expected)) {
      const m = subsetMismatch(v, actual[k], `${path}.${k}`, captures);
      if (m) return m;
    }
    return null;
  }
  return Object.is(expected, actual) ? null : `${path}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`;
}

/** Decoded query: { key: "v" } or { key: ["v1","v2"] } for repeated keys. */
function decodedQuery(search) {
  const out = {};
  for (const [k, v] of new URLSearchParams(search)) out[k] = k in out ? [].concat(out[k], v) : v;
  return out;
}

function requestMismatch(expect, req, captures) {
  if (req.method !== expect.method) return `method: expected ${expect.method}, got ${req.method}`;
  const [pathname, search = ""] = req.path.split("?");
  // A path without a query is compared to the pathname; one that spells a query is compared whole.
  if ((expect.path.includes("?") ? req.path : pathname) !== expect.path) return `path: expected ${expect.path}, got ${req.path}`;
  // Strict by default: a row that names no query allows none. `query: "<any>"` opts out; an object is a subset match.
  if (expect.query === undefined || expect.query === null) {
    if (!expect.path.includes("?") && search) return `query: expected none, got ?${search}`;
  } else if (expect.query !== "<any>") {
    const m = subsetMismatch(expect.query, decodedQuery(search), "query", captures);
    if (m) return m;
  }
  for (const [name, want] of Object.entries(expect.headers ?? {})) {
    const m = subsetMismatch(want, req.headers[name.toLowerCase()], `header ${name}`, captures);
    if (m) return m;
  }
  if ("body" in expect) {
    if (expect.body === null) {
      if (req.body !== undefined) return `body: expected none, got ${JSON.stringify(req.body)}`;
    } else {
      const m = subsetMismatch(expect.body, req.body, "body", captures);
      if (m) return m;
    }
  }
  return null;
}

const LOOPBACK = new Set(["127.0.0.1", "::1", "localhost"]);
// Headers whose values never leave the process through /__log (the control API is unauthenticated).
const SECRET_HEADERS = new Set(["authorization", "x-api-key", "cookie", "set-cookie", "proxy-authorization"]);
const masked = (headers) => Object.fromEntries(Object.entries(headers).map(([k, v]) => [k, SECRET_HEADERS.has(k.toLowerCase()) ? "<masked>" : v]));

/** Loopback only by default: the control API (/__arm /__reset /__log) is unauthenticated and the log holds request headers. */
export async function startMockServer({ host = "127.0.0.1", port = 0, allowNonLoopback = false } = {}) {
  if (!allowNonLoopback && !LOOPBACK.has(host)) throw new Error(`mock server refuses to listen on ${host}: loopback only unless allowNonLoopback is set`);
  let queue = [];
  let requests = [];
  let mismatches = [];
  let captures = new Map();
  const sockets = new Set();

  const arm = (mock) => {
    const resolved = resolveRefs(mock);
    queue = resolved.flatMap((ex) => Array.from({ length: ex.times ?? 1 }, () => ex));
    requests = [];
    mismatches = [];
    captures = new Map();
  };
  const reset = () => { queue = []; requests = []; mismatches = []; captures = new Map(); };

  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const raw = Buffer.concat(chunks).toString("utf8");
    const url = req.url ?? "/";

    if (url.startsWith("/__")) {
      const send = (obj, status = 200) => { res.writeHead(status, { "content-type": "application/json" }); res.end(JSON.stringify(obj)); };
      if (url === "/__arm" && req.method === "POST") { try { arm(JSON.parse(raw).mock); return send({ ok: true, armed: queue.length }); } catch (e) { return send({ ok: false, error: String(e.message) }, 400); } }
      if (url === "/__reset" && req.method === "POST") { reset(); return send({ ok: true }); }
      if (url === "/__log" && req.method === "GET") return send({ requests: requests.map((r) => ({ ...r, headers: masked(r.headers) })), mismatches, remaining: queue.length });
      return send({ error: "unknown control path" }, 404);
    }

    let body;
    if (raw) { try { body = JSON.parse(raw); } catch { body = raw; } }
    const seen = { method: req.method, path: url, headers: req.headers, body };
    requests.push(seen);

    const next = queue.shift();
    const reason = next ? requestMismatch(next.expect, seen, captures) : "unexpected request: no exchange left";
    if (reason) {
      mismatches.push({ request: `${seen.method} ${seen.path}`, reason });
      res.writeHead(599, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "mock mismatch", reason }));
    }
    const { status, headers = {}, body: replyBody, delayMs, drop } = next.reply;
    if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
    if (drop) return void req.socket.destroy(); // transport failure: no response at all
    res.writeHead(status, replyBody === undefined ? headers : { "content-type": "application/json", ...headers });
    res.end(status === 204 || replyBody === undefined ? undefined : typeof replyBody === "string" ? replyBody : JSON.stringify(replyBody));
  });

  // Control WebSocket: only the HANDSHAKE (101 + accept key), then silence. Enough for resume()/start()/connect() to
  // succeed; scripted frames (commands, files) are a later phase. Upgrades are logged like any request.
  server.on("upgrade", (req, socket) => {
    const key = req.headers["sec-websocket-key"];
    requests.push({ method: req.method, path: req.url ?? "/", headers: req.headers, body: undefined, upgrade: "websocket" });
    if (!key) return void socket.destroy();
    const accept = createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
  });

  await new Promise((resolve) => server.listen(port, host, resolve));
  const addr = server.address();
  return {
    url: `http://${host}:${addr.port}`,
    host: addr.address,
    port: addr.port,
    arm,
    reset,
    log: () => requests.slice(),
    mismatches: () => mismatches.slice(),
    remaining: () => queue.length,
    close: () => new Promise((resolve) => { for (const sk of sockets) sk.destroy(); server.closeAllConnections?.(); server.close(resolve); }),
  };
}
