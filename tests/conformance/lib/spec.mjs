// Loader + validator for conformance spec files (sdk/conformance/spec/*.json and, unchanged,
// sdk/compat-harness/spec/conformance.json). One shape for every layer:
//
//   a spec file is an object whose array-valued keys (or `{ rules: [...] }` values) are NAMED GROUPS of rows
//   (a list of plain strings is a prose rule list and is kept as metadata, not validated as rows);
//   a row is an object with a unique string `id` (unique within its group) and whatever its layer needs
//   (`op`, `input`, `env`, `options`, `expect`, ...).
//
// Fields this library knows how to check are listed below; any other field is TOLERATED with a warning, because
// each layer (core wire, compat, live) adds its own and must not need a change here to do so.
import { readFileSync } from "node:fs";

const KNOWN_FIELDS = new Set([
  "id", "op", "input", "expect", "mock", "live", "why", "description", // this suite
  "env", "options", "needsVendorUrl", "exceptions", "kind", "status", // compat-harness rows, as they are
]);

/** Parse a spec file. Returns { meta, groups, errors, warnings }; errors make a spec invalid, warnings do not. */
export function loadSpec(path) {
  return parseSpec(JSON.parse(readFileSync(path, "utf8")), path);
}

export function parseSpec(doc, label = "spec") {
  const errors = [];
  const warnings = [];
  const meta = {};
  const groups = {};
  if (!doc || typeof doc !== "object" || Array.isArray(doc)) {
    return { meta, groups, errors: [`${label}: top level must be an object`], warnings };
  }
  for (const [key, value] of Object.entries(doc)) {
    const rows = Array.isArray(value) ? value : Array.isArray(value?.rules) ? value.rules : null;
    if (!rows) {
      meta[key] = value;
      continue;
    }
    // A list of plain strings is a prose rule list (compat-harness `unsupported.rules`), not a table of rows.
    if (rows.every((r) => typeof r === "string")) {
      meta[key] = value;
      continue;
    }
    groups[key] = rows;
    const seen = new Set();
    rows.forEach((row, i) => {
      const where = `${label}: ${key}[${i}]`;
      if (!row || typeof row !== "object" || Array.isArray(row)) return errors.push(`${where}: a row must be an object`);
      if (typeof row.id !== "string" || !row.id) return errors.push(`${where}: missing string id`);
      if (seen.has(row.id)) errors.push(`${where}: duplicate id "${row.id}" in group ${key}`);
      seen.add(row.id);
      for (const f of Object.keys(row)) {
        if (!KNOWN_FIELDS.has(f)) warnings.push(`${where} (${row.id}): unknown field "${f}" tolerated`);
      }
      if ("live" in row && typeof row.live !== "boolean") errors.push(`${where} (${row.id}): live must be a boolean`);
      if ("mock" in row) for (const e of validateMock(row.mock)) errors.push(`${where} (${row.id}): mock ${e}`);
      for (const ref of collectRefs(row)) {
        if (!gatewayResponseIds().has(ref.replace(/^gateway:/, "")) || !ref.startsWith("gateway:")) errors.push(`${where} (${row.id}): unresolved reference ${JSON.stringify(ref)}`);
      }
    });
  }
  return { meta, groups, errors, warnings };
}

let ids;
/** Ids in spec/gateway-responses.json (read once; it has no references itself). */
function gatewayResponseIds() {
  if (!ids) {
    const doc = JSON.parse(readFileSync(new URL("../spec/gateway-responses.json", import.meta.url), "utf8"));
    ids = new Set((doc.responses ?? []).map((r) => r.id));
  }
  return ids;
}

/** Every reference in a row: `{ "$ref": "..." }` objects and string `reply` values of mock exchanges. */
function collectRefs(row) {
  const out = [];
  const walk = (v, inReply = false) => {
    if (typeof v === "string") return void (inReply && out.push(v));
    if (Array.isArray(v)) return v.forEach((x) => walk(x));
    if (v && typeof v === "object") {
      if (typeof v.$ref === "string") out.push(v.$ref);
      for (const [k, x] of Object.entries(v)) walk(x, k === "reply");
    }
  };
  walk(row);
  return out;
}

const METHODS = new Set(["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]);

/** A scripted gateway: an ordered list of exchanges the code under test must make, each with the reply to give. */
export function validateMock(mock) {
  const errors = [];
  if (!Array.isArray(mock) || mock.length === 0) return ["must be a non-empty array"];
  mock.forEach((ex, i) => {
    const w = `[${i}]`;
    if (!ex || typeof ex !== "object") return errors.push(`${w}: must be an object`);
    if (!ex.expect || !METHODS.has(ex.expect.method)) errors.push(`${w}.expect.method must be one of ${[...METHODS].join("/")}`);
    if (typeof ex.expect?.path !== "string" || !ex.expect.path.startsWith("/")) errors.push(`${w}.expect.path must start with "/"`);
    const reply = ex.reply;
    const refd = typeof ex.reply === "string"; // "gateway:<id>" resolved by resolveRefs
    if (!refd && !reply?.drop && (!reply || !Number.isInteger(reply.status) || reply.status < 100 || reply.status > 599)) {
      errors.push(`${w}.reply.status must be an HTTP status (or reply a "gateway:<id>" reference, or { drop: true })`);
    }
    if (ex.times !== undefined && (!Number.isInteger(ex.times) || ex.times < 1)) errors.push(`${w}.times must be a positive integer`);
  });
  return errors;
}

/** Table of gateway responses by id (spec/gateway-responses.json): plain { status, code?, retryable?, body? } objects. */
export function loadGatewayResponses(path = new URL("../spec/gateway-responses.json", import.meta.url)) {
  const { groups } = parseSpec(JSON.parse(readFileSync(path, "utf8")), "gateway-responses");
  return new Map((groups.responses ?? []).map((r) => [r.id, r]));
}

const toReply = (r) => ({
  status: r.status,
  body: r.body ?? { ...(r.code ? { code: r.code } : {}), error: r.error ?? r.code ?? `HTTP ${r.status}`, ...(r.retryable !== undefined ? { retryable: r.retryable } : {}) },
});

/** Resolve "gateway:<id>" references (a whole `reply`, or any `{ "$ref": "gateway:<id>" }` object) to plain objects. */
export function resolveRefs(value, table = loadGatewayResponses()) {
  const lookup = (ref) => {
    const m = /^gateway:(.+)$/.exec(ref);
    const hit = m && table.get(m[1]);
    if (!hit) throw new Error(`unresolved reference ${JSON.stringify(ref)}`);
    return hit;
  };
  const walk = (v, inReply = false) => {
    if (typeof v === "string" && inReply) return toReply(lookup(v));
    if (Array.isArray(v)) return v.map((x) => walk(x));
    if (v && typeof v === "object") {
      if (typeof v.$ref === "string" && Object.keys(v).length === 1) {
        const { id: _id, ...plain } = lookup(v.$ref);
        return { ...plain };
      }
      return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, walk(x, k === "reply")]));
    }
    return v;
  };
  return walk(value);
}
