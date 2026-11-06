#!/usr/bin/env node
// Print every row of spec/{wire,errors,retry}.json with its gateway references resolved, as one JSON array, for runners that are
// not written in JavaScript:  node dump-rows.mjs  ->  [{ key: "<group>/<id>", id, op, input, mock, expect, ... }]
// Not part of the vendored copy (SOURCE.txt): it only uses lib/spec.mjs.
import { loadSpec, resolveRefs } from "./lib/spec.mjs";

const rows = [];
for (const f of ["wire", "errors", "retry"]) {
  const spec = loadSpec(new URL(`./spec/${f}.json`, import.meta.url));
  if (spec.errors.length) throw new Error(`spec/${f}.json is invalid: ${spec.errors.join("; ")}`);
  for (const [group, list] of Object.entries(spec.groups)) {
    for (const row of list) rows.push({ ...row, key: `${group}/${row.id}`, mock: row.mock ? resolveRefs(row.mock) : undefined });
  }
}
console.log(JSON.stringify(rows));
