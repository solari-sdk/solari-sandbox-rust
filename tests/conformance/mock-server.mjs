#!/usr/bin/env node
// Run the mock gateway as its own process for out-of-process runners:  node mock-server.mjs [--port 8099]
import { startMockServer } from "./lib/mock-server.mjs";

const i = process.argv.indexOf("--port");
const mock = await startMockServer({ port: i > 0 ? Number(process.argv[i + 1]) : 0 });
console.log(JSON.stringify({ url: mock.url, port: mock.port }));
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => mock.close().then(() => process.exit(0)));
