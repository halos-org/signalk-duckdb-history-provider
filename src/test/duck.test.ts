import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  QueryFailedError,
  QueryOverloadedError,
  QueryRunner,
  QueryTimeoutError,
} from "../query/duck.js";
import type { QueryRequest } from "../query/duck.js";
import { eventually } from "./fixtures.js";

/**
 * The service side, without an engine.
 *
 * What matters here is that one process answers every request, that a failure
 * costs the request rather than the process, and that a process which dies is
 * replaced. None of it needs DuckDB; `reader.test.ts` covers what the queries
 * answer.
 */

let dir: string;
let runner: QueryRunner | null = null;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "query-"));
});

afterEach(() => {
  runner?.stop();
  runner = null;
  rmSync(dir, { recursive: true, force: true });
});

const RANGE: QueryRequest = {
  kind: "range",
  from: 0,
  to: 1,
  context: "self",
  paths: ["a.b", "c.d", "e.f"],
};

const child = (script: string): ChildProcess =>
  spawn(process.execPath, ["-e", script], {
    stdio: ["pipe", "pipe", "pipe"],
  });

/**
 * A stand-in service: reads a request per line and answers each one, staying
 * up in between, which is the whole shape under test.
 *
 * `perRequest` is JavaScript run with `req` in scope; it writes the answer.
 */
function service(perRequest: string): () => ChildProcess {
  return () =>
    child(
      `let buf = "";` +
        `process.stdin.setEncoding("utf8");` +
        `process.stdin.on("data", (c) => {` +
        `  buf += c;` +
        `  let cut;` +
        `  while ((cut = buf.indexOf("\\n")) >= 0) {` +
        `    const line = buf.slice(0, cut); buf = buf.slice(cut + 1);` +
        `    if (line === "") continue;` +
        `    const req = JSON.parse(line);` +
        `    ${perRequest}` +
        `  }` +
        `});`,
    );
}

/** Answers with one row and a valid summary. */
const ANSWERS = service(
  `console.log(JSON.stringify([1, "self", "a.b"]));` +
    `console.log(JSON.stringify({id: req.id, rows: 1, truncated: false, treeFiles: 2}));`,
);

const HANGS = (): ChildProcess => child("setTimeout(() => {}, 60000)");

function make(
  over: Partial<ConstructorParameters<typeof QueryRunner>[0]> = {},
): QueryRunner {
  runner = new QueryRunner({ dataDir: dir, spawnQuery: ANSWERS, ...over });
  return runner;
}

/** Counts starts, so "one process" can be asserted rather than assumed. */
function counted(spawnQuery: () => ChildProcess): {
  spawn: () => ChildProcess;
  starts: () => number;
} {
  let starts = 0;
  return {
    spawn: () => {
      starts += 1;
      return spawnQuery();
    },
    starts: () => starts,
  };
}

describe("the query service", () => {
  it("compiles every path in one request into one process", async () => {
    const fake = counted(ANSWERS);
    const result = await make({ spawnQuery: fake.spawn }).run(RANGE);

    assert.equal(fake.starts(), 1, "three paths must not cost three processes");
    assert.deepEqual(result.rows, [[1, "self", "a.b"]]);
    assert.equal(result.treeFiles, 2);
    assert.equal(result.truncated, false);
  });

  it("answers a second request from the same process", async () => {
    // The reason the service exists: starting an engine costs 336–375 ms on
    // the device, more than an ordinary request costs to answer.
    const fake = counted(ANSWERS);
    const runner = make({ spawnQuery: fake.spawn });

    await runner.run(RANGE);
    await runner.run(RANGE);
    await runner.run({ kind: "contexts", from: 0, to: 1 });

    assert.equal(fake.starts(), 1, "three requests started three processes");
    assert.equal(runner.running, true);
  });

  it("starts nothing until something is asked", async () => {
    const fake = counted(ANSWERS);
    const runner = make({ spawnQuery: fake.spawn });
    assert.equal(fake.starts(), 0);
    assert.equal(runner.running, false);
    await runner.run(RANGE);
    assert.equal(fake.starts(), 1);
  });

  it("hands the request to the service on stdin", async () => {
    const echo = service(
      `console.log(JSON.stringify([req.paths.length, req.kind]));` +
        `console.log(JSON.stringify({id: req.id, rows: 1, truncated: false, treeFiles: 0}));`,
    );
    const result = await make({ spawnQuery: echo }).run(RANGE);
    assert.deepEqual(result.rows, [[3, "range"]]);
  });

  it("reassembles a row whose text is split across two chunks", async () => {
    // A vessel name or a notification message can put a multi-byte character
    // on a 64 kB boundary. Decoding each chunk on its own replaces the split
    // bytes with U+FFFD in both halves, and the mangled row still parses as
    // JSON — so it would be returned as data rather than reported.
    const written = JSON.stringify([1, "self", "n.name", "Kärppä ⛵"]);
    const bytes = Buffer.from(`${written}\n`, "utf8");
    const cut = bytes.indexOf(Buffer.from("ä", "utf8")) + 1;
    const splitWriter = service(
      `const b = Buffer.from(${JSON.stringify(bytes.toString("base64"))}, "base64");` +
        `process.stdout.write(b.subarray(0, ${cut}));` +
        `setTimeout(() => {` +
        `  process.stdout.write(b.subarray(${cut}));` +
        `  console.log(JSON.stringify({id: req.id, rows: 1, truncated: false, treeFiles: 0}));` +
        `}, 10);`,
    );

    const result = await make({ spawnQuery: splitWriter }).run(RANGE);

    assert.deepEqual(result.rows, [[1, "self", "n.name", "Kärppä ⛵"]]);
  });

  it("passes the row limit's verdict through", async () => {
    const truncating = service(
      `console.log(JSON.stringify([1]));` +
        `console.log(JSON.stringify({id: req.id, rows: 1, truncated: true, treeFiles: 9}));`,
    );
    const result = await make({ spawnQuery: truncating }).run(RANGE);
    assert.equal(result.truncated, true);
  });

  it("reports the service's memory, which is what it will not give back", async () => {
    const reporting = service(
      `console.log(JSON.stringify({id: req.id, rows: 0, truncated: false, treeFiles: 0,` +
        ` rssBytes: 123456789, peakRssBytes: 234567890}));`,
    );
    const result = await make({ spawnQuery: reporting }).run(RANGE);
    assert.equal(result.rssBytes, 123_456_789);
    assert.equal(result.peakRssBytes, 234_567_890);
  });
});

describe("a query that fails", () => {
  it("costs the request and not the process", async () => {
    // The engine is worth more than one answer. A bad range must not make the
    // next request pay to start a new service.
    const fake = counted(
      service(
        `if (req.kind === "range") {` +
          `  console.log(JSON.stringify({id: req.id, error: "the tree could not be read"}));` +
          `} else {` +
          `  console.log(JSON.stringify({id: req.id, rows: 0, truncated: false, treeFiles: 0}));` +
          `}`,
      ),
    );
    const runner = make({ spawnQuery: fake.spawn });

    await assert.rejects(
      runner.run(RANGE),
      (err: Error) =>
        err instanceof QueryFailedError &&
        err.message === "the tree could not be read",
    );
    await runner.run({ kind: "contexts", from: 0, to: 1 });

    assert.equal(fake.starts(), 1, "the failure took the service with it");
  });

  it("refuses an answer with no summary", async () => {
    // The service died mid-answer. Half an answer must not be reported as a
    // whole one.
    const halfAnswer = service(
      `console.log(JSON.stringify([1, "self", "a.b"])); process.exit(0);`,
    );
    await assert.rejects(
      make({ spawnQuery: halfAnswer }).run(RANGE),
      (err: Error) =>
        err instanceof QueryFailedError &&
        /exited with code 0/.test(err.message),
    );
  });

  it("reports what the service said as it died, not a stack frame", async () => {
    const dies = service(
      `console.error("the store is corrupt\\n    at some.frame"); process.exit(1);`,
    );
    await assert.rejects(
      make({ spawnQuery: dies }).run(RANGE),
      (err: Error) =>
        err instanceof QueryFailedError &&
        /code 1: the store is corrupt$/.test(err.message),
    );
  });

  it("starts a new service for the next request after one dies", async () => {
    const fake = counted(
      service(
        `if (req.id === 1) { process.exit(7); }` +
          `console.log(JSON.stringify({id: req.id, rows: 0, truncated: false, treeFiles: 0}));`,
      ),
    );
    const errors: string[] = [];
    const runner = make({
      spawnQuery: fake.spawn,
      onError: (l) => errors.push(l),
    });

    await assert.rejects(runner.run(RANGE), QueryFailedError);
    const second = await runner.run(RANGE);

    assert.equal(fake.starts(), 2, "the second request reused a dead process");
    assert.equal(second.rows.length, 0);
    assert.match(errors.join("\n"), /exited with code 7/);
  });

  it("does not let a dead process take its replacement down with it", async () => {
    // The exit of a process this side already killed arrives an event loop
    // turn or more later — by which time the next request has started a
    // replacement and is being served by it. Acting on that exit rejects the
    // wrong request and kills the wrong process.
    let started = 0;
    const runner = make({
      // Long enough for the replacement, a real node process, to start and
      // answer beside other suites.
      timeoutMs: 2_000,
      spawnQuery: () => {
        started += 1;
        return started === 1
          ? HANGS()
          : service(
              `console.log(JSON.stringify({id: req.id, rows: 0, truncated: false, treeFiles: 0}));`,
            )();
      },
    });

    await assert.rejects(runner.run(RANGE), QueryTimeoutError);
    const second = await runner.run(RANGE);

    assert.equal(started, 2, "the replacement was killed and respawned");
    assert.equal(second.rows.length, 0);
    assert.equal(runner.running, true, "the replacement was taken down");
  });

  it("restarts rather than guessing when the answers get out of step", async () => {
    const confused = service(
      `console.log(JSON.stringify({id: req.id + 100, rows: 0, truncated: false, treeFiles: 0}));`,
    );
    const runner = make({ spawnQuery: confused });

    await assert.rejects(
      runner.run(RANGE),
      (err: Error) =>
        err instanceof QueryFailedError &&
        /while 1 was outstanding/.test(err.message),
    );

    // The restart is the recovery, and the message alone would pass with a
    // desynchronised process left running.
    assert.equal(runner.running, false);
  });
});

describe("the queue", () => {
  it("answers one at a time, and answers all of them", async () => {
    const fake = counted(
      service(
        `setTimeout(() => {` +
          `  console.log(JSON.stringify([req.id]));` +
          `  console.log(JSON.stringify({id: req.id, rows: 1, truncated: false, treeFiles: 0}));` +
          `}, 20);`,
      ),
    );
    const runner = make({ spawnQuery: fake.spawn });

    const all = await Promise.all(
      Array.from({ length: 6 }, () => runner.run(RANGE)),
    );

    assert.equal(fake.starts(), 1);
    assert.deepEqual(
      all.map((result) => result.rows[0][0]),
      [1, 2, 3, 4, 5, 6],
      "answers came back out of order",
    );
  });

  it("refuses rather than queueing without limit", async () => {
    const runner = make({ spawnQuery: HANGS, maxQueued: 2 });
    const accepted = [runner.run(RANGE), runner.run(RANGE), runner.run(RANGE)];
    // Attached before the rejection can arrive: an unhandled rejection ends
    // the test process under Node's default.
    for (const promise of accepted) promise.catch(() => {});

    await assert.rejects(
      runner.run(RANGE),
      (err: Error) =>
        err instanceof QueryOverloadedError &&
        /a query is running and 2 are already waiting/.test(err.message),
    );
    assert.deepEqual(runner.pendingWork, { active: 1, queued: 2 });
  });

  it("kills a query that outlives its deadline", async () => {
    const runner = make({ spawnQuery: HANGS, timeoutMs: 100 });
    const started = Date.now();
    await assert.rejects(
      runner.run(RANGE),
      (err: Error) =>
        err instanceof QueryTimeoutError && /within 100 ms/.test(err.message),
    );
    assert.ok(Date.now() - started < 5000, "the deadline did not end the wait");
    // The turn has to come back, or one hung query costs the plugin its
    // ability to answer anything.
    assert.equal(runner.pendingWork.active, 0);
  });

  it("spends the deadline on the queue as well as on the query", async () => {
    // A request that spent its whole deadline waiting has a client that has
    // stopped waiting too, so it must not then take a turn.
    const fake = counted(HANGS);
    const runner = make({ spawnQuery: fake.spawn, timeoutMs: 80 });
    const first = runner.run(RANGE);
    first.catch(() => {});

    await assert.rejects(runner.run(RANGE), QueryTimeoutError);

    assert.equal(fake.starts(), 1, "the queued query started a second service");
  });

  it("drops the queue and stops the service when the plugin stops", async () => {
    const runner = make({ spawnQuery: HANGS });
    const running = runner.run(RANGE);
    const queued = runner.run(RANGE);
    const outcomes: string[] = [];
    running.catch((err: Error) => outcomes.push(err.name));
    queued.catch((err: Error) => outcomes.push(err.name));

    runner.stop();

    await eventually(() => outcomes.length === 2, "both queries to end");
    assert.deepEqual(outcomes.sort(), ["QueryFailedError", "QueryFailedError"]);
    assert.equal(runner.running, false);
    await assert.rejects(runner.run(RANGE), /stopping/);
  });
});
