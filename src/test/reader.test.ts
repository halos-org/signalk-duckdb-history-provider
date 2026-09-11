import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DATA_LAYOUT } from "../data-dir.js";
import { QueryRunner } from "../query/duck.js";
import type { QueryRequest, QueryResult } from "../query/duck.js";
import { needsHotStore, treeFilesInRange } from "../query/reader.js";
import { roll } from "../roll/roll.js";
import { dateDirectory, rollFile } from "../roll/tree-path.js";
import { writerPaths } from "../writer/contract.js";
import { HotStore } from "../writer/hot-store.js";
import { NO_BUNDLED_EXTENSION, sample } from "./fixtures.js";
import type { Sample } from "../writer/protocol.js";

/**
 * The reader, through a real spawned process and a real engine.
 *
 * The seam is what these are about: the tree and the hot store hold the same
 * rows for as long as it takes the writer to truncate, and an answer that
 * counts them twice is as wrong as one that misses them.
 */

const DAY = 86_400_000;
const AUG_23 = Date.UTC(2026, 7, 23);
const AUG_24 = AUG_23 + DAY;

let dir: string;
let store: HotStore;
let runner: QueryRunner;
let seq = 0;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "reader-"));
  mkdirSync(join(dir, DATA_LAYOUT.hotStore), { recursive: true });
  store = HotStore.open(writerPaths(dir).store);
  store.beginSession("test");
  runner = new QueryRunner({ dataDir: dir });
  seq = 0;
});

afterEach(() => {
  runner.stop();
  try {
    store.close();
  } catch {
    // Already closed by the test.
  }
  rmSync(dir, { recursive: true, force: true });
});

function record(...samples: Sample[]): void {
  seq += 1;
  store.insertBatch(seq, samples);
}

/** Records `count` numeric samples a second apart from `start`. */
function series(start: number, count: number, path = "a.b"): void {
  for (let i = 0; i < count; i += 1) {
    record(sample({ ts: start + i * 1000, path, value: i }));
  }
}

/** Rolls everything in the store under `rollId` and returns the bound used. */
async function rollAll(rollId: number): Promise<number> {
  const bound = store.rollBound();
  assert.ok(bound !== null, "nothing to roll");
  await roll({ dataDir: dir, maxRowid: bound.maxRowid, rollId });
  return bound.maxRowid;
}

/** The record the writer leaves between a roll and the truncate that follows. */
function writePendingRoll(
  rollId: number,
  maxRowid: number,
  phase: "rolling" | "written",
): void {
  writeFileSync(
    writerPaths(dir).pendingRoll,
    `${JSON.stringify({ rollId, maxRowid, phase })}\n`,
  );
}

function timestamps(result: QueryResult): number[] {
  return result.rows.map((row) => row[0] as number);
}

const range = (from: number, to: number, over: Partial<QueryRequest> = {}) =>
  ({ kind: "range", from, to, context: "self", ...over }) as QueryRequest;

describe("a query across the seam", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("returns the tree's rows and the store's rows once each, in order", async () => {
    series(AUG_23 + 1000, 5);
    const maxRowid = await rollAll(1);
    store.deleteThrough(maxRowid);
    series(AUG_23 + 60_000, 3);

    const result = await runner.run(range(AUG_23, AUG_23 + DAY));

    assert.deepEqual(timestamps(result), [
      AUG_23 + 1000,
      AUG_23 + 2000,
      AUG_23 + 3000,
      AUG_23 + 4000,
      AUG_23 + 5000,
      AUG_23 + 60_000,
      AUG_23 + 61_000,
      AUG_23 + 62_000,
    ]);
    assert.equal(result.treeFiles, 1);
  });

  it("counts a rolled row once while it is still in both", async () => {
    // The window between a roll writing its Parquet and the writer deleting
    // those rows. It is milliseconds in the ordinary case and survives a
    // restart when the delete failed, which is why the reader subtracts rather
    // than hoping.
    series(AUG_23 + 1000, 5);
    const maxRowid = await rollAll(1);
    writePendingRoll(1, maxRowid, "written");
    series(AUG_23 + 60_000, 2);

    const result = await runner.run(range(AUG_23, AUG_23 + DAY));

    assert.deepEqual(timestamps(result), [
      AUG_23 + 1000,
      AUG_23 + 2000,
      AUG_23 + 3000,
      AUG_23 + 4000,
      AUG_23 + 5000,
      AUG_23 + 60_000,
      AUG_23 + 61_000,
    ]);
  });

  it("keeps the store's copy of a date the roll never wrote", async () => {
    // A roll killed between two date directories. Subtracting its whole bound
    // would drop the day it never got to, which is a gap — worse than the
    // duplicate the subtraction exists to prevent.
    series(AUG_23 + 1000, 2);
    series(AUG_24 + 1000, 2);
    const maxRowid = await rollAll(7);
    rmSync(rollFile(dir, AUG_24, 7));
    writePendingRoll(7, maxRowid, "rolling");

    const result = await runner.run(range(AUG_23, AUG_24 + DAY));

    assert.deepEqual(timestamps(result), [
      AUG_23 + 1000,
      AUG_23 + 2000,
      AUG_24 + 1000,
      AUG_24 + 2000,
    ]);
  });

  it("ignores a pending record whose file has not appeared", async () => {
    // The record is written before the roll is spawned, so this is the state
    // every roll passes through. Nothing is in the tree yet, so nothing may be
    // subtracted from the store.
    series(AUG_23 + 1000, 3);
    const bound = store.rollBound();
    assert.ok(bound !== null);
    writePendingRoll(1, bound.maxRowid, "rolling");

    const result = await runner.run(range(AUG_23, AUG_23 + DAY));

    assert.equal(result.rows.length, 3);
    assert.equal(result.treeFiles, 0);
  });
});

describe("what a query reads", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("opens no tree file for a range the hot store covers alone", async () => {
    series(AUG_23 + 1000, 2);
    store.deleteThrough(await rollAll(1));
    series(AUG_24 + 1000, 2);

    const result = await runner.run(range(AUG_24, AUG_24 + DAY));

    assert.equal(result.treeFiles, 0, "a dated directory outside the range");
    assert.equal(result.rows.length, 2);
  });

  it("answers a range before anything was recorded with no rows", async () => {
    series(AUG_23 + 1000, 2);
    store.deleteThrough(await rollAll(1));

    const result = await runner.run(
      range(AUG_23 - 30 * DAY, AUG_23 - 29 * DAY),
    );

    assert.deepEqual(result.rows, []);
    assert.equal(result.truncated, false);
  });

  it("answers on a data directory with neither a tree nor a store", async () => {
    const empty = mkdtempSync(join(tmpdir(), "reader-empty-"));
    const fresh = new QueryRunner({ dataDir: empty });
    try {
      const result = await fresh.run(range(AUG_23, AUG_23 + DAY));
      assert.deepEqual(result.rows, []);
    } finally {
      fresh.stop();
      rmSync(empty, { recursive: true, force: true });
    }
  });

  it("picks up a hot store that appeared after the engine started", async () => {
    // The first minute of a device's life: the plugin starts, the first query
    // arrives, and the writer has not created the store yet. The engine is
    // held open across both, so the store is attached per query rather than at
    // startup — which is what makes this answer rather than stay empty.
    const later = mkdtempSync(join(tmpdir(), "reader-later-"));
    const fresh = new QueryRunner({ dataDir: later });
    try {
      const before = await fresh.run(range(AUG_23, AUG_23 + DAY));
      assert.deepEqual(before.rows, [], "nothing has been recorded yet");

      mkdirSync(join(later, DATA_LAYOUT.hotStore), { recursive: true });
      const appeared = HotStore.open(writerPaths(later).store);
      appeared.beginSession("later");
      appeared.insertBatch(1, [sample({ ts: AUG_23 + 1000, path: "a.b" })]);
      appeared.close();

      const after = await fresh.run(range(AUG_23, AUG_23 + DAY));

      assert.equal(after.rows.length, 1);
      assert.equal(fresh.running, true, "and on the same engine");
    } finally {
      fresh.stop();
      rmSync(later, { recursive: true, force: true });
    }
  });

  it("answers a second query from the same engine", async () => {
    series(AUG_23 + 1000, 2);
    store.deleteThrough(await rollAll(1));
    series(AUG_23 + 60_000, 2);

    const first = await runner.run(range(AUG_23, AUG_23 + DAY));
    const second = await runner.run(range(AUG_23, AUG_23 + DAY));

    assert.equal(first.rows.length, 4);
    assert.deepEqual(second.rows, first.rows);
    assert.equal(runner.running, true);
    // The engine that answered both is the same one, so the second query paid
    // no startup — the only figure this side can see of that is that it was
    // quicker, and a timing assertion in CI is not worth having. `duck.test.ts`
    // asserts the process count instead.
  });

  it("refuses a request whose shape reached it from JSON", async () => {
    // The type the request is cast to on arrival is a claim, not a check. An
    // unrecognised `kind` used to compile to the contexts query and answer it,
    // and a `limit` that is not a number used to compile to `LIMIT NaN`.
    series(AUG_23 + 1000, 2);
    const refuses = async (request: unknown, why: RegExp) => {
      await assert.rejects(
        runner.run(request as QueryRequest),
        (err: Error) => why.test(err.message),
        `${JSON.stringify(request)} was not refused`,
      );
    };

    await refuses(
      { kind: "everything", from: AUG_23, to: AUG_23 + DAY, context: "self" },
      /is not a query kind/,
    );
    await refuses(
      range(AUG_23, AUG_23 + DAY, { limit: "x" as unknown as number }),
      /limit must be a number/,
    );
    await refuses(
      { kind: "range", from: AUG_23, to: undefined, context: "self" },
      /to must be a timestamp/,
    );
    await refuses({ kind: "snapshot", at: "x" }, /at must be a timestamp/);
    await refuses(
      { kind: "snapshot", at: AUG_23, paths: "a.b" },
      /paths must be an array/,
    );
  });

  it("opens no tree file for a snapshot the sidecar can answer", async () => {
    // The claim the snapshot rests on: one row per key, written by every roll,
    // so "the last value of everything now" needs the tree only when a key's
    // newest row is newer than the instant asked for. Here none is, and the
    // path's rows are not in the store any more either.
    series(AUG_23 + 1000, 2, "a.b");
    const maxRowid = await rollAll(1);
    store.deleteThrough(maxRowid);

    const result = await runner.run({
      kind: "snapshot",
      at: AUG_23 + DAY,
    });

    assert.equal(result.treeFiles, 0);
    assert.deepEqual(
      result.rows.map((row) => [row[2], row[5]]),
      [["a.b", 1]],
    );
  });

  it("reads the tree for a snapshot of an instant a later row has replaced", async () => {
    series(AUG_23 + 1000, 2, "a.b");
    const maxRowid = await rollAll(1);
    store.deleteThrough(maxRowid);

    const result = await runner.run({
      kind: "snapshot",
      at: AUG_23 + 1000,
    });

    assert.equal(result.treeFiles, 1);
    assert.deepEqual(
      result.rows.map((row) => [row[2], row[5]]),
      [["a.b", 0]],
    );
  });

  it("skips a partition a killed roll left half-written", async () => {
    series(AUG_23 + 1000, 2);
    const maxRowid = await rollAll(1);
    store.deleteThrough(maxRowid);
    // What a roll killed mid-`COPY` leaves. It is not Parquet at all, so a
    // reader that picked it up would fail rather than return a subset — which
    // is the assertion: the query succeeds.
    writeFileSync(join(dateDirectory(dir, AUG_23), "9999.parquet.tmp"), "no");

    const result = await runner.run(range(AUG_23, AUG_23 + DAY));

    assert.equal(result.rows.length, 2);
    assert.equal(result.treeFiles, 1);
  });

  it("returns only the paths the request named", async () => {
    series(AUG_23 + 1000, 2, "a.b");
    series(AUG_23 + 1000, 2, "c.d");
    series(AUG_23 + 1000, 2, "e.f");
    store.deleteThrough(await rollAll(1));

    const result = await runner.run(
      range(AUG_23, AUG_23 + DAY, { paths: ["a.b", "e.f"] }),
    );

    assert.deepEqual([...new Set(result.rows.map((row) => row[2]))].sort(), [
      "a.b",
      "e.f",
    ]);
  });

  it("reports a truncated answer rather than returning everything", async () => {
    series(AUG_23 + 1000, 6);

    const result = await runner.run(range(AUG_23, AUG_23 + DAY, { limit: 4 }));

    assert.equal(result.rows.length, 4);
    assert.equal(result.truncated, true);
    assert.deepEqual(timestamps(result), [
      AUG_23 + 1000,
      AUG_23 + 2000,
      AUG_23 + 3000,
      AUG_23 + 4000,
    ]);
  });

  it("lists the paths and contexts in a range from both sources", async () => {
    series(AUG_23 + 1000, 1, "a.b");
    record(sample({ ts: AUG_23 + 2000, path: "c.d", context: "other" }));
    store.deleteThrough(await rollAll(1));
    series(AUG_23 + 60_000, 1, "e.f");

    const paths = await runner.run({
      kind: "paths",
      from: AUG_23,
      to: AUG_23 + DAY,
      context: "self",
    });
    const contexts = await runner.run({
      kind: "contexts",
      from: AUG_23,
      to: AUG_23 + DAY,
    });

    assert.deepEqual(paths.rows, [["a.b"], ["e.f"]]);
    assert.deepEqual(contexts.rows, [["other"], ["self"]]);
  });

  it("carries every column of a row through unchanged", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "n.p", kind: "position" }),
      sample({
        ts: AUG_23 + 2000,
        path: "s.t",
        kind: "string",
        value: "moored",
      }),
    );
    store.deleteThrough(await rollAll(1));

    const result = await runner.run(range(AUG_23, AUG_23 + DAY));

    assert.deepEqual(result.rows[0], [
      AUG_23 + 1000,
      "self",
      "n.p",
      "n2k.0",
      "position",
      null,
      null,
      60.16,
      24.94,
    ]);
    assert.deepEqual(result.rows[1], [
      AUG_23 + 2000,
      "self",
      "s.t",
      "n2k.0",
      "string",
      null,
      "moored",
      null,
      null,
    ]);
  });
});

describe("a values query", { skip: NO_BUNDLED_EXTENSION }, () => {
  const values = (over: Partial<QueryRequest> = {}) =>
    ({
      kind: "values",
      from: AUG_23,
      to: AUG_23 + DAY,
      context: "self",
      bucketMs: 10_000,
      specs: [{ path: "a.b", aggregate: "average" }],
      ...over,
    }) as QueryRequest;

  it("reduces each bucket and skips the ones with nothing in them", async () => {
    // Two rows in the first bucket, one in the third, nothing in the second.
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", value: 10 }),
      sample({ ts: AUG_23 + 2000, path: "a.b", value: 20 }),
      sample({ ts: AUG_23 + 25_000, path: "a.b", value: 7 }),
    );

    const result = await runner.run(values());

    // spec, bucket, num — the empty bucket is the caller's to fabricate.
    assert.deepEqual(
      result.rows.map((row) => [row[0], row[1], row[2]]),
      [
        [0, AUG_23, 15],
        [0, AUG_23 + 20_000, 7],
      ],
    );
  });

  it("answers every series in one request", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }),
      sample({ ts: AUG_23 + 1000, path: "c.d", value: 2 }),
    );

    const result = await runner.run(
      values({
        specs: [
          { path: "a.b", aggregate: "average" },
          { path: "c.d", aggregate: "average" },
        ],
      }),
    );

    assert.deepEqual(
      result.rows.map((row) => [row[0], row[2]]),
      [
        [0, 1],
        [1, 2],
      ],
    );
  });

  it("picks first and last by timestamp rather than by arrival", async () => {
    // Written out of order on purpose: DuckDB's own first()/last() are
    // undefined within a group, so a reduction that used them would pass or
    // fail on the scan order.
    record(
      sample({ ts: AUG_23 + 5000, path: "a.b", value: 50 }),
      sample({ ts: AUG_23 + 1000, path: "a.b", value: 10 }),
      sample({ ts: AUG_23 + 9000, path: "a.b", value: 90 }),
    );

    const first = await runner.run(
      values({ specs: [{ path: "a.b", aggregate: "first" }] }),
    );
    const last = await runner.run(
      values({ specs: [{ path: "a.b", aggregate: "last" }] }),
    );
    const mid = await runner.run(
      values({ specs: [{ path: "a.b", aggregate: "mid" }] }),
    );

    assert.equal(first.rows[0][2], 10);
    assert.equal(last.rows[0][2], 90);
    assert.equal(mid.rows[0][2], 50);
  });

  it("returns a position that was recorded, not one assembled per axis", async () => {
    // Two sources in one bucket. Taking latitude from one and longitude from
    // the other would put the vessel somewhere it has never been.
    record(
      sample({
        ts: AUG_23 + 1000,
        path: "navigation.position",
        kind: "position",
        source: "gps.1",
        value: { latitude: 60.1, longitude: 24.1 },
      }),
      sample({
        ts: AUG_23 + 2000,
        path: "navigation.position",
        kind: "position",
        source: "gps.2",
        value: { latitude: 61.9, longitude: 25.9 },
      }),
    );

    const first = await runner.run(
      values({ specs: [{ path: "navigation.position", aggregate: "first" }] }),
    );
    const last = await runner.run(
      values({ specs: [{ path: "navigation.position", aggregate: "last" }] }),
    );

    assert.deepEqual([first.rows[0][5], first.rows[0][6]], [60.1, 24.1]);
    assert.deepEqual([last.rows[0][5], last.rows[0][6]], [61.9, 25.9]);
  });

  it("carries a text value and its kind, so a boolean stays a boolean", async () => {
    record(
      sample({
        ts: AUG_23 + 1000,
        path: "s.t",
        kind: "boolean",
        value: "false",
      }),
      sample({
        ts: AUG_23 + 5000,
        path: "s.t",
        kind: "boolean",
        value: "true",
      }),
    );

    const result = await runner.run(
      values({ specs: [{ path: "s.t", aggregate: "average" }] }),
    );

    // The numeric reduction has nothing to work with; the text one takes the
    // value in force at the end of the bucket.
    assert.equal(result.rows[0][2], null);
    assert.equal(result.rows[0][3], "true");
    assert.equal(result.rows[0][4], "boolean");
  });

  it("restricts a series to one source when asked", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", source: "n2k.0", value: 1 }),
      sample({ ts: AUG_23 + 2000, path: "a.b", source: "n2k.9", value: 99 }),
    );

    const result = await runner.run(
      values({
        specs: [{ path: "a.b", aggregate: "average", sourceRef: "n2k.9" }],
      }),
    );

    assert.equal(result.rows[0][2], 99);
  });

  it("returns rows unreduced for a client-side aggregate", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }),
      sample({ ts: AUG_23 + 2000, path: "a.b", value: 2 }),
      sample({ ts: AUG_23 + 3000, path: "a.b", value: 3 }),
    );

    const result = await runner.run(
      values({ specs: [{ path: "a.b", aggregate: "raw" }] }),
    );

    assert.deepEqual(
      result.rows.map((row) => [row[1], row[2]]),
      [
        [AUG_23 + 1000, 1],
        [AUG_23 + 2000, 2],
        [AUG_23 + 3000, 3],
      ],
    );
  });

  it("reads the tree and the store as one series", async () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b", value: 10 }));
    store.deleteThrough(await rollAll(1));
    record(sample({ ts: AUG_23 + 25_000, path: "a.b", value: 30 }));

    const result = await runner.run(values());

    assert.deepEqual(
      result.rows.map((row) => [row[1], row[2]]),
      [
        [AUG_23, 10],
        [AUG_23 + 20_000, 30],
      ],
    );
  });

  it("refuses an aggregate it does not know", async () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b" }));
    await assert.rejects(
      runner.run(
        values({
          specs: [{ path: "a.b", aggregate: "median" as never }],
        }),
      ),
      /is not an aggregate/,
    );
  });

  it("holds a raw series to the ceiling the caller sent", async () => {
    // The caller reduces a raw series in the Signal K process, so its limit is
    // the budget for that. It used to be discarded, and every raw branch
    // returned the whole-answer default instead.
    for (let i = 0; i < 6; i += 1) {
      record(sample({ ts: AUG_23 + i * 1000, path: "a.b", value: i }));
    }

    const result = await runner.run(
      values({
        specs: [{ path: "a.b", aggregate: "raw" }],
        limit: 3,
      }),
    );

    assert.deepEqual(
      result.rows.map((row) => row[2]),
      [0, 1, 2],
    );
  });

  it("holds each series to that ceiling rather than the answer to it", async () => {
    // The rows arrive ordered by bucket and then by spec, so a ceiling applied
    // to the answer would drop the trailing series outright instead of
    // shortening each of them.
    for (let i = 0; i < 4; i += 1) {
      record(
        sample({ ts: AUG_23 + i * 1000, path: "a.b", value: i }),
        sample({ ts: AUG_23 + i * 1000, path: "c.d", value: 10 + i }),
      );
    }

    const result = await runner.run(
      values({
        specs: [
          { path: "a.b", aggregate: "raw" },
          { path: "c.d", aggregate: "raw" },
        ],
        limit: 2,
      }),
    );

    assert.deepEqual(
      result.rows.map((row) => [row[0], row[2]]),
      [
        [0, 0],
        [1, 10],
        [0, 1],
        [1, 11],
      ],
    );
  });

  it("refuses more series than one statement may carry", async () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b" }));
    const specs = Array.from({ length: 201 }, () => ({
      path: "a.b",
      aggregate: "average" as const,
    }));

    await assert.rejects(
      runner.run(values({ specs })),
      /may name 200 series; this one names 201/,
    );
  });
});

describe("the tree's file selection", () => {
  it("keeps the dates that intersect the range and no others", async () => {
    for (const date of ["2026-08-22", "2026-08-23", "2026-08-24"]) {
      const directory = join(dir, DATA_LAYOUT.tree, `date=${date}`);
      mkdirSync(directory, { recursive: true });
      writeFileSync(join(directory, "1.parquet"), "");
    }

    const files = treeFilesInRange(dir, AUG_23 + 1000, AUG_23 + 2000);

    assert.deepEqual(
      files.map((file) => file.name),
      ["1.parquet"],
    );
    assert.match(files[0].path, /date=2026-08-23/);
  });

  it("takes the day either side when the range crosses midnight", () => {
    for (const date of ["2026-08-23", "2026-08-24", "2026-08-25"]) {
      const directory = join(dir, DATA_LAYOUT.tree, `date=${date}`);
      mkdirSync(directory, { recursive: true });
      writeFileSync(join(directory, "1.parquet"), "");
    }

    const files = treeFilesInRange(dir, AUG_23 + DAY - 1000, AUG_24 + 1000);

    assert.deepEqual(
      files.map((file) => file.day),
      [AUG_23, AUG_24],
    );
  });

  it("ignores a directory that does not name a date", () => {
    for (const name of ["date=not-a-date", "date=2026-08-32", "notes", ""]) {
      if (name === "") continue;
      const directory = join(dir, DATA_LAYOUT.tree, name);
      mkdirSync(directory, { recursive: true });
      writeFileSync(join(directory, "1.parquet"), "");
    }

    assert.deepEqual(treeFilesInRange(dir, 0, Date.UTC(2100, 0, 1)), []);
  });

  it("returns nothing rather than failing when there is no tree", () => {
    assert.deepEqual(treeFilesInRange(dir, 0, Date.UTC(2100, 0, 1)), []);
  });
});

/**
 * The hot store is read whenever a window could hold one of its rows, and not
 * otherwise.
 *
 * `sqlite_scanner` does not push a predicate into SQLite -- the plan puts a
 * FILTER above a SQLITE_SCAN that emits the whole table -- so a branch that
 * matches nothing still costs a scan of every row the current roll interval
 * has accumulated. Measured on a device: an unchanged query over a window
 * entirely inside the tree ran 25 ms with 2k rows in the store and 157 ms with
 * 197k, tracking the store's size at 0.708 ms per 1000 rows and resetting at
 * each roll. Indexing does not help, because nothing reaches the index.
 */
describe("needsHotStore", () => {
  it("skips a window that ends at or before the oldest row", () => {
    assert.equal(needsHotStore(1000, 1000), false);
    assert.equal(needsHotStore(1000, 999), false);
  });

  it("reads a window that reaches past the oldest row", () => {
    assert.equal(needsHotStore(1000, 1001), true);
  });

  /**
   * Only the oldest row is safe to decide on. It only ever moves forward -- a
   * roll truncates from the front and the writer appends to the back -- so a
   * window that ends before it stays that way. The newest row moves the other
   * way, and a window opening after it can be filled by the writer between the
   * planning read and the query.
   */
  it("reads an empty store, which the writer may fill mid-query", () => {
    assert.equal(needsHotStore(null, 1), true);
  });
});

/**
 * A sample whose timestamp precedes rows already stored.
 *
 * `insertBatch` enforces no timestamp order, and `ts` is the recorder's clock
 * reading at arrival -- which steps backwards when a device corrects its clock
 * at boot, the case `path-matcher.ts` already treats as a discontinuity. So the
 * lowest rowid does not carry the lowest `ts`, and a boundary read from the
 * first row would place the store later than it reaches. The query would then
 * skip it and answer without a row the store holds, silently.
 */
describe("an out-of-order sample still bounds the hot store", () => {
  it("reads a window the earliest row falls in, whatever its rowid", async () => {
    const late = AUG_23 + 60_000;
    const early = AUG_23 + 1_000;
    // Stored newest-first, so rowid order and timestamp order disagree.
    record(sample({ ts: late, path: "a.b", value: 1 }));
    record(sample({ ts: early, path: "a.b", value: 2 }));

    const rows = await runner.run({
      kind: "range",
      from: early,
      to: early + 1_000,
      context: "self",
    });
    assert.equal(
      rows.rows.length,
      1,
      "the window holds the out-of-order sample and must not be skipped",
    );
  });
});
