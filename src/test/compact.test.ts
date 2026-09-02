import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { compactHour } from "../compact/compact.js";
import { HOUR_MS } from "../compact/plan.js";
import { DATA_LAYOUT } from "../data-dir.js";
import { QueryRunner } from "../query/duck.js";
import type { QueryRequest } from "../query/duck.js";
import { roll } from "../roll/roll.js";
import { compactedFile, dateDirectory } from "../roll/tree-path.js";
import { writerPaths } from "../writer/contract.js";
import { HotStore } from "../writer/hot-store.js";
import { sample } from "./fixtures.js";
import type { Sample } from "../writer/protocol.js";

/**
 * A merged hour against the rolls it replaced, through the real reader.
 *
 * The assertion that matters is not that the merge produced a file: it is that
 * a query cannot tell the difference. Every row, once, in the same order.
 */

const DAY = Date.UTC(2026, 8, 2);
const HOUR = DAY + 11 * HOUR_MS;

let dir: string;
let store: HotStore;
let runner: QueryRunner;
let seq = 0;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "compact-"));
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
    /* already closed */
  }
  rmSync(dir, { recursive: true, force: true });
});

/** Records `count` samples a second apart from `start`, then rolls them. */
async function rollSlice(
  start: number,
  count: number,
  rollId: number,
): Promise<void> {
  const samples: Sample[] = [];
  for (let i = 0; i < count; i += 1) {
    samples.push(sample({ ts: start + i * 1000, path: i % 2 ? "a.b" : "c.d" }));
  }
  seq += 1;
  store.insertBatch(seq, samples);
  const bound = store.rollBound();
  assert.notEqual(bound, null);
  await roll({ dataDir: dir, rollId, maxRowid: bound!.maxRowid });
  store.deleteThrough(bound!.maxRowid);
}

async function read(request: QueryRequest): Promise<unknown[][]> {
  return (await runner.run(request)).rows;
}

/** Twelve five-minute rolls covering the hour, as the scheduler would write them. */
async function fillHour(): Promise<void> {
  for (let i = 0; i < 12; i += 1) {
    await rollSlice(HOUR + i * 300_000, 20, HOUR + (i + 1) * 300_000);
  }
}

describe("compactHour", () => {
  it("answers a range exactly as the rolls it replaced did", async () => {
    await fillHour();
    const request: QueryRequest = {
      kind: "range",
      from: HOUR,
      to: HOUR + HOUR_MS,
      context: "self",
    };
    const before = await read(request);
    assert.equal(before.length, 240);

    const result = await compactHour({ dataDir: dir, hourStartMs: HOUR });
    assert.equal(result.files.length, 1);
    assert.equal(result.files[0].rows, 240);
    assert.deepEqual(result.strayInputs, []);

    assert.deepEqual(await read(request), before);
  });

  it("leaves one file where there were twelve", async () => {
    await fillHour();
    const directory = dateDirectory(dir, HOUR);
    assert.equal(readdirSync(directory).length, 12);

    await compactHour({ dataDir: dir, hourStartMs: HOUR });

    assert.deepEqual(readdirSync(directory), [`hour-${HOUR}.parquet`]);
    assert.equal(existsSync(compactedFile(dir, HOUR, HOUR)), true);
  });

  /**
   * The window this design exists to avoid. Between the merge's rename and the
   * unlink of its inputs the directory holds both, and a reader that counted
   * both would answer every row in the hour twice.
   */
  it("does not double-count while the inputs are still on disk", async () => {
    await fillHour();
    const request: QueryRequest = {
      kind: "range",
      from: HOUR,
      to: HOUR + HOUR_MS,
      context: "self",
    };
    const before = await read(request);

    // A merge that got as far as the rename and no further.
    const units = (await import("../compact/plan.js")).planCompaction(
      dir,
      HOUR,
    );
    assert.equal(units.length, 1);
    await compactHour({ dataDir: dir, hourStartMs: HOUR });
    // Put the inputs back, which is the state a killed merge leaves.
    for (const input of units[0].inputs) {
      const { copyFileSync } = await import("node:fs");
      copyFileSync(compactedFile(dir, HOUR, HOUR), input);
    }

    assert.deepEqual(await read(request), before);
  });

  it("is a no-op for an hour with nothing in it", async () => {
    const result = await compactHour({ dataDir: dir, hourStartMs: HOUR });
    assert.deepEqual(result.files, []);
  });

  it("does not fold an earlier merge into a later one", async () => {
    await fillHour();
    await compactHour({ dataDir: dir, hourStartMs: HOUR });
    const again = await compactHour({ dataDir: dir, hourStartMs: HOUR });
    assert.deepEqual(again.files, []);
    assert.deepEqual(readdirSync(dateDirectory(dir, HOUR)), [
      `hour-${HOUR}.parquet`,
    ]);
  });
});
