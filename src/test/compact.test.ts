import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { compactHour } from "../compact/compact.js";
import { claimTheDataDirectory } from "../writer/claim.js";
import { EXIT_LOCKED } from "../writer/contract.js";
import { RollScheduler } from "../writer/roll-scheduler.js";
import { planCompaction } from "../compact/plan.js";
import { HOUR_MS } from "../roll/tree-path.js";
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

const COMPACT_ENTRY = join(
  resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  "compact",
  "main.js",
);

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

/**
 * The `path` column of a Parquet file in the order the file stores it.
 *
 * No ORDER BY: file order is the thing under test. A query would impose its
 * own and hide whether the merge sorted anything.
 */
function storedPaths(file: string): string[] {
  const probe = [
    `const { DuckDBInstance } = await import(${JSON.stringify("@duckdb/node-api")});`,
    `const instance = await DuckDBInstance.create(":memory:");`,
    `const c = await instance.connect();`,
    `const r = await c.runAndReadAll(${JSON.stringify(
      `SELECT path FROM read_parquet('${file}')`,
    )});`,
    `console.log(JSON.stringify(r.getRowsJS().map((row) => row[0])));`,
  ].join("\n");
  const output = execFileSync(
    process.execPath,
    ["--input-type=module", "-e", probe],
    { encoding: "utf8", timeout: 60_000, cwd: process.cwd() },
  );
  return JSON.parse(output.trim()) as string[];
}

/** How many times the `path` column changes value down the file. */
function runsOfEqualPath(paths: string[]): number {
  let runs = 0;
  for (let i = 0; i < paths.length; i += 1) {
    if (i === 0 || paths[i] !== paths[i - 1]) runs += 1;
  }
  return runs;
}

/** Twelve five-minute rolls covering the hour, as the scheduler would write them. */
async function fillHour(): Promise<void> {
  for (let i = 0; i < 12; i += 1) {
    await rollSlice(HOUR + i * 300_000, 20, HOUR + (i + 1) * 300_000);
  }
}

/**
 * The merge as a process, not as a function.
 *
 * Everything else here calls `compactHour` in-process, and the scheduler's own
 * tests answer with a stub. Between them sits a contract nothing was checking:
 * the argv `runCompaction` builds, the JSON `main.ts` prints, and the fields
 * `summariseCompaction` reads back out of it. The roll has exactly this
 * coverage; the merge had none.
 */
describe("the real merge process, driven by the scheduler", () => {
  it("merges the hour and reports what it wrote", async () => {
    await fillHour();
    const logged: string[] = [];
    const errors: string[] = [];
    const rolls = new RollScheduler({
      store,
      dataDir: dir,
      intervalMinutes: 5,
      now: () => HOUR + HOUR_MS + 60_000,
      log: (line) => logged.push(line),
      onError: (line) => errors.push(line),
    });

    // Nothing left to roll, so no roll child: this drives the merge alone,
    // through `COMPACT_ENTRY` and the default spawn.
    await rolls.rollOnce(HOUR + HOUR_MS);

    assert.deepEqual(errors, []);
    assert.equal(existsSync(compactedFile(dir, HOUR, HOUR)), true);
    assert.match(
      logged.join("\n"),
      new RegExp(
        `compacted the hour starting ${HOUR}: 1 file\\(s\\), 240 rows, \\d+ -> \\d+ bytes`,
      ),
    );
  });

  it("refuses to run beside a live roll", async () => {
    await fillHour();
    // The claim a roll holds, taken here so the merge meets it.
    const claim = await claimTheDataDirectory(writerPaths(dir).rollSocket);
    assert.notEqual(claim, null);
    try {
      const attempt = spawnSync(
        process.execPath,
        [COMPACT_ENTRY, "--data-dir", dir, "--hour", String(HOUR)],
        { encoding: "utf8", timeout: 60_000 },
      );
      assert.equal(attempt.status, EXIT_LOCKED);
      assert.match(
        attempt.stderr,
        /already running against this data directory/,
      );
    } finally {
      claim?.close();
    }
    assert.equal(existsSync(compactedFile(dir, HOUR, HOUR)), false);
  });

  it("says so rather than reporting success when the tree is not there", () => {
    const attempt = spawnSync(
      process.execPath,
      [COMPACT_ENTRY, "--data-dir", join(dir, "nope"), "--hour", String(HOUR)],
      { encoding: "utf8", timeout: 60_000 },
    );
    assert.equal(attempt.status, 1);
    assert.match(attempt.stderr, /there is no tree here/);
  });
});

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
    const units = planCompaction(dir, HOUR);
    assert.equal(units.length, 1);
    const inputs = units[0].inputs;
    const saved = inputs.map((input) => `${input}.saved`);
    for (const [i, input] of inputs.entries()) copyFileSync(input, saved[i]);
    await compactHour({ dataDir: dir, hourStartMs: HOUR });
    // Put the inputs back, byte for byte, which is the state a killed merge
    // leaves.
    for (const [i, input] of inputs.entries()) copyFileSync(saved[i], input);
    for (const copy of saved) rmSync(copy);

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

  /**
   * **The sort is the whole feature.** Clustering identical `path` strings is
   * what lets the dictionary and RLE work and what gives the row-group
   * statistics something to prune on; without it the merge buys a file count
   * and nothing else.
   *
   * Counted as runs rather than compared against a sorted copy, and never
   * asserted on the file's size: at this fixture's scale the unsorted output
   * is the smaller of the two, because per-file overhead dominates 240 rows.
   */
  it("stores the merged hour clustered by path", async () => {
    await fillHour();
    const inputs = planCompaction(dir, HOUR)[0].inputs;
    const beforeRuns = runsOfEqualPath(
      inputs.flatMap((input) => storedPaths(input)),
    );
    await compactHour({ dataDir: dir, hourStartMs: HOUR });

    const after = storedPaths(compactedFile(dir, HOUR, HOUR));
    assert.equal(after.length, 240);
    // Two paths in the fixture, alternating row by row in the rolls.
    assert.equal(beforeRuns, 240);
    assert.equal(runsOfEqualPath(after), 2);
  });

  /**
   * **The interrupted unlink.** A merge renames its output into place and
   * removes its inputs afterwards, deliberately not atomically. Killed in
   * between, the hour is correct and some of its inputs are still on disk.
   * Merging those survivors would write a fraction of the hour and rename it
   * over the whole of it — and the rows in the inputs already removed are by
   * then the only copy, because the hot store truncated them hours earlier.
   */
  it("removes the leftovers of an interrupted merge instead of merging them", async () => {
    await fillHour();
    const request: QueryRequest = {
      kind: "range",
      from: HOUR,
      to: HOUR + HOUR_MS,
      context: "self",
    };
    const before = await read(request);
    assert.equal(before.length, 240);

    const inputs = planCompaction(dir, HOUR)[0].inputs;
    const kept = inputs.slice(0, 5);
    const keptCopies = kept.map((input) => `${input}.saved`);
    for (const [i, input] of kept.entries()) copyFileSync(input, keptCopies[i]);
    await compactHour({ dataDir: dir, hourStartMs: HOUR });
    // The state a merge killed between the rename and the last unlink leaves.
    for (const [i, input] of kept.entries()) copyFileSync(keptCopies[i], input);
    for (const copy of keptCopies) rmSync(copy);

    const again = await compactHour({ dataDir: dir, hourStartMs: HOUR });
    assert.deepEqual(again.files, []);
    assert.deepEqual(again.alreadyCompacted, [compactedFile(dir, HOUR, HOUR)]);
    assert.deepEqual(again.orphanInputs, []);
    assert.deepEqual(again.quarantined, []);
    assert.deepEqual(readdirSync(dateDirectory(dir, HOUR)), [
      `hour-${HOUR}.parquet`,
    ]);
    assert.deepEqual(await read(request), before);
  });

  /**
   * **An existing output is a claim, not a fact.** Every leftover beside it is
   * a file whose rows the hot store truncated when it landed, so removing one
   * deletes the only copy. `commitFile` fsyncs before it renames, so a short
   * output means the storage lost bytes it acknowledged -- which
   * `writeSidecar` already handles, and names, for the same media.
   */
  it("merges the hour again when the existing output cannot be read", async () => {
    await fillHour();
    const request: QueryRequest = {
      kind: "range",
      from: HOUR,
      to: HOUR + HOUR_MS,
      context: "self",
    };
    const before = await read(request);
    assert.equal(before.length, 240);

    // The rename landed and the bytes did not.
    writeFileSync(compactedFile(dir, HOUR, HOUR), "");

    const result = await compactHour({ dataDir: dir, hourStartMs: HOUR });
    assert.deepEqual(result.quarantined, [compactedFile(dir, HOUR, HOUR)]);
    assert.equal(result.files.length, 1);
    assert.equal(result.files[0].rows, 240);
    assert.deepEqual(await read(request), before);
    assert.equal(
      existsSync(`${compactedFile(dir, HOUR, HOUR)}.unreadable`),
      true,
    );
  });

  /**
   * A roll that landed inside an already-merged hour is invisible to every
   * query, which is bad and recoverable: the file is still on disk. Deleting it
   * because a merged file happens to exist is what turns it into a loss, and
   * the deletion is what an operator reaches for to clean up.
   */
  it("leaves a roll the merged hour does not hold", async () => {
    await fillHour();
    await compactHour({ dataDir: dir, hourStartMs: HOUR });
    // A roll file inside the merged hour, holding rows the merge never saw.
    // Built by rolling into the *next* hour and renaming, because a roll can
    // no longer arrive here on its own -- `refuseMergedHour` refuses the id.
    // This is the state a build before that refusal leaves behind, and the one
    // an operator reaches for the merge to clean up.
    const late = HOUR + HOUR_MS + 300_000;
    await rollSlice(HOUR + HOUR_MS + 60_000, 20, late);
    const directory = dateDirectory(dir, HOUR);
    const inside = join(directory, `${HOUR + 45 * 60_000}.parquet`);
    copyFileSync(join(directory, `${late}.parquet`), inside);
    rmSync(join(directory, `${late}.parquet`));

    const result = await compactHour({ dataDir: dir, hourStartMs: HOUR });
    assert.deepEqual(result.alreadyCompacted, []);
    assert.deepEqual(result.orphanInputs, [inside]);
    assert.deepEqual(result.strayInputs, []);
    assert.equal(existsSync(inside), true);
  });

  /**
   * The window is what makes the sweep safe: it must collect what a killed
   * merge abandoned and never what a live one is still writing. Both directions
   * are asserted, because inverting the comparison passes a test that only
   * checks the old file is gone.
   */
  it("collects an abandoned temporary and leaves a fresh one", async () => {
    await fillHour();
    const directory = dateDirectory(dir, HOUR);
    const old = join(directory, "hour-1.parquet.999.tmp");
    const fresh = join(directory, "hour-2.parquet.998.tmp");
    writeFileSync(old, "");
    writeFileSync(fresh, "");
    const ago = (Date.now() - 7 * 60 * 60_000) / 1000;
    utimesSync(old, ago, ago);

    await compactHour({ dataDir: dir, hourStartMs: HOUR });

    assert.equal(existsSync(old), false);
    assert.equal(existsSync(fresh), true);
  });

  it("refuses an hour that does not start on the hour", async () => {
    await assert.rejects(
      () => compactHour({ dataDir: dir, hourStartMs: HOUR + 300_000 }),
      /not aligned/,
    );
  });

  /**
   * The hour is closed by the roll at its upper boundary, which runs at that
   * instant and lands seconds later. Merging before then hides every roll of
   * the hour still to come.
   */
  it("refuses an hour whose closing roll has not run", async () => {
    await assert.rejects(
      () =>
        compactHour({
          dataDir: dir,
          hourStartMs: HOUR,
          now: () => HOUR + HOUR_MS,
        }),
      /not closed yet/,
    );
    await assert.rejects(
      () =>
        compactHour({
          dataDir: dir,
          hourStartMs: HOUR,
          now: () => HOUR + HOUR_MS - 1,
        }),
      /not closed yet/,
    );
  });

  /**
   * A roll in flight is a roll whose file is not on disk yet. Merging past it
   * leaves it to arrive into a merged hour, which the roll then has to refuse
   * — costing recording rather than rows, but costing it for nothing.
   */
  it("refuses while a roll of that hour is unfinished", async () => {
    await fillHour();
    writeFileSync(
      writerPaths(dir).pendingRoll,
      `${JSON.stringify({
        rollId: HOUR + 600_000,
        maxRowid: 1,
        phase: "rolling",
      })}\n`,
    );
    await assert.rejects(
      () => compactHour({ dataDir: dir, hourStartMs: HOUR }),
      /is unfinished/,
    );
    assert.equal(readdirSync(dateDirectory(dir, HOUR)).length, 12);
  });

  /**
   * `written` means the rows are in the tree and still in the hot store, and
   * `rolledOverlap` resolves that seam by looking for `<rollId>.parquet` among
   * the live files. A merge renames it away, `liveTreeFiles` then hides it, and
   * every row of that roll is answered twice.
   */
  it("refuses while a roll of that hour is written but not truncated", async () => {
    await fillHour();
    writeFileSync(
      writerPaths(dir).pendingRoll,
      `${JSON.stringify({
        rollId: HOUR + 600_000,
        maxRowid: 1,
        phase: "written",
      })}\n`,
    );
    await assert.rejects(
      () => compactHour({ dataDir: dir, hourStartMs: HOUR }),
      /is unfinished/,
    );
  });

  it("merges past a pending roll that belongs to another hour", async () => {
    await fillHour();
    writeFileSync(
      writerPaths(dir).pendingRoll,
      `${JSON.stringify({
        rollId: HOUR + HOUR_MS + 300_000,
        maxRowid: 1,
        phase: "rolling",
      })}\n`,
    );
    const result = await compactHour({ dataDir: dir, hourStartMs: HOUR });
    assert.equal(result.files.length, 1);
  });

  it("unlinks nothing when an input cannot be read", async () => {
    await fillHour();
    const directory = dateDirectory(dir, HOUR);
    const corrupt = readdirSync(directory)[0];
    writeFileSync(join(directory, corrupt), "not a parquet file");

    await assert.rejects(() =>
      compactHour({ dataDir: dir, hourStartMs: HOUR }),
    );
    // Nothing is unlinked before a merge has landed, and the COPY failed while
    // binding `read_parquet` -- so it never created a temp either.
    assert.equal(readdirSync(directory).length, 12);
  });
});
