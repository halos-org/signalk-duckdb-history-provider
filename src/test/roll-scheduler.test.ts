import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DATA_LAYOUT } from "../data-dir.js";
import { dateDirectory } from "../roll/tree-path.js";
import { writerPaths } from "../writer/contract.js";
import { HotStore } from "../writer/hot-store.js";
import { RollScheduler, compactBudgetMs } from "../writer/roll-scheduler.js";
import { NO_BUNDLED_EXTENSION, eventually, sample } from "./fixtures.js";
import type { Sample } from "../writer/protocol.js";

const AUG_23 = Date.UTC(2026, 7, 23);
/** 14:37 on the 23rd: mid-slot for every interval under test. */
const NOW = Date.UTC(2026, 7, 23, 14, 37, 12);
/** The slot the schedule would have armed for at NOW. */
const SLOT = Date.UTC(2026, 7, 23, 15, 0, 0);

let dir: string;
let store: HotStore;
let seq = 0;
let logged: string[];
let errors: string[];

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "roll-scheduler-"));
  mkdirSync(join(dir, DATA_LAYOUT.hotStore), { recursive: true });
  store = HotStore.open(writerPaths(dir).store);
  store.beginSession("test");
  seq = 0;
  logged = [];
  errors = [];
  compactions = [];
});

afterEach(() => {
  try {
    store.close();
  } catch {
    // Already closed.
  }
  rmSync(dir, { recursive: true, force: true });
});

function record(...samples: Sample[]): void {
  seq += 1;
  store.insertBatch(seq, samples);
}

const argOf = (args: string[], flag: string): string =>
  args[args.indexOf(flag) + 1];

const child = (script: string): ChildProcess =>
  spawn(process.execPath, ["-e", script], {
    stdio: ["ignore", "pipe", "pipe"],
  });

/**
 * A stand-in roll that reports what a real one would for these arguments.
 *
 * It echoes back the id and bound it was given rather than carrying its own
 * constants, because the scheduler now refuses to truncate when those
 * disagree — a fake with hard-coded values would exercise a path production
 * never takes.
 */
function succeeds(rows?: number): (args: string[]) => ChildProcess {
  return (args) => {
    const count = rows ?? store.rollBound()?.rows ?? 0;
    const summary = {
      rollId: Number(argOf(args, "--roll-id")),
      maxRowid: Number(argOf(args, "--max-rowid")),
      rows: count,
      files: [{ date: "2026-08-23", path: "x", rows: count }],
      sidecarRows: 2,
      peakRssBytes: null,
    };
    return child(`console.log(${JSON.stringify(JSON.stringify(summary))})`);
  };
}

/** A stand-in roll whose summary carries extra fields the scheduler reads. */
function summarising(
  args: string[],
  extra: Record<string, unknown>,
): ChildProcess {
  const count = store.rollBound()?.rows ?? 0;
  const summary = {
    rollId: Number(argOf(args, "--roll-id")),
    maxRowid: Number(argOf(args, "--max-rowid")),
    rows: count,
    files: [{ date: "2026-08-23", path: "x", rows: count }],
    sidecarRows: 2,
    peakRssBytes: null,
    ...extra,
  };
  return child(`console.log(${JSON.stringify(JSON.stringify(summary))})`);
}

let compactions: string[][];

/**
 * A stand-in merge that reports what a real one would.
 *
 * The default in `scheduler()` below, because `SLOT` is a UTC hour boundary:
 * without it every roll test in this file spawns the real
 * `dist/compact/main.js` against a temp directory, which is both slow and a
 * merge nobody asked for.
 */
function merges(
  extra: Record<string, unknown> = {},
): (args: string[]) => ChildProcess {
  return (args) => {
    compactions.push(args);
    const summary = {
      hourStartMs: Number(argOf(args, "--hour")),
      files: [{ path: "x", rows: 240, bytesIn: 936_216, bytesOut: 343_746 }],
      alreadyCompacted: [],
      strayInputs: [],
      peakRssBytes: null,
      ...extra,
    };
    return child(`console.log(${JSON.stringify(JSON.stringify(summary))})`);
  };
}

const hoursMerged = (): number[] =>
  compactions.map((args) => Number(argOf(args, "--hour")));

const FAILS = (): ChildProcess =>
  child(
    `console.error("boom: the roll could not finish\\n    at some.frame"); process.exit(3)`,
  );
/** Exits the way a roll refused for a name it does not own does. */
const NAME_TAKEN = (): ChildProcess =>
  child(
    `console.error("x.parquet already exists and this roll is not a retry"); process.exit(4)`,
  );
const HANGS = (): ChildProcess => child(`setTimeout(() => {}, 60000)`);
const UNSPAWNABLE = (): ChildProcess =>
  spawn("/nonexistent/roll", [], { stdio: ["ignore", "pipe", "pipe"] });

function scheduler(
  over: Partial<ConstructorParameters<typeof RollScheduler>[0]> = {},
): RollScheduler {
  return new RollScheduler({
    store,
    dataDir: dir,
    intervalMinutes: 60,
    log: (line) => logged.push(line),
    onError: (line) => errors.push(line),
    now: () => NOW,
    spawnRoll: succeeds(),
    spawnCompact: merges(),
    ...over,
  });
}

function pendingFile(): Record<string, unknown> | null {
  try {
    return JSON.parse(
      readFileSync(writerPaths(dir).pendingRoll, "utf8"),
    ) as Record<string, unknown>;
  } catch {
    return null;
  }
}

describe("a scheduled roll", () => {
  it("truncates exactly what the roll was given", async () => {
    record(sample({ ts: AUG_23 }), sample({ ts: AUG_23 + 1 }));
    let seenRowid = "";
    const rolls = scheduler({
      spawnRoll: (args) => {
        seenRowid = argOf(args, "--max-rowid");
        const spawned = succeeds(2)(args);
        // The writer keeps ingesting while the roll runs, and this row was
        // never handed to it.
        record(sample({ ts: AUG_23 + 2 }));
        return spawned;
      },
    });

    await rolls.rollOnce(SLOT);

    assert.equal(seenRowid, "2");
    assert.equal(store.rowCount(), 1);
    assert.match(logged.join("\n"), /2 rows truncated/);
  });

  it("names the roll after the slot it was given, whichever side of it the clock sits", async () => {
    // The clock is made to disagree with the slot in both directions. A
    // scheduler that re-derived the slot from `now()` — the shipped bug —
    // produces a different id for at least one of these.
    for (const clock of [SLOT - 1, SLOT + 30_000]) {
      record(sample({ ts: AUG_23 }));
      let rollId = "";
      await scheduler({
        now: () => clock,
        spawnRoll: (args) => {
          rollId = argOf(args, "--roll-id");
          return succeeds()(args);
        },
      }).rollOnce(SLOT);
      assert.equal(Number(rollId), SLOT, `clock at ${clock}`);
    }
  });

  it("spawns nothing when there is nothing to roll", async () => {
    let spawned = 0;
    const rolls = scheduler({
      spawnRoll: (args) => {
        spawned += 1;
        return succeeds()(args);
      },
    });
    await rolls.rollOnce(SLOT);
    assert.equal(spawned, 0);
    // Not even a directory: an empty roll leaves no trace in the tree.
    assert.ok(!existsSync(join(dir, DATA_LAYOUT.tree)));
  });

  it("refuses to truncate when the roll reports a different bound", async () => {
    // The roll echoes its inputs back, and comparing them is the only
    // end-to-end check this two-process boundary can have before rows go.
    record(sample({ ts: AUG_23 }));
    await scheduler({
      spawnRoll: (args) =>
        child(
          `console.log(JSON.stringify({rollId:${argOf(args, "--roll-id")},maxRowid:99,rows:1,files:[],sidecarRows:0,peakRssBytes:null}))`,
        ),
    }).rollOnce(SLOT);

    assert.equal(store.rowCount(), 1);
    assert.match(errors.join("\n"), /bound 99, not 1/);
    assert.match(errors.join("\n"), /nothing truncated/);
  });

  it("refuses to truncate when the roll reports a different row count", async () => {
    record(sample({ ts: AUG_23 }), sample({ ts: AUG_23 + 1 }));
    await scheduler({ spawnRoll: succeeds(1) }).rollOnce(SLOT);

    assert.equal(store.rowCount(), 2);
    assert.match(errors.join("\n"), /1 rows written against 2 in the store/);
  });
});

describe("only a roll's own output may be replaced", () => {
  it("does not keep a name the roll refused, and the next slot takes a fresh one", async () => {
    // The P0. A refused roll used to keep its id, so the next slot inherited
    // `--replace` and overwrote the file the refusal had just protected.
    record(sample({ ts: AUG_23 }));
    await scheduler({ spawnRoll: NAME_TAKEN }).rollOnce(SLOT);

    assert.equal(pendingFile(), null, "the refused id must not be kept");
    assert.match(errors.join("\n"), /found its name already taken/);

    const args: string[][] = [];
    await scheduler({
      spawnRoll: (a) => {
        args.push(a);
        return succeeds()(a);
      },
      now: () => NOW + 3_600_000,
    }).rollOnce(SLOT + 3_600_000);

    assert.equal(Number(argOf(args[0], "--roll-id")), SLOT + 3_600_000);
    assert.ok(!args[0].includes("--replace"), args[0].join(" "));
  });

  it("passes --replace only when retrying its own unfinished roll", async () => {
    record(sample({ ts: AUG_23 }));
    const args: string[][] = [];
    await scheduler({
      spawnRoll: (a) => {
        args.push(a);
        return FAILS();
      },
    }).rollOnce(SLOT);
    assert.ok(!args[0].includes("--replace"), "the first attempt owns nothing");
    assert.equal(pendingFile()?.phase, "rolling");

    await scheduler({
      spawnRoll: (a) => {
        args.push(a);
        return succeeds()(a);
      },
      now: () => NOW + 3_600_000,
    }).rollOnce(SLOT + 3_600_000);

    assert.equal(Number(argOf(args[1], "--roll-id")), SLOT, "the same name");
    assert.ok(args[1].includes("--replace"), args[1].join(" "));
  });

  it("finishes a roll that completed but was not truncated, without rewriting its file", async () => {
    // A writer killed between the delete and the record's removal used to
    // leave an id its successor read as unfinished, and the successor then
    // replaced a file whose rows were already durable.
    record(sample({ ts: AUG_23 }), sample({ ts: AUG_23 + 1 }));
    writeFileSync(
      writerPaths(dir).pendingRoll,
      JSON.stringify({ rollId: SLOT, maxRowid: 2, phase: "written" }),
    );

    const args: string[][] = [];
    scheduler({
      spawnRoll: (a) => {
        args.push(a);
        return succeeds()(a);
      },
    }).start();

    assert.equal(store.rowCount(), 0, "the interrupted truncate is completed");
    assert.equal(pendingFile(), null);
    assert.match(logged.join("\n"), /had finished but not truncated/);
    assert.equal(args.length, 0, "and nothing was rolled again");
  });

  it("starts no new roll while a finished one's rows cannot be removed", async () => {
    // Reachable without a restart: if the delete throws — ENOSPC is the
    // realistic case — the record stays `written` and the timer re-arms. A
    // fresh name then writes rows that are already in the tree, under a
    // second name, permanently.
    record(sample({ ts: AUG_23 }));
    writeFileSync(
      writerPaths(dir).pendingRoll,
      JSON.stringify({ rollId: SLOT, maxRowid: 1, phase: "written" }),
    );
    const wedged = {
      oldestTimestamp: () => AUG_23,
      rollBound: () => ({ maxRowid: 1, rows: 1 }),
      deleteThrough: () => {
        throw new Error("database or disk is full");
      },
    } as unknown as HotStore;

    let spawned = 0;
    await new RollScheduler({
      store: wedged,
      dataDir: dir,
      intervalMinutes: 60,
      log: (line) => logged.push(line),
      onError: (line) => errors.push(line),
      now: () => NOW,
      spawnRoll: () => {
        spawned += 1;
        return succeeds()([]);
      },
    }).rollOnce(SLOT);

    assert.equal(spawned, 0, "no roll may start while the record stands");
    assert.equal(pendingFile()?.phase, "written", "and the record stands");
    assert.match(errors.join("\n"), /a second time/);
  });

  it("ignores a pending record it cannot read, or one naming roll id 0", async () => {
    // `nextRollAt` really can return 0 — for any clock set before the epoch —
    // and the roll process rejects it, so adopting one wedged every future
    // roll. A corrupt record must not be able to stop a device rolling.
    for (const body of [
      "not json",
      '{"rollId":0,"maxRowid":1,"phase":"rolling"}',
      "{}",
    ]) {
      writeFileSync(writerPaths(dir).pendingRoll, body);
      record(sample({ ts: AUG_23 }));
      const args: string[][] = [];
      await scheduler({
        spawnRoll: (a) => {
          args.push(a);
          return succeeds()(a);
        },
      }).rollOnce(SLOT);
      assert.equal(store.rowCount(), 0, body);
      assert.ok(!args[0].includes("--replace"), body);
    }
  });
});

describe("a roll that does not finish", () => {
  it("leaves the hot store intact and reports where an operator can see it", async () => {
    record(sample({ ts: AUG_23 }), sample({ ts: AUG_23 + 1 }));
    await scheduler({ spawnRoll: FAILS }).rollOnce(SLOT);

    assert.equal(store.rowCount(), 2);
    // The message, not the last frame of the stack under it.
    assert.match(errors.join("\n"), /boom: the roll could not finish/);
    assert.match(errors.join("\n"), /2 rows stay in the hot store/);
    assert.equal(logged.join("\n").includes("did not finish"), false);
  });

  it("survives a roll that cannot be spawned at all", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({ spawnRoll: UNSPAWNABLE }).rollOnce(SLOT);
    assert.equal(store.rowCount(), 1);
    assert.ok(errors.length > 0, "the failure has to be reported");
  });

  it("kills a roll that overruns and still does not truncate", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({ spawnRoll: HANGS, timeoutMs: 50 }).rollOnce(SLOT);

    assert.equal(store.rowCount(), 1);
    assert.match(logged.concat(errors).join("\n"), /exceeded its timeout/);
  });

  it("starts no second roll while one is in flight", async () => {
    record(sample({ ts: AUG_23 }));
    let spawned = 0;
    const rolls = scheduler({
      spawnRoll: () => {
        spawned += 1;
        return HANGS();
      },
      timeoutMs: 200,
    });
    const first = rolls.rollOnce(SLOT);
    await rolls.rollOnce(SLOT + 3_600_000);
    assert.equal(spawned, 1, "the second call must not spawn");
    await rolls.stop();
    await first;
    assert.equal(store.rowCount(), 1);
  });
});

describe("the timer", () => {
  /** A slot a few milliseconds away, so an armed timer fires in the test. */
  const almostDue = (intervalMs: number): number =>
    Math.floor(Date.now() / intervalMs) * intervalMs + intervalMs - 5;

  it("names the roll after the slot it armed for, not the clock at fire time", async () => {
    // `setTimeout` may fire a millisecond early. A slot re-read from the clock
    // then names the PREVIOUS slot — the defect that destroyed 2.5M rows on a
    // device, and the one every other test bypasses by passing the slot in.
    record(sample({ ts: AUG_23 }));
    const ids: number[] = [];
    const clock = almostDue(60_000);
    const rolls = scheduler({
      intervalMinutes: 1,
      now: () => clock,
      spawnRoll: (args) => {
        ids.push(Number(argOf(args, "--roll-id")));
        return succeeds()(args);
      },
    });
    rolls.start();
    await eventually(() => ids.length > 0, "the armed roll to fire");
    await rolls.stop();

    assert.equal(ids[0], clock + 5, "the boundary the timer was armed for");
  });

  it("re-arms after a roll, so a device rolls more than once", async () => {
    const ids: number[] = [];
    let clock = almostDue(60_000);
    // Recent, so the start-up backlog roll does not fire and supply the second
    // id this test is asking the timer for.
    record(sample({ ts: clock - 1000 }));
    const rolls = scheduler({
      intervalMinutes: 1,
      now: () => clock,
      spawnRoll: (args) => {
        ids.push(Number(argOf(args, "--roll-id")));
        // The summary is built here, from the bound the scheduler took.
        const spawned = succeeds()(args);
        // And this row arrives while the roll runs, as one does in production.
        record(sample({ ts: clock + ids.length }));
        clock += 60_000;
        return spawned;
      },
    });
    rolls.start();
    await eventually(() => ids.length >= 2, "a second roll");
    await rolls.stop();

    // Not asserting `errors` is empty: stop() kills whatever roll the third
    // slot had already started, and that failure is the correct report.
    assert.notEqual(ids[0], ids[1], "consecutive slots take different names");
  });

  it("rolls shortly after start when the store predates this process", async () => {
    // A boat powered up at 08:10 and shut down at 08:55 crosses no hourly
    // slot, so without this the store is never truncated on a short trip.
    record(sample({ ts: NOW - 7_200_000 }));
    const ids: number[] = [];
    const rolls = scheduler({
      startDelayMs: 5,
      spawnRoll: (args) => {
        ids.push(Number(argOf(args, "--roll-id")));
        return succeeds()(args);
      },
    });
    rolls.start();
    await eventually(
      () => store.rowCount() === 0,
      "the backlog roll to finish",
    );
    await rolls.stop();

    assert.match(logged.join("\n"), /older than one roll interval/);
    assert.equal(ids.length, 1);
  });

  it("does not roll at start when the store is within one interval", async () => {
    record(sample({ ts: NOW - 60_000 }));
    let spawned = 0;
    const rolls = scheduler({
      startDelayMs: 5,
      spawnRoll: (args) => {
        spawned += 1;
        return succeeds()(args);
      },
    });
    rolls.start();
    await new Promise((resolve) => setTimeout(resolve, 40));
    await rolls.stop();
    assert.equal(spawned, 0);
  });
});

describe("a failure inside the roll itself", () => {
  it("costs one roll, not the writer", async () => {
    // Anything thrown here reaches an unhandled rejection under Node's
    // default and ends the process — so a hot store that could not be
    // truncated would stop recording rather than skip a roll.
    const exploding = {
      oldestTimestamp: () => null,
      rollBound: () => {
        throw new Error("the store is unreadable");
      },
    } as unknown as HotStore;
    const rolls = new RollScheduler({
      store: exploding,
      dataDir: dir,
      intervalMinutes: 1,
      log: (line) => logged.push(line),
      onError: (line) => errors.push(line),
      now: () => Math.floor(Date.now() / 60_000) * 60_000 + 60_000 - 5,
    });
    rolls.start();
    await eventually(
      () => errors.some((line) => /the store is unreadable/.test(line)),
      "the failure to be reported",
    );
    await rolls.stop();
  });
});

describe("stopping", () => {
  it("does not start a roll after stop", async () => {
    record(sample({ ts: AUG_23 }));
    let spawned = 0;
    const rolls = scheduler({
      spawnRoll: (args) => {
        spawned += 1;
        return succeeds()(args);
      },
    });
    await rolls.stop();
    await rolls.rollOnce(SLOT);
    assert.equal(spawned, 0);
    assert.equal(store.rowCount(), 1);
  });

  it("kills a roll in flight and truncates nothing", async () => {
    record(sample({ ts: AUG_23 }));
    const rolls = scheduler({ spawnRoll: HANGS });
    const inFlight = rolls.rollOnce(SLOT);
    await eventually(
      () => existsSync(writerPaths(dir).pendingRoll),
      "the roll to start",
    );
    await rolls.stop();
    await inFlight;

    assert.equal(store.rowCount(), 1);
  });
});

describe("retention", () => {
  it("hands the roll the configured days", async () => {
    record(sample({ ts: AUG_23 }));
    let seen = "";
    await scheduler({
      retentionDays: 30,
      spawnRoll: (args) => {
        seen = argOf(args, "--retention-days");
        return succeeds(1)(args);
      },
    }).rollOnce(SLOT);

    assert.equal(seen, "30");
  });

  it("hands the roll a zero when nothing is configured", async () => {
    record(sample({ ts: AUG_23 }));
    let seen = "";
    await scheduler({
      spawnRoll: (args) => {
        seen = argOf(args, "--retention-days");
        return succeeds(1)(args);
      },
    }).rollOnce(SLOT);

    assert.equal(seen, "0");
  });

  it("names the directories the roll expired", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({
      spawnRoll: (args) => summarising(args, { expired: ["2026-07-01"] }),
    }).rollOnce(SLOT);

    assert.match(logged.join("\n"), /expiring 2026-07-01/);
  });

  it("reports a directory it could not expire on the error sink", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({
      spawnRoll: (args) =>
        summarising(args, {
          expiryFailures: [{ date: "2026-07-01", why: "EACCES" }],
        }),
    }).rollOnce(SLOT);

    // Rows still truncated: a tree over its bound is a disk problem, and
    // refusing to truncate would turn it into a recording problem too.
    assert.equal(store.rowCount(), 0);
    assert.match(errors.join("\n"), /retention could not remove 1 date/);
    assert.match(errors.join("\n"), /2026-07-01: EACCES/);
  });

  it("reports a tree it could not list apart from a directory it could not remove", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({
      spawnRoll: (args) => summarising(args, { expiryTreeError: "EIO" }),
    }).rollOnce(SLOT);

    assert.equal(store.rowCount(), 0);
    assert.match(errors.join("\n"), /could not list the Parquet tree \(EIO\)/);
    assert.doesNotMatch(errors.join("\n"), /could not remove/);
  });
});

describe(
  "the real roll process, driven by the scheduler",
  { skip: NO_BUNDLED_EXTENSION },
  () => {
    it("expires a date directory the window has passed", async () => {
      // A directory from a device that has been recording for a while, and one
      // row today. The roll writes today's file and then drops the old day.
      const stale = dateDirectory(dir, AUG_23 - 40 * 86_400_000);
      mkdirSync(stale, { recursive: true });
      writeFileSync(join(stale, "1.parquet"), "not read, only removed");
      record(sample({ ts: AUG_23 + 1000, path: "a.b" }));

      const rolls = new RollScheduler({
        store,
        dataDir: dir,
        intervalMinutes: 60,
        retentionDays: 7,
        log: (line) => logged.push(line),
        onError: (line) => errors.push(line),
        now: () => NOW,
      });
      await rolls.rollOnce(SLOT);

      assert.deepEqual(errors, []);
      assert.equal(existsSync(stale), false);
      assert.deepEqual(readdirSync(dateDirectory(dir, AUG_23)), [
        `${SLOT}.parquet`,
      ]);
      assert.match(logged.join("\n"), /expiring 2026-07-14/);
    });

    it("expires nothing when retention is off", async () => {
      const stale = dateDirectory(dir, AUG_23 - 400 * 86_400_000);
      mkdirSync(stale, { recursive: true });
      writeFileSync(join(stale, "1.parquet"), "kept");
      record(sample({ ts: AUG_23 + 1000, path: "a.b" }));

      await new RollScheduler({
        store,
        dataDir: dir,
        intervalMinutes: 60,
        log: (line) => logged.push(line),
        onError: (line) => errors.push(line),
        now: () => NOW,
      }).rollOnce(SLOT);

      assert.deepEqual(errors, []);
      assert.ok(existsSync(stale));
    });

    it("writes the tree and truncates the store", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b" }),
        sample({ ts: AUG_23 + 2000, path: "c.d" }),
      );
      // No spawnRoll: this is the production path, DuckDB and all.
      const rolls = new RollScheduler({
        store,
        dataDir: dir,
        intervalMinutes: 60,
        log: (line) => logged.push(line),
        onError: (line) => errors.push(line),
        now: () => NOW,
      });
      await rolls.rollOnce(SLOT);

      assert.deepEqual(errors, []);
      assert.equal(store.rowCount(), 0);
      assert.deepEqual(readdirSync(dateDirectory(dir, AUG_23)), [
        `${SLOT}.parquet`,
      ]);
      assert.match(
        logged.join("\n"),
        /2 rows to 2026-08-23 and 2 sidecar rows/,
      );
    });

    it("survives a retry over its own half-written output", async () => {
      // The composition the suite was missing: an attempt writes its Parquet,
      // fails before reporting, and the retry replaces its own file.
      record(sample({ ts: AUG_23 + 1000, path: "a.b" }));
      await scheduler({
        spawnRoll: (args) => {
          // Runs the real roll to completion, then reports a failure anyway.
          execFileSync(process.execPath, args, { timeout: 120_000 });
          return FAILS();
        },
      }).rollOnce(SLOT);

      assert.equal(store.rowCount(), 1, "nothing truncated");
      assert.deepEqual(readdirSync(dateDirectory(dir, AUG_23)), [
        `${SLOT}.parquet`,
      ]);

      record(sample({ ts: AUG_23 + 2000, path: "c.d" }));
      const errorsBefore = errors.length;
      await scheduler({
        spawnRoll: (args) =>
          spawn(process.execPath, args, { stdio: ["ignore", "pipe", "pipe"] }),
        now: () => NOW + 3_600_000,
      }).rollOnce(SLOT + 3_600_000);

      assert.equal(errors.length, errorsBefore, errors.join("\n"));
      assert.equal(store.rowCount(), 0);
      assert.deepEqual(
        readdirSync(dateDirectory(dir, AUG_23)),
        [`${SLOT}.parquet`],
        "one file, replaced rather than joined by a second",
      );
    });
  },
);

/**
 * The merge, from the side that decides when it runs.
 *
 * Every test here drives it through `spawnCompact`, which exists for exactly
 * this and which nothing used. The point is not that the merge works — that is
 * `compact.test.ts` — but that the scheduler triggers it when it should and
 * skips it when it should not.
 */
describe("compaction, scheduled", () => {
  const FIVE = { intervalMinutes: 5 };

  it("merges the hour a roll closed", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler(FIVE).rollOnce(SLOT);

    assert.deepEqual(hoursMerged(), [SLOT - 3_600_000]);
    assert.match(logged.join("\n"), /compacted the hour starting/);
  });

  it("moves on to the next hour once a roll closes one", async () => {
    const rolls = scheduler(FIVE);
    for (const slot of [SLOT, SLOT + 3_600_000]) {
      record(sample({ ts: AUG_23 }));
      await rolls.rollOnce(slot);
    }

    assert.deepEqual(hoursMerged(), [SLOT - 3_600_000, SLOT]);
  });

  /**
   * The trigger used to be `slot % HOUR_MS === 0`, which fires only on rolls
   * landing exactly on the hour. Ten of the intervals that divide 1440 do not
   * divide 60, and at those it compacted a fraction of the hours and left the
   * rest for ever — with the UI still reporting compaction as on.
   */
  it("merges at an interval that divides the day but not the hour", async () => {
    const rolls = scheduler({ intervalMinutes: 45 });
    record(sample({ ts: AUG_23 }));
    // 15:45, which closes no hour boundary and is not a multiple of one.
    await rolls.rollOnce(SLOT + 45 * 60_000);

    assert.deepEqual(hoursMerged(), [SLOT - 3_600_000]);
  });

  /**
   * An interval with nothing recorded in it is ordinary at the shipped
   * default, and hanging the merge off the tail of a successful roll alone
   * left those hours uncompacted for ever: there is no catch-up pass.
   */
  it("merges even when the interval had nothing to roll", async () => {
    await scheduler(FIVE).rollOnce(SLOT);

    assert.equal(store.rowCount(), 0);
    assert.deepEqual(hoursMerged(), [SLOT - 3_600_000]);
  });

  /**
   * At an hour or more, an hour holds at most one roll and the planner skips
   * it — so the spawn maps the engine once an hour to do nothing. This is the
   * upgrade path for every install that has not changed the interval.
   */
  /**
   * `arm()` re-reads the clock after `rollOnce` settles, so a merge allowed a
   * whole interval costs a roll rather than delaying one. Asserted against the
   * arithmetic rather than by waiting one out: the ceiling is five minutes.
   */
  it("gives a merge half a roll interval, capped at five minutes", () => {
    assert.equal(compactBudgetMs(1), 30_000);
    assert.equal(compactBudgetMs(5), 150_000);
    assert.equal(compactBudgetMs(45), 300_000);
    assert.equal(compactBudgetMs(720), 300_000);
  });

  it("does not spawn a merge at an interval that cannot fill an hour", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({ intervalMinutes: 60 }).rollOnce(SLOT);

    assert.deepEqual(compactions, []);
  });

  /**
   * A roll that failed leaves a `rolling` record, and its rows are neither in
   * the tree nor out of the store. Merging the hour it belongs to would rename
   * away the file `rolledOverlap` looks for by name.
   *
   * The rule itself is `compactHour`'s, which reads the record from disk --
   * `compact.test.ts` drives it there. This checks the cheaper property the
   * scheduler owns: a roll that did not finish does not reach a merge at all.
   */
  it("does not merge after a roll that failed", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({ ...FIVE, spawnRoll: FAILS }).rollOnce(SLOT);

    assert.deepEqual(compactions, []);
    assert.equal(pendingFile()?.phase, "rolling");
    assert.equal(store.rowCount(), 1);
  });

  /**
   * These files are the state a half-finished merge leaves: ignored by every
   * query, counted by no retention, and never revisited, because each hour is
   * merged once. The error sink is where an operator sees them; the success
   * line is debug-level.
   */
  it("reports inputs the merge could not remove on the error sink", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({
      ...FIVE,
      spawnCompact: merges({ strayInputs: ["a.parquet", "b.parquet"] }),
    }).rollOnce(SLOT);

    assert.match(errors.join("\n"), /could not remove 2 of the roll files/);
  });

  /**
   * Every reachable merge failure is a transient: a spawn that could not get
   * memory, the SIGKILL at the timeout, an EIO on a failing card. Recording the
   * hour as done before awaiting the child meant any one of them cost that hour
   * for the life of the writer, and there is no catch-up pass anywhere.
   */
  it("offers a failed hour again at the next roll", async () => {
    let attempts = 0;
    const rolls = scheduler({
      ...FIVE,
      spawnCompact: (args) => {
        attempts += 1;
        if (attempts > 1) return merges()(args);
        compactions.push(args);
        return FAILS();
      },
    });
    for (const slot of [SLOT, SLOT + 300_000]) {
      record(sample({ ts: AUG_23 }));
      await rolls.rollOnce(slot);
    }

    assert.deepEqual(hoursMerged(), [SLOT - 3_600_000, SLOT - 3_600_000]);
    assert.match(errors.join("\n"), /did not finish/);
    assert.match(logged.join("\n"), /compacted the hour starting/);
  });

  it("stops offering an hour once a merge of it has succeeded", async () => {
    const rolls = scheduler(FIVE);
    for (const slot of [SLOT, SLOT + 300_000, SLOT + 600_000]) {
      record(sample({ ts: AUG_23 }));
      await rolls.rollOnce(slot);
    }

    assert.deepEqual(hoursMerged(), [SLOT - 3_600_000]);
  });

  /**
   * A merge killed after `commitFile` has already renamed one unit into place,
   * and from that instant `liveTreeFiles` stops returning the rolls it
   * superseded. Telling the operator they are untouched points them away from
   * the only thing that finishes the job.
   */
  it("does not claim the rolls are untouched when a merge fails", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({ ...FIVE, spawnCompact: FAILS }).rollOnce(SLOT);

    const line = errors.join("\n");
    assert.doesNotMatch(line, /untouched/);
    assert.match(line, /compact\/main\.js --hour \d+/);
  });

  it("reports a quarantined output and orphaned rolls on the error sink", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({
      ...FIVE,
      spawnCompact: merges({
        quarantined: ["hour-x.parquet"],
        orphanInputs: ["a.parquet", "b.parquet"],
      }),
    }).rollOnce(SLOT);

    assert.match(errors.join("\n"), /could not be read/);
    assert.match(errors.join("\n"), /2 roll file\(s\).*hold rows the merged/s);
  });

  it("reports a merge that fails, and keeps the roll's own success", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({ ...FIVE, spawnCompact: FAILS }).rollOnce(SLOT);

    assert.match(errors.join("\n"), /did not finish/);
    assert.match(logged.join("\n"), /1 rows truncated/);
    assert.equal(store.rowCount(), 0);
  });

  /**
   * The merge logs each unit it finishes to stderr before it reaches the one
   * that fails, so the first stderr line is progress and the last is the
   * error. The roll's rule -- first line is the message, the rest is a stack
   * -- reports the wrong line here.
   */
  it("reports the merge's error, not its progress, when it fails", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({
      ...FIVE,
      spawnCompact: () =>
        child(
          `console.error("compacted 12 files into hour-1.parquet: 240 rows"); ` +
            `console.error("ENOSPC: no space left on device"); process.exit(1)`,
        ),
    }).rollOnce(SLOT);

    assert.match(errors.join("\n"), /code 1: ENOSPC: no space left on device/);
  });

  it("names the signal when a merge is killed", async () => {
    record(sample({ ts: AUG_23 }));
    await scheduler({
      ...FIVE,
      spawnCompact: () => child(`process.kill(process.pid, "SIGKILL")`),
    }).rollOnce(SLOT);

    assert.match(errors.join("\n"), /signal SIGKILL/);
  });

  /**
   * `runCompaction` is the only bound on a wedged merge, and a merge holds
   * `this.running`, which is what `rollOnce` checks before starting anything.
   * So a merge that never exits does cost recording, however much the comment
   * above it says nothing waits on it.
   */
  it("kills a merge that overruns its budget", async () => {
    // Nothing recorded, so no roll child: `timeoutMs` is one knob for both,
    // and a roll racing a 50 ms budget is the thing that times out instead.
    await scheduler({ ...FIVE, spawnCompact: HANGS, timeoutMs: 50 }).rollOnce(
      SLOT,
    );

    assert.match(errors.join("\n"), /ran past 50 ms/);
    assert.match(errors.join("\n"), /did not finish/);
  });

  it("kills a merge in flight when the writer stops", async () => {
    let child: ChildProcess | null = null;
    const rolls = scheduler({
      ...FIVE,
      spawnCompact: () => {
        child = HANGS();
        return child;
      },
      timeoutMs: 60_000,
    });
    const rolling = rolls.rollOnce(SLOT);
    await eventually(() => child !== null, "the merge to be spawned");
    await rolls.stop();
    await rolling;

    assert.notEqual(child, null);
    await eventually(
      () =>
        (child as unknown as ChildProcess).exitCode !== null ||
        (child as unknown as ChildProcess).signalCode !== null,
      "the merge to be killed",
    );
  });

  /**
   * `COMPACT_ENTRY` is resolved from `import.meta.url` at load time, so a
   * build that moved or dropped the merge's entry point would fail at the
   * first hour rather than at compile.
   */
  it("points at a merge entry point that exists", async () => {
    let entry = "";
    record(sample({ ts: AUG_23 }));
    await scheduler({
      ...FIVE,
      spawnCompact: (args) => {
        entry = args[0];
        return merges()(args);
      },
    }).rollOnce(SLOT);

    assert.equal(existsSync(entry), true, entry);
    assert.equal(argOf(compactions[0], "--data-dir"), dir);
  });
});
