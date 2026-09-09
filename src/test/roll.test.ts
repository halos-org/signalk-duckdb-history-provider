import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createServer } from "node:net";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { DATA_LAYOUT } from "../data-dir.js";
import { roll } from "../roll/roll.js";
import { dateDirectory, rollTempFile, sidecarFile } from "../roll/tree-path.js";
import { EXIT_LOCKED, EXIT_NAME_TAKEN } from "../writer/contract.js";
import { writerPaths } from "../writer/contract.js";
import { HotStore } from "../writer/hot-store.js";
import { NO_BUNDLED_EXTENSION, sample } from "./fixtures.js";
import type { Sample } from "../writer/protocol.js";

const DIST = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ROLL_ENTRY = join(DIST, "roll", "main.js");

let dir: string;
let store: HotStore;
let seq = 0;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "roll-"));
  mkdirSync(join(dir, DATA_LAYOUT.hotStore), { recursive: true });
  store = HotStore.open(writerPaths(dir).store);
  store.beginSession("test");
  seq = 0;
});

afterEach(() => {
  try {
    store.close();
  } catch {
    // Already closed by the test.
  }
  rmSync(dir, { recursive: true, force: true });
});

/** Runs the roll and returns its failure rather than throwing. */
function attemptRoll(extra: string[]): { status: number; stderr: string } {
  try {
    execFileSync(process.execPath, [ROLL_ENTRY, "--data-dir", dir, ...extra], {
      encoding: "utf8",
      timeout: 60_000,
      stdio: "pipe",
    });
    throw new Error("the roll was expected to fail");
  } catch (err) {
    const failure = err as { status?: number; stderr?: string };
    return { status: failure.status ?? -1, stderr: failure.stderr ?? "" };
  }
}

function record(...samples: Sample[]): void {
  seq += 1;
  store.insertBatch(seq, samples);
}

/** Reads a Parquet file back through a throwaway DuckDB, as a reader would. */
function readParquet(pattern: string): Record<string, unknown>[] {
  const probe = [
    `const { DuckDBInstance } = await import(${JSON.stringify("@duckdb/node-api")});`,
    `const instance = await DuckDBInstance.create(":memory:");`,
    `const c = await instance.connect();`,
    `const r = await c.runAndReadAll(${JSON.stringify(
      `SELECT ts, context, path, value_kind, value_num, value_str FROM read_parquet('${pattern}', union_by_name = true) ORDER BY ts, path`,
    )});`,
    `console.log(JSON.stringify(r.getRowsJS(), (k, v) => (typeof v === "bigint" ? Number(v) : v)));`,
  ].join("\n");
  const output = execFileSync(
    process.execPath,
    ["--input-type=module", "-e", probe],
    { encoding: "utf8", timeout: 60_000, cwd: process.cwd() },
  );
  return (JSON.parse(output.trim()) as unknown[][]).map((row) => ({
    ts: row[0],
    context: row[1],
    path: row[2],
    value_kind: row[3],
    value_num: row[4],
    value_str: row[5],
  }));
}

const DAY = 86_400_000;
const AUG_23 = Date.UTC(2026, 7, 23);
const AUG_24 = Date.UTC(2026, 7, 24);

describe("a roll", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("writes every covered row and no more", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }),
      sample({ ts: AUG_23 + 2000, path: "c.d", value: 2 }),
    );
    const bound = store.rollBound();
    assert.ok(bound !== null);
    // Arrives after the bound was read, exactly as it does in production.
    record(sample({ ts: AUG_23 + 3000, path: "e.f", value: 3 }));

    const result = await roll({
      dataDir: dir,
      maxRowid: bound.maxRowid,
      rollId: 1,
    });

    assert.equal(result.rows, 2);
    assert.equal(result.files.length, 1);
    const rows = readParquet(result.files[0].path);
    assert.deepEqual(
      rows.map((row) => row.path),
      ["a.b", "c.d"],
    );
  });

  it("places rows by their own date, so a roll may span midnight", async () => {
    record(
      sample({ ts: AUG_23 + DAY - 1000, path: "before.midnight" }),
      sample({ ts: AUG_23 + DAY + 1000, path: "after.midnight" }),
    );
    const result = await roll({
      dataDir: dir,
      maxRowid: store.rollBound()!.maxRowid,
      rollId: 7,
    });

    assert.deepEqual(
      result.files.map((file) => file.date),
      ["2026-08-23", "2026-08-24"],
    );
    // One roll, one name, two directories.
    for (const file of result.files) {
      assert.equal(basename(file.path), "7.parquet");
      assert.equal(file.rows, 1);
    }
  });

  it("writes a directory per date that has rows, and none for the gap", async () => {
    // Two dates two days apart. A `coveredDays` that returned the contiguous
    // range rather than the distinct days would add date=2026-08-24 here, and
    // a device that had been off for a week would grow seven directories of
    // empty files that every reader then opens and skips.
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b" }),
      sample({ ts: AUG_23 + 2 * DAY + 1000, path: "c.d" }),
    );
    const result = await roll({ dataDir: dir, maxRowid: 2, rollId: 1 });

    assert.deepEqual(readdirSync(join(dir, DATA_LAYOUT.tree)).sort(), [
      "date=2026-08-23",
      "date=2026-08-25",
    ]);
    assert.equal(result.files.length, 2);
  });

  it("leaves nothing a reader would mistake for a finished file", async () => {
    record(sample({ ts: AUG_23 + 1000 }));
    await roll({ dataDir: dir, maxRowid: 1, rollId: 1 });
    const written = readdirSync(dateDirectory(dir, AUG_23));
    assert.deepEqual(written, ["1.parquet"]);
  });

  it("overwrites its own files when a retry reuses the roll id", async () => {
    // What the writer does after a roll that died before it could report:
    // the id is kept, so the second attempt replaces the first attempt's
    // files instead of leaving them beside its own as duplicates.
    record(sample({ ts: AUG_23 + 1000, path: "a.b" }));
    await roll({ dataDir: dir, maxRowid: 1, rollId: 42 });
    record(sample({ ts: AUG_23 + 2000, path: "c.d" }));
    const second = await roll({
      dataDir: dir,
      maxRowid: store.rollBound()!.maxRowid,
      rollId: 42,
      replace: true,
    });

    assert.deepEqual(readdirSync(dateDirectory(dir, AUG_23)), ["42.parquet"]);
    assert.equal(second.rows, 2);
    assert.equal(readParquet(second.files[0].path).length, 2);
  });

  it("refuses to replace a file when it is not a retry", async () => {
    // The failure this exists for: a schedule that read the clock a
    // millisecond early named the slot before it, whose file was already
    // written, and replaced 2.5M rows with the two minutes since. Measured on
    // a device. A roll arriving at a name already taken now fails instead.
    record(sample({ ts: AUG_23 + 1000, path: "a.b" }));
    await roll({ dataDir: dir, maxRowid: 1, rollId: 42 });
    const before = readParquet(join(dateDirectory(dir, AUG_23), "42.parquet"));

    record(sample({ ts: AUG_23 + 2000, path: "c.d" }));
    await assert.rejects(
      () => roll({ dataDir: dir, maxRowid: 2, rollId: 42 }),
      /already exists/,
    );
    assert.deepEqual(
      readParquet(join(dateDirectory(dir, AUG_23), "42.parquet")),
      before,
    );
  });

  /**
   * A merge unlinks the rolls it folded in, so a free name proves nothing
   * about whether the hour has been merged. `liveTreeFiles` suppresses by the
   * id range a compacted file covers, not by the set that merge read — so a
   * roll written into a merged hour would be on disk, reported as written, and
   * returned by no query, while the writer truncated its rows on the strength
   * of that report.
   */
  it("refuses a roll id an existing merge already covers", async () => {
    const hour = Math.floor((AUG_23 + 5000) / 3_600_000) * 3_600_000;
    mkdirSync(dateDirectory(dir, AUG_23), { recursive: true });
    writeFileSync(join(dateDirectory(dir, AUG_23), `hour-${hour}.parquet`), "");

    record(sample({ ts: AUG_23 + 5000, path: "a.b" }));
    await assert.rejects(
      () => roll({ dataDir: dir, maxRowid: 1, rollId: hour + 300_000 }),
      /a merge has already/,
    );
    assert.equal(
      existsSync(join(dateDirectory(dir, AUG_23), `${hour + 300_000}.parquet`)),
      false,
    );
    assert.equal(store.rowCount(), 1);
  });

  /**
   * A retry inherits the failed attempt's id, and an hour merged in between
   * makes that id just as invisible as a fresh one. `--replace` grants
   * permission to overwrite this roll's own output, never to write into a
   * range a reader has stopped looking at.
   */
  it("refuses a covered id even for a retry", async () => {
    const hour = Math.floor((AUG_23 + 5000) / 3_600_000) * 3_600_000;
    mkdirSync(dateDirectory(dir, AUG_23), { recursive: true });
    writeFileSync(join(dateDirectory(dir, AUG_23), `hour-${hour}.parquet`), "");
    record(sample({ ts: AUG_23 + 5000, path: "a.b" }));
    await assert.rejects(
      () =>
        roll({
          dataDir: dir,
          maxRowid: 1,
          rollId: hour + 300_000,
          replace: true,
        }),
      /a merge has already/,
    );
  });

  /**
   * A roll spanning midnight writes one file per date. Refusing part way
   * through would leave rows in the tree that the failure tells the scheduler
   * are not there, and the next roll would write them a second time.
   */
  it("writes nothing at all when one of its dates is inside a merged hour", async () => {
    const rollId = AUG_24 + 300_000;
    const hour = AUG_24;
    mkdirSync(dateDirectory(dir, AUG_24), { recursive: true });
    writeFileSync(join(dateDirectory(dir, AUG_24), `hour-${hour}.parquet`), "");

    record(
      sample({ ts: AUG_24 - 1000, path: "a.b" }),
      sample({ ts: AUG_24 + 1000, path: "a.b" }),
    );
    await assert.rejects(
      () => roll({ dataDir: dir, maxRowid: 2, rollId }),
      /a merge has already/,
    );
    assert.equal(
      existsSync(join(dateDirectory(dir, AUG_23), `${rollId}.parquet`)),
      false,
    );
  });

  it("expires a day it wrote in this same roll, when the backlog is older than the window", async () => {
    // A device that was off for a fortnight comes back and rolls all of it at
    // once. Every date it writes is real, and most of them are already past
    // the window — so expiry runs after the write and takes them, rather than
    // leaving a fortnight in a tree configured to hold three days.
    for (let day = 0; day <= 14; day += 1) {
      record(sample({ ts: AUG_23 + day * DAY + 1000, path: "a.b" }));
    }
    const result = await roll({
      dataDir: dir,
      maxRowid: store.rollBound()!.maxRowid,
      rollId: 1,
      retentionDays: 3,
      // The clock is on the newest day, which is where a device catching up
      // would have it.
      now: () => AUG_23 + 14 * DAY + 2000,
    });

    assert.equal(result.files.length, 15, "every date was written first");
    assert.deepEqual(readdirSync(join(dir, DATA_LAYOUT.tree)).sort(), [
      "date=2026-09-03",
      "date=2026-09-04",
      "date=2026-09-05",
      "date=2026-09-06",
    ]);
    assert.equal(result.expired.length, 11);
    assert.deepEqual(result.expiryFailures, []);
  });

  it("keeps every date when retention is off", async () => {
    for (let day = 0; day <= 5; day += 1) {
      record(sample({ ts: AUG_23 + day * DAY + 1000, path: "a.b" }));
    }
    const result = await roll({
      dataDir: dir,
      maxRowid: store.rollBound()!.maxRowid,
      rollId: 1,
    });

    assert.equal(readdirSync(join(dir, DATA_LAYOUT.tree)).length, 6);
    assert.deepEqual(result.expired, []);
  });

  it("refuses a bound that is not a rowid", async () => {
    record(sample({ ts: AUG_23 }));
    await assert.rejects(
      () => roll({ dataDir: dir, maxRowid: 0, rollId: 1 }),
      /rowid/,
    );
  });
});

describe("the sidecar", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("keeps a path that stopped reporting before this roll", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "gone.away", value: 11 }),
      sample({ ts: AUG_23 + 1000, path: "still.here", value: 22 }),
    );
    await roll({ dataDir: dir, maxRowid: 2, rollId: 1 });
    store.deleteThrough(2);

    record(sample({ ts: AUG_23 + 5000, path: "still.here", value: 33 }));
    const second = await roll({
      dataDir: dir,
      maxRowid: store.rollBound()!.maxRowid,
      rollId: 2,
    });

    assert.equal(second.sidecarRows, 2);
    const rows = readParquet(sidecarFile(dir));
    assert.deepEqual(
      rows.map((row) => [row.path, row.value_num]),
      [
        ["gone.away", 11],
        ["still.here", 33],
      ],
    );
  });

  it("rebuilds from the tree when the previous sidecar cannot be read", async () => {
    // Rebuilding from this roll's window alone would drop every path that
    // went quiet earlier — the one question the sidecar exists to answer.
    record(
      sample({ ts: AUG_23 + 1000, path: "went.quiet", value: 7 }),
      sample({ ts: AUG_23 + 1000, path: "still.here", value: 1 }),
    );
    await roll({ dataDir: dir, maxRowid: 2, rollId: 1 });
    store.deleteThrough(2);

    // What flash media leaves after a power cut: present, unreadable.
    writeFileSync(sidecarFile(dir), Buffer.alloc(4096));

    record(sample({ ts: AUG_23 + 5000, path: "still.here", value: 2 }));
    const second = await roll({
      dataDir: dir,
      maxRowid: store.rollBound()!.maxRowid,
      rollId: 2,
    });

    assert.equal(second.sidecarRows, 2);
    assert.deepEqual(
      readParquet(sidecarFile(dir)).map((row) => [row.path, row.value_num]),
      [
        ["went.quiet", 7],
        ["still.here", 2],
      ],
      "the path that went quiet survives the rebuild",
    );
    assert.ok(existsSync(`${sidecarFile(dir)}.unreadable`));
  });

  it("stays out of a glob over the tree", async () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b" }));
    await roll({ dataDir: dir, maxRowid: 1, rollId: 1 });
    // The rows in the sidecar are copies of rows in the tree. A reader that
    // found it here would count every path's last value twice.
    const tree = readParquet(join(dir, DATA_LAYOUT.tree, "**", "*.parquet"));
    assert.equal(tree.length, 1);
  });
});

describe(
  "the claim on the data directory",
  { skip: NO_BUNDLED_EXTENSION },
  () => {
    it("refuses to run beside a live roll", async () => {
      record(sample({ ts: AUG_23 + 1000 }));
      // Stands in for an orphan roll left by a killed writer.
      const held = createServer();
      await new Promise<void>((resolve) =>
        held.listen(writerPaths(dir).rollSocket, () => resolve()),
      );
      try {
        const refused = attemptRoll(["--max-rowid", "1", "--roll-id", "1"]);
        assert.equal(refused.status, EXIT_LOCKED);
        assert.match(refused.stderr, /already running/);
        // And nothing was written while the other roll holds the directory.
        assert.ok(!existsSync(join(dir, DATA_LAYOUT.tree, "date=2026-08-23")));
      } finally {
        held.close();
      }
    });

    it("takes over a socket nothing answers on", async () => {
      // What a roll killed mid-flight leaves behind. The file is not the claim;
      // a process answering on it is. Killed abruptly, because a clean close
      // unlinks the socket and there would be nothing stale to take over.
      const socketPath = writerPaths(dir).rollSocket;
      const holder = spawn(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          [
            'import { createServer } from "node:net";',
            `createServer().listen(${JSON.stringify(socketPath)}, () =>`,
            '  console.log("listening"));',
          ].join("\n"),
        ],
        { stdio: ["ignore", "pipe", "inherit"] },
      );
      await new Promise<void>((resolve, reject) => {
        holder.stdout.on("data", (chunk: Buffer) => {
          if (chunk.toString().includes("listening")) resolve();
        });
        holder.on("exit", () => reject(new Error("the holder never listened")));
      });
      holder.kill("SIGKILL");
      await new Promise<void>((resolve) => holder.on("exit", () => resolve()));
      assert.ok(
        statSync(socketPath).isSocket(),
        "the file outlives the process",
      );

      record(sample({ ts: AUG_23 + 1000 }));
      execFileSync(
        process.execPath,
        [ROLL_ENTRY, "--data-dir", dir, "--max-rowid", "1", "--roll-id", "1"],
        { encoding: "utf8", timeout: 60_000 },
      );
      assert.deepEqual(readdirSync(dateDirectory(dir, AUG_23)), ["1.parquet"]);
    });

    it("names no path an operator configured when it refuses", async () => {
      record(sample({ ts: AUG_23 + 1000 }));
      const held = createServer();
      await new Promise<void>((resolve) =>
        held.listen(writerPaths(dir).rollSocket, () => resolve()),
      );
      try {
        const refused = attemptRoll(["--max-rowid", "1", "--roll-id", "1"]);
        // Anchored to a refusal that happened: empty stderr would satisfy the
        // absence check on its own and prove nothing about the message.
        assert.equal(refused.status, EXIT_LOCKED);
        assert.match(refused.stderr, /already running/);
        assert.ok(
          !refused.stderr.includes(dir),
          `the refusal must not echo the data directory: ${refused.stderr}`,
        );
      } finally {
        held.close();
      }
    });
  },
);

describe("the roll process", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("prints what it wrote and exits 0", () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b" }));
    const output = execFileSync(
      process.execPath,
      [ROLL_ENTRY, "--data-dir", dir, "--max-rowid", "1", "--roll-id", "123"],
      { encoding: "utf8", timeout: 60_000 },
    );
    const result = JSON.parse(output.trim()) as {
      rows: number;
      files: { date: string }[];
    };
    assert.equal(result.rows, 1);
    assert.deepEqual(
      result.files.map((file) => file.date),
      ["2026-08-23"],
    );
  });

  it("exits 1 and leaves the store alone when it cannot roll", () => {
    record(sample({ ts: AUG_23 + 1000 }));
    const failure = attemptRoll(["--max-rowid", "0", "--roll-id", "1"]);
    assert.equal(failure.status, 1);
    assert.match(failure.stderr, /must be a positive whole number/);
    assert.equal(store.rowCount(), 1);
    assert.ok(!existsSync(join(dir, DATA_LAYOUT.tree, "date=2026-08-23")));
  });

  it("exits with the name-taken code, which the scheduler treats apart", () => {
    // The scheduler drops the roll id on this exit and keeps it on every
    // other, so the two must be distinguishable from outside the process.
    record(sample({ ts: AUG_23 + 1000 }));
    execFileSync(
      process.execPath,
      [ROLL_ENTRY, "--data-dir", dir, "--max-rowid", "1", "--roll-id", "5"],
      { encoding: "utf8", timeout: 60_000 },
    );
    record(sample({ ts: AUG_23 + 2000 }));
    const refused = attemptRoll(["--max-rowid", "2", "--roll-id", "5"]);

    assert.equal(refused.status, EXIT_NAME_TAKEN);
    assert.match(refused.stderr.split("\n")[0], /already exists/);
  });
});

describe(
  "a tree the roll did not finish",
  { skip: NO_BUNDLED_EXTENSION },
  () => {
    it("leaves a temporary a *.parquet glob skips, and collects it later", () => {
      // The previous version of this test wrote its own `1.parquet.tmp` and
      // asserted that filtering for `.endsWith(".parquet")` found nothing — it
      // exercised String.endsWith, not the roll. This uses the roll's own
      // naming, and checks the sweep that eventually collects one.
      const stale = rollTempFile(dir, AUG_23, 1);
      mkdirSync(dateDirectory(dir, AUG_23), { recursive: true });
      writeFileSync(stale, "half a parquet file");
      // Older than the sweep's cutoff, as a roll killed an hour ago would be.
      utimesSync(
        stale,
        new Date(Date.now() - 7_200_000),
        new Date(Date.now() - 7_200_000),
      );

      assert.deepEqual(
        readdirSync(dateDirectory(dir, AUG_23)).filter((name) =>
          name.endsWith(".parquet"),
        ),
        [],
        "nothing a reader would treat as finished",
      );

      record(sample({ ts: AUG_23 + 1000 }));
      return roll({ dataDir: dir, maxRowid: 1, rollId: 2 }).then(() => {
        assert.deepEqual(readdirSync(dateDirectory(dir, AUG_23)), [
          "2.parquet",
        ]);
      });
    });
  },
);
