import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert";
import { sample } from "./fixtures.js";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { HotStore, LAYOUT_VERSION } from "../writer/hot-store.js";
import type { Sample } from "../writer/protocol.js";

let dir: string;
let store: HotStore;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "hot-store-"));
  store = HotStore.open(join(dir, "hot.sqlite"));
});

afterEach(() => {
  try {
    store.close();
  } catch {
    // Already closed by the test.
  }
  rmSync(dir, { recursive: true, force: true });
});

/** Reads rows back through a second handle, the way the roll will. */
function rows(path = join(dir, "hot.sqlite")): Record<string, unknown>[] {
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    return db.prepare("SELECT * FROM sample ORDER BY rowid").all() as Record<
      string,
      unknown
    >[];
  } finally {
    db.close();
  }
}

describe("the store is configured for cheap, durable-enough writes", () => {
  it("runs in WAL with synchronous NORMAL", () => {
    // WAL is what lets the roll read while the writer holds the store, and
    // NORMAL is the difference between 435 bytes written per row and
    // signalk-parquet's measured 35 KB from per-record FULL commits.
    assert.strictEqual(store.pragma("journal_mode"), "wal");
    assert.strictEqual(store.pragma("synchronous"), 1);
  });

  it("carries no index on the sample table", () => {
    // Still deliberate, and re-checked when the query service began reading
    // this table's lower bound. Every index is paid on each insert, and the
    // ingest path is what this design is judged on: a ts index measured
    // +29.6% bytes at a 5-minute interval and +30.8% at an hour, which would
    // take the writer's 17.53 KB/s to about 22.8.
    //
    // What reads by key is `query/reader.ts`, which asks SELECT MIN(ts) per
    // history request to decide whether the store is worth attaching. Scanned
    // rather than sought, that is 0.84 ms over a 5-minute interval's 16k rows
    // and 13.99 ms over an hour's 197k -- against the ~11 ms and ~139 ms of
    // DuckDB scan the answer saves. The scan is the cheaper side of the trade.
    store.beginSession("s1");
    const db = new DatabaseSync(join(dir, "hot.sqlite"), { readOnly: true });
    try {
      const indexes = db
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'sample'",
        )
        .all();
      assert.deepStrictEqual(indexes, []);
    } finally {
      db.close();
    }
  });
});

describe("value fidelity", () => {
  beforeEach(() => store.beginSession("s1"));

  it("round-trips every kind with its value in its own column", () => {
    store.insertBatch(1, [
      sample({ kind: "number", value: -4.25 }),
      sample({ kind: "string", value: "moored", path: "navigation.state" }),
      sample({
        kind: "position",
        path: "navigation.position",
        value: { latitude: -60.166_666_7, longitude: 24.943_333_3 },
      }),
      sample({ kind: "identity", path: "name", value: "Øresund ⛵" }),
    ]);

    const stored = rows();
    assert.strictEqual(stored[0].value_kind, "number");
    assert.strictEqual(stored[0].value_num, -4.25);
    assert.strictEqual(stored[0].value_str, null);

    assert.strictEqual(stored[1].value_kind, "string");
    assert.strictEqual(stored[1].value_str, "moored");
    assert.strictEqual(stored[1].value_num, null);

    assert.strictEqual(stored[2].value_kind, "position");
    assert.strictEqual(stored[2].value_lat, -60.166_666_7);
    assert.strictEqual(stored[2].value_lon, 24.943_333_3);

    assert.strictEqual(stored[3].value_kind, "identity");
    assert.strictEqual(stored[3].path, "name");
    assert.strictEqual(stored[3].value_str, "Øresund ⛵");
  });

  it("tells a recorded boolean from a string that says true", () => {
    // The reason value_kind exists. Without it both are the text "true" and
    // Unit 4c cannot replay one as a boolean and the other as a string, while
    // a round-trip test that only checks the value still passes.
    store.insertBatch(1, [
      sample({ kind: "boolean", value: "true", path: "electrical.switch.1" }),
      sample({ kind: "string", value: "true", path: "navigation.state" }),
    ]);

    const stored = rows();
    assert.strictEqual(stored[0].value_str, stored[1].value_str);
    assert.notStrictEqual(stored[0].value_kind, stored[1].value_kind);
    assert.strictEqual(stored[0].value_kind, "boolean");
    assert.strictEqual(stored[1].value_kind, "string");
  });

  it("keeps a null source null rather than turning it into a string", () => {
    store.insertBatch(1, [sample({ source: null })]);
    assert.strictEqual(rows()[0].source, null);
  });

  it("refuses a value kind the schema does not know", () => {
    // The store is written by a process reading a socket, so the schema states
    // the closed set rather than trusting the reader to have checked.
    assert.throws(() =>
      store.insertBatch(1, [
        { ...sample(), kind: "elephant", value: 1 } as unknown as Sample,
      ]),
    );
  });
});

describe("a batch is one transaction", () => {
  beforeEach(() => store.beginSession("s1"));

  it("stores none of a batch when one row is rejected", () => {
    store.insertBatch(1, [sample({ ts: 1 })]);
    assert.strictEqual(store.rowCount(), 1);

    assert.throws(() =>
      store.insertBatch(2, [
        sample({ ts: 2 }),
        { ...sample({ ts: 3 }), kind: "elephant" } as unknown as Sample,
        sample({ ts: 4 }),
      ]),
    );

    assert.strictEqual(store.rowCount(), 1, "the good rows rolled back too");
  });

  it("reports why the batch failed, not why the rollback did", () => {
    // The failure has to happen *inside* the transaction. Closing the store
    // makes BEGIN IMMEDIATE throw before the try block, which exercises none
    // of the rollback path this test is named for. A CHECK violation on the
    // second row does: the transaction is open, the insert fails, and the
    // reported error must still be the insert's.
    let raised: unknown;
    try {
      store.insertBatch(1, [
        sample({ ts: 1 }),
        { ...sample({ ts: 2 }), kind: "elephant" } as unknown as Sample,
      ]);
    } catch (err) {
      raised = err;
    }

    assert.ok(raised instanceof Error);
    assert.match(
      (raised as Error).message,
      /CHECK constraint/,
      `the insert's error was replaced by the rollback's: ${String(raised)}`,
    );
    assert.strictEqual(store.rowCount(), 0, "the good row was not rolled back");
  });

  it("leaves the sequence counter untouched when a batch rolls back", () => {
    // Otherwise the failed batch is remembered as applied and its retry is
    // discarded as a duplicate — silent loss on the path that already failed.
    store.insertBatch(1, [sample()]);
    assert.throws(() =>
      store.insertBatch(2, [
        { ...sample(), kind: "elephant" } as unknown as Sample,
      ]),
    );

    assert.deepStrictEqual(store.insertBatch(2, [sample()]), {
      stored: 1,
      skipped: false,
    });
  });
});

describe("a resend after a lost acknowledgement is not stored twice", () => {
  it("skips a sequence number it has already committed", () => {
    store.beginSession("s1");
    assert.deepStrictEqual(store.insertBatch(1, [sample(), sample()]), {
      stored: 2,
      skipped: false,
    });

    assert.deepStrictEqual(store.insertBatch(1, [sample(), sample()]), {
      stored: 0,
      skipped: true,
    });
    assert.strictEqual(store.rowCount(), 2);
  });

  it("resumes the counter when the same session reopens the store", () => {
    // The writer restarting under a live plugin. Its sequence numbers keep
    // climbing, so the counter has to survive in the file, not just in memory.
    store.beginSession("s1");
    store.insertBatch(7, [sample()]);
    store.close();

    store = HotStore.open(join(dir, "hot.sqlite"));
    assert.strictEqual(store.beginSession("s1"), 7);
    assert.deepStrictEqual(store.insertBatch(7, [sample()]), {
      stored: 0,
      skipped: true,
    });
  });

  it("resets the counter for a different session", () => {
    // The plugin restarting. Its counter goes back to 1, and treating those as
    // duplicates of the previous run would acknowledge every batch and store
    // none of them, with the plugin reporting healthy throughout.
    store.beginSession("s1");
    store.insertBatch(40_000, [sample()]);

    assert.strictEqual(store.beginSession("s2"), 0);
    assert.deepStrictEqual(store.insertBatch(1, [sample()]), {
      stored: 1,
      skipped: false,
    });
    assert.strictEqual(store.rowCount(), 2, "the earlier run's rows stay");
  });

  it("refuses a batch before a session is bound", () => {
    assert.throws(() => store.insertBatch(1, [sample()]));
  });
});

describe("an abruptly killed writer", () => {
  it("loses nothing it committed, and the store reopens", () => {
    // The issue's error path: SIGKILL mid-flush, loss bounded to the flush
    // window. A normal close would exercise SQLite's shutdown rather than its
    // recovery, so this kills a real process holding a real WAL.
    const dbPath = join(dir, "kill.sqlite");
    // Driven through the compiled module in a child so the kill lands on a
    // process that really holds the store open.
    const script = `
      import { HotStore } from ${JSON.stringify(
        new URL("../writer/hot-store.js", import.meta.url).href,
      )};
      const store = HotStore.open(${JSON.stringify(dbPath)});
      store.beginSession("s1");
      for (let seq = 1; seq <= 5; seq++) {
        store.insertBatch(seq, [
          { ts: seq, context: "vessels.self", path: "a.b", source: null, kind: "number", value: seq },
        ]);
      }
      process.stdout.write("committed\\n");
      setInterval(() => {}, 1000);
    `;
    const victim = spawn(
      process.execPath,
      [
        "--disable-warning=ExperimentalWarning",
        "--input-type=module",
        "-e",
        script,
      ],
      { stdio: ["ignore", "pipe", "inherit"] },
    );

    return new Promise<void>((resolve, reject) => {
      let seen = "";
      let killed = false;
      // Without this the promise settles only from inside the stdout handler,
      // so a child that dies before printing -- node:sqlite failing to load,
      // say -- hangs the suite instead of failing it.
      victim.on("exit", (code, signal) => {
        if (!killed) {
          reject(
            new Error(
              `the child exited before committing (${code ?? signal}): ${seen}`,
            ),
          );
        }
      });
      victim.stdout.on("data", (chunk: Buffer) => {
        seen += chunk.toString();
        if (killed || !seen.includes("committed")) return;
        killed = true;
        victim.kill("SIGKILL");
        victim.on("exit", () => {
          try {
            const reopened = HotStore.open(dbPath);
            try {
              assert.strictEqual(reopened.rowCount(), 5);
              assert.strictEqual(reopened.beginSession("s1"), 5);
            } finally {
              reopened.close();
            }
            resolve();
          } catch (err) {
            reject(err);
          }
        });
      });
      victim.on("error", reject);
    });
  });
});

describe("a store this build does not understand", () => {
  it("is refused rather than adopted", () => {
    // CREATE TABLE IF NOT EXISTS accepts whatever table already carries the
    // name, so without a marker a store written by a different build is used:
    // a dropped column surfaces as "writer exited (code 1)" and a changed
    // CHECK not until a row hits it.
    store.close();
    const db = new DatabaseSync(join(dir, "hot.sqlite"));
    db.exec("PRAGMA user_version = 99");
    db.close();

    assert.throws(
      () => HotStore.open(join(dir, "hot.sqlite")),
      /layout version 99/,
    );
    store = HotStore.open(join(dir, "other.sqlite"));
  });

  it("stamps a fresh store with the version it writes", () => {
    assert.strictEqual(store.pragma("user_version"), LAYOUT_VERSION);
  });
});

describe("integrity", () => {
  it("passes SQLite's own integrity check after a batch", () => {
    store.beginSession("s1");
    store.insertBatch(1, [sample(), sample({ kind: "string", value: "x" })]);
    assert.strictEqual(store.integrityCheck(), "ok");
  });
});

describe("the bound a roll is truncated against", () => {
  it("reports nothing to roll for an empty store", () => {
    assert.equal(store.rollBound(), null);
  });

  it("reports the highest rowid and how many rows sit at or below it", () => {
    store.beginSession("s");
    store.insertBatch(1, [sample({ ts: 1 }), sample({ ts: 2 })]);
    store.insertBatch(2, [sample({ ts: 3 })]);
    assert.deepEqual(store.rollBound(), { maxRowid: 3, rows: 3 });
  });

  it("deletes through the bound and leaves everything after it", () => {
    store.beginSession("s");
    store.insertBatch(1, [sample({ ts: 1 }), sample({ ts: 2 })]);
    const bound = store.rollBound();
    assert.ok(bound !== null);
    // What a roll does between reading the bound and the delete: the writer
    // keeps ingesting throughout, and those rows were never rolled.
    store.insertBatch(2, [sample({ ts: 3 })]);

    assert.equal(store.deleteThrough(bound.maxRowid), 2);
    assert.deepEqual(
      rows().map((row) => row.ts),
      [3],
    );
  });

  it("does not delete a late sample that arrived after the bound was read", () => {
    // The recorder stamps ts when the delta arrives and the buffer holds it
    // for up to a flush interval, so a sample older than the roll's window
    // routinely reaches the store after the roll started. Truncating by
    // timestamp would delete it unrolled; truncating by rowid cannot.
    store.beginSession("s");
    store.insertBatch(1, [sample({ ts: 5000 })]);
    const bound = store.rollBound();
    assert.ok(bound !== null);
    store.insertBatch(2, [sample({ ts: 1 })]);

    store.deleteThrough(bound.maxRowid);
    assert.deepEqual(
      rows().map((row) => row.ts),
      [1],
    );
  });

  it("starts a new rowid sequence once the store has been emptied", () => {
    // SQLite hands out max(rowid)+1, so emptying the table restarts at 1.
    // That is why a bound is read, used and dropped inside one roll and is
    // never carried across two.
    store.beginSession("s");
    store.insertBatch(1, [sample({ ts: 1 })]);
    store.deleteThrough(store.rollBound()!.maxRowid);
    assert.equal(store.rollBound(), null);

    store.insertBatch(2, [sample({ ts: 2 })]);
    assert.deepEqual(store.rollBound(), { maxRowid: 1, rows: 1 });
  });

  it("refuses a bound that is not a rowid", () => {
    store.beginSession("s");
    store.insertBatch(1, [sample({ ts: 1 })]);
    for (const bound of [0, -1, 1.5, NaN]) {
      assert.throws(() => store.deleteThrough(bound), /rowid/, `${bound}`);
    }
    assert.equal(rows().length, 1);
  });
});

/**
 * The index the query service's boundary read depends on.
 *
 * Without it `SELECT MIN(ts)` is a scan of a table holding an interval's whole
 * ingest, and that read happens on every history request. It is asserted here
 * rather than left to the schema string because nothing else fails when it is
 * missing -- queries stay correct and quietly get slower.
 */
