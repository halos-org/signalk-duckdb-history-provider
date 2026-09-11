import { DatabaseSync } from "node:sqlite";
import type { StatementSync } from "node:sqlite";
import { VALUE_KINDS } from "./protocol.js";
import type { Sample } from "./protocol.js";

/**
 * The SQLite hot store, owned by the writer process.
 *
 * `node:sqlite` rather than a native dependency: its API is identical on Node
 * 22, 24 and 26 — the versions this package supports, the Signal K container
 * runs, and development uses — so the store needs no compiled module, which
 * also keeps it out of the platform staging a device deploy would otherwise
 * have to do.
 *
 * Nothing here imports DuckDB. The roll and the query layer read this file
 * with `sqlite_scanner` from their own processes; this one only writes.
 */

/**
 * The closed set of value kinds, stated in the schema rather than trusted from
 * the reader. The process writing this store is reading a socket, and a CHECK
 * is the last place a kind nobody planned for can be stopped.
 */
/**
 * What this build writes. Bumped when the shape of `sample` changes.
 *
 * `CREATE TABLE IF NOT EXISTS` adopts whatever table already carries the name,
 * so without a marker a store written by a different build is used rather than
 * refused — a dropped column surfaces as "writer exited (code 1)" and a
 * changed CHECK not until a row hits it.
 */
export const LAYOUT_VERSION = 1;

const KIND_CHECK = VALUE_KINDS.map((kind) => `'${kind}'`).join(",");

const SCHEMA = `
CREATE TABLE IF NOT EXISTS sample (
  ts         INTEGER NOT NULL,
  context    TEXT    NOT NULL,
  path       TEXT    NOT NULL,
  source     TEXT,
  value_kind TEXT    NOT NULL
             CHECK (value_kind IN (${KIND_CHECK})),
  value_num  REAL,
  value_str  TEXT,
  value_lat  REAL,
  value_lon  REAL
);

CREATE TABLE IF NOT EXISTS writer_state (
  id       INTEGER PRIMARY KEY CHECK (id = 0),
  session  TEXT    NOT NULL,
  last_seq INTEGER NOT NULL
);
`;

const INSERT_SAMPLE = `
INSERT INTO sample (ts, context, path, source, value_kind, value_num, value_str, value_lat, value_lon)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`;

export interface BatchResult {
  /** Rows written. Zero when the batch was recognised as already applied. */
  stored: number;
  /** True when this sequence number had already been committed. */
  skipped: boolean;
}

export class HotStore {
  private readonly db: DatabaseSync;
  private readonly insertSample: StatementSync;
  private readonly saveSeq: StatementSync;
  private session: string | null = null;
  private lastSeq = 0;

  private constructor(db: DatabaseSync) {
    this.db = db;
    this.insertSample = db.prepare(INSERT_SAMPLE);
    this.saveSeq = db.prepare(
      "UPDATE writer_state SET session = ?, last_seq = ? WHERE id = 0",
    );
  }

  static open(path: string): HotStore {
    const db = new DatabaseSync(path);
    // WAL is what lets the roll read the store while the writer still holds
    // it. NORMAL is the durability trade the design already accepts: crash
    // loss is bounded by the flush window either way, and FULL is what costs
    // signalk-parquet 35 KB of writes per 337-byte row.
    db.exec("PRAGMA journal_mode = WAL");
    db.exec("PRAGMA synchronous = NORMAL");

    // Refuse a store this build does not understand, rather than adopting it.
    // A fresh file reports 0 and is stamped; anything else must match.
    const row = db.prepare("PRAGMA user_version").get() as {
      user_version: number;
    };
    const existing = row.user_version;
    if (existing !== 0 && existing !== LAYOUT_VERSION) {
      db.close();
      throw new Error(
        `the hot store at ${path} has layout version ${existing}; this build writes ${LAYOUT_VERSION}`,
      );
    }

    db.exec(SCHEMA);
    if (existing === 0) db.exec(`PRAGMA user_version = ${LAYOUT_VERSION}`);
    return new HotStore(db);
  }

  /**
   * Binds the store to one run of the plugin and returns the last sequence
   * number committed for it.
   *
   * A different session resets the counter to zero. Without that, a writer
   * outliving its plugin would compare the new run's sequence numbers against
   * the old run's, discard every batch as a duplicate, acknowledge them all,
   * and record nothing while the plugin reported healthy.
   */
  beginSession(session: string): number {
    const row = this.db
      .prepare("SELECT session, last_seq FROM writer_state WHERE id = 0")
      .get() as { session: string; last_seq: number } | undefined;

    if (row === undefined) {
      this.db
        .prepare(
          "INSERT INTO writer_state (id, session, last_seq) VALUES (0, ?, 0)",
        )
        .run(session);
      this.session = session;
      this.lastSeq = 0;
    } else if (row.session === session) {
      this.session = session;
      this.lastSeq = row.last_seq;
    } else {
      this.saveSeq.run(session, 0);
      this.session = session;
      this.lastSeq = 0;
    }
    return this.lastSeq;
  }

  /**
   * Writes one batch as one transaction.
   *
   * One transaction per flush, not per row: `signalk-parquet` commits each
   * record on its own with `synchronous = FULL` and pays 35 KB of writes for a
   * 337-byte row. The sequence number moves inside the same transaction, so a
   * batch that rolls back is not remembered as applied — otherwise its retry
   * would be discarded as a duplicate, losing data on the path that had
   * already failed once.
   */
  insertBatch(seq: number, samples: Sample[]): BatchResult {
    if (this.session === null) {
      throw new Error("insertBatch before a session was bound");
    }
    if (seq <= this.lastSeq) return { stored: 0, skipped: true };

    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const sample of samples) {
        const num = sample.kind === "number" ? sample.value : null;
        const str =
          sample.kind === "string" ||
          sample.kind === "boolean" ||
          sample.kind === "identity"
            ? sample.value
            : null;
        const lat = sample.kind === "position" ? sample.value.latitude : null;
        const lon = sample.kind === "position" ? sample.value.longitude : null;
        this.insertSample.run(
          sample.ts,
          sample.context,
          sample.path,
          sample.source,
          sample.kind,
          num,
          str,
          lat,
          lon,
        );
      }
      this.saveSeq.run(this.session, seq);
      this.db.exec("COMMIT");
    } catch (err) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        // Swallowed deliberately: the caller needs the reason the batch
        // failed, not the bookkeeping that followed. SQLITE_FULL and
        // SQLITE_IOERR can roll the transaction back themselves, after which
        // this ROLLBACK fails with "no transaction is active" — and rethrowing
        // that would report a rollback problem on a disk-full.
      }
      throw err;
    }

    this.lastSeq = seq;
    return { stored: samples.length, skipped: false };
  }

  /**
   * What a roll should cover: the highest rowid present now, and how many
   * rows sit at or below it. `null` when there is nothing to roll.
   *
   * The bound is a rowid rather than a timestamp because the two sets have to
   * be identical — what the roll writes to Parquet and what is then deleted
   * from here. A timestamp cannot do that: the recorder stamps `ts` when the
   * delta arrives and the flush buffer holds it for up to a flush interval,
   * so a sample older than the roll's window routinely lands in the store
   * after the roll has read it. Deleting by timestamp would drop that sample
   * unrolled; deleting by rowid cannot, because a row inserted after the
   * bound was read has a higher one.
   *
   * **Read it, use it, drop it.** SQLite hands out `max(rowid) + 1`, so a
   * store that has just been emptied starts again at 1 — a bound carried
   * across two rolls could then name rows it never covered.
   */
  rollBound(): { maxRowid: number; rows: number } | null {
    const row = this.db
      .prepare("SELECT max(rowid) AS maxRowid, count(*) AS rows FROM sample")
      .get() as { maxRowid: number | null; rows: number };
    if (row.maxRowid === null) return null;
    return { maxRowid: row.maxRowid, rows: row.rows };
  }

  /**
   * Delete every row through `maxRowid`, in one transaction, and report how
   * many went. Called only after a roll has written those rows to Parquet and
   * exited successfully.
   */
  deleteThrough(maxRowid: number): number {
    if (!Number.isSafeInteger(maxRowid) || maxRowid < 1) {
      throw new Error(`${maxRowid} is not a rowid to delete through`);
    }
    const result = this.db
      .prepare("DELETE FROM sample WHERE rowid <= ?")
      .run(maxRowid);
    return Number(result.changes);
  }

  /**
   * The timestamp of the oldest row still in the store, or `null` when it is
   * empty.
   *
   * By rowid rather than `min(ts)`: rowid order is insertion order, so this is
   * one row rather than a scan of a table that can hold millions.
   */
  oldestTimestamp(): number | null {
    const row = this.db
      .prepare("SELECT ts FROM sample ORDER BY rowid LIMIT 1")
      .get() as { ts: number } | undefined;
    return row?.ts ?? null;
  }

  /**
   * Push the WAL into the database file.
   *
   * `synchronous = NORMAL` means a commit is not fsynced, so a delete can be
   * lost to an unclean power-off while whatever the caller did afterwards
   * survives. The roll's truncate is the one place that matters: losing it
   * while its bookkeeping survives puts the same rows in the tree twice.
   */
  checkpoint(): void {
    this.db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
  }

  rowCount(): number {
    const row = this.db.prepare("SELECT count(*) AS n FROM sample").get() as {
      n: number;
    };
    return row.n;
  }

  /** Reads back one pragma, so the settings above can be asserted rather than assumed. */
  pragma(name: string): string | number {
    const row = this.db.prepare(`PRAGMA ${name}`).get() as
      Record<string, string | number> | undefined;
    if (row === undefined) throw new Error(`no such pragma: ${name}`);
    return Object.values(row)[0];
  }

  integrityCheck(): string {
    const row = this.db.prepare("PRAGMA integrity_check").get() as {
      integrity_check: string;
    };
    return row.integrity_check;
  }

  close(): void {
    this.db.close();
  }
}
