import {
  existsSync,
  mkdirSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeSync,
} from "node:fs";
import { join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import type { DuckDBConnection } from "@duckdb/node-api";
import { DATA_DIR_MODE, DATA_LAYOUT } from "../data-dir.js";
import { commitFile } from "../durable-write.js";
import { BASE_DUCKDB_CONFIG, loadSqliteScanner } from "../duckdb/extension.js";
import { sqlLiteral } from "../duckdb/sql.js";
import { expire } from "../retention/expire.js";
import type { ExpiryFailure } from "../retention/expire.js";
import { writerPaths } from "../writer/contract.js";
import {
  compactedHourCovering,
  dateDirectory,
  rollFile,
  rollTempFile,
  sidecarFile,
  sidecarTempFile,
  treeRoot,
  utcDateSegment,
} from "./tree-path.js";

/**
 * The roll: the hot store's rows, as Parquet.
 *
 * It runs in its own process and exits, and that exit is the mechanism rather
 * than tidiness — DuckDB's allocator does not return this memory in-process,
 * so an engine held open would turn a transient into a standing cost.
 *
 * One streaming `COPY` per UTC date, never `PARTITION_BY`. A partitioned
 * write's peak rises with the row count and runs out of memory at this
 * package's path cardinality; a plain `COPY` streams and stays near 150 MB at
 * 1.27M rows. Measured in `docs/layout-decision.md`.
 *
 * The set it writes is `rowid <= maxRowid`, decided by the writer before this
 * process starts. Every row in that set lands in the date directory its own
 * timestamp names, so a roll spanning midnight writes two files and both are
 * correct.
 */

/**
 * The roll's name is already taken by a roll that is not this one.
 *
 * Its own class because the scheduler has to tell this failure from every
 * other: a retry inherits the failed attempt's id so it can replace its own
 * half-written output, and inheriting it for *this* failure turns the refusal
 * into permission one slot later.
 */
export class NameTakenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NameTakenError";
  }
}

/** Milliseconds in a UTC day. The date directory is cut on these. */
const DAY_MS = 86_400_000;

/**
 * What the engine may allocate.
 *
 * Explicit because DuckDB's default is 80% of system RAM, which on a 4 GB
 * device is memory the marine stack is already using. A streaming COPY does
 * not need much; the limit is a ceiling, not a reservation.
 */
export const DEFAULT_MEMORY_LIMIT = "256MB";

/** Columns the tree carries, in the order every file writes them. */
export const COLUMN_LIST = [
  "ts",
  "context",
  "path",
  "source",
  "value_kind",
  "value_num",
  "value_str",
  "value_lat",
  "value_lon",
].join(", ");

export interface RollOptions {
  dataDir: string;
  /** The highest rowid this roll covers, read by the writer beforehand. */
  maxRowid: number;
  /**
   * Names the roll's files. The writer keeps it stable across a retry, so a
   * second attempt overwrites the first attempt's files instead of leaving
   * them beside its own as duplicates.
   */
  rollId: number;
  memoryLimit?: string;
  /**
   * Whole days of tree to keep. Zero keeps everything.
   *
   * Expiry runs here because the roll already holds the claim on the data
   * directory, so the one thing that could delete a directory another process
   * is writing into cannot be running. It costs the roll a readdir and an
   * unlink; nothing about it needs the engine.
   */
  retentionDays?: number;
  /** Injected in tests, for the retention boundary. Production reads the
   * clock. */
  now?: () => number;
  /**
   * Whether this roll may replace a file that already carries its name.
   *
   * Only a retry may. A fresh roll that finds its name taken has been handed
   * an id that belongs to an earlier roll, and overwriting would replace that
   * roll's rows with a fraction of them — which is exactly what a schedule
   * reading the clock a millisecond early once did on a device.
   */
  replace?: boolean;
}

export interface RolledFile {
  date: string;
  path: string;
  rows: number;
}

export interface RollResult {
  rollId: number;
  maxRowid: number;
  rows: number;
  /** One per UTC date the covered rows fell in. */
  files: RolledFile[];
  sidecarRows: number;
  /** Date directories retention removed after this roll wrote. */
  expired: string[];
  /** Directories retention should have removed and could not. */
  expiryFailures: ExpiryFailure[];
  /** Why retention could not list the tree at all, if it could not. */
  expiryTreeError: string | null;
}

export async function roll(options: RollOptions): Promise<RollResult> {
  const { dataDir, maxRowid, rollId } = options;
  if (!Number.isSafeInteger(maxRowid) || maxRowid < 1) {
    throw new Error(`${maxRowid} is not a rowid to roll through`);
  }

  // Per roll, never the shared root. Two rolls can overlap — an orphan left
  // by a killed writer and its successor — and a `finally` that removed the
  // root would delete the other's live spill. The root is also a directory an
  // operator's data directory may already have had: `tmp` is the most
  // conventional name there is, and removing it recursively on a schedule is
  // not something this package may do to a path it did not create.
  //
  // 0700 like everything else under the data directory: a spill holds the same
  // rows the hot store does, and mkdir's mode is masked by umask, so the mode
  // has to be asked for explicitly.
  const scratchRoot = join(dataDir, DATA_LAYOUT.scratch);
  mkdirSync(scratchRoot, { recursive: true, mode: DATA_DIR_MODE });
  sweepStaleScratch(scratchRoot);
  const scratch = join(scratchRoot, `${rollId}-${process.pid}`);
  mkdirSync(scratch, { recursive: true, mode: DATA_DIR_MODE });

  const instance = await DuckDBInstance.create(":memory:", {
    ...BASE_DUCKDB_CONFIG,
    memory_limit: options.memoryLimit ?? DEFAULT_MEMORY_LIMIT,
    // Explicit, because the default for an in-memory database is relative to
    // the current working directory — which for a process the writer spawned
    // is the Signal K server's, not anywhere this package owns.
    temp_directory: scratch,
  });
  const connection = await instance.connect();
  let written: { files: RolledFile[]; sidecarRows: number };
  try {
    await loadSqliteScanner(connection, {
      cacheDir: join(dataDir, DATA_LAYOUT.extensionCache),
    });
    // READ_ONLY: the writer owns this file and keeps ingesting throughout.
    // SQLite in WAL mode takes concurrent readers, which is why the hot store
    // is SQLite at all.
    await connection.run(
      `ATTACH '${sqlLiteral(writerPaths(dataDir).store)}' AS hot (TYPE SQLITE, READ_ONLY)`,
    );

    const days = await coveredDays(connection, maxRowid);
    refuseMergedHour(dataDir, days, rollId);
    const files: RolledFile[] = [];
    for (const day of days) {
      files.push(
        await writeDay({
          connection,
          dataDir,
          rollId,
          maxRowid,
          day,
          replace: options.replace === true,
        }),
      );
    }
    written = {
      files,
      sidecarRows: await writeSidecar({ connection, dataDir, maxRowid }),
    };
  } finally {
    connection.closeSync();
    instance.closeSync();
    // This directory is this roll's alone, which is what makes removing it
    // safe. A roll killed by a signal never reaches here, so the sweep at the
    // top is what eventually collects those.
    rmSync(scratch, { recursive: true, force: true });
  }

  // After the engine is closed, and after this roll's own files are in the
  // tree: the boundary is measured from the newest date directory, and the day
  // this roll just wrote is the one that has to set it.
  const expired = expire({
    dataDir,
    retentionDays: options.retentionDays ?? 0,
    now: options.now,
  });

  return {
    rollId,
    maxRowid,
    rows: written.files.reduce((total, file) => total + file.rows, 0),
    files: written.files,
    sidecarRows: written.sidecarRows,
    expired: expired.removed,
    expiryFailures: expired.failures,
    expiryTreeError: expired.treeError,
  };
}

/**
 * Which UTC days the covered rows fall in, as day numbers.
 *
 * Integer division rather than a DuckDB date function, so the tree's date and
 * `tree-path.ts`'s date come out of the same arithmetic and cannot disagree
 * about a timezone.
 */
async function coveredDays(
  connection: DuckDBConnection,
  maxRowid: number,
): Promise<number[]> {
  const result = await connection.runAndReadAll(
    `SELECT DISTINCT CAST(floor(ts / ${DAY_MS}.0) AS BIGINT) AS day
     FROM hot.sample WHERE rowid <= ${maxRowid} ORDER BY day`,
  );
  return result.getRowsJS().map((row) => Number(row[0]));
}

async function writeDay(args: {
  connection: DuckDBConnection;
  dataDir: string;
  rollId: number;
  maxRowid: number;
  day: number;
  replace: boolean;
}): Promise<RolledFile> {
  const { connection, dataDir, rollId, maxRowid, day, replace } = args;
  const from = day * DAY_MS;
  mkdirSync(dateDirectory(dataDir, from), {
    recursive: true,
    mode: DATA_DIR_MODE,
  });
  const temp = rollTempFile(dataDir, from, rollId);
  const final = rollFile(dataDir, from, rollId);
  sweepStaleTemporaries(dateDirectory(dataDir, from), temp);
  if (!replace && existsSync(final)) {
    throw new NameTakenError(
      `${final} already exists and this roll is not a retry. Overwriting it ` +
        `would replace an earlier roll's rows with a subset of them.`,
    );
  }

  // No ORDER BY. The scan follows rowid, which is insertion order, which is
  // arrival order — so rows come out in timestamp order already and the row
  // group statistics prune on time without paying for a sort. Sorting a
  // window this size costs three times the memory the rest of the roll uses,
  // measured in docs/layout-decision.md.
  const select =
    `SELECT ${COLUMN_LIST} FROM hot.sample ` +
    `WHERE rowid <= ${maxRowid} AND ts >= ${from} AND ts < ${from + DAY_MS}`;
  await connection.run(
    `COPY (${select}) TO '${sqlLiteral(temp)}' (FORMAT parquet, COMPRESSION zstd)`,
  );
  commitFile(temp, final);

  const counted = await connection.runAndReadAll(
    `SELECT count(*) FROM read_parquet('${sqlLiteral(final)}')`,
  );
  return {
    date: utcDateSegment(from),
    path: final,
    rows: Number(counted.getRowsJS()[0][0]),
  };
}

/**
 * Rewrite the cumulative last-value sidecar.
 *
 * Cumulative is the whole point: a sidecar holding only this roll's paths
 * answers "the latest value of everything" wrong for any path that stopped
 * reporting before this roll. The previous sidecar is folded in and the
 * newest row per `(context, path)` wins.
 *
 * Written after the tree files. If the process dies between the two, the
 * writer never sees a successful exit, never truncates, and the retry rebuilds
 * both from a store that still holds every row.
 */
async function writeSidecar(args: {
  connection: DuckDBConnection;
  dataDir: string;
  maxRowid: number;
}): Promise<number> {
  const { connection, dataDir, maxRowid } = args;
  const final = sidecarFile(dataDir);
  const temp = sidecarTempFile(dataDir);
  mkdirSync(join(dataDir, DATA_LAYOUT.sidecar), {
    recursive: true,
    mode: DATA_DIR_MODE,
  });

  // `read_parquet` on a missing file is an error rather than an empty
  // relation, so the first roll on a device has to ask for something else.
  // `union_by_name` so a sidecar written by a build with a different column
  // set is folded in rather than failing — the alternative is that adding a
  // column stops every roll on every device until someone deletes this file.
  const fold = (): string =>
    ` UNION ALL BY NAME SELECT ${COLUMN_LIST} FROM read_parquet('${sqlLiteral(final)}', union_by_name = true)`;
  const write = async (previous: string): Promise<void> => {
    const sources = `SELECT ${COLUMN_LIST} FROM hot.sample WHERE rowid <= ${maxRowid}${previous}`;
    await connection.run(
      `COPY (SELECT DISTINCT ON (context, path) ${COLUMN_LIST} FROM (${sources}) ` +
        `ORDER BY context, path, ts DESC) ` +
        `TO '${sqlLiteral(temp)}' (FORMAT parquet, COMPRESSION zstd)`,
    );
  };

  if (existsSync(final)) {
    try {
      await write(fold());
    } catch (err) {
      // A file that exists and cannot be read would otherwise throw here on
      // every roll for ever, and nothing else regenerates it — so the store
      // would never be truncated again. Flash media that acknowledges a flush
      // without writing produces exactly this after a power cut.
      //
      // Rebuilt from the tree, not from this roll's window alone: the sidecar
      // answers "the last value of everything", and a window carries only the
      // paths that reported inside it. Rebuilding from the window would drop
      // every path that went quiet earlier — which is the one question the
      // sidecar exists to answer. Reading the whole tree is expensive and this
      // is the path where that is the cheaper mistake.
      const quarantine = `${final}.unreadable`;
      rmSync(quarantine, { force: true });
      renameSync(final, quarantine);
      const why = message(err);
      try {
        await write(foldTree(dataDir));
        writeStderr(
          `the sidecar could not be read (${why}); moved aside and rebuilt from the tree\n`,
        );
      } catch (treeErr) {
        // A tree with no files yet, or one that cannot be read either. The
        // window alone is worse than the tree and better than nothing — and
        // the operator has to be told which of the two they got, because the
        // difference is every path that went quiet before this window.
        await write("");
        writeStderr(
          `the sidecar could not be read (${why}) and neither could the tree ` +
            `(${message(treeErr)}); rebuilt from this roll's window alone, so ` +
            `paths that stopped reporting earlier are no longer in it\n`,
        );
      }
    }
  } else {
    await write("");
  }
  commitFile(temp, final);

  const counted = await connection.runAndReadAll(
    `SELECT count(*) FROM read_parquet('${sqlLiteral(final)}')`,
  );
  return Number(counted.getRowsJS()[0][0]);
}

/** The whole tree as a sidecar source, for the rebuild path. */
function foldTree(dataDir: string): string {
  const glob = join(treeRoot(dataDir), "**", "*.parquet");
  return (
    ` UNION ALL BY NAME SELECT ${COLUMN_LIST} ` +
    `FROM read_parquet('${sqlLiteral(glob)}', union_by_name = true)`
  );
}

/** An error's first line, which is its message rather than a stack frame. */
function message(err: unknown): string {
  return err instanceof Error ? err.message.split("\n")[0] : String(err);
}

/** Write to stderr and be sure it arrives: this process exits soon after. */
function writeStderr(line: string): void {
  writeSync(2, line);
}

/**
 * Refuse a roll id an existing merge already covers, before anything is
 * written.
 *
 * A merge unlinks the rolls it folded in, so a free name proves nothing about
 * whether the hour it belongs to has been merged. And `liveTreeFiles`
 * suppresses a roll by the id range a compacted file covers, not by the set
 * that merge actually read -- so such a roll would be on disk, reported as
 * written, and returned by no query, while the writer truncated its rows out
 * of the hot store on the strength of that report. Refusing is what makes the
 * reader's range rule safe to keep.
 *
 * `NameTakenError` because the outcome the scheduler must reach is the same
 * one: do not retry under this id, and leave the rows in the hot store. They
 * are recorded again by the next roll, under a name past the merged hour.
 *
 * Decided for every date this roll touches before the first is written, and
 * not subject to `--replace`. A roll spanning midnight writes one file per
 * date, and refusing part way through would leave rows in the tree that the
 * failure tells the scheduler are not there.
 */
function refuseMergedHour(
  dataDir: string,
  days: number[],
  rollId: number,
): void {
  for (const day of days) {
    const directory = dateDirectory(dataDir, day * DAY_MS);
    let names: string[];
    try {
      names = readdirSync(directory);
    } catch {
      continue; // Not created yet, which is the ordinary case.
    }
    const covering = compactedHourCovering(names, rollId);
    if (covering === null) continue;
    throw new NameTakenError(
      `roll ${rollId} falls inside ${join(directory, covering)}, an hour a ` +
        `merge has already written. A roll there is on disk and invisible to ` +
        `every query, so these rows stay in the hot store instead.`,
    );
  }
}

/**
 * Remove `.tmp` files a killed roll left in a date directory.
 *
 * A `*.parquet` glob skips them, so they are disk cost rather than a
 * correctness problem — but nothing else collects them, and the rolls most
 * likely to be killed are the ones with the least free disk. Age is what
 * makes this safe against a roll running right now.
 */
function sweepStaleTemporaries(directory: string, own: string): void {
  const cutoff = Date.now() - STALE_SCRATCH_MS;
  for (const entry of readdirSync(directory)) {
    if (!entry.endsWith(".tmp")) continue;
    const path = join(directory, entry);
    if (path === own) continue;
    try {
      if (statSync(path).mtimeMs > cutoff) continue;
      rmSync(path, { force: true });
    } catch {
      /* another roll collected it, or it is being written right now */
    }
  }
}

/**
 * Remove scratch directories a killed roll left behind.
 *
 * A roll dies at default disposition on SIGTERM, so its `finally` never runs
 * and its spill — which is backlog-sized — stays. Age is what makes this safe
 * against a concurrent roll: nothing legitimate is older than the timeout that
 * would have killed it.
 */
function sweepStaleScratch(root: string): void {
  const cutoff = Date.now() - STALE_SCRATCH_MS;
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const path = join(root, entry.name);
    try {
      if (statSync(path).mtimeMs > cutoff) continue;
      rmSync(path, { recursive: true, force: true });
    } catch {
      /* another roll swept it, or it is being written right now */
    }
  }
}

/** How long a scratch directory must sit untouched before it counts as an
 * orphan. Comfortably past the roll timeout that would have killed its
 * owner. */
const STALE_SCRATCH_MS = 60 * 60_000;
