import { chownSync, mkdirSync, readdirSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { DATA_DIR_MODE, DATA_LAYOUT } from "../data-dir.js";
import { commitFile } from "../durable-write.js";
import { BASE_DUCKDB_CONFIG, lockDownFileAccess } from "../duckdb/extension.js";
import { sqlLiteral } from "../duckdb/sql.js";
import { COLUMN_LIST, DEFAULT_MEMORY_LIMIT } from "../roll/roll.js";
import { HOUR_MS, coversHour } from "../roll/tree-path.js";
import { readPendingRoll } from "../writer/contract.js";
import { planCompaction } from "./plan.js";
import type { CompactionUnit } from "./plan.js";

/**
 * One completed hour's roll files, merged into one file sorted by path.
 *
 * **Why sorted, and why this is not the daily compaction the layout decision
 * rejected.** Clustering identical `path` strings is what lets the dictionary
 * and RLE do their work: measured on a device, one hour of twelve 5-minute
 * files went from 936,216 to 343,746 bytes -- 1.81 bytes per row against 4.94
 * for the inputs and 3.17 for the unsorted hourly file the roll used to write.
 * A single-path range over a day's files went from 42.2 ms to 3.4 ms. The
 * rejected pass merged a whole day, 1.27M-11M rows, at a 466.5-484.6 MB peak;
 * an hour is 190k rows at 202 MB, which is the order the roll beside it
 * already costs.
 *
 * Nothing here reads the hot store, so this needs none of the writer's
 * ownership of that file. It runs in its own short-lived process for the
 * reason the roll does: DuckDB's allocator does not return its memory, and the
 * exit is what keeps a transient from becoming a standing cost.
 */

export interface CompactOptions {
  dataDir: string;
  /** UTC start of the hour to merge. Its rolls must all be complete. */
  hourStartMs: number;
  memoryLimit?: string;
  log?: (line: string) => void;
  /** Injectable for tests; production reads the wall clock. */
  now?: () => number;
}

export interface CompactedFile {
  path: string;
  rows: number;
  bytesIn: number;
  bytesOut: number;
}

export interface CompactResult {
  hourStartMs: number;
  files: CompactedFile[];
  /**
   * Outputs that were already on disk. Their surviving inputs were removed,
   * never re-merged.
   */
  alreadyCompacted: string[];
  /** Inputs that were merged and then could not be removed. Reported, not fatal. */
  strayInputs: string[];
}

/**
 * How long a `.tmp` is left alone before it is treated as abandoned.
 *
 * Longer than any merge this package makes: the point is to collect what a
 * killed process left, never to delete what a live one is still writing.
 */
const STALE_TEMP_MS = 6 * 60 * 60_000;

export async function compactHour(
  options: CompactOptions,
): Promise<CompactResult> {
  const { dataDir, hourStartMs } = options;
  const now = (options.now ?? Date.now)();

  // The preconditions, checked here rather than in the callers, because both
  // the scheduler and the standalone entry point reach this function and only
  // one of them knows the schedule.
  //
  // Alignment: a merge names its output for the hour it covers, and an hour
  // that does not start on the hour has no such name -- `hour-<H>` would claim
  // the whole hour containing H.
  if (!Number.isSafeInteger(hourStartMs) || hourStartMs < 1) {
    throw new RangeError(`${hourStartMs} is not an hour start`);
  }
  if (hourStartMs % HOUR_MS !== 0) {
    throw new RangeError(
      `${hourStartMs} is not aligned to a UTC hour; a merge would name its ` +
        `output for an hour it does not cover`,
    );
  }
  // Past-ness: the hour is closed by the roll at its upper boundary, which
  // runs at that instant and lands seconds later. Merging before then hides
  // every roll of the hour still to come, and the roll that lands afterwards
  // has to refuse its own name.
  if (now <= hourStartMs + HOUR_MS) {
    throw new RangeError(
      `the hour starting ${hourStartMs} is not closed yet; the roll that ` +
        `closes it runs at ${hourStartMs + HOUR_MS} and the clock reads ${now}`,
    );
  }
  // A roll in flight is a roll whose file is not on disk. Merging past it
  // would leave it to arrive into a merged hour -- which `writeDay` now
  // refuses, so the cost is stopped recording rather than lost rows, and the
  // cheaper answer is to wait for the next hour boundary.
  const pending = readPendingRoll(dataDir);
  if (pending !== null && coversHour(pending.rollId, hourStartMs)) {
    throw new Error(
      `roll ${pending.rollId} is unfinished and belongs to the hour starting ` +
        `${hourStartMs}; merging now would leave its rows out`,
    );
  }

  const units = planCompaction(dataDir, hourStartMs);
  const toMerge = units.filter((unit) => !unit.alreadyMerged);
  const alreadyCompacted = units
    .filter((unit) => unit.alreadyMerged)
    .map((unit) => unit.output);

  const files: CompactedFile[] = [];
  const strayInputs: string[] = [];
  if (toMerge.length > 0) {
    // Its own scratch, for the same reason the roll takes one: an in-memory
    // database spills relative to the working directory, which for a process
    // the writer spawned is the Signal K server's.
    const scratchRoot = join(dataDir, DATA_LAYOUT.scratch);
    mkdirSync(scratchRoot, { recursive: true, mode: DATA_DIR_MODE });
    const scratch = join(scratchRoot, `compact-${hourStartMs}-${process.pid}`);
    mkdirSync(scratch, { recursive: true, mode: DATA_DIR_MODE });

    const instance = await DuckDBInstance.create(":memory:", {
      ...BASE_DUCKDB_CONFIG,
      memory_limit: options.memoryLimit ?? DEFAULT_MEMORY_LIMIT,
      temp_directory: scratch,
    });
    const connection = await instance.connect();
    try {
      // Nothing here loads an extension or attaches a database, so the
      // lockdown can go on immediately. The merge reads and writes only inside
      // the data directory; this is containment for the next statement added
      // to this file rather than for any statement in it today.
      await lockDownFileAccess(connection, [dataDir]);
      for (const unit of toMerge) {
        files.push(await mergeUnit(connection, unit, options.log));
      }
    } finally {
      connection.closeSync();
      instance.closeSync();
      rmSync(scratch, { recursive: true, force: true });
      // Every failure path, not only the ones that threw before the rename: a
      // COPY killed part way leaves a `.tmp` in a date directory, and for the
      // unit of a midnight-spanning hour that directory is yesterday's, which
      // no later roll sweeps.
      for (const unit of toMerge) rmSync(unit.temp, { force: true });
    }
    // Only after every unit has landed. A roll that spans midnight wrote the
    // same hour into two directories, and unlinking one directory's inputs
    // while the other's merge is still to come would leave the hour half
    // readable if this process died between them.
    for (const unit of toMerge) strayInputs.push(...unlinkInputs(unit));
  }

  // The leftovers of an interrupted merge. By `liveTreeFiles`'s rule the
  // existing output already supersedes them, so removing them is finishing the
  // step that was interrupted -- and it is the only safe action, because
  // re-merging would rename a fraction of the hour over the whole of it.
  for (const unit of units) {
    if (!unit.alreadyMerged) continue;
    options.log?.(
      `${unit.output} already holds this hour; removing ${unit.inputs.length} ` +
        `superseded input(s) rather than merging again`,
    );
    strayInputs.push(...unlinkInputs(unit));
  }

  for (const unit of units) sweepStaleTemporaries(unit.directory);
  return { hourStartMs, files, alreadyCompacted, strayInputs };
}

async function mergeUnit(
  connection: Awaited<ReturnType<DuckDBInstance["connect"]>>,
  unit: CompactionUnit,
  log?: (line: string) => void,
): Promise<CompactedFile> {
  const list = unit.inputs.map((path) => `'${sqlLiteral(path)}'`).join(", ");
  const inputRelation = `read_parquet([${list}], union_by_name = true)`;
  // `union_by_name`: a file written by a build with a different column set is
  // read with the missing columns as NULL rather than failing the merge, which
  // is the same tolerance the reader gives them.
  //
  // ORDER BY path, ts -- the whole point. `ts` second so that within a path the
  // rows stay in time order and the row group statistics still prune on time.
  await connection.run(
    `COPY (SELECT ${COLUMN_LIST} FROM ${inputRelation} ` +
      `ORDER BY path, ts) ` +
      `TO '${sqlLiteral(unit.temp)}' (FORMAT parquet, COMPRESSION zstd)`,
  );

  // Before the rename, not after. The roll refuses to truncate the hot store
  // unless the counts it was given come back, and this is the same check at
  // the same boundary: twelve files are about to be deleted on the strength of
  // one, and `union_by_name` was chosen so a mismatched input is read anyway
  // rather than failing the COPY. Checked while the output is still a `.tmp`,
  // so a mismatch costs a rewrite instead of an hour.
  const written = await countRows(connection, `'${sqlLiteral(unit.temp)}'`);
  const read = await countRows(connection, `[${list}], union_by_name = true`);
  if (written !== read) {
    throw new Error(
      `the merge of ${unit.output} wrote ${written} rows from ${read} in its ` +
        `${unit.inputs.length} inputs; they are left where they are`,
    );
  }

  // The tree's owner, not the merge's. `commitFile` fchmods to 0600 and the
  // file belongs to whoever ran the process -- so a merge run as root over a
  // tree Signal K owns leaves an hour the query service cannot open, with the
  // rolls that were readable already unlinked. Before the rename and before
  // any unlink, so a refusal costs nothing.
  inheritOwner(unit.directory, unit.temp);

  // Atomic for this one file, and from here the reader stops seeing the
  // inputs -- `liveTreeFiles` drops a roll an existing merge supersedes.
  commitFile(unit.temp, unit.output);

  const result: CompactedFile = {
    path: unit.output,
    rows: written,
    bytesIn: totalBytes(unit.inputs),
    bytesOut: totalBytes([unit.output]),
  };
  log?.(
    `compacted ${unit.inputs.length} files into ${unit.output}: ` +
      `${written} rows, ${result.bytesIn} -> ${result.bytesOut} bytes`,
  );
  return result;
}

async function countRows(
  connection: Awaited<ReturnType<DuckDBInstance["connect"]>>,
  argument: string,
): Promise<number> {
  const counted = await connection.runAndReadAll(
    `SELECT count(*) FROM read_parquet(${argument})`,
  );
  return Number(counted.getRowsJS()[0][0]);
}

/**
 * Give a file the uid and gid of the directory it is going into.
 *
 * A no-op whenever they already match, which is every in-process merge the
 * writer spawns. It matters for a merge run by hand as another user, where the
 * only alternative is an unreadable hour.
 */
function inheritOwner(directory: string, path: string): void {
  const owner = statSync(directory);
  const file = statSync(path);
  if (file.uid === owner.uid && file.gid === owner.gid) return;
  chownSync(path, owner.uid, owner.gid);
}

/**
 * Remove the rolls the merge folded in.
 *
 * Deliberately after the rename and deliberately not atomic with it: a file
 * left here is ignored rather than read, so the worst a failure costs is disk.
 * Reported so it is not silent, because a directory that keeps accumulating
 * them is a merge that never finishes its last step.
 */
function unlinkInputs(unit: CompactionUnit): string[] {
  const stray: string[] = [];
  for (const input of unit.inputs) {
    try {
      rmSync(input, { force: true });
    } catch {
      stray.push(input);
    }
  }
  return stray;
}

/**
 * Collect `.tmp` files an earlier merge or roll abandoned in this directory.
 *
 * The roll sweeps only the directory it is writing to, and the hour that spans
 * midnight leaves its temp in the day before -- a directory no later roll ever
 * touches again.
 */
function sweepStaleTemporaries(directory: string): void {
  const cutoff = Date.now() - STALE_TEMP_MS;
  let entries: string[];
  try {
    entries = readdirSync(directory);
  } catch {
    return;
  }
  for (const entry of entries) {
    if (!entry.endsWith(".tmp")) continue;
    const path = join(directory, entry);
    try {
      if (statSync(path).mtimeMs > cutoff) continue;
      rmSync(path, { force: true });
    } catch {
      /* another process collected it, or it is being written right now */
    }
  }
}

function totalBytes(paths: string[]): number {
  let total = 0;
  for (const path of paths) {
    try {
      total += statSync(path).size;
    } catch {
      /* Counted as zero; this is a report, not a decision. */
    }
  }
  return total;
}
