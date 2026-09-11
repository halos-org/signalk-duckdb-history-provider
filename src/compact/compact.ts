import { mkdirSync, readdirSync, renameSync, rmSync, statSync } from "node:fs";
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
   * Outputs that were already on disk and were read back to prove they hold
   * every row of the leftovers beside them. Those leftovers were removed.
   */
  alreadyCompacted: string[];
  /**
   * Existing outputs that could not be read. Renamed aside rather than
   * trusted, and their hour merged again from the rolls that survived.
   */
  quarantined: string[];
  /**
   * Leftover rolls beside an existing output that does not hold their rows.
   * Left exactly where they are: they may be the only copy.
   */
  orphanInputs: string[];
  /**
   * Inputs this run folded in, or found already folded in, and then could not
   * remove. Reported, not fatal.
   */
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
  // would leave it to arrive into a merged hour -- which `refuseMergedHour`
  // rejects, so the cost is one deferred roll rather than lost rows, and
  // waiting for the next hour boundary is cheaper than paying it.
  //
  // Belt and braces behind the claim on the data directory that `main.ts`
  // takes: this record is written best-effort and read fail-open, so it cannot
  // be the interlock. It catches a roll this process is not racing -- one left
  // unfinished by a writer that died.
  const pending = readPendingRoll(dataDir);
  if (pending !== null && coversHour(pending.rollId, hourStartMs)) {
    throw new Error(
      `roll ${pending.rollId} is unfinished and belongs to the hour starting ` +
        `${hourStartMs}; merging now would leave its rows out`,
    );
  }

  const units = planCompaction(dataDir, hourStartMs);
  const files: CompactedFile[] = [];
  const alreadyCompacted: string[] = [];
  const quarantined: string[] = [];
  const orphanInputs: string[] = [];
  const strayInputs: string[] = [];
  if (units.length === 0) {
    return {
      hourStartMs,
      files,
      alreadyCompacted,
      quarantined,
      orphanInputs,
      strayInputs,
    };
  }

  // Its own scratch, for the same reason the roll takes one: an in-memory
  // database spills relative to the working directory, which for a process the
  // writer spawned is the Signal K server's.
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
  const toMerge = units.filter((unit) => !unit.alreadyMerged);
  try {
    // Nothing here loads an extension or attaches a database, so the lockdown
    // can go on immediately. The merge reads and writes only inside the data
    // directory; this is containment for the next statement added to this file
    // rather than for any statement in it today.
    await lockDownFileAccess(connection, [dataDir]);

    // **An existing output is a claim, not a fact.** Every leftover roll beside
    // it is a file whose rows the hot store truncated when it landed, so
    // removing one is a deletion of the only copy — and the thing that licenses
    // it must be the rows, never a directory entry. The rest of this package
    // holds that line already: the roll reads `count(*)` back from its own
    // committed file and the scheduler compares it against the bound before
    // truncating, and `mergeUnit` below compares written against read before it
    // renames. This is the same boundary.
    for (const unit of units) {
      if (!unit.alreadyMerged) continue;
      const held = await outputHolds(connection, unit);
      if (held === "unreadable") {
        // Renamed aside rather than deleted, the way `writeSidecar` handles an
        // unreadable sidecar, and for the same cause: flash that acknowledges a
        // flush without writing leaves the rename and loses the bytes. Moving
        // it also un-breaks every query over this whole date, because the
        // reader passes the directory to one `read_parquet` list and one
        // unreadable member fails the statement.
        renameSync(unit.output, `${unit.output}.unreadable`);
        quarantined.push(unit.output);
        // The survivors are now the only copy of the hour, so merge them.
        toMerge.push(unit);
        continue;
      }
      if (held === "complete") {
        alreadyCompacted.push(unit.output);
        continue;
      }
      // Rows in the leftovers that the output does not hold. This is the state
      // a roll written into an already-merged hour leaves, and deleting it is
      // how a hidden roll becomes a lost one.
      orphanInputs.push(...unit.inputs);
    }

    for (const unit of toMerge) {
      files.push(await mergeUnit(connection, unit, options.log));
    }
  } finally {
    connection.closeSync();
    instance.closeSync();
    rmSync(scratch, { recursive: true, force: true });
    // Cheap insurance rather than the mechanism: a throw between the COPY and
    // the rename leaves a temp, and this removes it. It cannot cover the case
    // that actually produces one -- a process killed mid-COPY never reaches a
    // `finally` -- which is what `sweepStaleTemporaries` below is for.
    for (const unit of units) rmSync(unit.temp, { force: true });
  }

  // Only after every unit has landed. A roll that spans midnight wrote the same
  // hour into two directories, and unlinking one directory's inputs while the
  // other's merge is still to come would leave the hour half readable if this
  // process died between them.
  for (const unit of toMerge) strayInputs.push(...unlinkInputs(unit));
  // The leftovers of an interrupted merge, now that the output has been read
  // back and shown to hold them. Removing them is finishing the step the
  // interrupted run did not reach.
  for (const unit of units) {
    if (!unit.alreadyMerged) continue;
    if (!alreadyCompacted.includes(unit.output)) continue;
    options.log?.(
      `${unit.output} was read back and holds this hour; removing ` +
        `${unit.inputs.length} superseded input(s) rather than merging again`,
    );
    strayInputs.push(...unlinkInputs(unit));
  }

  for (const unit of units) sweepStaleTemporaries(unit.directory);
  return {
    hourStartMs,
    files,
    alreadyCompacted,
    quarantined,
    orphanInputs,
    strayInputs,
  };
}

/**
 * Whether an output already on disk holds every row of the leftovers beside it.
 *
 * Rows, not counts: a count can match while the rows differ, and what licenses
 * the delete is that these exact rows are already somewhere else. `EXCEPT ALL`
 * keeps duplicates apart, so a leftover holding a row twice is only covered by
 * an output holding it twice.
 *
 * A leftover that cannot be read counts as not held. It is then left alone,
 * which is right either way: unreadable here does not mean unreadable to a
 * later engine, and it is not this function's business to delete it.
 */
async function outputHolds(
  connection: Awaited<ReturnType<DuckDBInstance["connect"]>>,
  unit: CompactionUnit,
): Promise<"complete" | "incomplete" | "unreadable"> {
  try {
    await countRows(connection, `'${sqlLiteral(unit.output)}'`);
  } catch {
    return "unreadable";
  }
  const list = unit.inputs.map((path) => `'${sqlLiteral(path)}'`).join(", ");
  try {
    const missing = await connection.runAndReadAll(
      `SELECT count(*) FROM (` +
        `SELECT ${COLUMN_LIST} FROM read_parquet([${list}], union_by_name = true) ` +
        `EXCEPT ALL ` +
        `SELECT ${COLUMN_LIST} FROM read_parquet('${sqlLiteral(unit.output)}')` +
        `)`,
    );
    return Number(missing.getRowsJS()[0][0]) === 0 ? "complete" : "incomplete";
  } catch {
    return "incomplete";
  }
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

  // Atomic for this one file, and from here the reader stops seeing the inputs
  // -- `liveTreeFiles` drops a roll an existing merge supersedes.
  //
  // The owner is the tree's, not the merge's. `commitFile` fchmods to 0600 and
  // the file belongs to whoever ran the process, so a merge run by hand as
  // another user leaves an hour the query service cannot open, with the rolls
  // that were readable already unlinked. `commitFile` does it on the same
  // descriptor it fsyncs, which is what stops the temp being swapped for a
  // symlink between the check and the change.
  commitFile(unit.temp, unit.output, statSync(unit.directory));

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
 * By age, not by exclusion: this run's own temps are already gone by the time
 * it runs, and the window has to be wide enough that it never removes one a
 * concurrent process is still writing. The roll sweeps only the directory it is
 * writing to, and an hour that spans midnight leaves its temp in the day
 * before -- a directory no later roll ever touches again.
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
