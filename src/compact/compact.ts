import { mkdirSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { DATA_DIR_MODE, DATA_LAYOUT } from "../data-dir.js";
import { commitFile } from "../durable-write.js";
import { BASE_DUCKDB_CONFIG } from "../duckdb/extension.js";
import { sqlLiteral } from "../duckdb/sql.js";
import { COLUMN_LIST, DEFAULT_MEMORY_LIMIT } from "../roll/roll.js";
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
  /** Inputs that were merged and then could not be removed. Reported, not fatal. */
  strayInputs: string[];
}

export async function compactHour(
  options: CompactOptions,
): Promise<CompactResult> {
  const { dataDir, hourStartMs } = options;
  const units = planCompaction(dataDir, hourStartMs);
  if (units.length === 0) {
    return { hourStartMs, files: [], strayInputs: [] };
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
  const files: CompactedFile[] = [];
  const strayInputs: string[] = [];
  try {
    for (const unit of units) {
      files.push(await mergeUnit(connection, unit, options.log));
    }
    // Only after every unit has landed. A roll that spans midnight wrote the
    // same hour into two directories, and unlinking one directory's inputs
    // while the other's merge is still to come would leave the hour half
    // readable if this process died between them.
    for (const unit of units) strayInputs.push(...unlinkInputs(unit));
  } finally {
    connection.closeSync();
    instance.closeSync();
    rmSync(scratch, { recursive: true, force: true });
  }
  return { hourStartMs, files, strayInputs };
}

async function mergeUnit(
  connection: Awaited<ReturnType<DuckDBInstance["connect"]>>,
  unit: CompactionUnit,
  log?: (line: string) => void,
): Promise<CompactedFile> {
  const list = unit.inputs.map((path) => `'${sqlLiteral(path)}'`).join(", ");
  // `union_by_name`: a file written by a build with a different column set is
  // read with the missing columns as NULL rather than failing the merge, which
  // is the same tolerance the reader gives them.
  //
  // ORDER BY path, ts -- the whole point. `ts` second so that within a path the
  // rows stay in time order and the row group statistics still prune on time.
  await connection.run(
    `COPY (SELECT ${COLUMN_LIST} FROM read_parquet([${list}], union_by_name = true) ` +
      `ORDER BY path, ts) ` +
      `TO '${sqlLiteral(unit.temp)}' (FORMAT parquet, COMPRESSION zstd)`,
  );
  // Atomic for this one file, and from here the reader stops seeing the
  // inputs -- `liveTreeFiles` drops a roll an existing merge supersedes.
  commitFile(unit.temp, unit.output);

  const counted = await connection.runAndReadAll(
    `SELECT count(*) FROM read_parquet('${sqlLiteral(unit.output)}')`,
  );
  const rows = Number(counted.getRowsJS()[0][0]);
  const result: CompactedFile = {
    path: unit.output,
    rows,
    bytesIn: totalBytes(unit.inputs),
    bytesOut: totalBytes([unit.output]),
  };
  log?.(
    `compacted ${unit.inputs.length} files into ${unit.output}: ` +
      `${rows} rows, ${result.bytesIn} -> ${result.bytesOut} bytes`,
  );
  return result;
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
