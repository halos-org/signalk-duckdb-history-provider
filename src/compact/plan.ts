import { readdirSync } from "node:fs";
import { join } from "node:path";
import {
  compactedFile,
  compactedTempFile,
  dateDirectoryStart,
  rollIdFromName,
  treeRoot,
} from "../roll/tree-path.js";

/**
 * Which files a completed hour's merge reads, and what it writes.
 *
 * Pure apart from two directory listings, so the rule that decides the set can
 * be tested without an engine. Nothing here opens a Parquet file: the set is
 * chosen by roll id, because a roll places each row in the date directory that
 * row's own timestamp names and a set chosen by content could only be found by
 * reading every file first.
 */

/** Milliseconds in the hour a merge covers. */
export const HOUR_MS = 3_600_000;

/**
 * The rolls a merge folds in are those that *ran* inside `(hour, hour + 1h]`,
 * not those named inside `[hour, hour + 1h)`.
 *
 * A roll writes the interval that just ended, so the roll at 12:00 carries
 * 11:55–12:00. Taking the half-open range from the top of the hour would put
 * an hour of data under a name an hour ahead of it, and the file called
 * `hour-11:00` would hold 10:55 to 11:55. The closed upper end is what makes
 * the name describe the contents.
 */
export function coversHour(rollId: number, hourStartMs: number): boolean {
  return rollId > hourStartMs && rollId <= hourStartMs + HOUR_MS;
}

/** One date directory's share of an hour. A roll spanning midnight has two. */
export interface CompactionUnit {
  /** UTC midnight of the date directory, as `dateDirectoryStart` names it. */
  day: number;
  directory: string;
  /** Absolute paths of the roll files to merge, in name order. */
  inputs: string[];
  /** Where the merge renames to, and what it writes through first. */
  output: string;
  temp: string;
}

/**
 * The units a merge of `hourStartMs` would do, or an empty list when there is
 * nothing worth merging.
 *
 * A directory holding one roll file for the hour is skipped: rewriting it
 * under a second name would cost a read and a write to produce the same rows,
 * and the sort buys nothing across a single roll's worth of paths that a later
 * hour will not buy again.
 */
export function planCompaction(
  dataDir: string,
  hourStartMs: number,
): CompactionUnit[] {
  const root = treeRoot(dataDir);
  let entries: string[];
  try {
    entries = readdirSync(root);
  } catch {
    return []; // No tree yet, which is every device's first interval.
  }

  const units: CompactionUnit[] = [];
  for (const entry of entries.sort()) {
    const day = dateDirectoryStart(entry);
    if (day === null) continue;
    const directory = join(root, entry);
    let names: string[];
    try {
      names = readdirSync(directory);
    } catch {
      continue; // Removed between the two reads, by expiry or by hand.
    }
    const inputs = names
      .filter((name) => {
        const id = rollIdFromName(name);
        return id !== null && coversHour(id, hourStartMs);
      })
      .sort()
      .map((name) => join(directory, name));
    if (inputs.length < 2) continue;
    units.push({
      day,
      directory,
      inputs,
      output: compactedFile(dataDir, day, hourStartMs),
      temp: compactedTempFile(dataDir, day, hourStartMs),
    });
  }
  return units;
}
