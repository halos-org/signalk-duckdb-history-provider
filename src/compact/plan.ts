import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import {
  compactedFile,
  compactedTempFile,
  coversHour,
  dateDirectoryStart,
  rollIdFromName,
  treeRoot,
} from "../roll/tree-path.js";

/**
 * Which files a completed hour's merge reads, and what it writes.
 *
 * Pure apart from directory listings, so the rule that decides the set can be
 * tested without an engine. Nothing here opens a Parquet file: the set is
 * chosen by roll id, because a roll places each row in the date directory that
 * row's own timestamp names and a set chosen by content could only be found by
 * reading every file first.
 *
 * The hour predicate itself is `coversHour` in `roll/tree-path.ts`, beside the
 * names it reads. The reader's suppression rule, the roll's refusal and this
 * planner have to be the same predicate, and one of them living in a module
 * the other two do not import is how they would drift apart.
 */

/** One date directory's share of an hour. A roll spanning midnight has two. */
export interface CompactionUnit {
  directory: string;
  /** Absolute paths of the roll files to merge, in name order. */
  inputs: string[];
  /** Where the merge renames to, and what it writes through first. */
  output: string;
  temp: string;
  /**
   * The output is already on disk, so `inputs` are leftovers to remove rather
   * than rows to merge.
   *
   * A merge renames its output into place and unlinks its inputs afterwards,
   * and the two are deliberately not atomic. Interrupted in between — SIGKILL,
   * a plugin stop, a power cut, a card gone read-only — the hour is left
   * correct with some of its inputs still on disk. Planning a merge from those
   * survivors would produce a file holding a fraction of the hour and rename
   * it over the complete one, and the rows in the already-unlinked inputs are
   * by then the only copy: the hot store truncated them when the rolls landed.
   */
  alreadyMerged: boolean;
}

/**
 * The units a merge of `hourStartMs` would do, or an empty list when there is
 * nothing to merge and nothing left over to remove.
 *
 * A directory holding one roll file for the hour is skipped: rewriting it
 * under a second name would cost a read and a write to produce the same rows,
 * and the sort buys nothing across a single roll's worth of paths that a later
 * hour will not buy again. That threshold does not apply once the output
 * exists — one leftover input is still one file to remove.
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
    const output = compactedFile(dataDir, day, hourStartMs);
    const alreadyMerged = existsSync(output);
    if (inputs.length < (alreadyMerged ? 1 : 2)) continue;
    units.push({
      directory,
      inputs,
      output,
      temp: compactedTempFile(dataDir, day, hourStartMs, process.pid),
      alreadyMerged,
    });
  }
  return units;
}
