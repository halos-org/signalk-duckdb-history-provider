import { writeSync } from "node:fs";
import { ownPeakBytes } from "../bench/one-shot.js";
import { compactHour } from "./compact.js";

/**
 * The compaction process.
 *
 * Spawned by the writer after a roll that closes an hour, and it exits when
 * the merge is done. The exit is what returns the memory, for the reason the
 * roll gives: DuckDB's allocator does not give it back in-process.
 *
 *   node dist/compact/main.js --data-dir <path> --hour <ms> [--memory-limit <n>]
 *
 * On success it prints one JSON line describing what it wrote, including this
 * process's own peak resident size, and exits 0.
 *
 * **Nothing depends on this succeeding.** It reads and rewrites files the tree
 * already holds; it never touches the hot store, so a failure costs disk and
 * some query time and never costs recording. The scheduler logs a non-zero
 * exit and carries on.
 *
 * Exit codes:
 *   0  merged, fsynced and renamed into place
 *   1  anything else; the inputs are left where they are and still readable
 *
 * There is no lock here, unlike the roll. Within a writer the scheduler runs
 * one child at a time, and across writers the sets do not collide: a roll
 * writes a file named for a slot this hour has already closed over, while the
 * merge only ever reads ids at or below that boundary. What a second merge of
 * the same hour cannot do is overwrite the first -- `planCompaction` reports
 * an hour whose output exists as already merged, and the only thing done to it
 * is the removal of inputs the first run did not get to.
 */

function writeStderr(line: string): void {
  writeSync(2, line);
}

function argValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

async function main(): Promise<void> {
  const dataDir = argValue("--data-dir");
  if (dataDir === undefined) throw new Error("--data-dir is required");
  // Only that it is a number. `compactHour` owns what a mergeable hour is --
  // aligned, closed, and with no roll of it still in flight -- because the
  // scheduler reaches it without passing through here.
  const hour = Number(argValue("--hour"));

  const result = await compactHour({
    dataDir,
    hourStartMs: hour,
    memoryLimit: argValue("--memory-limit"),
    log: (line) => writeStderr(`${line}\n`),
  });

  process.stdout.write(
    `${JSON.stringify({ ...result, peakRssBytes: ownPeakBytes() })}\n`,
  );
}

main().catch((err: unknown) => {
  writeStderr(`${err instanceof Error ? err.message : String(err)}\n`);
  process.exitCode = 1;
});
