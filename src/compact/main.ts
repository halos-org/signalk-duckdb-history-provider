import { existsSync, writeSync } from "node:fs";
import { ownPeakBytes } from "../bench/one-shot.js";
import { treeRoot } from "../roll/tree-path.js";
import { claimTheDataDirectory } from "../writer/claim.js";
import { EXIT_LOCKED, writerPaths } from "../writer/contract.js";
import { compactHour } from "./compact.js";

/**
 * The compaction process.
 *
 * Spawned by the writer at a slot that closes an hour — whether or not that
 * slot had rows to roll — and it exits when the merge is done. The exit is what returns the memory, for the reason the
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
 * **It takes the same claim on the data directory a roll takes**, and exits 3
 * when a roll or another merge holds it. Serialisation inside one writer is
 * not enough: this entry point exists to be run by hand -- backfilling a tree
 * recorded before compaction is a loop over it -- and a merge running beside a
 * live roll lands that roll inside a merged hour, where no query reads it.
 *
 * Exit codes:
 *   0  merged, fsynced and renamed into place
 *   3  a roll or another merge is already running against this data directory
 *   1  anything else; the inputs are left where they are and still readable
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
  // A path with no tree under it is indistinguishable from a device's first
  // interval once `planCompaction` has swallowed the ENOENT, and both report
  // "nothing to merge" and exit 0. For a hand-run backfill of a hundred hours
  // that is a hundred green exits over a typo, which is the outcome most
  // easily mistaken for success.
  if (!existsSync(treeRoot(dataDir))) {
    throw new Error(
      `${treeRoot(dataDir)} does not exist; there is no tree here`,
    );
  }

  const claim = await claimTheDataDirectory(writerPaths(dataDir).rollSocket);
  if (claim === null) {
    writeStderr(
      "a roll or another merge is already running against this data " +
        "directory; refusing to start a second one\n",
    );
    process.exit(EXIT_LOCKED);
  }
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
  claim.close();
}

main().catch((err: unknown) => {
  writeStderr(`${err instanceof Error ? err.message : String(err)}\n`);
  process.exitCode = 1;
});
