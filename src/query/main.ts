import { writeSync } from "node:fs";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { ownPeakBytes, ownResidentBytes } from "../bench/one-shot.js";
import { openReader } from "./reader.js";
import type { Reader } from "./reader.js";
import type { QueryRequest } from "./duck.js";

/**
 * The query process.
 *
 * One of these runs for as long as the plugin does, and it answers every
 * history query. It is a separate process rather than part of the server
 * because `@duckdb/node-api` maps a ~100 MB native addon and DuckDB's
 * allocator does not return what a query allocates — neither belongs in a
 * process that also serves the vessel's data.
 *
 * It stays alive because starting it is 336–375 ms on the device, more than an
 * ordinary request costs to answer. What that costs is memory the process never
 * gives back: it settles at the high-water mark of the largest query it has
 * served. Recycling it is
 * [halos-org/halos#178](https://github.com/halos-org/halos/issues/178).
 *
 *   node dist/query/main.js --data-dir <path> [--memory-limit 256MB]
 *
 * Requests are one JSON object per line on stdin, answered in order. Each
 * answer is newline-delimited JSON on stdout: one array per row, then one
 * object summarising the request — rows are arrays and the summary is an
 * object, so the two need no marker to be told apart. The summary carries back
 * the request's `id`, which is what lets the caller tell an answer from the
 * answer to something else after a restart.
 *
 * It exits 0 at end of input, and 1 if the engine cannot be started at all. A
 * failed query is an answer, not an exit: one bad request must not cost every
 * later one its warm engine.
 */

/**
 * Write to stderr and be sure it arrives.
 *
 * `process.stderr.write` is asynchronous when stderr is a pipe — which it
 * always is here — and `process.exit` right after it truncates whatever is
 * still queued.
 */
function writeStderr(line: string): void {
  writeSync(2, line);
}

/** Rows per write. Large enough that the syscalls do not dominate a big
 * answer, small enough that the text of one batch is not worth measuring. */
const OUTPUT_BATCH_ROWS = 1000;

function argValue(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

async function answer(reader: Reader, line: string): Promise<void> {
  const started = performance.now();
  let request: QueryRequest & { id?: number };
  try {
    request = JSON.parse(line) as QueryRequest & { id?: number };
  } catch {
    // No id to answer under, so the caller cannot match this to its request —
    // but it is still an answer, and the alternative is a caller waiting out
    // its whole deadline for a line that will never come.
    await emit({ error: "the request is not JSON", queryMs: 0 });
    return;
  }

  try {
    const result = await reader.read(request);
    const batch: string[] = [];
    const flush = async (): Promise<void> => {
      if (batch.length === 0) return;
      const full = !process.stdout.write(`${batch.join("\n")}\n`);
      batch.length = 0;
      // Waiting for the pipe rather than serialising ahead of it. Without this
      // an answer the caller reads slowly accumulates inside this process
      // instead — on top of the rows it already holds, and on the one figure
      // this design is judged on.
      if (full) await once(process.stdout, "drain");
    };
    for (const row of result.rows) {
      batch.push(JSON.stringify(row));
      if (batch.length >= OUTPUT_BATCH_ROWS) await flush();
    }
    // Rows first, summary last: the caller treats a missing summary as a
    // failed query, so it can only ever be written once everything before it
    // is.
    batch.push(
      JSON.stringify({
        id: request.id,
        rows: result.rows.length,
        truncated: result.truncated,
        treeFiles: result.treeFiles,
        // In-engine time. The caller measures the round trip, and the
        // difference is the pipe and this process's own bookkeeping.
        queryMs: Math.round(performance.now() - started),
        rssBytes: ownResidentBytes(),
        peakRssBytes: ownPeakBytes(),
      }),
    );
    await flush();
  } catch (err) {
    // The query failed; the process has not. Its engine is worth more than
    // this answer, and the caller turns the message into a failed request.
    await emit({
      id: request.id,
      error: err instanceof Error ? err.message : String(err),
      queryMs: Math.round(performance.now() - started),
      rssBytes: ownResidentBytes(),
      peakRssBytes: ownPeakBytes(),
    });
  }
}

async function emit(summary: Record<string, unknown>): Promise<void> {
  if (!process.stdout.write(`${JSON.stringify(summary)}\n`)) {
    await once(process.stdout, "drain");
  }
}

async function main(): Promise<void> {
  const dataDir = argValue("--data-dir");
  if (dataDir === undefined || dataDir === "") {
    writeStderr("usage: query/main.js --data-dir <path> < requests.ndjson\n");
    process.exit(1);
  }

  const reader = await openReader({
    dataDir,
    memoryLimit: argValue("--memory-limit"),
  });
  try {
    // A line at a time, and one at a time. Two queries on one connection would
    // interleave their rows on this pipe; the caller serialises for the same
    // reason, and at the time one request takes, a queue is not what anyone
    // would notice.
    for await (const line of createInterface({ input: process.stdin })) {
      if (line.trim() !== "") await answer(reader, line);
    }
  } finally {
    reader.close();
  }
}

main().catch((err: unknown) => {
  // Only the engine failing to start reaches here. The message first, then the
  // stack: the caller reports the first line, and a stack frame tells an
  // operator nothing about why history stopped working.
  writeStderr(
    `${err instanceof Error ? `${err.message}\n${err.stack ?? ""}` : String(err)}\n`,
  );
  process.exit(1);
});
