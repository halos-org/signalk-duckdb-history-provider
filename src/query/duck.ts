import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";

/**
 * Queries, from the side that must never hold an engine.
 *
 * This file runs inside the Signal K process, so it starts `query/main.js` and
 * reads what it prints. Nothing here imports `@duckdb/node-api`, and
 * `src/test/plugin-import-graph.test.ts` is what keeps it that way — a lazily
 * loaded engine inside a request handler is exactly the shape this design
 * exists to prevent.
 *
 * **One process, kept.** Starting an engine costs 336–375 ms on the device,
 * which is more than an ordinary request costs to answer — so a process per
 * query spent more on starting than on working. What a request costs depends
 * on how much data the device holds; `docs/query-layer.md` carries the
 * measurements and the date they were taken.
 *
 * The price is memory the query process does not give back. Bounding it is
 * [halos-org/halos#178](https://github.com/halos-org/halos/issues/178).
 *
 * **One request, one statement.** The sibling provider issues a query per
 * pathSpec and a second for non-numeric paths. Here a request naming ten paths
 * compiles to one statement, which matters more now that the statement is most
 * of what a query costs.
 */

const QUERY_ENTRY = fileURLToPath(new URL("./main.js", import.meta.url));

/**
 * Rows one query returns before the answer is reported as truncated.
 *
 * The caller holds all of them in the Signal K process, so this is a ceiling
 * on that rather than on the engine. History playback already reads a long
 * range as a series of shorter ones; this is what makes an unbounded request
 * degrade into a truncated answer rather than into the server's heap.
 *
 * Here rather than beside the reader that applies it, because the history
 * surface has to know the ceiling to say which request exceeded it, and it
 * may not import anything that reaches the engine.
 */
export const DEFAULT_ROW_LIMIT = 100_000;

/**
 * The columns a `range` row carries, in the order the tree writes them.
 *
 * A `snapshot` row carries the same nine in the same order — it is one row per
 * `(context, path)` rather than one per sample, and everything the caller does
 * with it afterwards is identical, so it decodes through the same indices.
 */
export const RANGE_COLUMNS = [
  "ts",
  "context",
  "path",
  "source",
  "value_kind",
  "value_num",
  "value_str",
  "value_lat",
  "value_lon",
] as const;

/**
 * The columns a `values` row carries.
 *
 * One row per bucket per series. All four value columns come back on every
 * row and the caller picks: a path has one kind, so at most one of them is
 * ever set, and which one is not known before the query runs. That is what
 * replaces the sibling provider's "query the numeric table, and if it came
 * back empty query the string table" — here they are one table and one pass.
 */
export const VALUE_COLUMNS = [
  "spec",
  "bucket",
  "num",
  "str",
  "kind",
  "lat",
  "lon",
] as const;

/**
 * How a bucket is reduced.
 *
 * `raw` returns the rows themselves, for the aggregates the API defines as
 * client-side: a moving average cannot be computed a bucket at a time.
 */
export type ValueAggregate =
  "average" | "min" | "max" | "first" | "last" | "mid" | "raw";

export interface ValueSpec {
  path: string;
  aggregate: ValueAggregate;
  /** Restrict the series to rows recorded from this source. */
  sourceRef?: string;
}

/**
 * What a query asks for. `from` is inclusive and `to` exclusive, both in
 * milliseconds, which is what the store and the tree hold.
 */
export type QueryRequest =
  | {
      kind: "range";
      from: number;
      to: number;
      /**
       * Absent means every context. The v2 surface always names one; history
       * playback never does, because it replays the whole vessel picture —
       * every AIS target as well as the own vessel.
       */
      context?: string;
      /** Empty or absent means every path in the context. */
      paths?: string[];
      /** Rows to return before the answer is reported as truncated. */
      limit?: number;
    }
  | {
      /**
       * Whether the range holds anything at all, as one row of one count.
       *
       * Separate from `range` because it must not sort. History playback asks
       * this before every session and answering `false` disables playback, so
       * it is asked for a range that may be the whole tree — where a top-N over
       * the union costs a full scan and this stops at the first row.
       */
      kind: "exists";
      from: number;
      to: number;
    }
  | {
      /**
       * The last value per `(context, path)` at an instant.
       *
       * What the v1 snapshot API asks for, and the one question this storage
       * has no index for: "newest row before T" is a backward scan whose depth
       * is the retention window rather than the request. `reader.ts` states the
       * rule it answers under.
       */
      kind: "snapshot";
      /** The instant the snapshot describes. Rows at or before it count. */
      at: number;
      /** Restrict to these paths. Empty or absent means every path. */
      paths?: string[];
    }
  | {
      /** One statement for every series a history request asked for. */
      kind: "values";
      from: number;
      to: number;
      context: string;
      specs: ValueSpec[];
      /** Bucket width in milliseconds. Absent means every spec is raw. */
      bucketMs?: number;
      /**
       * Rows a raw branch returns. Per series rather than per answer: a
       * bucketed branch is already bounded by the bucket count, and it is the
       * raw ones the caller reduces in its own heap.
       */
      limit?: number;
    }
  | {
      kind: "paths";
      from: number;
      to: number;
      /** Absent lists every context's paths, which is what the v2 surface asks
       * for — its request carries a range and nothing else. */
      context?: string;
    }
  | { kind: "contexts"; from: number; to: number };

export interface QueryResult {
  /** Column order for `range` is `RANGE_COLUMNS`; the other two are one column. */
  rows: unknown[][];
  /** The range held more rows than the limit, and these are the first of them. */
  truncated: boolean;
  /**
   * Request to answer, measured here rather than in the engine.
   *
   * The engine's own timing omits the queue, the pipe and — on the first
   * request, or the first after a restart — starting the engine. Only this
   * side can see all of it.
   */
  wallMs: number;
  /** Tree files the reader opened. Zero means the range was inside the hot store. */
  treeFiles: number;
  /** The query process's resident size after answering, or null off Linux. */
  rssBytes: number | null;
  /** Its high-water mark, which is what it will not give back. */
  peakRssBytes: number | null;
}

/**
 * How many requests may wait before the answer is "no".
 *
 * The query process answers one at a time — two on one connection would
 * interleave their rows on one pipe — so a burst queues. A Grafana dashboard
 * opens with one request per panel, and at the tens to hundreds of
 * milliseconds each one takes, a queue is the right response to that.
 *
 * A refusal is the right response to a backlog: past this many, the requests
 * already waiting will not be served inside their own deadline either.
 */
export const MAX_QUEUED_QUERIES = 8;

/** The deadline for one request, queue wait included. */
export const QUERY_TIMEOUT_MS = 30_000;

/** How long a killed query process gets to die before it is killed harder. */
const KILL_GRACE_MS = 1000;

/** The queue was full. The API surface turns this into a 503. */
export class QueryOverloadedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueryOverloadedError";
  }
}

/** The deadline passed, in the queue or in the engine. */
export class QueryTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueryTimeoutError";
  }
}

/** The query failed, and this is what it said. */
export class QueryFailedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueryFailedError";
  }
}

export interface QueryRunnerOptions {
  dataDir: string;
  maxQueued?: number;
  timeoutMs?: number;
  /** Passed to the engine. The default lives in the reader. */
  memoryLimit?: string;
  /** Injected in tests, so a query need not be a real DuckDB process. */
  spawnQuery?: (args: string[]) => ChildProcess;
  /** Anything an operator should see: the service dying, and why. */
  onError?: (line: string) => void;
}

interface Waiter {
  resolve: () => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout | null;
}

/** What the query process printed for one request. */
interface Summary {
  id?: number;
  error?: string;
  truncated?: boolean;
  treeFiles?: number;
  rssBytes?: number | null;
  peakRssBytes?: number | null;
}

/** One request in flight, and what to do with the lines coming back. */
interface Pending {
  id: number;
  rows: unknown[][];
  resolve: (summary: Summary) => void;
  reject: (err: Error) => void;
}

export class QueryRunner {
  private readonly options: QueryRunnerOptions;
  private readonly maxQueued: number;
  private readonly timeoutMs: number;
  private busy = false;
  private readonly waiting: Waiter[] = [];
  private stopped = false;

  private child: ChildProcess | null = null;
  /** Processes this side ended, so their exit is not reported as a surprise. */
  private readonly killed = new Set<ChildProcess>();
  private pending: Pending | null = null;
  private pendingLine = "";
  private stderr = "";
  private nextId = 1;

  constructor(options: QueryRunnerOptions) {
    this.options = options;
    this.maxQueued = options.maxQueued ?? MAX_QUEUED_QUERIES;
    this.timeoutMs = options.timeoutMs ?? QUERY_TIMEOUT_MS;
  }

  /** Whether a query is running, and how many are waiting, for a status line. */
  get pendingWork(): { active: number; queued: number } {
    return { active: this.busy ? 1 : 0, queued: this.waiting.length };
  }

  /** Whether the query process is up. It starts on the first request. */
  get running(): boolean {
    return this.child !== null;
  }

  /**
   * One request, answered by the one query process.
   *
   * The deadline starts here and covers the wait as well as the work. A
   * request that spent its whole deadline queued is one whose client has
   * stopped waiting, and running it then would delay the requests behind it
   * for nothing.
   */
  async run(request: QueryRequest): Promise<QueryResult> {
    const deadline = Date.now() + this.timeoutMs;
    await this.acquire(deadline);
    const started = performance.now();
    try {
      // Both of these are the state at the moment the turn became this
      // request's, which is not the state when it asked for one.
      if (this.stopped) throw new QueryFailedError("the plugin is stopping");
      if (Date.now() >= deadline) {
        throw new QueryTimeoutError(
          `no query slot came free within ${this.timeoutMs} ms`,
        );
      }
      const { summary, rows } = await this.ask(request, deadline);
      if (summary.error !== undefined) {
        throw new QueryFailedError(summary.error);
      }
      return {
        rows,
        truncated: summary.truncated === true,
        wallMs: performance.now() - started,
        treeFiles: summary.treeFiles ?? 0,
        rssBytes: summary.rssBytes ?? null,
        peakRssBytes: summary.peakRssBytes ?? null,
      };
    } finally {
      this.release();
    }
  }

  private acquire(deadline: number): Promise<void> {
    if (this.stopped) {
      return Promise.reject(new QueryFailedError("the plugin is stopping"));
    }
    if (!this.busy) {
      this.busy = true;
      return Promise.resolve();
    }
    if (this.waiting.length >= this.maxQueued) {
      return Promise.reject(
        new QueryOverloadedError(
          `a query is running and ${this.waiting.length} are already waiting; ` +
            `this one is refused rather than queued behind them`,
        ),
      );
    }
    return new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, timer: null };
      waiter.timer = setTimeout(
        () => {
          this.drop(waiter);
          reject(
            new QueryTimeoutError(
              `no query slot came free within ${this.timeoutMs} ms`,
            ),
          );
        },
        Math.max(0, deadline - Date.now()),
      );
      waiter.timer.unref();
      this.waiting.push(waiter);
    });
  }

  /**
   * Hand the turn to whoever is next, or give it back.
   *
   * The turn moves without passing through `busy`, because clearing it and
   * letting the next waiter re-acquire would let a request that arrived in
   * between overtake the queue.
   */
  private release(): void {
    const next = this.waiting.shift();
    if (next === undefined) {
      this.busy = false;
      return;
    }
    if (next.timer !== null) clearTimeout(next.timer);
    next.resolve();
  }

  private drop(waiter: Waiter): void {
    const index = this.waiting.indexOf(waiter);
    if (index >= 0) this.waiting.splice(index, 1);
  }

  private ask(
    request: QueryRequest,
    deadline: number,
  ): Promise<{ summary: Summary; rows: unknown[][] }> {
    const child = this.ensureChild();
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const rows: unknown[][] = [];
      const killer = setTimeout(
        () => {
          // The engine cannot be interrupted from here, so a query that
          // overran its deadline costs the whole process — and the next
          // request pays to start a new one. That is the honest trade for
          // keeping one: a runaway query must not hold the only engine.
          this.fail(
            new QueryTimeoutError(
              `the query did not finish within ${this.timeoutMs} ms`,
            ),
          );
        },
        Math.max(0, deadline - Date.now()),
      );
      killer.unref();

      this.pending = {
        id,
        rows,
        resolve: (summary) => {
          clearTimeout(killer);
          this.pending = null;
          resolve({ summary, rows });
        },
        reject: (err) => {
          clearTimeout(killer);
          this.pending = null;
          reject(err);
        },
      };
      child.stdin?.write(`${JSON.stringify({ ...request, id })}\n`);
    });
  }

  /** The query process, started if it is not up. */
  private ensureChild(): ChildProcess {
    if (this.child !== null) return this.child;
    const args = [QUERY_ENTRY, "--data-dir", this.options.dataDir];
    if (this.options.memoryLimit !== undefined) {
      args.push("--memory-limit", this.options.memoryLimit);
    }
    const child = (this.options.spawnQuery ?? defaultSpawn)(args);
    this.child = child;
    this.pendingLine = "";
    this.stderr = "";

    // Decoded by the stream, never per chunk. A chunk boundary can fall inside
    // a multi-byte UTF-8 sequence, and `Buffer.toString()` on each half
    // replaces the split bytes with U+FFFD in both — so a vessel name
    // straddling a 64 kB boundary comes back mangled, and still parses as
    // JSON. With an encoding set, Node holds the partial sequence.
    child.stdout?.setEncoding("utf8");
    child.stderr?.setEncoding("utf8");
    // **Every handler is gated on this process still being the current one.**
    // A killed process's `close` arrives an event loop turn or more later, and
    // by then the next request has started a replacement and is being served
    // by it. Ungated, the dead one's exit rejected that request and killed its
    // replacement — one timeout took down the two requests after it.
    child.stdout?.on("data", (chunk: string) => {
      if (this.child !== child) return;
      this.take(chunk);
    });
    child.stderr?.on("data", (chunk: string) => {
      if (this.child !== child) return;
      // Bounded: this accumulates for the life of the process, and a query
      // service that logs on every request must not grow the server's heap.
      this.stderr = `${this.stderr}${chunk}`.slice(-STDERR_KEPT);
    });
    // An EventEmitter with no error listener rethrows the event, which would
    // take the Signal K process down for a failed query.
    child.stdout?.on("error", () => {});
    child.stderr?.on("error", () => {});
    child.stdin?.on("error", () => {});
    child.on("error", (err) => {
      if (this.child !== child) return;
      this.fail(new QueryFailedError(err.message));
    });
    child.on("close", (code, signal) => {
      const how = code === null ? `signal ${signal}` : `code ${code}`;
      const asked = this.killed.delete(child);
      if (this.child !== child) {
        // Already replaced. Its exit says nothing about the request the
        // replacement is serving, and nothing about the replacement.
        if (!asked) {
          this.options.onError?.(
            `a query service this side had already replaced exited with ${how}`,
          );
        }
        return;
      }
      this.forget(child);
      this.fail(
        new QueryFailedError(
          `the query service exited with ${how}: ${lastMessage(this.stderr)}`,
        ),
        // Only when it went on its own. An exit this side asked for — a
        // deadline, or the plugin stopping — is not news, and reporting it
        // would put "the query service exited" in the log of every clean
        // shutdown.
        asked
          ? undefined
          : `the query service exited with ${how}; the next query starts a new one`,
      );
    });
    return child;
  }

  private take(chunk: string): void {
    this.pendingLine += chunk;
    let cut = this.pendingLine.indexOf("\n");
    while (cut >= 0) {
      const line = this.pendingLine.slice(0, cut);
      this.pendingLine = this.pendingLine.slice(cut + 1);
      if (line !== "") this.consume(line);
      cut = this.pendingLine.indexOf("\n");
    }
  }

  private consume(line: string): void {
    const pending = this.pending;
    let parsed: unknown;
    try {
      // Rows are arrays and the summary is an object, so the end of one answer
      // needs no marker to be told from the rows before it.
      parsed = JSON.parse(line);
    } catch {
      pending?.reject(
        new QueryFailedError("the query printed something that is not JSON"),
      );
      return;
    }
    if (pending === null) return; // An answer to a request nobody is waiting for.
    if (Array.isArray(parsed)) {
      pending.rows.push(parsed as unknown[]);
      return;
    }
    const summary = parsed as Summary;
    if (summary.id !== undefined && summary.id !== pending.id) {
      // The stream and the caller disagree about which request this is, and
      // nothing good comes of guessing. Restarting is the recovery.
      this.fail(
        new QueryFailedError(
          `the query service answered request ${summary.id} while ${pending.id} was outstanding`,
        ),
      );
      return;
    }
    pending.resolve(summary);
  }

  /** Fail whatever is in flight and take the service down with it. */
  private fail(err: Error, log?: string): void {
    const pending = this.pending;
    this.pending = null;
    if (log !== undefined) this.options.onError?.(log);
    this.kill();
    pending?.reject(err);
  }

  private forget(child: ChildProcess): void {
    if (this.child === child) this.child = null;
  }

  private kill(): void {
    const child = this.child;
    if (child === null) return;
    this.child = null;
    this.killed.add(child);
    child.kill("SIGTERM");
    const hard = setTimeout(() => child.kill("SIGKILL"), KILL_GRACE_MS);
    hard.unref();
  }

  /**
   * Refuse new work, drop the queue and stop the service.
   *
   * A query outliving the plugin holds a read on the hot store the writer is
   * being asked to close.
   */
  stop(): void {
    this.stopped = true;
    while (this.waiting.length > 0) {
      const waiter = this.waiting.shift() as Waiter;
      if (waiter.timer !== null) clearTimeout(waiter.timer);
      waiter.reject(new QueryFailedError("the plugin is stopping"));
    }
    const pending = this.pending;
    this.pending = null;
    this.kill();
    pending?.reject(new QueryFailedError("the plugin is stopping"));
  }
}

/** How much of the service's stderr to keep for the next failure message. */
const STDERR_KEPT = 4096;

/**
 * The last thing the service said that was not a stack frame.
 *
 * Not the first line: this stderr spans the whole life of the process, so the
 * beginning is whatever went wrong first — or nothing, once the buffer above
 * has rolled past it. Not the last line either, which is a frame.
 */
function lastMessage(text: string): string {
  const lines = text
    .split("\n")
    .map((line) => line.trimEnd())
    .filter((line) => line !== "" && !/^\s+at /.test(line));
  return lines[lines.length - 1] ?? "";
}

function defaultSpawn(args: string[]): ChildProcess {
  // An argument array, never a shell: the data directory is a string an
  // operator typed into the Admin UI.
  return spawn(process.execPath, args, {
    stdio: ["pipe", "pipe", "pipe"],
  });
}
