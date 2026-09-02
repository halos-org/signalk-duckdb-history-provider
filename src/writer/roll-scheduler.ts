import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { rmSync, writeFileSync } from "node:fs";
import { commitFile } from "../durable-write.js";
import { fileURLToPath } from "node:url";
import { delayToNextRoll, nextRollAt } from "../roll/schedule.js";
import {
  EXIT_NAME_TAKEN,
  ROLL_KILL_GRACE_MS,
  readPendingRoll,
  writerPaths,
} from "./contract.js";
import type { PendingRoll } from "./contract.js";
import type { HotStore } from "./hot-store.js";

/**
 * Rolls, scheduled and truncated by the process that owns the store.
 *
 * The writer runs this, not the plugin, for one reason: **only the writer may
 * write to the hot store.** The roll opens it read-only, so the delete that
 * follows a successful roll has to happen here — and putting the schedule
 * anywhere else would mean a protocol message whose only job is to ask the
 * owner to do what the owner already knows how to do.
 *
 * The roll itself is a separate process. DuckDB's allocator does not return
 * its memory, so the exit is what keeps a two-second transient from becoming
 * a standing cost inside a process that runs for weeks. Nothing in this file's
 * import graph reaches the engine; `src/test/plugin-import-graph.test.ts`
 * checks that against the compiled writer.
 */

const ROLL_ENTRY = fileURLToPath(new URL("../roll/main.js", import.meta.url));
const COMPACT_ENTRY = fileURLToPath(
  new URL("../compact/main.js", import.meta.url),
);

/** Milliseconds in the hour a compaction covers. */
const HOUR_MS = 3_600_000;

/**
 * How long a compaction gets before it is killed.
 *
 * Shorter than a roll's, because nothing waits on it: a merge that overruns
 * costs disk and some query time, while a roll that overruns never truncates
 * and the store grows without bound.
 */
const COMPACT_TIMEOUT_MS = 5 * 60_000;

/**
 * How long a roll gets before it is killed.
 *
 * Generous on purpose. An hourly roll is a second or two, but a device that
 * was off for a week wakes up with a week in the store, and a roll killed for
 * being slow never truncates — so a timeout that is too short does not fail
 * safe, it fails permanently and the store grows without bound.
 */
const ROLL_TIMEOUT_MS = 15 * 60_000;

/** How long after start a backlog roll waits, so it does not collide with
 * the plugin's own startup work. */
const START_ROLL_DELAY_MS = 5000;

export interface RollSchedulerOptions {
  store: HotStore;
  dataDir: string;
  intervalMinutes: number;
  /**
   * Whole days of tree to keep, passed through to the roll. Zero keeps
   * everything.
   *
   * Expiry is a step of the roll rather than a timer of its own, so a changed
   * setting takes effect at the next roll and a device that records nothing
   * expires nothing. Neither matters: the tree only grows when a roll writes.
   */
  retentionDays?: number;
  /** Ordinary progress. The plugin routes the writer's stdout to `app.debug`. */
  log: (line: string) => void;
  /** Anything an operator has to see. The plugin routes stderr to `app.error`;
   * without a separate sink every roll failure is debug-level, and a device
   * can fail every roll for weeks while the status line says "Recording". */
  onError?: (line: string) => void;
  /** Injected in tests. Production reads the clock. */
  now?: () => number;
  /** Injected in tests, so a roll need not be a real DuckDB process. */
  spawnRoll?: (args: string[]) => ChildProcess;
  /** Injected in tests, so a compaction need not be a real DuckDB process. */
  spawnCompact?: (args: string[]) => ChildProcess;
  /**
   * Merge each completed hour's rolls into one file sorted by path. Off makes
   * the tree exactly what it was before compaction existed.
   */
  compactHourly?: boolean;
  /** Injected in tests. Production waits ROLL_TIMEOUT_MS. */
  timeoutMs?: number;
  /** Injected in tests. Production waits START_ROLL_DELAY_MS. */
  startDelayMs?: number;
}

export class RollScheduler {
  private readonly options: RollSchedulerOptions;
  private readonly now: () => number;
  private timer: NodeJS.Timeout | null = null;
  private running: ChildProcess | null = null;
  private stopped = false;
  /**
   * What a roll that has not been truncated yet was doing.
   *
   * It records a phase, not just an id, and the distinction is what keeps a
   * retry from destroying somebody else's file. `rolling` means a roll may
   * have written files under this id and nothing has been truncated: a retry
   * may replace them, because they are its own. `written` means the roll
   * finished and the truncate had not happened yet: the successor must NOT
   * reuse the name — those rows are already durable — and instead completes
   * the truncate, which is idempotent.
   *
   * It outlives the writer, in a file, because the writer is not the only
   * thing that can die: a SIGKILL or an OOM kill leaves the roll running, and
   * an orphan that finishes has written a file this scheduler would otherwise
   * know nothing about.
   */
  private pending: PendingRoll | null;

  constructor(options: RollSchedulerOptions) {
    this.options = options;
    this.now = options.now ?? (() => Date.now());
    this.pending = readPendingRoll(options.dataDir);
  }

  /**
   * Finish whatever the last writer left, then arm the timer.
   *
   * Two things can be outstanding. A roll that completed and was not truncated
   * is finished here, because its rows are already in the tree and leaving
   * them in the store would write them a second time. And a store holding rows
   * older than one interval means this device's uptime never crosses a slot —
   * a boat powered up at 08:10 and shut down at 08:55 would otherwise never
   * roll at all — so one roll is scheduled shortly rather than at the next
   * boundary.
   */
  start(): void {
    this.stopped = false;
    if (this.pending?.phase === "rolling") {
      this.options.log(
        `roll ${this.pending.rollId} was left unfinished; the next roll reuses its name`,
      );
    }
    // Its failure must not reach main(), which calls start() outside any try:
    // a truncate that could not be written would end the writer, and
    // recording with it, for something that should cost one roll.
    this.finishPendingRoll();
    this.arm();
    if (this.backlogPredatesThisProcess()) {
      this.options.log(
        "the hot store holds rows older than one roll interval; rolling shortly",
      );
      const soon = setTimeout(() => {
        // Named for the instant it runs, not for a slot. The armed timer owns
        // the next slot, and two rolls sharing one name would make the second
        // a stranger to the first's file.
        void this.rollOnce(this.now()).catch((err: unknown) =>
          this.reportFailure(err),
        );
      }, this.options.startDelayMs ?? START_ROLL_DELAY_MS);
      soon.unref();
    }
  }

  /**
   * Finish a roll that exited 0 and whose rows were not deleted, and say
   * whether a new roll may start.
   *
   * Completing it is the whole reason the record carries a phase. This runs
   * before every roll and not only at startup, because the state is reachable
   * without a restart: if the delete or the checkpoint throws — ENOSPC, or an
   * IO error, the failures those lines exist for — the record stays `written`
   * and the timer re-arms. A new roll then would take a fresh name and write
   * rows that are already in the tree, under a second name, permanently.
   *
   * So while it cannot be finished, nothing else rolls. The store grows and
   * the reason is reported every slot, which is the recoverable failure; the
   * duplicate is not.
   */
  private finishPendingRoll(): boolean {
    const pending = this.pending;
    if (pending === null || pending.phase !== "written") return true;
    try {
      const removed = this.options.store.deleteThrough(pending.maxRowid);
      this.options.store.checkpoint();
      this.pending = null;
      clearPendingRoll(this.options.dataDir, this.options.onError);
      this.options.log(
        `roll ${pending.rollId} had finished but not truncated; removed ${removed} rows now`,
      );
      return true;
    } catch (err) {
      this.reportFailure(
        `roll ${pending.rollId} wrote its rows and they could not be removed from the hot store ` +
          `(${err instanceof Error ? err.message : String(err)}); no roll will start until they are, ` +
          `because rolling again would write them to the tree a second time`,
      );
      return false;
    }
  }

  /** Whether the store holds rows from before this process started. */
  private backlogPredatesThisProcess(): boolean {
    const oldest = this.options.store.oldestTimestamp();
    if (oldest === null) return false;
    return this.now() - oldest > this.options.intervalMinutes * 60_000;
  }

  private arm(): void {
    if (this.stopped) return;
    const now = this.now();
    // The slot is decided here and carried into the roll, never re-derived
    // from the clock when the timer fires. setTimeout may fire a millisecond
    // early, and a clock read on the wrong side of the boundary names the
    // PREVIOUS slot — whose file already exists and would be overwritten with
    // a fraction of its rows. That destroyed 2.5M rows on a device before this
    // was a parameter.
    const slot = nextRollAt(now, this.options.intervalMinutes);
    this.timer = setTimeout(
      () => {
        this.timer = null;
        // Caught, not just finally'd. An unhandled rejection ends the process
        // under Node's default, so a hot store that could not be truncated —
        // a full disk, say — would take recording down with it instead of
        // costing one roll.
        void this.rollOnce(slot)
          .catch((err: unknown) => this.reportFailure(err))
          .finally(() => this.arm());
      },
      delayToNextRoll(now, this.options.intervalMinutes),
    );
    // The writer must not be held open by a pending roll: the plugin stops it
    // with SIGTERM and waits, and an armed timer would not delay that anyway.
    this.timer.unref();
  }

  /**
   * One roll: choose the set, write it, and delete exactly that set.
   *
   * `slot` names the roll, and the caller supplies it — the schedule knows
   * which instant it armed for, and nothing here should have to guess it back
   * out of a clock reading.
   */
  async rollOnce(slot: number): Promise<void> {
    if (this.stopped || this.running !== null) return;
    // Before anything else: a record left in `written` means rows are in the
    // tree and still in the store, and starting a fresh roll would put them
    // there twice.
    if (!this.finishPendingRoll()) return;
    const bound = this.options.store.rollBound();
    if (bound === null) {
      // Nothing recorded since the last roll. No file, no empty directory.
      this.pending = null;
      clearPendingRoll(this.options.dataDir, this.options.onError);
      return;
    }

    // Only a `rolling` record grants a replacement, and only ever of files
    // that record's own attempt wrote. A `written` record is finished by
    // start(); anything else takes a fresh name.
    const retry = this.pending?.phase === "rolling";
    const rollId = retry ? (this.pending as PendingRoll).rollId : slot;
    // Before the spawn, so a writer that dies during the roll still leaves the
    // name behind for its successor.
    this.pending = { rollId, maxRowid: bound.maxRowid, phase: "rolling" };
    writePendingRoll(this.options.dataDir, this.pending, this.options.onError);

    const outcome = await this.runRoll(rollId, bound.maxRowid, retry);
    if (!outcome.ok) {
      if (outcome.nameTaken) {
        // The one failure a retry cannot fix. Keeping the id would hand the
        // next slot a `--replace` for a file this roll did not write, and it
        // would overwrite an earlier roll's rows with a fraction of them.
        this.pending = null;
        clearPendingRoll(this.options.dataDir, this.options.onError);
        this.reportFailure(
          `roll ${rollId} found its name already taken and will not reuse it (${outcome.why}); ` +
            `the next roll takes a fresh name and ${bound.rows} rows stay in the hot store`,
        );
        return;
      }
      this.reportFailure(
        `roll ${rollId} did not finish (${outcome.why}); ${bound.rows} rows stay in the hot store`,
      );
      return;
    }

    // The roll echoes back the inputs it was given. They must match, and
    // checking them is the only end-to-end verification this two-process
    // boundary can have before rows are deleted.
    const mismatch = describeMismatch(outcome.result, rollId, bound);
    if (mismatch !== null) {
      this.reportFailure(
        `roll ${rollId} reported ${mismatch}; nothing truncated`,
      );
      return;
    }

    // Recorded as finished BEFORE the delete. A writer killed between the two
    // otherwise leaves a record its successor reads as unfinished, and that
    // successor replaces a file whose rows are already durable.
    this.pending = { rollId, maxRowid: bound.maxRowid, phase: "written" };
    writePendingRoll(this.options.dataDir, this.pending, this.options.onError);

    // Only now, and only through the bound the roll was given: the store has
    // kept ingesting throughout, and those rows were never written.
    const removed = this.options.store.deleteThrough(bound.maxRowid);
    // Durable before the record goes. The store runs `synchronous = NORMAL`,
    // so without this an unclean power-off can keep the record's removal and
    // lose the delete — and the next roll then writes those rows a second
    // time, under a second name, for ever.
    this.options.store.checkpoint();
    this.pending = null;
    clearPendingRoll(this.options.dataDir, this.options.onError);
    this.options.log(
      `roll ${rollId} wrote ${outcome.summary}; ${removed} rows truncated`,
    );
    this.reportExpiry(outcome.result);
    await this.compactClosedHour(slot);
  }

  /**
   * What retention could not do, on the error sink.
   *
   * Not in the success line above, because a tree growing past its configured
   * bound is a failure an operator has to see, and it repeats every roll until
   * the cause goes. The two states are reported apart: a tree that could not be
   * listed means nothing was even considered, and a directory that could not be
   * unlinked means the rest of the expiry ran.
   */
  private reportExpiry(result: RollSummary): void {
    if (typeof result.expiryTreeError === "string") {
      this.reportFailure(
        `retention could not list the Parquet tree (${result.expiryTreeError}); ` +
          `nothing is being expired and the tree grows without bound`,
      );
    }
    const failures = result.expiryFailures ?? [];
    if (failures.length === 0) return;
    this.reportFailure(
      `retention could not remove ${failures.length} date ` +
        `${failures.length === 1 ? "directory" : "directories"} ` +
        `(${failures[0].date}: ${failures[0].why}); the tree is over its ` +
        `configured retention until they go`,
    );
  }

  /** Roll failures go to the error sink, which the plugin routes to
   * `app.error`. The success log is debug-level; a failure nobody sees is the
   * shape this design exists to make impossible. */
  private reportFailure(problem: unknown): void {
    const line =
      problem instanceof Error
        ? `roll failed: ${problem.message}`
        : String(problem);
    (this.options.onError ?? this.options.log)(line);
  }

  /**
   * Merge the hour this roll just closed, if it closed one.
   *
   * Runs here rather than on a timer of its own so it inherits the scheduler's
   * serialisation: one child at a time, and never beside the roll whose files
   * it is about to read. It runs only once the pending-roll record is cleared,
   * which is what keeps the seam correct -- `rolledOverlap` resolves a pending
   * roll by looking for `<rollId>.parquet`, and a merge that renamed those
   * rows under a new name while the record still pointed at the old one would
   * make the seam conclude the tree does not hold them.
   *
   * A failure is logged and nothing else. The merge touches no hot store row,
   * so there is nothing here that can cost recording.
   */
  private async compactClosedHour(slot: number): Promise<void> {
    if (this.options.compactHourly === false) return;
    if (this.stopped) return;
    // A backlog roll is named for the instant it ran, not for a slot, so it
    // closes no hour. The next boundary catches up.
    if (slot % HOUR_MS !== 0) return;
    if (this.pending !== null) return;

    const hour = slot - HOUR_MS;
    const outcome = await this.runCompaction(hour);
    if (outcome.ok) {
      if (outcome.summary !== null) this.options.log(outcome.summary);
      return;
    }
    this.reportFailure(
      `compacting the hour starting ${hour} did not finish (${outcome.why}); ` +
        `its rolls are untouched and still answer every query`,
    );
  }

  private runCompaction(
    hour: number,
  ): Promise<
    { ok: true; summary: string | null } | { ok: false; why: string }
  > {
    const args = [
      COMPACT_ENTRY,
      "--data-dir",
      this.options.dataDir,
      "--hour",
      String(hour),
    ];
    const child = (this.options.spawnCompact ?? defaultSpawn)(args);
    this.running = child;

    return new Promise((resolve) => {
      let out = "";
      let err = "";
      let settled = false;
      const settle = (
        result:
          { ok: true; summary: string | null } | { ok: false; why: string },
      ) => {
        if (settled) return;
        settled = true;
        clearTimeout(killer);
        this.running = null;
        resolve(result);
      };
      child.stdout?.on("data", (chunk: Buffer) => (out += chunk.toString()));
      child.stderr?.on("data", (chunk: Buffer) => (err += chunk.toString()));
      child.stdout?.on("error", () => {});
      child.stderr?.on("error", () => {});
      child.on("error", (error) =>
        settle({ ok: false, why: (error as Error).message }),
      );
      const killer = setTimeout(() => {
        child.kill("SIGKILL");
        settle({ ok: false, why: `it ran past ${COMPACT_TIMEOUT_MS} ms` });
      }, this.options.timeoutMs ?? COMPACT_TIMEOUT_MS);
      killer.unref();
      child.on("exit", (code) => {
        if (code !== 0) {
          settle({
            ok: false,
            why: err.trim() || `it exited ${String(code)}`,
          });
          return;
        }
        settle({ ok: true, summary: summariseCompaction(out) });
      });
    });
  }

  private runRoll(
    rollId: number,
    maxRowid: number,
    retry: boolean,
  ): Promise<RollOutcome> {
    const args = [
      ROLL_ENTRY,
      "--data-dir",
      this.options.dataDir,
      "--max-rowid",
      String(maxRowid),
      "--roll-id",
      String(rollId),
      "--retention-days",
      String(this.options.retentionDays ?? 0),
      // Only a retry is allowed to replace a file. Otherwise a roll that
      // arrives at a name already taken fails loudly instead of overwriting
      // history with a fraction of it.
      ...(retry ? ["--replace"] : []),
    ];
    const child = (this.options.spawnRoll ?? defaultSpawn)(args);
    this.running = child;

    return new Promise((resolve) => {
      let out = "";
      let err = "";
      let settled = false;
      const settle = (result: RollOutcome) => {
        if (settled) return;
        settled = true;
        clearTimeout(killer);
        this.running = null;
        resolve(result);
      };

      child.stdout?.on("data", (chunk: Buffer) => (out += chunk.toString()));
      child.stderr?.on("data", (chunk: Buffer) => (err += chunk.toString()));
      // A ChildProcess with no error listener rethrows the event, and the
      // writer would exit for a failure that should cost one roll.
      child.stdout?.on("error", () => {});
      child.stderr?.on("error", () => {});
      child.on("error", (error) =>
        settle({ ok: false, why: error.message, nameTaken: false }),
      );

      const killer = setTimeout(() => {
        this.options.log(`roll ${rollId} exceeded its timeout; killing it`);
        child.kill("SIGTERM");
        const hard = setTimeout(
          () => child.kill("SIGKILL"),
          ROLL_KILL_GRACE_MS,
        );
        hard.unref();
      }, this.options.timeoutMs ?? ROLL_TIMEOUT_MS);
      killer.unref();

      // `close`, not `exit`: exit can fire while the last line is still in the
      // pipe, and that line is the roll's whole report.
      child.on("close", (code, signal) => {
        if (code === 0) {
          const result = parseSummary(out);
          settle(
            result === null
              ? { ok: false, why: "an unreadable summary", nameTaken: false }
              : { ok: true, summary: describe(result), result },
          );
          return;
        }
        const how = code === null ? `signal ${signal}` : `code ${code}`;
        // The first line of stderr is the message; the rest is a stack, and a
        // stack frame tells an operator nothing about why the roll stopped.
        const reason = err.trim().split("\n")[0] ?? "";
        settle({
          ok: false,
          why: `${how}: ${reason}`,
          nameTaken: code === EXIT_NAME_TAKEN,
        });
      });
    });
  }

  /**
   * Disarm the timer and kill a roll in flight.
   *
   * It does not promise that nothing is truncated: a roll that exits 0 while
   * this waits has done its work, and `rollOnce` goes on to delete the rows it
   * wrote. That is correct — the alternative is a roll whose output is in the
   * tree and whose rows are still in the store.
   *
   * The wait is bounded, because the plugin SIGKILLs the writer when its own
   * budget runs out and an unbounded wait here would spend that budget and
   * leave the store unclosed.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    const running = this.running;
    if (running === null || running.exitCode !== null) return;
    const exited = new Promise<void>((resolve) =>
      running.once("exit", () => resolve()),
    );
    running.kill("SIGTERM");
    const hard = setTimeout(() => running.kill("SIGKILL"), ROLL_KILL_GRACE_MS);
    hard.unref();
    // Bounded by the same budget: past it the roll is unkillable from here
    // (uninterruptible IO on a failing card), and holding the writer open
    // only means the plugin kills the writer too.
    const gaveUp = new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, ROLL_KILL_GRACE_MS * 2);
      timer.unref();
    });
    await Promise.race([exited, gaveUp]);
    clearTimeout(hard);
  }
}

function writePendingRoll(
  dataDir: string,
  pending: PendingRoll,
  onError?: (line: string) => void,
): void {
  const path = writerPaths(dataDir).pendingRoll;
  try {
    // Temp, fsync, rename, fsync the directory — the same order the tree's
    // files get. Rewriting in place leaves a torn record after a power cut,
    // and `readPendingRoll` reads a torn record as absent, which is the state
    // that duplicates rows for a `written` roll and republishes files for a
    // `rolling` one.
    writeFileSync(`${path}.tmp`, `${JSON.stringify(pending)}\n`, {
      mode: 0o600,
    });
    commitFile(`${path}.tmp`, path);
  } catch (err) {
    // Recording this is a safeguard against one narrow failure, and refusing
    // to roll because the safeguard could not be written would be worse than
    // the failure it guards. But a device running without it must say so —
    // ENOSPC is both the likeliest cause and the condition under which the
    // safeguard matters.
    onError?.(
      `could not record the pending roll (${err instanceof Error ? err.message : String(err)}); ` +
        `a roll interrupted now could be written to the tree twice`,
    );
  }
}

function clearPendingRoll(
  dataDir: string,
  onError?: (line: string) => void,
): void {
  try {
    rmSync(writerPaths(dataDir).pendingRoll, { force: true });
  } catch (err) {
    // Unguarded, this threw after the rows were already deleted — leaving a
    // record naming a completed roll and a log line that never printed.
    onError?.(
      `could not clear the pending roll record (${err instanceof Error ? err.message : String(err)})`,
    );
  }
}

type RollOutcome =
  | { ok: true; summary: string; result: RollSummary }
  | { ok: false; why: string; nameTaken: boolean };

/** The fields of the roll's JSON line this side depends on. */
interface RollSummary {
  rollId: number;
  maxRowid: number;
  rows: number;
  files: { date: string }[];
  sidecarRows: number;
  peakRssBytes: number | null;
  /** Optional, because a roll built before retention existed omits all three. */
  expired?: string[];
  expiryFailures?: { date: string; why: string }[];
  expiryTreeError?: string | null;
}

function parseSummary(stdout: string): RollSummary | null {
  try {
    const parsed = JSON.parse(
      stdout.trim().split("\n").pop() ?? "",
    ) as Partial<RollSummary>;
    if (
      !Number.isSafeInteger(parsed.rollId) ||
      !Number.isSafeInteger(parsed.maxRowid) ||
      !Number.isSafeInteger(parsed.rows) ||
      !Array.isArray(parsed.files)
    ) {
      return null;
    }
    return parsed as RollSummary;
  } catch {
    return null;
  }
}

/**
 * Whether the roll wrote what it was asked to write.
 *
 * The roll echoes its inputs back, and under this design `bound.rows` and the
 * roll's own count must be equal — nothing but the writer inserts, and nothing
 * deletes while a roll runs. It is the only end-to-end check available on this
 * boundary, and it costs a comparison.
 */
function describeMismatch(
  result: RollSummary,
  rollId: number,
  bound: { maxRowid: number; rows: number },
): string | null {
  if (result.rollId !== rollId) {
    return `roll id ${result.rollId}, not ${rollId}`;
  }
  if (result.maxRowid !== bound.maxRowid) {
    return `bound ${result.maxRowid}, not ${bound.maxRowid}`;
  }
  if (result.rows !== bound.rows) {
    return `${result.rows} rows written against ${bound.rows} in the store`;
  }
  return null;
}

function defaultSpawn(args: string[]): ChildProcess {
  // An argument array, never a shell: the data directory is a string an
  // operator typed into the Admin UI.
  return spawn(process.execPath, args, { stdio: ["ignore", "pipe", "pipe"] });
}

/** The roll's own summary, reduced to a log line. */
function describe(result: RollSummary): string {
  const dates = result.files.map((file) => file.date).join(", ") || "nothing";
  const peak =
    typeof result.peakRssBytes === "number"
      ? `, peaking at ${Math.round(result.peakRssBytes / 1048576)} MB`
      : "";
  const expired =
    result.expired !== undefined && result.expired.length > 0
      ? `, expiring ${result.expired.join(", ")}`
      : "";
  return `${result.rows} rows to ${dates} and ${result.sidecarRows} sidecar rows${peak}${expired}`;
}

/**
 * One line about what a merge did, or null when it did nothing.
 *
 * Parsed rather than trusted: the process prints JSON on success, and a build
 * mismatch or a truncated pipe should read as "no summary" rather than as an
 * exception inside the scheduler.
 */
function summariseCompaction(stdout: string): string | null {
  try {
    const parsed = JSON.parse(stdout.trim().split("\n").pop() ?? "") as {
      hourStartMs?: number;
      files?: { rows: number; bytesIn: number; bytesOut: number }[];
      strayInputs?: string[];
    };
    const files = parsed.files ?? [];
    if (files.length === 0) return null;
    const rows = files.reduce((sum, file) => sum + file.rows, 0);
    const bytesIn = files.reduce((sum, file) => sum + file.bytesIn, 0);
    const bytesOut = files.reduce((sum, file) => sum + file.bytesOut, 0);
    const stray = parsed.strayInputs?.length ?? 0;
    return (
      `compacted the hour starting ${parsed.hourStartMs ?? "?"}: ` +
      `${files.length} file(s), ${rows} rows, ${bytesIn} -> ${bytesOut} bytes` +
      (stray > 0 ? `; ${stray} input(s) could not be removed` : "")
    );
  } catch {
    return null;
  }
}
