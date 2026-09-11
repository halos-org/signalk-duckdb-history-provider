import { readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import {
  assertUnderDataDir,
  dateDirectoryStart,
  treeRoot,
  utcDateSegment,
} from "../roll/tree-path.js";

/**
 * Retention: whole date directories, dropped once the window has passed them.
 *
 * **This is a bound on what is stored, not a promise that everything older is
 * gone.** A date directory is the finest thing this layout can drop — a roll
 * file is named for the roll that wrote it and holds whatever rows of that day
 * the roll covered, so nothing in a path names a narrower boundary. A
 * directory therefore survives until its whole day is behind the window, and
 * the oldest surviving sample can be up to a day older than the instant an
 * operator computes as "now minus retentionDays". An operator who sets
 * retention for privacy has to read it that way.
 *
 * No aggregation, no per-path rule: one duration, applied to every path in the
 * tree. That is what `signalk-questdb-history-provider` gets from a QuestDB
 * table TTL, which drops whole day partitions and nothing finer. The
 * resolution ladder is a separate capability and a separate unit (#173).
 *
 * The sidecar is not expired. It holds one row per `(context, path)` — the last
 * value of everything, which is the one question this storage has no index for
 * — and pruning it to the boundary would make a path that stopped reporting
 * inside the window vanish from a snapshot of the present. It is bounded by key
 * count rather than by time, so retention is not what keeps it small.
 */

/** Milliseconds in a UTC day. The tree's directories are cut on these. */
const DAY_MS = 86_400_000;

export interface ExpireOptions {
  dataDir: string;
  /** Whole days to keep. Zero, fractional or nonsense means keep forever. */
  retentionDays: number;
  /** Injected in tests. Production reads the clock. */
  now?: () => number;
}

export interface ExpiryFailure {
  date: string;
  /** An error code, never a message: a message carries the configured path. */
  why: string;
}

export interface ExpiryResult {
  /** The date segments removed, oldest first. */
  removed: string[];
  /** Directories the window had passed and that could not be removed. */
  failures: ExpiryFailure[];
  /**
   * Why the tree could not be listed at all, if it could not.
   *
   * Apart from the failures above, because it is a different state and a
   * different remedy: not "these three are stuck" but "nothing was even
   * considered". A missing tree is not one of these — that is every device's
   * first hour.
   */
  treeError: string | null;
}

/**
 * Drop every date directory the retention window has passed.
 *
 * Total by construction: it reports what it could not do rather than throwing.
 * It runs at the end of a roll, and a roll that fails is a roll whose rows are
 * never truncated from the hot store — so a directory this cannot unlink must
 * not be allowed to stop recording.
 */
export function expire(options: ExpireOptions): ExpiryResult {
  const result: ExpiryResult = { removed: [], failures: [], treeError: null };
  const days = Math.floor(options.retentionDays);
  if (!Number.isFinite(days) || days < 1) return result;

  const root = treeRoot(options.dataDir);
  let entries: string[];
  try {
    entries = readdirSync(root);
  } catch (err) {
    // A missing tree is every device's first hour and says nothing. Anything
    // else — EACCES, EIO, a card that has gone read-only — means retention did
    // not run, and returning the same empty answer for both would let the
    // scheduler report a clean roll while the tree grows without bound.
    const code = errorCode(err);
    if (code !== "ENOENT") result.treeError = code;
    return result;
  }

  const present = entries
    .map((entry) => dateDirectoryStart(entry))
    .filter((day): day is number => day !== null);
  if (present.length === 0) return result;

  const boundary = referenceInstant(present, options.now) - days * DAY_MS;
  // Oldest first, so a failure part-way through leaves the newest days intact
  // rather than a window with a hole in the middle of it.
  for (const day of present.sort((a, b) => a - b)) {
    if (day + DAY_MS >= boundary) break;
    const date = utcDateSegment(day);
    try {
      rmSync(assertUnderDataDir(options.dataDir, join(root, `date=${date}`)), {
        recursive: true,
        force: true,
      });
      result.removed.push(date);
    } catch (err) {
      result.failures.push({ date, why: errorCode(err) });
    }
  }
  return result;
}

/**
 * An error reduced to something safe to log.
 *
 * The code, or failing that the class — never the message. A filesystem error's
 * message carries the whole path it failed on, and these lines reach the Signal
 * K log and ship in support bundles from a public plugin. The date already
 * names which directory is stuck, which is the part an operator can act on;
 * `EACCES` against `ENOTEMPTY` against `EROFS` is the rest of it.
 */
function errorCode(err: unknown): string {
  const code = (err as NodeJS.ErrnoException | undefined)?.code;
  if (typeof code === "string" && code !== "") return code;
  return err instanceof Error ? err.name : "unknown";
}

/**
 * The instant the window is measured back from.
 *
 * `min(clock, the end of the newest date directory)`, which is the formula
 * QuestDB's TTL uses with its table's latest timestamp in place of the
 * directory. Both caps earn their place, and in opposite directions: a device
 * whose RTC reads a year ahead before NTP corrects it would otherwise have its
 * whole tree deleted by the first roll, and one row stamped in the far future
 * would otherwise take every real day with it.
 *
 * The consequence worth stating is that expiry follows the data rather than the
 * calendar: a device that stops recording stops expiring, because the newest
 * directory stops moving. The tree is not growing then either.
 */
function referenceInstant(present: number[], now?: () => number): number {
  const newest = present.reduce((a, b) => (b > a ? b : a), present[0]);
  return Math.min((now ?? Date.now)(), newest + DAY_MS - 1);
}
