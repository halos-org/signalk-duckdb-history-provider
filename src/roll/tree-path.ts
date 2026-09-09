import { isAbsolute, join, relative, resolve } from "node:path";
import { DATA_LAYOUT } from "../data-dir.js";

/**
 * Where a roll's output goes.
 *
 * The tree carries time and nothing else: `parquet/date=<YYYY-MM-DD>/`, with
 * `context` and `path` as columns inside the file. That is the layout Unit 3a
 * settled, and it is also why there is no path guard here — no delta-supplied
 * string ever becomes a directory name, so there is nothing to sanitise.
 *
 * Rows are placed by their own timestamp, not by when the roll ran. A roll
 * that spans midnight writes one file per date and stays correct; the roll
 * interval's divisibility rule is about the schedule, not about this.
 */

/**
 * The range `YYYY-MM-DD` can express.
 *
 * Narrower than `Date`'s own ±8.64e15: `toISOString` switches to an extended
 * form outside years 0000–9999, and slicing that to ten characters yields
 * `+011476-08` — a directory name no reader can parse as a date and that does
 * not sort with the others.
 */
const MAX_TIMESTAMP = 253402300799999;
const MIN_TIMESTAMP = -62167219200000;

/**
 * The UTC date a row belongs to, as the directory segment names it.
 *
 * UTC rather than local: the device's timezone is an installation detail, and
 * a tree cut on local dates cannot be read by anything that does not know
 * which timezone wrote it.
 */
export function utcDateSegment(ts: number): string {
  if (!Number.isFinite(ts) || ts > MAX_TIMESTAMP || ts < MIN_TIMESTAMP) {
    throw new RangeError(`${ts} is not a timestamp this can name a date from`);
  }
  return new Date(ts).toISOString().slice(0, 10);
}

/** The tree root — every roll file lives under this and nothing else does. */
export function treeRoot(dataDir: string): string {
  return join(resolve(dataDir), DATA_LAYOUT.tree);
}

/**
 * The UTC midnight a `date=YYYY-MM-DD` directory names, or null for an entry
 * that is not one.
 *
 * Shared by the reader, which selects files with it, and by expiry, which
 * deletes directories with it. Two copies of this rule would be two answers to
 * "is this entry ours", and the one that says yes too readily removes a
 * directory the tree does not own.
 *
 * Round-tripped rather than range-checked: `Date.parse` accepts
 * `2026-08-32T00:00:00.000Z` in some engines and rolls it into September, which
 * would name a directory a day that is not the one it holds.
 */
export function dateDirectoryStart(entry: string): number | null {
  if (!entry.startsWith("date=")) return null;
  const date = entry.slice("date=".length);
  const parsed = Date.parse(`${date}T00:00:00.000Z`);
  if (!Number.isFinite(parsed) || utcDateSegment(parsed) !== date) return null;
  return parsed;
}

/** The directory for one date. The roll creates it before writing into it. */
export function dateDirectory(dataDir: string, ts: number): string {
  return join(treeRoot(dataDir), `date=${utcDateSegment(ts)}`);
}

/**
 * One roll's file for one date, named by the instant the roll began.
 *
 * The name is unique per roll, and a roll that writes two dates writes the
 * same name in two directories — which is why the date is a directory rather
 * than part of the filename.
 */
export function rollFile(
  dataDir: string,
  ts: number,
  rollStartMs: number,
): string {
  return assertUnderDataDir(
    dataDir,
    join(dateDirectory(dataDir, ts), `${rollIdSegment(rollStartMs)}.parquet`),
  );
}

/**
 * Where a roll writes before it renames.
 *
 * The `.tmp` suffix is the whole mechanism that keeps a killed roll from
 * leaving something a reader treats as complete: a reader globs `*.parquet`
 * and this does not match. No marker file, no lock.
 */
export function rollTempFile(
  dataDir: string,
  ts: number,
  rollStartMs: number,
): string {
  return `${rollFile(dataDir, ts, rollStartMs)}.tmp`;
}

/**
 * The cumulative last-value sidecar, deliberately outside the tree.
 *
 * Its rows are copies of rows already in the tree, so a reader globbing the
 * tree would count every path's last value twice.
 */
export function sidecarFile(dataDir: string): string {
  return assertUnderDataDir(
    dataDir,
    join(resolve(dataDir), DATA_LAYOUT.sidecar, "latest.parquet"),
  );
}

/** Where the sidecar is written before it is renamed into place. */
export function sidecarTempFile(dataDir: string): string {
  return `${sidecarFile(dataDir)}.tmp`;
}

function rollIdSegment(rollStartMs: number): string {
  // `>= 1`, the same floor the roll process and the pending-roll record use.
  // `nextRollAt` returns 0 for any clock set before the epoch, and a roll id
  // of 0 that one side accepts and another rejects wedged every future roll.
  if (!Number.isInteger(rollStartMs) || rollStartMs < 1) {
    throw new RangeError(`${rollStartMs} is not a roll id`);
  }
  return String(rollStartMs);
}

/**
 * Assert a composed path did not escape the data directory.
 *
 * Every argument that reaches these functions is the plugin's own — a clock
 * reading and a configured directory — so this cannot fire today. It is here
 * for the change that reintroduces an untrusted segment, which is the change
 * that would otherwise write outside the data directory in silence.
 */
export function assertUnderDataDir(dataDir: string, path: string): string {
  const root = resolve(dataDir);
  const candidate = resolve(path);
  const inside = relative(root, candidate);
  if (inside === "" || inside.startsWith("..") || isAbsolute(inside)) {
    throw new Error(`${path} is not inside the data directory ${root}`);
  }
  return candidate;
}

/**
 * One compacted hour's file for one date.
 *
 * **The `hour-` prefix is not decoration.** `rolledOverlap` resolves the seam
 * by looking for `<rollId>.parquet` in a date directory, so a merged file
 * named with a bare number could be mistaken for the output of the roll whose
 * id it matched — and the seam would then subtract rows the merge had already
 * folded in. The two name spaces have to stay apart, and `liveTreeFiles` is
 * what tells them apart for a reader.
 *
 * Named for the hour it covers rather than for the instant the merge ran, so
 * a retry after a failure writes the same name and replaces its own output
 * rather than accumulating a second copy under a new one.
 */
export function compactedFile(
  dataDir: string,
  ts: number,
  hourStartMs: number,
): string {
  return assertUnderDataDir(
    dataDir,
    join(
      dateDirectory(dataDir, ts),
      `${COMPACTED_PREFIX}${rollIdSegment(hourStartMs)}.parquet`,
    ),
  );
}

/**
 * Where a merge writes before it renames, for the reason `rollTempFile` gives.
 *
 * The pid is in the name because a merge takes no claim on the hour — no lock,
 * no pending record, nothing the roll's `NameTakenError` can catch. Two merges
 * of one hour sharing a temp path would interleave one COPY's bytes with the
 * other's and rename the mixture into place.
 */
export function compactedTempFile(
  dataDir: string,
  ts: number,
  hourStartMs: number,
  pid: number,
): string {
  if (!Number.isInteger(pid) || pid < 1) {
    throw new RangeError(`${pid} is not a process id`);
  }
  return `${compactedFile(dataDir, ts, hourStartMs)}.${pid}.tmp`;
}

const COMPACTED_PREFIX = "hour-";

/**
 * The roll id a file name carries, or null when the name is not a roll's.
 *
 * Compaction chooses its inputs by id rather than by the timestamps inside
 * them: a roll places each row in the date directory that row's own timestamp
 * names, so a set chosen by content could only be found by opening every file.
 * A compacted file is deliberately not a roll and returns null here, which is
 * what stops a second pass from folding one merge into another.
 */
export function rollIdFromName(name: string): number | null {
  if (!name.endsWith(".parquet")) return null;
  const stem = name.slice(0, -".parquet".length);
  if (!/^[0-9]+$/.test(stem)) return null;
  const id = Number(stem);
  return Number.isSafeInteger(id) && id >= 1 ? id : null;
}

/** The hour a compacted file covers, or null when the name is not one. */
export function compactedHourFromName(name: string): number | null {
  if (!name.startsWith(COMPACTED_PREFIX) || !name.endsWith(".parquet")) {
    return null;
  }
  const stem = name.slice(COMPACTED_PREFIX.length, -".parquet".length);
  if (!/^[0-9]+$/.test(stem)) return null;
  const hour = Number(stem);
  return Number.isSafeInteger(hour) && hour >= 1 ? hour : null;
}

/** Milliseconds in the hour one merged file covers. */
export const HOUR_MS = 3_600_000;

/**
 * Whether a roll belongs to the hour a merge covers.
 *
 * The rolls a merge folds in are those that *ran* inside `(hour, hour + 1h]`,
 * not those named inside `[hour, hour + 1h)`.
 *
 * A roll writes the interval that just ended, so the roll at 12:00 carries
 * 11:55–12:00. Taking the half-open range from the top of the hour would put
 * an hour of data under a name an hour ahead of it, and the file called
 * `hour-11:00` would hold 10:55 to 11:55. The closed upper end is what makes
 * the name describe the contents.
 *
 * It lives here, beside the names it reads, because three separate rules
 * depend on being the same predicate: which rolls a merge takes, which rolls a
 * reader stops returning once that merge lands, and which roll ids the roll
 * itself must now refuse. Two copies of it would be two answers to the same
 * question, and the disagreement would be silent in both directions.
 */
export function coversHour(rollId: number, hourStartMs: number): boolean {
  return rollId > hourStartMs && rollId <= hourStartMs + HOUR_MS;
}

/**
 * The compacted file in this directory that already holds `rollId`'s hour, or
 * null when no merge covers it.
 *
 * `names` is one date directory's listing.
 */
export function compactedHourCovering(
  names: string[],
  rollId: number,
): string | null {
  for (const name of names) {
    const hour = compactedHourFromName(name);
    if (hour !== null && coversHour(rollId, hour)) return name;
  }
  return null;
}

/**
 * The files in one date directory a reader should read.
 *
 * **This is what makes compaction safe without a window.** A merge cannot
 * rename its output in and then unlink its inputs — between those two the
 * directory holds both and every row in the hour is answered twice — and it
 * cannot unlink first either, because then the hour is missing until the
 * rename lands. Neither order is atomic across a set of files.
 *
 * So the merge does not try. It renames its output into place, which is atomic
 * for that one file, and from that instant this function stops returning the
 * rolls it superseded. The inputs are unlinked afterwards at no particular
 * moment, and a merge killed before it gets to them leaves files that are
 * ignored rather than files that are wrong.
 *
 * The suppression is by id range, not by the set the merge actually read, so a
 * roll that lands in an already-merged hour would be dropped here too. That is
 * why `writeDay` refuses such an id outright: this function is allowed to
 * assume no roll ever arrives inside a merged hour, and the roll is what makes
 * the assumption true.
 */
export function liveTreeFiles(names: string[]): string[] {
  return names.filter((name) => {
    const id = rollIdFromName(name);
    if (id === null) return true;
    return compactedHourCovering(names, id) === null;
  });
}
