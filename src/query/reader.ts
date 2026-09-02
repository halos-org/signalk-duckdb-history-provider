import { existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { join } from "node:path";
import { DuckDBInstance, listValue } from "@duckdb/node-api";
import type { DuckDBConnection, DuckDBValue } from "@duckdb/node-api";
import { DATA_DIR_MODE, DATA_LAYOUT } from "../data-dir.js";
import {
  BASE_DUCKDB_CONFIG,
  loadSqliteScanner,
  lockDownFileAccess,
} from "../duckdb/extension.js";
import { sqlLiteral } from "../duckdb/sql.js";
import {
  dateDirectoryStart,
  liveTreeFiles,
  sidecarFile,
  treeRoot,
} from "../roll/tree-path.js";
import { readPendingRoll, writerPaths } from "../writer/contract.js";
import { DEFAULT_ROW_LIMIT, RANGE_COLUMNS, VALUE_COLUMNS } from "./duck.js";
import type { QueryRequest, ValueAggregate } from "./duck.js";

/**
 * The hot store and the Parquet tree, read as one.
 *
 * This runs in the query service — it is the only file besides `roll/roll.ts`
 * that may import the engine, and it may because that process is not the
 * Signal K server. Everything is one statement: the tree files that intersect
 * the range, the unrolled remainder of the hot store, and one filter over
 * both.
 *
 * **The seam between them is the interesting part.** A roll writes its rows to
 * Parquet and the writer deletes them from the store afterwards, so between
 * those two steps every covered row exists twice. `overlap` below is what
 * subtracts one copy, exactly, without a `DISTINCT` over the whole answer.
 */

/**
 * What the engine may allocate. The same ceiling the roll takes, and for the
 * same reason: DuckDB's default is 80% of system RAM, which on a 4 GB device
 * is memory the marine stack is already using.
 */
export const DEFAULT_MEMORY_LIMIT = "256MB";

/**
 * Series one values request may name.
 *
 * A bound on the statement rather than on the answer: every spec is another
 * `UNION ALL` branch over the materialized source and two more bound
 * parameters. The caller's bucket budget does not contain it, because that
 * counts buckets × series — a coarse resolution leaves room for thousands of
 * paths inside it. The service answers one request at a time, so a statement
 * that wide holds the only engine until the deadline kills it.
 */
const MAX_SPECS = 200;

/** Milliseconds in a UTC day. The tree's directories are cut on these. */
const DAY_MS = 86_400_000;

/**
 * Date directories a snapshot may read when the sidecar cannot answer it.
 *
 * **The rule a snapshot answers under**, and it is a bound rather than a
 * preference. "The last value of every path at T" has no index here: a path
 * that stopped reporting a week before T still has a last value, so an exact
 * answer is a backward scan whose depth is the retention window rather than
 * the request. QuestDB answers it with `LATEST ON` over an index, and the
 * sibling provider records that timing out past 30 seconds on a real install
 * even so.
 *
 * So a snapshot resolves a path from the sidecar when it can — which is exact
 * over all of history, and is the whole of a snapshot taken at or after the
 * newest recorded row — and from this many date directories, ending at T's own,
 * when it cannot. A path whose last row before T is older than that window is
 * absent from the snapshot rather than searched for.
 *
 * Two rather than more because each directory is a day of rows to scan and
 * nothing prunes on path: a day answers every path still reporting at T, and
 * the day before it answers one that went quiet overnight. Raising this trades
 * seconds per snapshot for paths that have not reported in days.
 */
const SNAPSHOT_SCAN_DAYS = 2;

const COLUMNS = RANGE_COLUMNS.join(", ");

/**
 * The `values` projection, in the order `VALUE_COLUMNS` declares.
 *
 * `lat` and `lon` are unpacked from the position struct the branches pack;
 * the rest pass through. Derived rather than written out, because the decoder
 * reads the row by position and a reordering here would mislabel every value
 * it produced without failing anything.
 */
const VALUES_PROJECTION = VALUE_COLUMNS.map((name) =>
  name === "lat" || name === "lon" ? `pos.${name} AS ${name}` : name,
).join(", ");

export interface ReaderOptions {
  dataDir: string;
  memoryLimit?: string;
}

export interface ReadResult {
  rows: unknown[][];
  truncated: boolean;
  treeFiles: number;
}

/** One Parquet file, with the UTC day its directory names. */
interface TreeFile {
  day: number;
  path: string;
  /** The file's own name, which is the id of the roll that wrote it. */
  name: string;
}

/**
 * The rows a completed-but-not-truncated roll put in both places.
 *
 * `maxRowid` bounds the set the roll covered, and `dayStarts` names the days
 * whose file it actually managed to write — a roll killed between two dates
 * has written one of them and not the other, and the store is still the only
 * copy of the rest.
 */
interface Overlap {
  maxRowid: number;
  dayStarts: number[];
}

interface Plan {
  files: TreeFile[];
  overlap: Overlap | null;
  /** Whether this window is read from the hot store at all. See `needsHotStore`. */
  readStore: boolean;
}

/**
 * Whether a window can hold a row the hot store still has.
 *
 * `sqlite_scanner` does not push a predicate into SQLite: the plan puts a
 * FILTER above a SQLITE_SCAN that emits every row, so a branch matching
 * nothing still costs a scan of the whole store. Attaching it for a window it
 * cannot answer therefore charges each query for however much the current roll
 * interval has accumulated -- measured on a device at 0.708 ms per 1000 rows,
 * which took one unchanged historical query from 25 ms just after a roll to
 * 157 ms just before the next. An index does not help, because nothing reaches
 * it.
 *
 * The decision rests on the oldest row and never on the newest. The oldest
 * only moves forward -- the roll truncates from the front, the writer appends
 * to the back -- so a window ending before it will still end before it when
 * the query runs. The newest moves the other way, and a window opening after
 * it can be filled by the writer between this read and the scan, so a store
 * with nothing in it yet is read rather than skipped.
 */
export function needsHotStore(oldestTs: number | null, to: number): boolean {
  if (oldestTs === null) return true;
  return to > oldestTs;
}

/**
 * The timestamp of the oldest row the hot store still holds, `null` when it is
 * empty, and `undefined` when there is no store to read.
 *
 * **`MIN(ts)`, and not the first row by rowid.** `HotStore.oldestTimestamp`
 * reads by rowid and says why -- insertion order, one row instead of a scan --
 * and that is sound for its own caller, which only asks whether a roll is
 * overdue. It is not sound here. `insertBatch` enforces no timestamp order and
 * `ts` is the recorder's clock reading at arrival, so a device correcting its
 * clock at boot -- the case `path-matcher.ts` already treats as a discontinuity
 * -- stores a smaller `ts` at a higher rowid. A boundary taken from the first
 * row would then place the store later than it reaches, and `needsHotStore`
 * would skip a store holding rows inside the window. That is a query missing
 * rows silently, which is worse than the scan this exists to avoid.
 *
 * Scanned, not sought, and deliberately so. An index on `ts` would make this
 * a seek, and it would cost the ingest path ~30% more bytes -- measured on a
 * device at both interval sizes -- which is the axis this provider wins on.
 * The scan is 0.84 ms over a 5-minute interval's 16k rows and 13.99 ms over an
 * hour's 197k, against the ~11 ms and ~139 ms of DuckDB scan the answer saves.
 * It buys the DuckDB side nothing either way: `sqlite_scanner` pushes no
 * predicate down and reaches no index -- see `needsHotStore`.
 */
function hotStoreOldest(dataDir: string): number | null | undefined {
  const path = writerPaths(dataDir).store;
  if (!existsSync(path)) return undefined;
  let db: DatabaseSync | undefined;
  try {
    db = new DatabaseSync(path, { readOnly: true });
    const row = db.prepare("SELECT MIN(ts) AS ts FROM sample").get() as
      { ts: number | null } | undefined;
    // `MIN` over an empty table is one row holding NULL, not no row.
    return row?.ts === undefined || row.ts === null ? null : Number(row.ts);
  } catch {
    // A store being created, or one this build cannot open. Read it through
    // DuckDB as before rather than dropping rows the window may need.
    return null;
  } finally {
    db?.close();
  }
}

/** An engine held open, and the queries it answers. */
export interface Reader {
  read(request: QueryRequest): Promise<ReadResult>;
  close(): void;
}

/**
 * Start the engine.
 *
 * Everything expensive happens here and once: mapping the addon, creating the
 * instance, expanding and loading `sqlite_scanner`, and locking the engine to
 * the data directory. Measured on the device, that is 336–375 ms, paid once
 * instead of by every request.
 *
 * The extension is loaded even though no store is attached yet, because the
 * lockdown that follows would refuse it later. A device whose bundle cannot be
 * loaded still gets a reader — the tree needs no SQLite — and the reason is
 * kept for the first query that does need the store.
 */
export async function openReader(options: ReaderOptions): Promise<Reader> {
  const { dataDir } = options;
  // One directory for this process, not one per query. A query can spill while
  // a roll is spilling beside it, and DuckDB's default temp directory for an
  // in-memory database is the current working directory — which for a process
  // the plugin spawned is the Signal K server's.
  const scratch = join(dataDir, DATA_LAYOUT.scratch, `query-${process.pid}`);
  mkdirSync(scratch, { recursive: true, mode: DATA_DIR_MODE });

  let instance: DuckDBInstance | null = null;
  let connection: DuckDBConnection | null = null;
  try {
    instance = await DuckDBInstance.create(":memory:", {
      ...BASE_DUCKDB_CONFIG,
      memory_limit: options.memoryLimit ?? DEFAULT_MEMORY_LIMIT,
      temp_directory: scratch,
    });
    connection = await instance.connect();
  } catch (err) {
    connection?.closeSync();
    instance?.closeSync();
    rmSync(scratch, { recursive: true, force: true });
    throw err;
  }

  let scannerFailure: string | null = null;
  try {
    await loadSqliteScanner(connection, {
      cacheDir: join(dataDir, DATA_LAYOUT.extensionCache),
    });
  } catch (err) {
    // Not fatal, and not silent either. Without the extension the hot store
    // cannot be read, but the tree can — an old range still answers, and a
    // recent one says why it cannot.
    scannerFailure = err instanceof Error ? err.message : String(err);
  }

  // Everything legitimate is open, so nothing else may be. After this the
  // engine cannot read a path outside the data directory, cannot install an
  // extension and cannot have its configuration changed — which is what stops
  // a crafted path or an unknown function from turning a query into a
  // download. Attaching a database inside the allowed directory still works,
  // which is what the per-query attach below relies on.
  //
  // Inside the cleanup scope, like the engine above: a data directory these
  // settings will not accept leaves the caller no handle to close, and the
  // service is restarted on every request after that.
  try {
    await lockDownFileAccess(connection, [dataDir]);
  } catch (err) {
    connection.closeSync();
    instance.closeSync();
    rmSync(scratch, { recursive: true, force: true });
    throw err;
  }

  const live = connection;
  return {
    read: (request) =>
      runOne(request, { dataDir, connection: live, scannerFailure }),
    close: () => {
      live.closeSync();
      instance.closeSync();
      rmSync(scratch, { recursive: true, force: true });
    },
  };
}

/**
 * One request on an already-open engine.
 *
 * A snapshot is the one kind that does not name a time range, and it is
 * answered differently enough — a sidecar, a bounded window, and a statement
 * that groups rather than orders — to be its own path from here down.
 */
async function runOne(
  request: QueryRequest,
  context: ReadContext,
): Promise<ReadResult> {
  validate(request);
  if (request.kind === "snapshot") return runSnapshot(request, context);

  const plan = planSources(context.dataDir, request);
  const sql = compile(request, plan);
  if (sql === null) {
    // Neither a tree file nor a hot store: a device that has recorded nothing,
    // or a range that predates the oldest date directory. An empty answer, not
    // an error — the history API's own answer for a range with no data.
    return { rows: [], truncated: false, treeFiles: 0 };
  }
  requireScanner(plan.readStore, context);

  const answer = await execute(context, sql, {
    attach: plan.readStore,
    limit: rowLimit(request),
  });
  return { ...answer, treeFiles: plan.files.length };
}

interface ReadContext {
  dataDir: string;
  connection: DuckDBConnection;
  scannerFailure: string | null;
}

/**
 * A statement, run against the hot store and the tree.
 *
 * The attachment is per statement and lasts about a millisecond. Holding it
 * would work — measured across processes, a held attachment still sees rows
 * written after it and still lets the writer truncate its WAL — but attaching
 * here also picks up a store that did not exist when the engine started, which
 * on a fresh device is the first minute of its life.
 */
async function execute(
  context: ReadContext,
  sql: Compiled,
  options: { attach: boolean; limit: number },
): Promise<{ rows: unknown[][]; truncated: boolean }> {
  const { connection, dataDir } = context;
  let attached = false;
  try {
    if (options.attach) {
      // READ_ONLY: the writer owns this file and keeps ingesting throughout.
      // SQLite in WAL mode takes concurrent readers, which is why the hot
      // store is SQLite at all.
      await connection.run(
        `ATTACH '${sqlLiteral(writerPaths(dataDir).store)}' AS hot (TYPE SQLITE, READ_ONLY)`,
      );
      attached = true;
    }

    const result = await connection.runAndReadAll(sql.text, sql.params);
    const rows = result.getRowsJS();
    // In place. These arrays are freshly built and nothing else holds them,
    // and mapping into new ones cost 11 MB more at 82,000 rows on the device —
    // 230 MB against 219 MB, on the largest transient in this design.
    for (const row of rows) {
      for (let i = 0; i < row.length; i += 1) {
        // `ts` is BIGINT in the tree and INTEGER in the store, so it arrives
        // as a BigInt whenever a Parquet file is one of the branches — and
        // `JSON.stringify` throws on one rather than writing a number.
        if (typeof row[i] === "bigint") row[i] = Number(row[i]);
      }
    }
    return {
      // One row over the limit is asked for, so a full answer and a truncated
      // one can be told apart. It is dropped here.
      truncated: rows.length > options.limit,
      rows: rows.length > options.limit ? rows.slice(0, options.limit) : rows,
    };
  } finally {
    // A left-over attachment would make the next query's `ATTACH` fail on the
    // name, so every one of them would fail after the first that threw.
    if (attached) await connection.run("DETACH hot").catch(() => {});
  }
}

/** The store cannot be read without the extension, and says so once. */
function requireScanner(storeExists: boolean, context: ReadContext): void {
  if (storeExists && context.scannerFailure !== null) {
    throw new Error(
      `this range needs the hot store and sqlite_scanner could not be loaded: ${context.scannerFailure}`,
    );
  }
}

/**
 * The last value per `(context, path)` at an instant.
 *
 * Two statements, and the first is what keeps the second cheap. The sidecar
 * holds one row per key — the newest that has ever been rolled — so a key whose
 * sidecar row is at or before T is already answered by it, exactly, however
 * long ago that row was written. Only a key whose sidecar row is *newer* than T
 * needs the tree, because its value at T is an older row the sidecar has
 * replaced.
 *
 * So the first statement asks whether any such key exists, and the tree is read
 * only when one does. A snapshot of the present therefore reads the sidecar and
 * the hot store and nothing else; a snapshot of an instant the tree has rolled
 * past reads `SNAPSHOT_SCAN_DAYS` date directories under the rule that constant
 * states.
 *
 * A missing sidecar is treated as "every key needs the tree": it is written by
 * every roll, so its absence means either nothing has rolled — in which case
 * there is no tree to read either — or somebody removed it, and answering from
 * the hot store alone would silently drop every path that stopped reporting
 * before the last roll.
 */
async function runSnapshot(
  request: Extract<QueryRequest, { kind: "snapshot" }>,
  context: ReadContext,
): Promise<ReadResult> {
  const { dataDir } = context;
  const sidecar = sidecarFile(dataDir);
  const hasSidecar = existsSync(sidecar);
  const storeExists = existsSync(writerPaths(dataDir).store);
  requireScanner(storeExists, context);

  const at = Math.trunc(request.at);
  const needsTree =
    !hasSidecar || (await sidecarHasRowsAfter(context, sidecar, request));
  const files = needsTree
    ? treeFilesInRange(
        dataDir,
        dayStart(at) - (SNAPSHOT_SCAN_DAYS - 1) * DAY_MS,
        at + 1,
      )
    : [];

  const params: Record<string, DuckDBValue> = { at: BigInt(at) };
  const where = ["ts <= $at", ...pathFilter(request.paths, params)].join(
    " AND ",
  );

  // No seam predicate, unlike a range. A row the tree and the store both hold
  // is the same row twice, and the newest of two identical rows is that row —
  // so the duplicate a range has to subtract cannot change this answer.
  const branches: string[] = [];
  if (hasSidecar) {
    branches.push(
      `SELECT ${COLUMNS} FROM read_parquet('${sqlLiteral(sidecar)}', union_by_name = true) WHERE ${where}`,
    );
  }
  if (files.length > 0) {
    const list = files.map((file) => `'${sqlLiteral(file.path)}'`).join(", ");
    branches.push(
      `SELECT ${COLUMNS} FROM read_parquet([${list}], union_by_name = true) WHERE ${where}`,
    );
  }
  if (storeExists) {
    branches.push(`SELECT ${COLUMNS} FROM hot.sample WHERE ${where}`);
  }
  if (branches.length === 0) {
    return { rows: [], truncated: false, treeFiles: 0 };
  }

  const limit = DEFAULT_ROW_LIMIT;
  const answer = await execute(
    context,
    { text: snapshotSQL(branches.join(" UNION ALL "), limit + 1), params },
    { attach: storeExists, limit },
  );
  return { ...answer, treeFiles: files.length };
}

/**
 * Whether any key's newest rolled row is newer than the instant asked for.
 *
 * One row is enough to answer it, and the sidecar is a single file of one row
 * per key — 11 kB on the device that has been recording longest.
 *
 * A sidecar that cannot be read counts as "yes". It is the same answer a
 * missing one gets, for the same reason: the alternative is a snapshot that
 * quietly omits every path which stopped reporting before the last roll.
 */
async function sidecarHasRowsAfter(
  context: ReadContext,
  sidecar: string,
  request: Extract<QueryRequest, { kind: "snapshot" }>,
): Promise<boolean> {
  const params: Record<string, DuckDBValue> = {
    at: BigInt(Math.trunc(request.at)),
  };
  const filters = ["ts > $at", ...pathFilter(request.paths, params)];
  try {
    const result = await context.connection.runAndReadAll(
      `SELECT count(*) FROM (SELECT 1 FROM ` +
        `read_parquet('${sqlLiteral(sidecar)}', union_by_name = true) ` +
        `WHERE ${filters.join(" AND ")} LIMIT 1)`,
      params,
    );
    return Number(result.getRowsJS()[0][0]) > 0;
  } catch {
    return true;
  }
}

/**
 * One row per `(context, path)`, carrying the newest sample at or before T.
 *
 * `arg_max` over one struct rather than one per column: two of them tie-break
 * independently when two sources record in the same millisecond, and would
 * return a value and a source that never occurred together. It is also a hash
 * aggregate over the key count — hundreds of groups, whatever the row count —
 * where `DISTINCT ON` sorts the input.
 *
 * One row per key and not per source, which is what the sibling provider's
 * `LATEST ON ts PARTITION BY path, context` returns and what the snapshot the
 * server assembles from it can hold.
 */
function snapshotSQL(union: string, limit: number): string {
  const packed = [
    "ts := ts",
    "source := source",
    "value_kind := value_kind",
    "value_num := value_num",
    "value_str := value_str",
    "value_lat := value_lat",
    "value_lon := value_lon",
  ].join(", ");
  const newest =
    `SELECT context, path, arg_max(struct_pack(${packed}), ts) AS newest ` +
    `FROM (${union}) GROUP BY context, path`;
  // Projected back into RANGE_COLUMNS order, which is what the caller decodes.
  const columns = RANGE_COLUMNS.map((name) =>
    name === "context" || name === "path" ? name : `newest.${name} AS ${name}`,
  ).join(", ");
  return `SELECT ${columns} FROM (${newest}) ORDER BY context, path LIMIT ${limit}`;
}

/** The UTC midnight the instant falls in. */
function dayStart(at: number): number {
  return Math.floor(at / DAY_MS) * DAY_MS;
}

/**
 * Bind the requested paths, if any.
 *
 * A list rather than composed text: these come from an HTTP request, and they
 * are the only strings in here that do.
 */
function pathFilter(
  paths: string[] | undefined,
  params: Record<string, DuckDBValue>,
): string[] {
  const wanted = [...new Set(paths ?? [])];
  if (wanted.length === 0) return [];
  params.paths = listValue(wanted);
  return ["list_contains($paths, path)"];
}

/**
 * Which files answer this range, and which of the store's rows they already
 * hold.
 *
 * **The order of the two reads matters.** Files are listed first and the
 * pending-roll record second, and the overlap is anchored on the file list
 * rather than on the record. A record that names a roll whose file has not
 * appeared yet therefore subtracts nothing — the tree does not hold those rows
 * yet, and subtracting them would turn a duplicate into a gap. The reverse
 * race is harmless: a record cleared after the listing means the store's rows
 * are already gone.
 */
function planSources(dataDir: string, request: WindowedRequest): Plan {
  const files = treeFilesInRange(dataDir, request.from, request.to);
  const oldest = hotStoreOldest(dataDir);
  return {
    files,
    overlap: rolledOverlap(dataDir, files),
    readStore: oldest !== undefined && needsHotStore(oldest, request.to),
  };
}

/**
 * The tree's files whose date directory intersects `[from, to)`.
 *
 * A directory glob and a timestamp filter is all the pruning this layout
 * offers — nothing in the tree prunes on path — so this is where a long range
 * is turned into the smallest set of files that can hold it.
 *
 * `.tmp` is excluded by construction: a roll writes under that suffix and
 * renames, so a file killed mid-write does not end in `.parquet` and is not
 * matched here.
 */
export function treeFilesInRange(
  dataDir: string,
  from: number,
  to: number,
): TreeFile[] {
  const root = treeRoot(dataDir);
  let dates: string[];
  try {
    dates = readdirSync(root);
  } catch {
    // No tree yet. A device that has not rolled once is the normal case for
    // the first hour of its life.
    return [];
  }

  const found: TreeFile[] = [];
  for (const entry of dates.sort()) {
    const day = dateDirectoryStart(entry);
    if (day === null) continue;
    if (day >= to || day + DAY_MS <= from) continue;
    const directory = join(root, entry);
    let names: string[];
    try {
      names = readdirSync(directory);
    } catch {
      continue; // Removed between the two reads, by expiry or by hand.
    }
    // A compacted file supersedes the rolls it folded in, and the merge
    // unlinks those afterwards rather than atomically with its own rename.
    // Reading both would answer every row in the hour twice.
    for (const name of liveTreeFiles(names).sort()) {
      if (!name.endsWith(".parquet")) continue;
      found.push({ day, name, path: join(directory, name) });
    }
  }
  return found;
}

/**
 * What a roll has written to the tree and not yet removed from the store.
 *
 * Both phases of the record mean the same thing to a reader: some of the
 * covered rows may be in the tree already. Which ones is decided by which
 * files exist, not by the phase, so a `rolling` record that got one date
 * written and died before the next is handled by the same rule as a `written`
 * one that finished them all.
 */
function rolledOverlap(dataDir: string, files: TreeFile[]): Overlap | null {
  const pending = readPendingRoll(dataDir);
  if (pending === null) return null;
  const wanted = `${pending.rollId}.parquet`;
  const dayStarts = [
    ...new Set(
      files.filter((file) => file.name === wanted).map((file) => file.day),
    ),
  ];
  if (dayStarts.length === 0) return null;
  return { maxRowid: pending.maxRowid, dayStarts };
}

interface Compiled {
  text: string;
  params: Record<string, DuckDBValue>;
}

type WindowedRequest = Exclude<QueryRequest, { kind: "snapshot" }>;

function compile(request: WindowedRequest, plan: Plan): Compiled | null {
  const params: Record<string, DuckDBValue> = {
    fromMs: BigInt(Math.trunc(request.from)),
    toMs: BigInt(Math.trunc(request.to)),
  };
  const filters = ["ts >= $fromMs", "ts < $toMs"];
  const context = requestedContext(request);
  if (context !== undefined) {
    params.context = context;
    filters.push("context = $context");
  }
  filters.push(...pathFilter(requestedPaths(request), params));
  const where = filters.join(" AND ");

  const branches: string[] = [];
  if (plan.files.length > 0) {
    const list = plan.files
      .map((file) => `'${sqlLiteral(file.path)}'`)
      .join(", ");
    // `union_by_name`: a file written by a build with a different column set
    // is read with the missing columns as NULL rather than failing the query.
    branches.push(
      `SELECT ${COLUMNS} FROM read_parquet([${list}], union_by_name = true) WHERE ${where}`,
    );
  }
  if (plan.readStore) {
    branches.push(
      `SELECT ${COLUMNS} FROM hot.sample WHERE ${where}${seam(plan.overlap, params)}`,
    );
  }
  if (branches.length === 0) return null;

  const union =
    branches.length === 1 ? branches[0] : branches.join(" UNION ALL ");
  if (request.kind === "range") {
    return {
      // One over the limit, so the caller can be told the answer is partial.
      text: `SELECT ${COLUMNS} FROM (${union}) ORDER BY ts LIMIT ${rowLimit(request) + 1}`,
      params,
    };
  }
  if (request.kind === "values") {
    return compileValues(request, union, params);
  }
  if (request.kind === "exists") {
    // Deliberately unordered, and the `LIMIT 1` is the whole point: the caller
    // asks this for a range that may be the entire tree, and the engine stops
    // at the first row rather than scanning it to sort or to count.
    return {
      text: `SELECT count(*) FROM (SELECT 1 FROM (${union}) LIMIT 1)`,
      params,
    };
  }
  const column = request.kind === "paths" ? "path" : "context";
  return {
    text: `SELECT DISTINCT ${column} FROM (${union}) ORDER BY ${column}`,
    params,
  };
}

/** The one context a request restricts itself to, if it names one. */
function requestedContext(request: WindowedRequest): string | undefined {
  if (request.kind === "contexts" || request.kind === "exists")
    return undefined;
  return request.context;
}

/** Every path a request needs, so the scan is pruned to those and no more. */
function requestedPaths(request: WindowedRequest): string[] {
  if (request.kind === "range") return request.paths ?? [];
  if (request.kind === "values") {
    return [...new Set(request.specs.map((spec) => spec.path))];
  }
  return [];
}

/**
 * Every series a history request asked for, as one statement.
 *
 * The source is read once — `MATERIALIZED`, because DuckDB inlines a CTE by
 * default and would otherwise scan the tree once per series — and each series
 * is a branch over it. The sibling provider issues a query per series and a
 * second one for any series that turns out to be non-numeric; both are free
 * against a running server and neither is free here.
 *
 * **Every value column comes back on every row.** A path has one kind, so at
 * most one of them is set, and which one is not known until the rows are read:
 * asking for all four costs one pass and removes the fallback query entirely.
 *
 * Buckets are only the ones that hold something. The caller lays out the full
 * timeline and fills the gaps, which is the same answer the sibling provider's
 * `FILL(NULL)` produces without fabricating rows to send down a pipe.
 */
function compileValues(
  request: Extract<QueryRequest, { kind: "values" }>,
  source: string,
  params: Record<string, DuckDBValue>,
): Compiled {
  const bucketMs = Math.max(1, Math.trunc(request.bucketMs ?? 0));
  const bucketed = (request.bucketMs ?? 0) > 0;
  // Exact, unlike the range path's limit: nothing reports a truncated series,
  // so a row over the ceiling would only be a row over the ceiling.
  const branchLimit = perSpecLimit(request);

  const branches = request.specs.map((spec, index) => {
    const filters = [`path = $p${index}`];
    params[`p${index}`] = spec.path;
    if (spec.sourceRef !== undefined) {
      params[`s${index}`] = spec.sourceRef;
      filters.push(`source = $s${index}`);
    }
    const where = filters.join(" AND ");
    // One `arg_min`/`arg_max` over a struct, never one per axis: two of them
    // tie-break independently when two sources record in the same
    // millisecond, and can return a latitude and a longitude the vessel was
    // never at simultaneously — which is the same fabrication the per-axis
    // aggregates are refused for.
    const position = `struct_pack(lat := value_lat, lon := value_lon)`;
    if (!bucketed || spec.aggregate === "raw") {
      return (
        `(SELECT ${index} AS spec, ts AS bucket, value_num AS num, ` +
        `value_str AS str, value_kind AS kind, ${position} AS pos ` +
        `FROM src WHERE ${where} ORDER BY ts LIMIT ${branchLimit})`
      );
    }
    // `first` and `last` are `arg_min`/`arg_max` over `ts`, because DuckDB's
    // own `first()` and `last()` are undefined within a group.
    const pick = spec.aggregate === "last" ? "arg_max" : "arg_min";
    return (
      `SELECT ${index} AS spec, ` +
      `CAST(floor(ts / ${bucketMs}.0) AS BIGINT) * ${bucketMs} AS bucket, ` +
      `${numericAggregate(spec.aggregate)} AS num, ` +
      // Text is never averaged: a bucket takes the value in force at its end,
      // which is what a state channel means, and the kind travels with it so
      // a boolean is replayed as a boolean rather than as "true".
      `arg_max(value_str, ts) AS str, arg_max(value_kind, ts) AS kind, ` +
      `${pick}(${position}, ts) AS pos ` +
      `FROM src WHERE ${where} GROUP BY 1, 2`
    );
  });

  return {
    text:
      `WITH src AS MATERIALIZED (${source}) ` +
      `SELECT ${VALUES_PROJECTION} ` +
      `FROM (${branches.join(" UNION ALL ")}) ORDER BY bucket, spec`,
    params,
  };
}

/** The bucket reduction for a numeric series. */
function numericAggregate(aggregate: ValueAggregate): string {
  switch (aggregate) {
    case "min":
      return "min(value_num)";
    case "max":
      return "max(value_num)";
    case "mid":
      return "(min(value_num) + max(value_num)) / 2";
    case "first":
      return "arg_min(value_num, ts)";
    case "last":
      return "arg_max(value_num, ts)";
    default:
      return "avg(value_num)";
  }
}

/**
 * The predicate that keeps a rolled row from being counted twice.
 *
 * Read it as: a row is the store's alone unless the roll covered it *and* the
 * file for its own day is on disk. `rowid` is the same bound the roll used, so
 * the two sets are identical by construction rather than by matching
 * timestamps — a row that arrived after the roll read its bound has a higher
 * rowid and is kept, which is exactly why the roll works on rowids.
 */
function seam(
  overlap: Overlap | null,
  params: Record<string, DuckDBValue>,
): string {
  if (overlap === null) return "";
  params.maxRowid = BigInt(overlap.maxRowid);
  const days = overlap.dayStarts
    .map((day) => `(ts >= ${day} AND ts < ${day + DAY_MS})`)
    .join(" OR ");
  return ` AND (rowid > $maxRowid OR NOT (${days}))`;
}

/** The cap on what one answer returns, whatever it is made of. */
function rowLimit(request: WindowedRequest): number {
  if (request.kind !== "range") return DEFAULT_ROW_LIMIT;
  return clampLimit(request.limit);
}

/**
 * The cap on what one raw branch returns.
 *
 * Separate from `rowLimit`, and deliberately: that one bounds the answer, and
 * this one bounds a series inside it. A values request sends the ceiling its
 * caller reduces a raw series under, which is smaller than the answer's — and
 * applying it to the answer instead would drop whole trailing series rather
 * than shortening each, because the rows arrive ordered by bucket and spec.
 */
function perSpecLimit(request: Extract<QueryRequest, { kind: "values" }>) {
  return clampLimit(request.limit);
}

function clampLimit(asked: number | undefined): number {
  if (asked === undefined) return DEFAULT_ROW_LIMIT;
  return Math.max(1, Math.min(Math.floor(asked), DEFAULT_ROW_LIMIT));
}

/** The kinds a request may name. Anything else is not a query. */
const KINDS = [
  "range",
  "values",
  "paths",
  "contexts",
  "exists",
  "snapshot",
] as const;

/** The bucket reductions a spec may name. */
const AGGREGATES: readonly ValueAggregate[] = [
  "average",
  "min",
  "max",
  "first",
  "last",
  "mid",
  "raw",
];

/**
 * Check the request at the boundary it crosses.
 *
 * It arrives as JSON from another process, which built it from an HTTP
 * request, and the type it is cast to on arrival is a claim rather than a
 * check. Everything user-supplied is bound rather than composed, so this is
 * about shape rather than about injection — but a shape nobody checked still
 * reaches SQL: an unrecognised `kind` compiled to the contexts query and
 * answered it, and a `limit` of `"x"` compiled to `LIMIT NaN`.
 */
function validate(request: QueryRequest): void {
  if (!KINDS.includes(request.kind)) {
    throw new Error(
      `${JSON.stringify(request.kind)} is not a query kind; expected one of ${KINDS.join(", ")}`,
    );
  }
  const time = (name: "from" | "to" | "at", value: unknown) => {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new Error(`${name} must be a timestamp in milliseconds`);
    }
  };
  if (request.kind === "snapshot") {
    time("at", request.at);
    validatePaths(request.paths);
    return;
  }
  time("from", request.from);
  time("to", request.to);
  if (request.kind === "values") {
    if (typeof request.context !== "string") {
      throw new Error("context must be a string");
    }
  } else if (
    (request.kind === "range" || request.kind === "paths") &&
    request.context !== undefined &&
    typeof request.context !== "string"
  ) {
    throw new Error("context must be a string");
  }
  if (
    (request.kind === "range" || request.kind === "values") &&
    request.limit !== undefined &&
    (typeof request.limit !== "number" || !Number.isFinite(request.limit))
  ) {
    throw new Error("limit must be a number of rows");
  }
  if (request.kind === "values") {
    if (!Array.isArray(request.specs) || request.specs.length === 0) {
      throw new Error("a values request needs at least one spec");
    }
    if (request.specs.length > MAX_SPECS) {
      throw new Error(
        `a values request may name ${MAX_SPECS} series; this one names ` +
          `${request.specs.length}`,
      );
    }
    for (const spec of request.specs) {
      if (typeof spec?.path !== "string") {
        throw new Error("every spec needs a path");
      }
      if (!AGGREGATES.includes(spec.aggregate)) {
        throw new Error(
          `${JSON.stringify(spec.aggregate)} is not an aggregate; expected one of ${AGGREGATES.join(", ")}`,
        );
      }
      if (spec.sourceRef !== undefined && typeof spec.sourceRef !== "string") {
        throw new Error("sourceRef must be a string");
      }
    }
    if (
      request.bucketMs !== undefined &&
      (typeof request.bucketMs !== "number" ||
        !Number.isFinite(request.bucketMs))
    ) {
      throw new Error("bucketMs must be a number of milliseconds");
    }
    return;
  }
  if (request.kind !== "range") return;
  validatePaths(request.paths);
}

function validatePaths(paths: string[] | undefined): void {
  if (paths === undefined) return;
  if (!Array.isArray(paths) || paths.some((path) => typeof path !== "string")) {
    throw new Error("paths must be an array of strings");
  }
}
