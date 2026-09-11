import { QueryRunner, RANGE_COLUMNS } from "./query/duck.js";

/**
 * The history v1 WebSocket surface: playback, and the snapshot the REST API
 * builds a full tree from.
 *
 * Three methods, not one. `signalk-server/src/interfaces/ws.ts` ends the spark
 * with "No data found" when `hasAnyData` answers false — so that method decides
 * whether playback happens at all — and `src/interfaces/rest.js` feeds
 * `getHistory`'s deltas to `buildFullFromDeltas` to serve
 * `/signalk/v1/snapshot/`.
 *
 * The chunk/resume machine, the `(ts, context, source)` grouping and the
 * vessel-name replay shape are copied from
 * `signalk-questdb-history-provider/src/history-v1.ts`: they are contract
 * behaviour rather than storage, and a client replaying against one provider
 * has to see what it sees against the other. Only the queries are this
 * package's own — and there are fewer of them, because this store keeps typed
 * columns where the sibling unions three tables into a text column and parses
 * it back.
 *
 * **Playback runs on the one query service** (`query/duck.ts`), not on a
 * process of its own. Every chunk is an ordinary query, so a session holds no
 * engine between chunks and holds at most one outstanding request at any
 * moment: the queue can only be as deep as there are clients. Past
 * `MAX_QUEUED_QUERIES` a request is refused, which reaches a session as an
 * error and becomes the same backoff as any other failure. That is the whole of
 * the admission control, deliberately — the v1 interface gives a provider no
 * channel to refuse a session on, and a cap enforced through `hasAnyData` would
 * tell the client its vessel has no history.
 */

/** How much wall time one read covers, at playback rate 1. */
export const CHUNK_SECONDS = 60;

/**
 * Rows one read returns. A window holding more is drained across several reads
 * rather than truncated — see the resume rule below.
 */
const CHUNK_ROW_LIMIT = 10000;

/**
 * How long a completed but empty window pauses before the next one is read.
 *
 * Short, because there is nothing to pace: a gap in the recording should not
 * replay as a gap in wall time. It is only ever paid on windows that are over,
 * so it cannot become a poll of the present.
 */
const EMPTY_WINDOW_MS = 100;

/**
 * The upper bound of the "is there anything to play back" range.
 *
 * `hasAnyData` asks about everything from the start time onwards, and the query
 * layer takes a half-open range in milliseconds. A clock-forward device would
 * hide its own recordings behind a `Date.now()` bound.
 */
const FOREVER = Number.MAX_SAFE_INTEGER;

interface HistoryOptions {
  startTime: Date;
  playbackRate: number;
  subscribe?: string;
}

interface Spark {
  write: (data: unknown) => void;
  on: (event: string, cb: (...args: unknown[]) => void) => void;
}

interface Delta {
  context: string;
  updates: {
    timestamp: string;
    $source?: string;
    values: { path: string; value: unknown }[];
  }[];
}

/** Where each value lives in a row, from the order the query layer declares. */
const AT = Object.fromEntries(
  RANGE_COLUMNS.map((name, index) => [name, index]),
) as Record<(typeof RANGE_COLUMNS)[number], number>;

interface Row {
  ts: number;
  context: string;
  path: string;
  source: string | null;
  kind: string | null;
  num: number | null;
  str: string | null;
  lat: number | null;
  lon: number | null;
}

function toRow(row: unknown[]): Row {
  return {
    ts: row[AT.ts] as number,
    context: (row[AT.context] as string | null) ?? "self",
    path: (row[AT.path] as string | null) ?? "",
    source: (row[AT.source] as string | null) ?? null,
    kind: (row[AT.value_kind] as string | null) ?? null,
    num: (row[AT.value_num] as number | null) ?? null,
    str: (row[AT.value_str] as string | null) ?? null,
    lat: (row[AT.value_lat] as number | null) ?? null,
    lon: (row[AT.value_lon] as number | null) ?? null,
  };
}

/**
 * The value a delta carried, from the columns that hold it.
 *
 * The kind decides, never the shape of what is stored: a path whose value
 * genuinely is the word "true" is a string and replays as one, because its rows
 * are not tagged `boolean`. The sibling provider parses this back out of a text
 * column and needs the same tag for the same reason.
 */
function decodeValue(row: Row): unknown {
  switch (row.kind) {
    case "number":
      return row.num;
    case "boolean":
      return row.str === null ? null : row.str === "true";
    case "position":
      return row.lat !== null && row.lon !== null
        ? { latitude: row.lat, longitude: row.lon }
        : null;
    default:
      return row.str;
  }
}

/**
 * Rows into the deltas they were recorded from.
 *
 * Grouped by `(ts, context, source)`: mixing two sources' rows into one update
 * would force a single `$source` label onto both, so each source gets its own
 * update — exactly how the live deltas arrived. Rows with no recorded source
 * group together and replay without a `$source`.
 */
function groupRowsIntoDeltas(rows: unknown[][]): Delta[] {
  const byTimestamp = new Map<
    string,
    Map<
      string,
      {
        context: string;
        source?: string;
        values: { path: string; value: unknown }[];
      }
    >
  >();

  for (const raw of rows) {
    const row = toRow(raw);
    const timestamp = new Date(row.ts).toISOString();
    const source = row.source ?? undefined;
    const value = decodeValue(row);

    let byGroup = byTimestamp.get(timestamp);
    if (byGroup === undefined) {
      byGroup = new Map();
      byTimestamp.set(timestamp, byGroup);
    }
    // A NUL byte can appear in neither a context nor a sourceRef, so the
    // composite key is unambiguous.
    const groupKey =
      source === undefined ? row.context : `${row.context}\u0000${source}`;
    let group = byGroup.get(groupKey);
    if (group === undefined) {
      group = { context: row.context, source, values: [] };
      byGroup.set(groupKey, group);
    }
    // Vessel identity is recorded under the synthetic path "name" tagged
    // `identity`, because it arrives as an empty-path object delta —
    // `{path: "", value: {name}}` is the only shape consumers (Freeboard) read
    // names from. Replay it as it arrived. The kind gate keeps a data path
    // literally named "name" replaying as the plain string it is.
    group.values.push(
      row.path === "name" &&
        row.kind === "identity" &&
        typeof value === "string"
        ? { path: "", value: { name: value } }
        : { path: row.path, value },
    );
  }

  const deltas: Delta[] = [];
  for (const [timestamp, byGroup] of byTimestamp) {
    for (const { context, source, values } of byGroup.values()) {
      deltas.push({
        context,
        updates: [
          source === undefined
            ? { timestamp, values }
            : { timestamp, $source: source, values },
        ],
      });
    }
  }
  return deltas;
}

export function createHistoryProviderV1(
  runner: QueryRunner,
  selfContext: string,
  debug: (msg: string) => void,
) {
  /**
   * The own vessel is recorded as the literal "self"; the wire calls it
   * `vessels.<id>`. A delta labelled "self" is not merely unlabelled — the
   * snapshot route maps `self` in the URL to the vessel's id before it walks
   * the tree `buildFullFromDeltas` assembles, so it would find nothing there.
   */
  const onTheWire = (context: string): string =>
    context === "self" ? selfContext : context;

  function hasAnyData(
    options: HistoryOptions,
    callback: (hasResults: boolean) => void,
  ): void {
    const from = options.startTime.getTime();
    if (!Number.isFinite(from)) {
      // `new Date(spark.query.startTime)` is whatever the client sent.
      callback(false);
      return;
    }
    runner
      .run({ kind: "exists", from, to: FOREVER })
      .then((answer) => {
        callback(Number(answer.rows[0]?.[0] ?? 0) > 0);
      })
      .catch((err: unknown) => {
        debug(`hasAnyData failed: ${describe(err)}`);
        callback(false);
      });
  }

  /**
   * Last-known name per context AT the playback start.
   *
   * Names are static: a vessel's identity report repeats on its own cadence
   * (AIS Class A static data every ~6 minutes) and the recorder writes only
   * changes, so a playback window almost never contains a name row — the name
   * was written when the vessel was first seen, possibly days ago. Bounding at
   * the start keeps a LATER rename out of a historical replay; a rename inside
   * the window still plays back as a row.
   *
   * A snapshot restricted to one path, so a playback of the present answers it
   * from the sidecar without reading the tree at all. Errors degrade to an
   * empty map: playback proceeds unlabelled rather than not at all.
   */
  async function fetchLatestNames(at: number): Promise<Map<string, string>> {
    try {
      const answer = await runner.run({
        kind: "snapshot",
        at,
        paths: ["name"],
      });
      const names = new Map<string, string>();
      for (const raw of answer.rows) {
        const row = toRow(raw);
        // The kind gate again: a data path literally named "name" is not an
        // identity, and labelling a vessel with its depth reading is worse than
        // not labelling it.
        if (row.kind !== "identity" || row.str === null || row.str === "") {
          continue;
        }
        names.set(row.context, row.str);
      }
      return names;
    } catch (err) {
      debug(`vessel names for playback are unavailable: ${describe(err)}`);
      return new Map();
    }
  }

  function streamHistory(
    spark: Spark,
    options: HistoryOptions,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    onChange: () => void,
  ): () => void {
    let stopped = false;
    // The pending inter-chunk timer. Tracked so stopping actually CANCELS it:
    // clearing `stopped` alone leaves a scheduled callback holding the event
    // loop open for up to CHUNK_SECONDS.
    let chunkTimer: ReturnType<typeof setTimeout> | null = null;
    const scheduleChunk = (delayMs: number) => {
      if (stopped) return;
      chunkTimer = setTimeout(() => {
        chunkTimer = null;
        void streamChunk();
      }, delayMs);
      // Never let a playback timer be the only thing keeping the process
      // alive — the server owns the lifecycle, not this stream.
      chunkTimer.unref?.();
    };

    const startTime = options.startTime.getTime();
    // The same guard `hasAnyData` applies, because the failure without it is
    // not a bad answer: every read would be refused by the query layer, and
    // every refusal reschedules, so a start time that is not a time becomes a
    // query a second for as long as the client stays connected.
    if (!Number.isFinite(startTime)) {
      debug(`streamHistory: ${String(options.startTime)} is not a start time`);
      return () => {};
    }
    // `spark.query.playbackRate || 1` is what the server passes, and a query
    // string is text: `?playbackRate=fast` arrives here as a string that
    // `Math.max` alone turns into NaN, and `setTimeout(fn, NaN)` fires
    // immediately. That is a replay with no delay between windows — the whole
    // recording read back as fast as the query service can answer.
    const asked = Number(options.playbackRate);
    const playbackRate = Number.isFinite(asked) ? Math.max(1, asked) : 1;

    let latestNames: Map<string, string> | null = null;
    const namedContexts = new Set<string>();
    let currentTime = startTime;

    async function streamChunk(): Promise<void> {
      if (stopped) return;
      const chunkEnd = currentTime + CHUNK_SECONDS * 1000;

      // A window that has not happened yet is not read at all. Reading it is
      // what turns a replay that has caught up with real time into a permanent
      // poll: it comes back empty because it is in the future, the cursor
      // advances past it anyway, and the next one is further into the future
      // still — a query every 100 ms per connected client, for ever, against
      // the one service every other history request shares. Waiting for the
      // window costs the client the same 60 seconds the replay's own cadence
      // costs it, and the window is read once, complete.
      const untilComplete = chunkEnd - Date.now();
      if (untilComplete > 0) {
        scheduleChunk(untilComplete);
        return;
      }

      try {
        const answer = await runner.run({
          kind: "range",
          from: currentTime,
          to: chunkEnd,
          // No context: playback replays the whole vessel picture, every AIS
          // target included, which is what a client rewinding the plotter sees.
          limit: CHUNK_ROW_LIMIT,
        });
        if (stopped) return;

        if (answer.rows.length === 0) {
          // A window that is over and holds nothing — the vessel was off, or
          // the replay started before recording did. Skipped quickly rather
          // than in wall time, because there is nothing to pace.
          currentTime = chunkEnd;
          scheduleChunk(EMPTY_WINDOW_MS);
          return;
        }

        const deltas = groupRowsIntoDeltas(answer.rows);
        if (deltas.length > 0 && latestNames === null) {
          latestNames = await fetchLatestNames(startTime);
          if (stopped) return;
        }

        for (const delta of deltas) {
          if (stopped) return;
          const context = onTheWire(delta.context);
          const name = latestNames?.get(delta.context);
          if (name !== undefined && !namedContexts.has(delta.context)) {
            namedContexts.add(delta.context);
            // Ahead of the context's first delta, so a consumer can label the
            // target the moment it appears.
            spark.write({
              context,
              updates: [
                {
                  timestamp: delta.updates[0].timestamp,
                  values: [{ path: "", value: { name } }],
                },
              ],
            });
          }
          spark.write({ ...delta, context });
        }

        // A busy interval can hold more rows than one read returns — a live
        // install already reaches ~6k rows in 60 s. Advancing to chunkEnd after
        // a truncated read would skip the remainder silently, so drain the
        // window before moving on.
        //
        // Resume AT the last sent row's timestamp, not past it: a single
        // instrument update commonly stamps several paths within the same
        // millisecond, and stepping past it would drop the siblings that did
        // not fit in this page. Re-reading that millisecond can re-send rows
        // already delivered, which is harmless on replay — losing them is not.
        //
        // Only when the whole page shared currentTime's millisecond (so
        // resuming at it would repeat the identical read forever) does the
        // cursor step forward by 1 ms, trading that millisecond's tail for
        // guaranteed progress.
        if (answer.truncated) {
          const lastTs = toRow(answer.rows[answer.rows.length - 1]).ts;
          const resumeAt = lastTs > currentTime ? lastTs : currentTime + 1;
          currentTime = Math.min(resumeAt, chunkEnd);
          scheduleChunk(0);
          return;
        }

        currentTime = chunkEnd;
        scheduleChunk((CHUNK_SECONDS * 1000) / playbackRate);
      } catch (err) {
        // Every failure lands here, including a refusal from a full query
        // queue, and every one of them is retried on the same window: the
        // client asked to replay from a point, and skipping the window it
        // could not read would hand it a silent gap instead of a late one.
        debug(`streamHistory error: ${describe(err)}`);
        scheduleChunk(1000);
      }
    }

    void streamChunk();

    // Cancel the pending chunk as well as setting the flag: the flag only
    // stops the NEXT scheduling decision, while an already-scheduled timer
    // keeps the event loop alive until it fires.
    const stop = () => {
      stopped = true;
      if (chunkTimer !== null) {
        clearTimeout(chunkTimer);
        chunkTimer = null;
      }
    };
    spark.on("end", stop);
    return stop;
  }

  /**
   * Every path's last value at `date`, for the v1 snapshot API.
   *
   * `path` is deliberately unused and must NOT become a filter. Despite the
   * name — and the `string` in the server's provider interface — the server
   * passes the REQUEST URL SEGMENTS, e.g. `["vessels", "<selfId>",
   * "navigation"]`, then feeds every returned delta to `buildFullFromDeltas()`
   * and walks into the assembled tree with those segments
   * (`signalk-server/src/interfaces/rest.js`). Filtering the query by it would
   * both mismatch the type and starve the snapshot the caller is building.
   *
   * What the answer covers is bounded — `SNAPSHOT_SCAN_DAYS` in
   * `query/reader.ts` states the rule — and the bound only bites for an instant
   * the tree has already rolled past. A snapshot of the present is exact over
   * all of history.
   */
  function getHistory(
    date: Date,

    path: string,
    callback: (deltas: Delta[]) => void,
  ): void {
    const at = date.getTime();
    if (!Number.isFinite(at)) {
      callback([]);
      return;
    }
    runner
      .run({ kind: "snapshot", at })
      .then((answer) => {
        callback(
          groupRowsIntoDeltas(answer.rows).map((delta) => ({
            ...delta,
            context: onTheWire(delta.context),
          })),
        );
      })
      .catch((err: unknown) => {
        debug(`getHistory error: ${describe(err)}`);
        // An empty answer is a 404 from the snapshot route. The alternative is
        // a 500 the route has no branch for.
        callback([]);
      });
  }

  return { hasAnyData, streamHistory, getHistory };
}

function describe(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
