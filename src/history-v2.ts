import type { Context, Path, SourceRef, Timestamp } from "@signalk/server-api";
import type {
  AggregateMethod,
  ContextsRequest,
  HistoryApi,
  PathSpec,
  PathsRequest,
  ValuesRequest,
  ValuesResponse,
} from "@signalk/server-api/history";
import { fieldKey, setField } from "./pointer.js";
import type { ObjectValue } from "./pointer.js";
import { DEFAULT_ROW_LIMIT, QueryRunner, VALUE_COLUMNS } from "./query/duck.js";
import type { ObjectField, ValueAggregate, ValueSpec } from "./query/duck.js";
import { resolveTimeRange } from "./time-range.js";

/**
 * The history v2 REST surface.
 *
 * Most of this is contract behaviour rather than storage: `computeSMA`,
 * `computeEMA`, `normalizeContext`, the timestamp union and the per-spec
 * column assembly are copied from `signalk-questdb-history-provider` and must
 * keep answering the way it does — a chart drawn against one provider has to
 * look the same drawn against the other. Only the query construction is this
 * package's own. Fix contract bugs in both.
 *
 * **One request compiles to one statement.** The sibling issues a query per
 * pathSpec and a second for any path that turns out to be non-numeric, which
 * is free against a running server and is not free here. Its three tables are
 * one table with a `value_kind` column, so the fallback has nowhere to go: the
 * numeric reduction, the text one and the position one all come back on the
 * same row and the decoder picks.
 */

/**
 * Buckets one request may lay out, counted before anything is queried.
 *
 * The response is a matrix: a row per bucket, a column per series. A caller
 * asking weeks at one-second resolution would build millions of rows in the
 * server's heap and stream them to a client that budgeted a few thousand
 * points. The sibling provider carries the same ceiling for the same reason,
 * and its comment records what it costs a Pi-class host to omit it.
 */
export const MAX_SAMPLE_BUCKETS = 1_000_000;

/** Rows one raw series may return, for the aggregates computed here. */
const RAW_ROW_LIMIT = 50_000;

/**
 * The bucket width a resolution asks for, in whole seconds.
 *
 * Clamped up to one second, as in the sibling provider. This storage is
 * millisecond-resolution and could honour 0.5 s, but a caller asking for it
 * would then get twice the points from one provider and not the other, and
 * the two rendering the same chart is what this unit is judged on.
 */
export function effectiveResolution(resolution: number): number {
  return Math.max(1, Math.floor(resolution));
}

/** Which aggregates this side computes, because a bucket cannot. */
function needsClientSideAggregation(method: string): boolean {
  return method === "middle_index" || method === "sma" || method === "ema";
}

/** The aggregates the engine can reduce a bucket with. */
const SQL_AGGREGATES: readonly string[] = [
  "average",
  "min",
  "max",
  "first",
  "last",
  "mid",
];

/**
 * An aggregate name as the query layer spells it.
 *
 * Resolved through a fixed set, never interpolated: an aggregate reaching the
 * statement as text would reach `read_parquet` and `COPY`, which on this
 * storage is local file read and write rather than a database's own tables.
 * An unknown name takes the same default the sibling provider takes.
 */
function toQueryAggregate(method: AggregateMethod): ValueAggregate {
  if (needsClientSideAggregation(method)) return "raw";
  return SQL_AGGREGATES.includes(method)
    ? (method as ValueAggregate)
    : "average";
}

/**
 * The window and the smoothing factor a client-side aggregate is given.
 *
 * The API defines `parameter` as strings, and the value reaches here from a
 * query string. Anything the whole string does not spell takes the documented
 * default rather than refusing the request, which is what the sibling provider
 * does for an absent one — the alternative is a series of nulls the caller
 * cannot tell from a gap, because `NaN` serialises as `null`.
 *
 * `Number` rather than `parseInt` or `parseFloat`, which read a leading number
 * and discard the rest: `"2x"` is not a window of 2 and `"0.9x"` is not an
 * alpha of 0.9, and reading them as such honours a parameter nobody wrote.
 */
const SMA_WINDOW = 5;
const EMA_ALPHA = 0.2;

function smaWindow(parameter: string | undefined): number {
  const n = Number(parameter);
  return Number.isInteger(n) && n >= 1 ? n : SMA_WINDOW;
}

function emaAlpha(parameter: string | undefined): number {
  const alpha = Number(parameter);
  return alpha > 0 && alpha <= 1 ? alpha : EMA_ALPHA;
}

function computeSMA(values: (number | null)[], n: number): (number | null)[] {
  const result: (number | null)[] = [];
  const window: number[] = [];
  for (const v of values) {
    if (v === null) {
      result.push(null);
      continue;
    }
    window.push(v);
    if (window.length > n) window.shift();
    result.push(window.reduce((a, b) => a + b, 0) / window.length);
  }
  return result;
}

function computeEMA(
  values: (number | null)[],
  alpha: number,
): (number | null)[] {
  const result: (number | null)[] = [];
  let prev: number | null = null;
  for (const v of values) {
    if (v === null) {
      result.push(prev);
      continue;
    }
    if (prev === null) {
      prev = v;
    } else {
      prev = alpha * v + (1 - alpha) * prev;
    }
    result.push(prev);
  }
  return result;
}

/**
 * Map a Signal K context value to the storage form.
 *
 * Per the v2 History API spec, callers may send the context as `vessels.self`
 * or fully qualified. The own vessel is stored as the literal string "self"
 * for compactness, so anything referring to it normalizes to that.
 */
function normalizeContext(context: string, selfContext: string): string {
  if (
    context === "self" ||
    context === "vessels.self" ||
    context === selfContext
  ) {
    return "self";
  }
  return context;
}

/**
 * Whether an object path may be read with this aggregate.
 *
 * An aggregate on an object applies to the object as a whole, and a per-field
 * average has no general meaning — an angle, a vector component and a
 * coordinate average differently, and nothing here says which a field is. So
 * an object takes only the aggregates that select a recorded delta, and a
 * read without a resolution, which returns every delta as recorded.
 * Smoothing is refused either way: it would average across deltas too.
 */
function appliesToObject(aggregate: string, bucketed: boolean): boolean {
  if (aggregate === "sma" || aggregate === "ema") return false;
  if (!bucketed) return true;
  return (
    aggregate === "first" ||
    aggregate === "last" ||
    aggregate === "middle_index"
  );
}

/**
 * Refuses a request whose series would lay out more than the bucket ceiling.
 *
 * Counted on the range the caller asked for rather than on what comes back —
 * a budget that only refuses after the work is done is not a budget. Only
 * specs that bucket contribute: a client-side aggregate reads raw rows under
 * their own limit.
 */
function guardBuckets(
  specs: PathSpec[],
  resolution: number,
  bucketSeconds: number,
  rangeMs: number,
): void {
  const bucketedSpecs = specs.filter(
    (spec) => !needsClientSideAggregation(spec.aggregate),
  ).length;
  if (bucketSeconds <= 0 || bucketedSpecs === 0) return;
  const buckets = Math.ceil(rangeMs / 1000 / bucketSeconds) * bucketedSpecs;
  if (buckets > MAX_SAMPLE_BUCKETS) {
    throw new Error(
      `resolution ${resolution}s over this range produces up to ` +
        `${buckets} buckets across ${bucketedSpecs} paths ` +
        `(max ${MAX_SAMPLE_BUCKETS}) — use a coarser resolution or ` +
        `a shorter range`,
    );
  }
}

/**
 * One column of the answer. `source` is set on a column `sourcePolicy: all`
 * split out: the source it reads, or null for the rows recorded without one.
 */
interface Column {
  spec: PathSpec;
  source?: string | null;
}

/** Named sources in order, the rows without a source last. */
function orderSources(sources: (string | null)[]): (string | null)[] {
  const named = sources.filter((s): s is string => s !== null).sort();
  return sources.includes(null) ? [...named, null] : named;
}

/** One row of the query layer's `values` answer. */
interface ValueRow {
  spec: number;
  bucket: number;
  num: number | null;
  str: string | null;
  kind: string | null;
  lat: number | null;
  lon: number | null;
  obj: ObjectField[] | null;
}

/** Where each value lives in a row, from the order the query layer declares. */
const AT = Object.fromEntries(
  VALUE_COLUMNS.map((name, index) => [name, index]),
) as Record<(typeof VALUE_COLUMNS)[number], number>;

function toValueRow(row: unknown[]): ValueRow {
  return {
    spec: row[AT.spec] as number,
    bucket: row[AT.bucket] as number,
    num: (row[AT.num] as number | null) ?? null,
    str: (row[AT.str] as string | null) ?? null,
    kind: (row[AT.kind] as string | null) ?? null,
    lat: (row[AT.lat] as number | null) ?? null,
    lon: (row[AT.lon] as number | null) ?? null,
    obj: (row[AT.obj] as ObjectField[] | null) ?? null,
  };
}

/**
 * What a bucket's row means, given that a path's kind is not known in advance.
 *
 * A path has one kind, so at most one of these is set. Booleans were stored as
 * the text "true"/"false" with their kind beside them, and are replayed as
 * real booleans so the same path reads the same through both API versions.
 */
function decode(row: ValueRow): unknown {
  if (row.obj !== null) return decodeObject(row.obj);
  if (row.lat !== null && row.lon !== null) {
    return { latitude: row.lat, longitude: row.lon };
  }
  return scalar(row.num, row.str, row.kind);
}

function scalar(
  num: number | null,
  str: string | null,
  kind: string | null,
): number | string | boolean | null {
  if (num !== null) return num;
  if (str === null) return null;
  return kind === "boolean" ? str === "true" : str;
}

/** The fields of one delta, keyed by name. A field with no value is omitted. */
function decodeObject(fields: ObjectField[]): ObjectValue {
  const value: ObjectValue = {};
  for (const field of fields) {
    const leaf = scalar(field.num, field.str, field.kind);
    if (leaf !== null) setField(value, fieldKey(field.key), leaf);
  }
  return value;
}

export function createHistoryV2(
  runner: QueryRunner,
  selfContext: string,
): HistoryApi {
  async function getValues(query: ValuesRequest): Promise<ValuesResponse> {
    const resolved = resolveTimeRange(query);
    const range = {
      from: resolved.from as Timestamp,
      to: resolved.to as Timestamp,
    };
    const fromMs = Date.parse(range.from);
    const toMs = Date.parse(range.to);
    const resolution = query.resolution ?? 0;
    const bucketSeconds = resolution > 0 ? effectiveResolution(resolution) : 0;

    guardBuckets(query.pathSpecs, resolution, bucketSeconds, toMs - fromMs);

    const requestedContext = query.context ?? "vessels.self";
    const context = normalizeContext(requestedContext, selfContext);
    let columns: Column[] = query.pathSpecs.map((spec: PathSpec) => ({ spec }));
    if (query.sourcePolicy === "all") {
      columns = await splitBySource(query.pathSpecs, fromMs, toMs, context);
      guardBuckets(
        columns.map((column) => column.spec),
        resolution,
        bucketSeconds,
        toMs - fromMs,
      );
    }
    const pathSpecs = columns.map((column) => column.spec);

    const specs: ValueSpec[] = columns.map(({ spec, source }) => {
      const sourceRef =
        source === undefined ? spec.sourceRef || undefined : source;
      return {
        path: spec.path,
        aggregate: toQueryAggregate(spec.aggregate),
        ...(sourceRef !== undefined ? { sourceRef } : {}),
      };
    });

    const values = columns.map(({ spec, source }) => {
      const entry: {
        path: Path;
        method: AggregateMethod;
        $source?: SourceRef;
      } = { path: spec.path, method: spec.aggregate };
      const label = source === undefined ? spec.sourceRef || undefined : source;
      if (typeof label === "string") entry.$source = label as SourceRef;
      return entry;
    });

    if (specs.length === 0) {
      return { context: requestedContext as Context, range, values, data: [] };
    }

    const answer = await runner.run({
      kind: "values",
      from: fromMs,
      to: toMs,
      context,
      specs,
      ...(bucketSeconds > 0 ? { bucketMs: bucketSeconds * 1000 } : {}),
      limit: RAW_ROW_LIMIT,
    });

    // The ceilings that bound a request are per series — a raw one's rows, and
    // the buckets counted above — and the answer carries every series, so
    // enough of them together still exceed what one answer may return. Refused
    // rather than served short: the rows arrive ordered by bucket, so what a
    // truncated answer drops is the end of the range across every series at
    // once, and a moving average over a series cut in half is wrong for its
    // tail rather than merely absent there.
    if (answer.truncated) {
      throw new Error(
        `this request returns more than ${DEFAULT_ROW_LIMIT} rows — use a ` +
          `shorter range, fewer paths, or a coarser resolution`,
      );
    }

    // Keyed by spec index rather than by path: a request may name the same
    // path twice with different sources, one column per receiver, and a
    // path-keyed map would let the second overwrite the first.
    const bySpec = new Map<number, Map<number, unknown>>();
    const fromText = new Set<number>();
    const objects = new Set<number>();
    let firstBucket = Infinity;
    let lastBucket = -Infinity;

    // Only a bucketed spec reports bucket boundaries. A client-side aggregate
    // is read raw, so its rows carry their own timestamps, and a walk started
    // from one of those would land between boundaries at every step and
    // fabricate an all-null row for each.
    const onGrid = new Set(
      pathSpecs
        .map((spec: PathSpec, index: number) => ({ spec, index }))
        .filter(({ spec }) => !needsClientSideAggregation(spec.aggregate))
        .map(({ index }) => index),
    );

    for (const raw of answer.rows) {
      const row = toValueRow(raw);
      const series = bySpec.get(row.spec) ?? new Map<number, unknown>();
      bySpec.set(row.spec, series);
      series.set(row.bucket, decode(row));
      if (row.num === null && row.str !== null) fromText.add(row.spec);
      if (row.obj !== null) objects.add(row.spec);
      if (!onGrid.has(row.spec)) continue;
      if (row.bucket < firstBucket) firstBucket = row.bucket;
      if (row.bucket > lastBucket) lastBucket = row.bucket;
    }

    // After the query rather than before it: whether a path is an object is
    // known only from its rows, and the request is one statement.
    for (const index of objects) {
      const { aggregate, path } = pathSpecs[index];
      if (!appliesToObject(aggregate, bucketSeconds > 0)) {
        throw new Error(
          `Aggregate ${aggregate} does not apply to object path ${path}: ` +
            `use first, last or middle_index`,
        );
      }
    }

    // The client-side aggregates run over their series' raw rows, in order.
    for (const [index, spec] of pathSpecs.entries()) {
      if (!needsClientSideAggregation(spec.aggregate)) continue;
      const series = bySpec.get(index);
      if (series === undefined) continue;
      const stamps = [...series.keys()].sort((a, b) => a - b);
      if (objects.has(index)) {
        // Only middle_index is left for an object: the middle delta, whole.
        const middle = stamps[Math.floor(stamps.length / 2)];
        bySpec.set(
          index,
          new Map(
            stamps.map((ts) => [ts, ts === middle ? series.get(ts) : null]),
          ),
        );
        continue;
      }
      const numbers = stamps.map((ts) => {
        const value = series.get(ts);
        return typeof value === "number" ? value : null;
      });
      let computed: (number | null)[];
      if (spec.aggregate === "sma") {
        computed = computeSMA(numbers, smaWindow(spec.parameter?.[0]));
      } else if (spec.aggregate === "ema") {
        computed = computeEMA(numbers, emaAlpha(spec.parameter?.[0]));
      } else {
        const middle = Math.floor(numbers.length / 2);
        computed = numbers.map((value, i) => (i === middle ? value : null));
      }
      bySpec.set(index, new Map(stamps.map((ts, i) => [ts, computed[i]])));
    }

    // Report the reduction that ran. A downsampled text series always takes
    // the value in force at the bucket's end, so leaving the caller's
    // requested method in the response would label a series with an
    // aggregation that never happened. Positions keep the requested name, as
    // they do in the sibling provider, even though anything but `last` gets
    // the first fix in the bucket.
    if (bucketSeconds > 0) {
      for (const index of fromText) values[index].method = "last";
    }

    // The timeline: every bucket between the first and the last that holds
    // anything, so a gap inside the data is a row of nulls and a chart breaks
    // its line there. Only the interior is filled — the sibling's `FILL(NULL)`
    // spans the data rather than the request, and a range with two points at
    // its ends must not fabricate a week of empty rows. A request naming no
    // bucketed spec fills nothing: its rows are the real stamps below, which
    // is again what the sibling returns for one.
    const stamps = new Set<number>();
    if (bucketSeconds > 0 && firstBucket <= lastBucket) {
      const step = bucketSeconds * 1000;
      for (let at = firstBucket; at <= lastBucket; at += step) stamps.add(at);
    }
    for (const series of bySpec.values()) {
      for (const at of series.keys()) stamps.add(at);
    }

    const data = [...stamps]
      .sort((a, b) => a - b)
      .map((at) => {
        const row: [Timestamp, ...unknown[]] = [
          new Date(at).toISOString() as Timestamp,
        ];
        for (let index = 0; index < pathSpecs.length; index += 1) {
          row.push(bySpec.get(index)?.get(at) ?? null);
        }
        return row;
      });

    return { context: requestedContext as Context, range, values, data };
  }

  /**
   * `sourcePolicy: all`: each spec without a sourceRef becomes one column per
   * source with rows of its path in the range, and a spec with a sourceRef
   * stays a filter. A path with no rows gets no column. This is the one
   * request that costs two statements: which sources exist has to be known
   * before the series can be laid out.
   */
  async function splitBySource(
    requested: PathSpec[],
    from: number,
    to: number,
    context: string,
  ): Promise<Column[]> {
    const paths = [
      ...new Set(
        requested.filter((spec) => !spec.sourceRef).map((spec) => spec.path),
      ),
    ];
    const sources = new Map<string, (string | null)[]>();
    if (paths.length > 0) {
      const answer = await runner.run({
        kind: "sources",
        from,
        to,
        context,
        paths,
      });
      // A cut list would silently drop whole columns.
      if (answer.truncated) {
        throw new Error(
          `these paths have more than ${DEFAULT_ROW_LIMIT} sources — name ` +
            `the sources to read, or fewer paths`,
        );
      }
      for (const [path, source] of answer.rows) {
        const list = sources.get(path as string) ?? [];
        list.push((source as string | null) ?? null);
        sources.set(path as string, list);
      }
    }
    return requested.flatMap((spec): Column[] =>
      spec.sourceRef
        ? [{ spec }]
        : orderSources(sources.get(spec.path) ?? []).map((source) => ({
            spec,
            source,
          })),
    );
  }

  async function getPaths(query: PathsRequest): Promise<Path[]> {
    const range = resolveTimeRange(query);
    // No context filter: the request carries none, and a caller enumerating
    // paths wants every vessel's.
    const answer = await runner.run({
      kind: "paths",
      from: Date.parse(range.from),
      to: Date.parse(range.to),
    });
    return answer.rows.map((row) => row[0] as Path);
  }

  async function getContexts(query: ContextsRequest): Promise<Context[]> {
    const range = resolveTimeRange(query);
    const answer = await runner.run({
      kind: "contexts",
      from: Date.parse(range.from),
      to: Date.parse(range.to),
    });
    // Translated back to the spec's spelling: the own vessel is stored as
    // "self" and the API calls it "vessels.self".
    return answer.rows.map((row) =>
      row[0] === "self" ? ("vessels.self" as Context) : (row[0] as Context),
    );
  }

  return { getValues, getPaths, getContexts };
}
