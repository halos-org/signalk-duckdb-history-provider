import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Temporal } from "@js-temporal/polyfill";
import type { HistoryApi, ValuesRequest } from "@signalk/server-api/history";
import { DATA_LAYOUT } from "../data-dir.js";
import { createHistoryV2, MAX_SAMPLE_BUCKETS } from "../history-v2.js";
import { QueryRunner } from "../query/duck.js";
import { roll } from "../roll/roll.js";
import { writerPaths } from "../writer/contract.js";
import { HotStore } from "../writer/hot-store.js";
import { NO_BUNDLED_EXTENSION, sample } from "./fixtures.js";
import type { Sample } from "../writer/protocol.js";

/**
 * The v2 surface, through a real query service and a real engine.
 *
 * These assert the API contract rather than the SQL: what a chart receives for
 * a gap, what a downsampled boolean reads as, and which reduction the response
 * says was applied. The sibling provider's answers are the reference — a chart
 * drawn against one has to look the same drawn against the other.
 */

const DAY = 86_400_000;
const AUG_23 = Date.UTC(2026, 7, 23);

let dir: string;
let store: HotStore;
let runner: QueryRunner;
let history: HistoryApi;
let seq = 0;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "history-v2-"));
  mkdirSync(join(dir, DATA_LAYOUT.hotStore), { recursive: true });
  store = HotStore.open(writerPaths(dir).store);
  store.beginSession("test");
  runner = new QueryRunner({ dataDir: dir });
  history = createHistoryV2(runner, "vessels.urn:mrn:imo:mmsi:230099999");
  seq = 0;
});

afterEach(() => {
  runner.stop();
  try {
    store.close();
  } catch {
    // Already closed by the test.
  }
  rmSync(dir, { recursive: true, force: true });
});

function record(...samples: Sample[]): void {
  seq += 1;
  store.insertBatch(seq, samples);
}

const instant = (ms: number) => Temporal.Instant.fromEpochMilliseconds(ms);

/** A values request over a window, with the awkward Temporal shape filled in. */
function ask(
  over: Partial<ValuesRequest> & Pick<ValuesRequest, "pathSpecs">,
  windowMs = 60_000,
): ValuesRequest {
  return {
    from: instant(AUG_23),
    to: instant(AUG_23 + windowMs),
    ...over,
  } as ValuesRequest;
}

const spec = (path: string, aggregate = "average", over = {}) =>
  ({ path, aggregate, parameter: [], ...over }) as never;

describe("getValues", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("returns one row per bucket, with the gaps filled in", async () => {
    // Two buckets with data and one without between them. A chart has to be
    // able to break its line at the gap, which is what the null row is for.
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", value: 10 }),
      sample({ ts: AUG_23 + 3000, path: "a.b", value: 20 }),
      sample({ ts: AUG_23 + 25_000, path: "a.b", value: 7 }),
    );

    const answer = await history.getValues(
      ask({ pathSpecs: [spec("a.b")], resolution: 10 }),
    );

    assert.deepEqual(answer.data, [
      [new Date(AUG_23).toISOString(), 15],
      [new Date(AUG_23 + 10_000).toISOString(), null],
      [new Date(AUG_23 + 20_000).toISOString(), 7],
    ]);
    assert.deepEqual(answer.values, [{ path: "a.b", method: "average" }]);
    assert.equal(answer.context, "vessels.self");
  });

  it("does not fabricate buckets beyond the data", async () => {
    // The window is a minute; the data is two seconds of it. The sibling's
    // FILL(NULL) spans the data rather than the request, and a request whose
    // range dwarfs its data must not return a screenful of empty rows.
    record(sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }));

    const answer = await history.getValues(
      ask({ pathSpecs: [spec("a.b")], resolution: 1 }),
    );

    assert.equal(answer.data.length, 1);
  });

  it("gives every series its own column, aligned on the timeline", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }),
      sample({ ts: AUG_23 + 12_000, path: "c.d", value: 2 }),
    );

    const answer = await history.getValues(
      ask({ pathSpecs: [spec("a.b"), spec("c.d")], resolution: 10 }),
    );

    assert.deepEqual(answer.data, [
      [new Date(AUG_23).toISOString(), 1, null],
      [new Date(AUG_23 + 10_000).toISOString(), null, 2],
    ]);
  });

  it("keeps two specs on one path apart when they name different sources", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b", source: "n2k.0", value: 1 }),
      sample({ ts: AUG_23 + 2000, path: "a.b", source: "n2k.9", value: 99 }),
    );

    const answer = await history.getValues(
      ask({
        pathSpecs: [
          spec("a.b", "average", { sourceRef: "n2k.0" }),
          spec("a.b", "average", { sourceRef: "n2k.9" }),
        ],
        resolution: 10,
      }),
    );

    assert.deepEqual(answer.data, [[new Date(AUG_23).toISOString(), 1, 99]]);
    assert.deepEqual(answer.values, [
      { path: "a.b", method: "average", sourceRef: "n2k.0" },
      { path: "a.b", method: "average", sourceRef: "n2k.9" },
    ]);
  });

  it("replays a downsampled boolean as a boolean, and says it took the last", async () => {
    record(
      sample({
        ts: AUG_23 + 1000,
        path: "s.t",
        kind: "boolean",
        value: "false",
      }),
      sample({
        ts: AUG_23 + 5000,
        path: "s.t",
        kind: "boolean",
        value: "true",
      }),
    );

    const answer = await history.getValues(
      ask({ pathSpecs: [spec("s.t")], resolution: 10 }),
    );

    assert.deepEqual(answer.data, [[new Date(AUG_23).toISOString(), true]]);
    // Averaging text never happened, so the response must not claim it did.
    assert.deepEqual(answer.values, [{ path: "s.t", method: "last" }]);
  });

  it("returns a position that was recorded", async () => {
    record(
      sample({
        ts: AUG_23 + 1000,
        path: "navigation.position",
        kind: "position",
        value: { latitude: 60.1, longitude: 24.1 },
      }),
      sample({
        ts: AUG_23 + 5000,
        path: "navigation.position",
        kind: "position",
        value: { latitude: 60.2, longitude: 24.2 },
      }),
    );

    const answer = await history.getValues(
      ask({
        pathSpecs: [spec("navigation.position", "last")],
        resolution: 10,
      }),
    );

    assert.deepEqual(answer.data, [
      [new Date(AUG_23).toISOString(), { latitude: 60.2, longitude: 24.2 }],
    ]);
  });

  it("computes a moving average over the raw series", async () => {
    for (let i = 0; i < 4; i += 1) {
      record(sample({ ts: AUG_23 + i * 1000, path: "a.b", value: i * 10 }));
    }

    const answer = await history.getValues(
      ask({
        pathSpecs: [spec("a.b", "sma", { parameter: ["2"] })],
        resolution: 10,
      }),
    );

    // A two-sample window over 0, 10, 20, 30 — at the raw timestamps, because
    // a moving average is not a bucket reduction.
    assert.deepEqual(
      answer.data.map((row) => row[1]),
      [0, 5, 15, 25],
    );
  });

  it("fills the gaps on the bucket grid when one spec is not on it", async () => {
    // A client-side aggregate is read raw, so its rows carry their own
    // timestamps rather than bucket boundaries. Taking the fill's bounds over
    // those started the walk between two boundaries, and every step after it
    // landed on a stamp no series holds — an all-null row apiece, up to one
    // per bucket over the whole range.
    record(
      sample({ ts: AUG_23 + 1000, path: "c.d", value: 0 }),
      sample({ ts: AUG_23 + 2000, path: "c.d", value: 10 }),
      sample({ ts: AUG_23 + 3000, path: "c.d", value: 20 }),
      sample({ ts: AUG_23 + 12_000, path: "a.b", value: 7 }),
      sample({ ts: AUG_23 + 35_000, path: "a.b", value: 9 }),
    );

    const answer = await history.getValues(
      ask({
        pathSpecs: [spec("a.b"), spec("c.d", "sma", { parameter: ["2"] })],
        resolution: 10,
      }),
    );

    // Every fabricated stamp is a boundary: 20 s is the gap in `a.b`, and the
    // walk never starts from `c.d`'s 1 s. Bounding it over both series instead
    // filled 11 s and 21 s, which no series holds and no boundary names.
    assert.deepEqual(answer.data, [
      [new Date(AUG_23 + 1000).toISOString(), null, 0],
      [new Date(AUG_23 + 2000).toISOString(), null, 5],
      [new Date(AUG_23 + 3000).toISOString(), null, 15],
      [new Date(AUG_23 + 10_000).toISOString(), 7, null],
      [new Date(AUG_23 + 20_000).toISOString(), null, null],
      [new Date(AUG_23 + 30_000).toISOString(), 9, null],
    ]);
  });

  it("takes the documented window and alpha when the parameter is unusable", async () => {
    // Both arrive as text from a query string. A window of "0" divided by an
    // empty window and an alpha of "abc" multiplied the series by NaN — and
    // NaN serialises as null, so the caller saw a gap rather than an error.
    for (let i = 0; i < 4; i += 1) {
      record(sample({ ts: AUG_23 + i * 1000, path: "a.b", value: i * 10 }));
    }
    const over = async (aggregate: string, parameter: string[] | undefined) =>
      (
        await history.getValues(
          ask({
            pathSpecs: [spec("a.b", aggregate, { parameter })],
            resolution: 10,
          }),
        )
      ).data.map((row) => row[1]);

    // A five-sample window over 0, 10, 20, 30 never fills, so each value is
    // the mean of everything before it. "2x" and "2.7" are neither a window of
    // 2 nor an error, which is what reading a leading number would make them.
    assert.deepEqual(await over("sma", ["0"]), [0, 5, 10, 15]);
    assert.deepEqual(await over("sma", ["-1"]), [0, 5, 10, 15]);
    assert.deepEqual(await over("sma", ["2x"]), [0, 5, 10, 15]);
    assert.deepEqual(await over("sma", ["2.7"]), [0, 5, 10, 15]);
    assert.deepEqual(await over("sma", [""]), [0, 5, 10, 15]);
    assert.deepEqual(await over("sma", undefined), [0, 5, 10, 15]);
    // And a window the whole string does spell is honoured.
    assert.deepEqual(await over("sma", ["2"]), [0, 5, 15, 25]);

    // 0.9 would leave the last value at 27.09; the default leaves it at 10.48.
    assert.deepEqual(await over("ema", ["0.9x"]), await over("ema", undefined));

    for (const parameter of [["abc"], ["0"], ["2"], undefined]) {
      const values = await over("ema", parameter);
      assert.ok(
        values.every((value) => typeof value === "number"),
        `ema with ${JSON.stringify(parameter)} returned ${JSON.stringify(values)}`,
      );
      assert.equal(values[0], 0);
    }
  });

  it("reads across the seam between the tree and the store", async () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b", value: 10 }));
    const bound = store.rollBound();
    assert.ok(bound !== null);
    await roll({ dataDir: dir, maxRowid: bound.maxRowid, rollId: 1 });
    store.deleteThrough(bound.maxRowid);
    record(sample({ ts: AUG_23 + 25_000, path: "a.b", value: 30 }));

    const answer = await history.getValues(
      ask({ pathSpecs: [spec("a.b")], resolution: 10 }),
    );

    assert.deepEqual(
      answer.data.map((row) => row[1]),
      [10, null, 30],
    );
  });

  it("refuses a resolution that would build more buckets than the budget", async () => {
    // A year at one second, before any query runs.
    await assert.rejects(
      history.getValues(
        ask({ pathSpecs: [spec("a.b")], resolution: 1 }, 365 * DAY),
      ),
      (err: Error) =>
        err.message.includes(String(MAX_SAMPLE_BUCKETS)) &&
        /coarser resolution/.test(err.message),
    );
  });

  it("counts the budget per series, not per request", async () => {
    // Half the budget each: one path passes, two do not.
    const seconds = MAX_SAMPLE_BUCKETS * 0.6;
    const one = ask(
      { pathSpecs: [spec("a.b")], resolution: 1 },
      seconds * 1000,
    );
    await history.getValues(one);

    await assert.rejects(
      history.getValues(
        ask(
          { pathSpecs: [spec("a.b"), spec("c.d")], resolution: 1 },
          seconds * 1000,
        ),
      ),
      /buckets across 2 paths/,
    );
  });

  it("does not divide by a zero-width bucket", async () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b", value: 5 }));

    const answer = await history.getValues(
      ask({ pathSpecs: [spec("a.b")], resolution: 0.4 }),
    );

    // Clamped to a second, as in the sibling provider.
    assert.deepEqual(answer.data, [[new Date(AUG_23 + 1000).toISOString(), 5]]);
  });

  it("answers an empty request without querying anything", async () => {
    const answer = await history.getValues(ask({ pathSpecs: [] }));
    assert.deepEqual(answer.data, []);
    assert.equal(runner.running, false);
  });
});

describe("an answer that did not fit", () => {
  it("refuses it rather than serving the range with its end cut off", async () => {
    // The reader's own ceiling is on the answer, and a request's ceilings are
    // per series, so enough series together exceed it. Nothing downstream
    // reads `truncated`, and the rows come back ordered by bucket — so a
    // served answer would be every series stopping at the same early moment,
    // with nothing to say so.
    const truncating = {
      run: async () => ({
        rows: [],
        truncated: true,
        wallMs: 1,
        treeFiles: 0,
        rssBytes: null,
        peakRssBytes: null,
      }),
    } as unknown as QueryRunner;

    await assert.rejects(
      createHistoryV2(truncating, "vessels.self").getValues(
        ask({ pathSpecs: [spec("a.b")], resolution: 10 }),
      ),
      /more than 100000 rows/,
    );
  });
});

describe("getPaths and getContexts", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("list what was recorded in the range, across contexts", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b" }),
      sample({ ts: AUG_23 + 1000, path: "c.d", context: "vessels.urn:x" }),
      sample({ ts: AUG_23 + 2 * DAY, path: "later.path" }),
    );

    const range = {
      from: instant(AUG_23),
      to: instant(AUG_23 + DAY),
    } as never;

    assert.deepEqual(await history.getPaths(range), ["a.b", "c.d"]);
    assert.deepEqual(await history.getContexts(range), [
      "vessels.self",
      "vessels.urn:x",
    ]);
  });
});
