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
      { path: "a.b", method: "average", $source: "n2k.0" },
      { path: "a.b", method: "average", $source: "n2k.9" },
    ]);
  });

  it("replays a downsampled boolean as a boolean under first and last", async () => {
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
      ask({
        pathSpecs: [spec("s.t", "first"), spec("s.t", "last")],
        resolution: 10,
      }),
    );

    assert.deepEqual(answer.data, [
      [new Date(AUG_23).toISOString(), false, true],
    ]);
    assert.deepEqual(answer.values, [
      { path: "s.t", method: "first" },
      { path: "s.t", method: "last" },
    ]);
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

describe(
  "getValues with sourcePolicy all",
  { skip: NO_BUNDLED_EXTENSION },
  () => {
    const iso = (ms: number) => new Date(ms).toISOString();
    const all = (
      over: Partial<ValuesRequest> & Pick<ValuesRequest, "pathSpecs">,
      windowMs?: number,
    ) => ask({ resolution: 10, ...over, sourcePolicy: "all" }, windowMs);

    it("splits a path into one column per source, ordered by source", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b", source: "n2k.9", value: 9 }),
        sample({ ts: AUG_23 + 2000, path: "a.b", source: "n2k.0", value: 1 }),
        sample({ ts: AUG_23 + 3000, path: "c.d", source: "other", value: 5 }),
      );

      const answer = await history.getValues(all({ pathSpecs: [spec("a.b")] }));

      assert.deepEqual(answer.values, [
        { path: "a.b", method: "average", $source: "n2k.0" },
        { path: "a.b", method: "average", $source: "n2k.9" },
      ]);
      assert.deepEqual(answer.data, [[iso(AUG_23), 1, 9]]);
    });

    it("keeps a spec that names a source as a filter", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b", source: "n2k.0", value: 1 }),
        sample({ ts: AUG_23 + 2000, path: "a.b", source: "n2k.9", value: 9 }),
      );

      const answer = await history.getValues(
        all({
          pathSpecs: [
            spec("a.b", "average", { sourceRef: "n2k.9" }),
            spec("a.b"),
          ],
        }),
      );

      assert.deepEqual(
        answer.values.map((v) => v.$source),
        ["n2k.9", "n2k.0", "n2k.9"],
      );
      assert.deepEqual(answer.data, [[iso(AUG_23), 9, 1, 9]]);
    });

    it("gives rows without a source their own unlabelled column, last", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b", source: null, value: 7 }),
        sample({ ts: AUG_23 + 2000, path: "a.b", source: "n2k.0", value: 1 }),
      );

      const answer = await history.getValues(all({ pathSpecs: [spec("a.b")] }));

      assert.deepEqual(answer.values, [
        { path: "a.b", method: "average", $source: "n2k.0" },
        { path: "a.b", method: "average" },
      ]);
      assert.deepEqual(answer.data, [[iso(AUG_23), 1, 7]]);
    });

    it("gives a path with no rows no column", async () => {
      record(sample({ ts: AUG_23 + 1000, path: "c.d", value: 5 }));

      const answer = await history.getValues(
        all({ pathSpecs: [spec("a.b"), spec("c.d")] }),
      );

      assert.deepEqual(answer.values, [
        { path: "c.d", method: "average", $source: "n2k.0" },
      ]);
      assert.deepEqual(answer.data, [[iso(AUG_23), 5]]);
    });

    it("answers an empty response when no path has rows", async () => {
      const answer = await history.getValues(all({ pathSpecs: [spec("a.b")] }));

      assert.deepEqual(answer.values, []);
      assert.deepEqual(answer.data, []);
    });

    it("splits an object path by the sources of its fields", async () => {
      record(
        sample({
          ts: AUG_23 + 1000,
          path: "navigation.attitude#/roll",
          source: "a",
          value: 1,
        }),
        sample({
          ts: AUG_23 + 1000,
          path: "navigation.attitude#/pitch",
          source: "a",
          value: 2,
        }),
      );
      record(
        sample({
          ts: AUG_23 + 2000,
          path: "navigation.attitude#/yaw",
          source: "b",
          value: 3,
        }),
      );

      const answer = await history.getValues(
        all({ pathSpecs: [spec("navigation.attitude", "last")] }),
      );

      assert.deepEqual(
        answer.values.map((v) => v.$source),
        ["a", "b"],
      );
      assert.deepEqual(answer.data, [
        [iso(AUG_23), { roll: 1, pitch: 2 }, { yaw: 3 }],
      ]);
    });

    it("splits the position path", async () => {
      record(
        sample({
          ts: AUG_23 + 1000,
          path: "navigation.position",
          source: "gps.b",
          kind: "position",
          value: { latitude: 60, longitude: 24 },
        }),
        sample({
          ts: AUG_23 + 2000,
          path: "navigation.position",
          source: "gps.a",
          kind: "position",
          value: { latitude: 61, longitude: 25 },
        }),
      );

      const answer = await history.getValues(
        all({ pathSpecs: [spec("navigation.position", "first")] }),
      );

      assert.deepEqual(
        answer.values.map((v) => v.$source),
        ["gps.a", "gps.b"],
      );
      assert.deepEqual(answer.data, [
        [
          iso(AUG_23),
          { latitude: 61, longitude: 25 },
          { latitude: 60, longitude: 24 },
        ],
      ]);
    });

    it("finds the sources of rows already rolled into the tree", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b", source: "tree", value: 1 }),
        sample({
          ts: AUG_23 + 1000,
          path: "navigation.attitude#/roll",
          source: "tree",
          value: 2,
        }),
      );
      const bound = store.rollBound();
      assert.ok(bound !== null);
      await roll({ dataDir: dir, maxRowid: bound.maxRowid, rollId: 1 });
      store.deleteThrough(bound.maxRowid);
      record(
        sample({ ts: AUG_23 + 2000, path: "a.b", source: "store", value: 3 }),
      );

      const answer = await history.getValues(
        all({
          pathSpecs: [spec("a.b"), spec("navigation.attitude", "last")],
        }),
      );

      assert.deepEqual(
        answer.values.map((v) => [v.path, v.$source]),
        [
          ["a.b", "store"],
          ["a.b", "tree"],
          ["navigation.attitude", "tree"],
        ],
      );
      assert.deepEqual(answer.data, [[iso(AUG_23), 3, 1, { roll: 2 }]]);
    });

    it("budgets the expanded columns in the bucket guard", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b", source: "n2k.0", value: 1 }),
        sample({ ts: AUG_23 + 2000, path: "a.b", source: "n2k.9", value: 9 }),
      );
      const windowMs = MAX_SAMPLE_BUCKETS * 0.6 * 1000;

      await history.getValues(
        ask({ pathSpecs: [spec("a.b")], resolution: 1 }, windowMs),
      );
      await assert.rejects(
        history.getValues(
          all({ pathSpecs: [spec("a.b")], resolution: 1 }, windowMs),
        ),
        /across 2 paths/,
      );
    });
  },
);

describe(
  "getValues with an aggregate a path cannot take",
  { skip: NO_BUNDLED_EXTENSION },
  () => {
    const iso = (ms: number) => new Date(ms).toISOString();
    const refused = (kind: string, path: string, aggregate: string) =>
      new RegExp(
        `^Error: Aggregate ${aggregate} does not apply to ${kind} path ` +
          `${path.replace(/\./g, "\\.")}: use first, last or middle_index$`,
      );

    function positions(): void {
      record(
        ...[60, 61, 62].map((latitude, i) =>
          sample({
            ts: AUG_23 + 1000 * (i + 1),
            path: "navigation.position",
            kind: "position",
            value: { latitude, longitude: 24 },
          }),
        ),
      );
    }

    function states(): void {
      record(
        ...["a", "b", "c"].map((value, i) =>
          sample({
            ts: AUG_23 + 1000 * (i + 1),
            path: "s.t",
            kind: "string",
            value,
          }),
        ),
      );
    }

    it("rejects an unknown aggregate name", async () => {
      record(sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }));
      await assert.rejects(
        history.getValues(ask({ pathSpecs: [spec("a.b", "bogus")] })),
        /^Error: Unknown aggregate bogus: use average, min, max, first, last, mid, middle_index, sma or ema$/,
      );
    });

    it("rejects avg, which Skip and KIP send, as unknown", async () => {
      record(sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }));
      await assert.rejects(
        history.getValues(
          ask({ pathSpecs: [spec("a.b", "avg")], resolution: 10 }),
        ),
        /^Error: Unknown aggregate avg: /,
      );
    });

    it("refuses a downsampled arithmetic aggregate on a position", async () => {
      positions();
      for (const aggregate of ["average", "min", "max", "mid"]) {
        await assert.rejects(
          history.getValues(
            ask({
              pathSpecs: [spec("navigation.position", aggregate)],
              resolution: 10,
            }),
          ),
          refused("position", "navigation.position", aggregate),
        );
      }
    });

    it("refuses smoothing a position or a text path without a resolution", async () => {
      positions();
      states();
      for (const aggregate of ["sma", "ema"]) {
        await assert.rejects(
          history.getValues(
            ask({ pathSpecs: [spec("navigation.position", aggregate)] }),
          ),
          refused("position", "navigation.position", aggregate),
        );
        await assert.rejects(
          history.getValues(ask({ pathSpecs: [spec("s.t", aggregate)] })),
          refused("text", "s.t", aggregate),
        );
      }
    });

    it("refuses a downsampled arithmetic aggregate on a text path", async () => {
      states();
      for (const aggregate of ["average", "min", "max", "mid"]) {
        await assert.rejects(
          history.getValues(
            ask({ pathSpecs: [spec("s.t", aggregate)], resolution: 10 }),
          ),
          refused("text", "s.t", aggregate),
        );
      }
    });

    it("returns recorded values without a resolution, labelled as asked", async () => {
      positions();
      states();
      const answer = await history.getValues(
        ask({
          pathSpecs: [spec("navigation.position", "average"), spec("s.t")],
        }),
      );
      assert.deepEqual(answer.data, [
        [iso(AUG_23 + 1000), { latitude: 60, longitude: 24 }, "a"],
        [iso(AUG_23 + 2000), { latitude: 61, longitude: 24 }, "b"],
        [iso(AUG_23 + 3000), { latitude: 62, longitude: 24 }, "c"],
      ]);
      assert.deepEqual(
        answer.values.map((v) => v.method),
        ["average", "average"],
      );
    });

    it("keeps the middle position and text value for middle_index", async () => {
      positions();
      states();
      const answer = await history.getValues(
        ask({
          pathSpecs: [
            spec("navigation.position", "middle_index"),
            spec("s.t", "middle_index"),
          ],
          resolution: 10,
        }),
      );
      assert.deepEqual(answer.data, [
        [iso(AUG_23 + 1000), null, null],
        [iso(AUG_23 + 2000), { latitude: 61, longitude: 24 }, "b"],
        [iso(AUG_23 + 3000), null, null],
      ]);
    });

    it("serves a mixed path's numbers and ignores its text, as QuestDB does", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }),
        sample({ ts: AUG_23 + 3000, path: "a.b", value: 2 }),
        sample({
          ts: AUG_23 + 25_000,
          path: "a.b",
          kind: "string",
          value: "x",
        }),
      );

      const averaged = await history.getValues(
        ask({ pathSpecs: [spec("a.b")], resolution: 10 }),
      );
      assert.deepEqual(averaged.data, [[iso(AUG_23), 1.5]]);

      const smoothed = await history.getValues(
        ask({ pathSpecs: [spec("a.b", "sma", { parameter: ["2"] })] }),
      );
      assert.deepEqual(smoothed.data, [
        [iso(AUG_23 + 1000), 1],
        [iso(AUG_23 + 3000), 1.5],
      ]);
    });

    it("keeps a mixed path's number when its text shares the timestamp", async () => {
      record(
        sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }),
        sample({ ts: AUG_23 + 1000, path: "a.b", kind: "string", value: "x" }),
        sample({ ts: AUG_23 + 3000, path: "a.b", value: 2 }),
      );

      const averaged = await history.getValues(
        ask({ pathSpecs: [spec("a.b")], resolution: 10 }),
      );
      assert.deepEqual(averaged.data, [[iso(AUG_23), 1.5]]);

      const smoothed = await history.getValues(
        ask({ pathSpecs: [spec("a.b", "sma", { parameter: ["2"] })] }),
      );
      assert.deepEqual(smoothed.data, [
        [iso(AUG_23 + 1000), 1],
        [iso(AUG_23 + 3000), 1.5],
      ]);
    });

    it("keeps the middle numeric value for middle_index", async () => {
      record(
        ...[10, 20, 30].map((value, i) =>
          sample({ ts: AUG_23 + 1000 * (i + 1), path: "a.b", value }),
        ),
      );
      for (const resolution of [undefined, 10]) {
        const answer = await history.getValues(
          ask({
            pathSpecs: [spec("a.b", "middle_index")],
            ...(resolution === undefined ? {} : { resolution }),
          }),
        );
        assert.deepEqual(answer.data, [
          [iso(AUG_23 + 1000), null],
          [iso(AUG_23 + 2000), 20],
          [iso(AUG_23 + 3000), null],
        ]);
      }
    });

    it("answers an empty column for a downsampled average with no rows", async () => {
      const answer = await history.getValues(
        ask({ pathSpecs: [spec("s.t")], resolution: 10 }),
      );
      assert.deepEqual(answer.data, []);
      assert.deepEqual(answer.values, [{ path: "s.t", method: "average" }]);
    });
  },
);

describe("getValues on an object path", { skip: NO_BUNDLED_EXTENSION }, () => {
  const iso = (ms: number) => new Date(ms).toISOString();

  /** One attitude delta: every field shares its ts. */
  function attitude(ts: number, fields: Record<string, number>): void {
    record(
      ...Object.entries(fields).map(([key, value]) =>
        sample({ ts, path: `navigation.attitude#/${key}`, value }),
      ),
    );
  }

  const refusal = (aggregate: string) =>
    new RegExp(
      `^Error: Aggregate ${aggregate} does not apply to object path ` +
        `navigation\\.attitude: use first, last or middle_index$`,
    );

  it("returns the latest delta of each bucket whole, with gaps null", async () => {
    attitude(AUG_23 + 1000, { roll: 1, pitch: 1, yaw: 1 });
    attitude(AUG_23 + 5000, { roll: 2, pitch: 2 });
    attitude(AUG_23 + 25_000, { roll: 3, pitch: 3, yaw: 3 });

    const answer = await history.getValues(
      ask({
        pathSpecs: [spec("navigation.attitude", "last")],
        resolution: 10,
      }),
    );

    assert.deepEqual(answer.data, [
      [iso(AUG_23), { roll: 2, pitch: 2 }],
      [iso(AUG_23 + 10_000), null],
      [iso(AUG_23 + 20_000), { roll: 3, pitch: 3, yaw: 3 }],
    ]);
    assert.deepEqual(answer.values, [
      { path: "navigation.attitude", method: "last" },
    ]);
  });

  it("returns the earliest delta of each bucket for first", async () => {
    attitude(AUG_23 + 1000, { roll: 1 });
    attitude(AUG_23 + 5000, { roll: 2, pitch: 2 });

    const answer = await history.getValues(
      ask({
        pathSpecs: [spec("navigation.attitude", "first")],
        resolution: 10,
      }),
    );

    assert.deepEqual(answer.data, [[iso(AUG_23), { roll: 1 }]]);
  });

  it("unescapes field names and replays text fields as recorded", async () => {
    record(
      sample({
        ts: AUG_23 + 1000,
        path: "notifications.mob#/state",
        kind: "string",
        value: "emergency",
      }),
      sample({
        ts: AUG_23 + 1000,
        path: "notifications.mob#/silenced",
        kind: "boolean",
        value: "false",
      }),
      sample({ ts: AUG_23 + 1000, path: "notifications.mob#/a~1b~0c" }),
      sample({ ts: AUG_23 + 1000, path: "notifications.mob#/__proto__" }),
    );

    const answer = await history.getValues(
      ask({
        pathSpecs: [spec("notifications.mob", "last")],
        resolution: 10,
      }),
    );

    const value = answer.data[0][1] as Record<string, unknown>;
    assert.deepEqual(Object.keys(value).sort(), [
      "__proto__",
      "a/b~c",
      "silenced",
      "state",
    ]);
    assert.equal(value.state, "emergency");
    assert.equal(value.silenced, false);
    assert.equal(Object.getPrototypeOf(value), Object.prototype);
    assert.deepEqual(answer.values, [
      { path: "notifications.mob", method: "last" },
    ]);
  });

  it("returns one object per delta without a resolution, labelled as asked", async () => {
    attitude(AUG_23 + 1000, { roll: 1, pitch: 1 });
    attitude(AUG_23 + 2000, { roll: 2 });

    for (const aggregate of ["average", "min", "last"]) {
      const answer = await history.getValues(
        ask({ pathSpecs: [spec("navigation.attitude", aggregate)] }),
      );

      assert.deepEqual(
        answer.data,
        [
          [iso(AUG_23 + 1000), { roll: 1, pitch: 1 }],
          [iso(AUG_23 + 2000), { roll: 2 }],
        ],
        aggregate,
      );
      assert.equal(answer.values[0].method, aggregate);
    }
  });

  it("keeps the middle delta whole for middle_index", async () => {
    attitude(AUG_23 + 1000, { roll: 1 });
    attitude(AUG_23 + 2000, { roll: 2, pitch: 2 });
    attitude(AUG_23 + 3000, { roll: 3 });

    for (const resolution of [undefined, 10]) {
      const answer = await history.getValues(
        ask({
          pathSpecs: [spec("navigation.attitude", "middle_index")],
          ...(resolution === undefined ? {} : { resolution }),
        }),
      );

      assert.deepEqual(answer.data, [
        [iso(AUG_23 + 1000), null],
        [iso(AUG_23 + 2000), { roll: 2, pitch: 2 }],
        [iso(AUG_23 + 3000), null],
      ]);
    }
  });

  it("refuses a downsampled arithmetic aggregate", async () => {
    attitude(AUG_23 + 1000, { roll: 1 });

    for (const aggregate of ["average", "min", "max", "mid"]) {
      await assert.rejects(
        history.getValues(
          ask({
            pathSpecs: [spec("navigation.attitude", aggregate)],
            resolution: 10,
          }),
        ),
        (err: Error) => refusal(aggregate).test(String(err)),
      );
    }
  });

  it("refuses smoothing with or without a resolution", async () => {
    attitude(AUG_23 + 1000, { roll: 1 });

    for (const aggregate of ["sma", "ema"]) {
      for (const resolution of [undefined, 10]) {
        await assert.rejects(
          history.getValues(
            ask({
              pathSpecs: [spec("navigation.attitude", aggregate)],
              ...(resolution === undefined ? {} : { resolution }),
            }),
          ),
          (err: Error) => refusal(aggregate).test(String(err)),
        );
      }
    }
  });

  it("never refuses a path with no field rows", async () => {
    record(sample({ ts: AUG_23 + 1000, path: "a.b", value: 1 }));

    for (const aggregate of ["average", "sma"]) {
      const answer = await history.getValues(
        ask({
          pathSpecs: [
            spec("a.b", aggregate),
            spec("navigation.attitude", aggregate),
          ],
          resolution: 10,
        }),
      );
      assert.equal(answer.data.length, 1, aggregate);
    }
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

  it("refuses a source list that did not fit rather than dropping columns", async () => {
    const kinds: string[] = [];
    const truncating = {
      run: async (request: { kind: string }) => {
        kinds.push(request.kind);
        return {
          rows: [["a.b", "n2k.0"]],
          truncated: true,
          wallMs: 1,
          treeFiles: 0,
          rssBytes: null,
          peakRssBytes: null,
        };
      },
    } as unknown as QueryRunner;

    await assert.rejects(
      createHistoryV2(truncating, "vessels.self").getValues(
        ask({ pathSpecs: [spec("a.b")], resolution: 10, sourcePolicy: "all" }),
      ),
      /more than 100000 sources/,
    );
    assert.deepEqual(kinds, ["sources"]);
  });
});

describe("getPaths and getContexts", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("list what was recorded in the range, across contexts", async () => {
    record(
      sample({ ts: AUG_23 + 1000, path: "a.b" }),
      sample({ ts: AUG_23 + 1000, path: "c.d", context: "vessels.urn:x" }),
      sample({ ts: AUG_23 + 2 * DAY, path: "later.path" }),
      sample({ ts: AUG_23 + 1000, path: "navigation.attitude#/roll" }),
      sample({ ts: AUG_23 + 1000, path: "navigation.attitude#/pitch" }),
    );

    const range = {
      from: instant(AUG_23),
      to: instant(AUG_23 + DAY),
    } as never;

    // An object path once, never its fields' pointer names.
    assert.deepEqual(await history.getPaths(range), [
      "a.b",
      "c.d",
      "navigation.attitude",
    ]);
    assert.deepEqual(await history.getContexts(range), [
      "vessels.self",
      "vessels.urn:x",
    ]);
  });
});
