import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, renameSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DATA_LAYOUT } from "../data-dir.js";
import { CHUNK_SECONDS, createHistoryProviderV1 } from "../history-v1.js";
import { QueryRunner } from "../query/duck.js";
import { roll } from "../roll/roll.js";
import { writerPaths } from "../writer/contract.js";
import { HotStore } from "../writer/hot-store.js";
import { eventually, NO_BUNDLED_EXTENSION, sample } from "./fixtures.js";
import type { Sample } from "../writer/protocol.js";

/**
 * The v1 surface, through a real query service and a real engine.
 *
 * Three methods rather than one: `hasAnyData` decides whether the server offers
 * playback at all, `streamHistory` is the playback, and `getHistory` is what
 * `/signalk/v1/snapshot/` builds a full tree from. These assert the contract a
 * client sees — the shape of a replayed name, what a snapshot holds for a path
 * that went quiet, and where the snapshot's stated bound cuts it off.
 */

const DAY = 86_400_000;
const AUG_23 = Date.UTC(2026, 7, 23);
const SELF = "vessels.urn:mrn:imo:mmsi:230099999";

let dir: string;
let store: HotStore;
let runner: QueryRunner;
let history: ReturnType<typeof createHistoryProviderV1>;
let debugLines: string[];
let spawns: number;
let seq = 0;
let rollId = 0;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "history-v1-"));
  mkdirSync(join(dir, DATA_LAYOUT.hotStore), { recursive: true });
  store = HotStore.open(writerPaths(dir).store);
  store.beginSession("test");
  debugLines = [];
  spawns = 0;
  runner = new QueryRunner({
    dataDir: dir,
    // The default spawn, counted. Playback reads a window every chunk, and the
    // whole design rests on those sharing one engine rather than starting one.
    spawnQuery: (args) => {
      spawns += 1;
      return spawn(process.execPath, args, { stdio: ["pipe", "pipe", "pipe"] });
    },
  });
  history = createHistoryProviderV1(runner, SELF, (line) =>
    debugLines.push(line),
  );
  seq = 0;
  rollId = 0;
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

/** Rolls everything in the store into the tree and truncates it, as the writer does. */
async function rollAndTruncate(): Promise<void> {
  const bound = store.rollBound();
  assert.ok(bound !== null, "nothing to roll");
  rollId += 1;
  await roll({ dataDir: dir, maxRowid: bound.maxRowid, rollId });
  store.deleteThrough(bound.maxRowid);
}

interface Written {
  context: string;
  updates: {
    timestamp: string;
    $source?: string;
    values: { path: string; value: unknown }[];
  }[];
}

/** A spark that records what was written to it and can be ended. */
function fakeSpark() {
  const writes: Written[] = [];
  const handlers = new Map<string, (...args: unknown[]) => void>();
  return {
    writes,
    end: () => handlers.get("end")?.(),
    spark: {
      write: (data: unknown) => {
        writes.push(data as Written);
      },
      on: (event: string, cb: (...args: unknown[]) => void) => {
        handlers.set(event, cb);
      },
    },
  };
}

/** Every `(path, value)` a spark received, in the order it received them. */
function replayed(writes: Written[]): [string, unknown][] {
  return writes.flatMap((delta) =>
    delta.updates.flatMap((update) =>
      update.values.map(
        (value) => [value.path, value.value] as [string, unknown],
      ),
    ),
  );
}

/**
 * The runner with every request counted, for the assertions about queries that
 * should not happen.
 *
 * Those cannot be made against the debug log: starting the engine takes the
 * better part of a second, so a request that is going to fail has not failed
 * yet at the moment the log is read, and a test that only reads the log passes
 * whether or not the code under it is right.
 */
function countingRunner(): { runner: QueryRunner; runs: () => number } {
  let runs = 0;
  const target = runner;
  const proxy = new Proxy(target, {
    get(owner, property, receiver) {
      if (property === "run") {
        return (request: never) => {
          runs += 1;
          return owner.run(request);
        };
      }
      const value = Reflect.get(owner, property, receiver) as unknown;
      return typeof value === "function" ? value.bind(owner) : value;
    },
  });
  return { runner: proxy, runs: () => runs };
}

/** Playback at a rate that makes the 60-second chunk delay a millisecond. */
const FAST = 60_000;

function ask(startTime: number, playbackRate = FAST) {
  return { startTime: new Date(startTime), playbackRate };
}

function answersHasAnyData(startTime: number): Promise<boolean> {
  return new Promise((resolve) => {
    history.hasAnyData(ask(startTime), resolve);
  });
}

function snapshotAt(at: number): Promise<Written[]> {
  return new Promise((resolve) => {
    history.getHistory(new Date(at), "", (deltas) =>
      resolve(deltas as Written[]),
    );
  });
}

/** The value each `(context, path)` carries in a snapshot. */
function valuesByKey(deltas: Written[]): Map<string, unknown> {
  const found = new Map<string, unknown>();
  for (const delta of deltas) {
    for (const update of delta.updates) {
      for (const value of update.values) {
        found.set(`${delta.context}|${value.path}`, value.value);
      }
    }
  }
  return found;
}

describe("hasAnyData", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("answers false for a store that has recorded nothing", async () => {
    assert.equal(await answersHasAnyData(AUG_23), false);
  });

  it("answers true for a recorded window, and false past it", async () => {
    record(sample({ ts: AUG_23 }));
    assert.equal(await answersHasAnyData(AUG_23), true);
    assert.equal(await answersHasAnyData(AUG_23 + 1000), false);
  });

  it("finds data the roll has already moved into the tree", async () => {
    record(sample({ ts: AUG_23 }));
    await rollAndTruncate();
    assert.equal(await answersHasAnyData(AUG_23), true);
  });

  it("answers true for a vessel recording only string channels", async () => {
    // The sibling provider had to union three tables to get this right, and
    // answering false here disables playback for that vessel entirely.
    record(sample({ ts: AUG_23, kind: "string", value: "moored" }));
    assert.equal(await answersHasAnyData(AUG_23), true);
  });

  it("answers false for a start time that is not a time", async () => {
    record(sample({ ts: AUG_23 }));
    assert.equal(await answersHasAnyData(Number.NaN), false);
  });
});

describe("getHistory", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("returns the last value per path and context at the instant", async () => {
    record(
      sample({ ts: AUG_23, path: "a", value: 1 }),
      sample({ ts: AUG_23 + 10_000, path: "a", value: 2 }),
      sample({ ts: AUG_23 + 20_000, path: "a", value: 3 }),
      sample({ ts: AUG_23 + 5_000, path: "b", value: 9 }),
      sample({
        ts: AUG_23,
        context: "vessels.urn:mrn:imo:mmsi:200000001",
        path: "a",
        value: 7,
      }),
    );

    const found = valuesByKey(await snapshotAt(AUG_23 + 15_000));
    assert.equal(found.get(`${SELF}|a`), 2);
    assert.equal(found.get(`${SELF}|b`), 9);
    assert.equal(found.get("vessels.urn:mrn:imo:mmsi:200000001|a"), 7);
  });

  it("labels the own vessel the way the snapshot route reads it", async () => {
    // The route maps `self` in the URL to the vessel's id before walking the
    // tree it builds from these deltas, so a delta labelled "self" — which is
    // how the store spells it — would build a tree the route cannot find.
    record(sample({ ts: AUG_23 }));
    const deltas = await snapshotAt(AUG_23);
    assert.deepEqual(
      deltas.map((delta) => delta.context),
      [SELF],
    );
  });

  it("replays each recorded kind as the value it was", async () => {
    record(
      sample({ ts: AUG_23, path: "n", value: 4.5 }),
      sample({ ts: AUG_23, path: "s", kind: "string", value: "moored" }),
      sample({ ts: AUG_23, path: "b", kind: "boolean", value: "true" }),
      sample({ ts: AUG_23, path: "t", kind: "string", value: "true" }),
      sample({ ts: AUG_23, path: "navigation.position", kind: "position" }),
      sample({ ts: AUG_23, path: "name", kind: "identity", value: "Kaikki" }),
    );

    const found = valuesByKey(await snapshotAt(AUG_23));
    assert.equal(found.get(`${SELF}|n`), 4.5);
    assert.equal(found.get(`${SELF}|s`), "moored");
    assert.equal(found.get(`${SELF}|b`), true);
    // Tagged `string`, so it stays the word rather than becoming a boolean.
    assert.equal(found.get(`${SELF}|t`), "true");
    assert.deepEqual(found.get(`${SELF}|navigation.position`), {
      latitude: 60.16,
      longitude: 24.94,
    });
    // An identity replays in the empty-path shape it arrived in.
    assert.deepEqual(found.get(`${SELF}|`), { name: "Kaikki" });
  });

  it("holds a path whose newest row is many rolls older than the instant", async () => {
    // What the sidecar is for: `quiet` last reported before two more rolls
    // happened, and its date directory is far outside the snapshot's scan
    // window. A backward walk bounded by days would miss it; the sidecar
    // carries one row per key, whenever it was written.
    record(sample({ ts: AUG_23, path: "quiet", value: 1 }));
    await rollAndTruncate();
    record(sample({ ts: AUG_23 + 3 * DAY, path: "busy", value: 2 }));
    await rollAndTruncate();
    record(sample({ ts: AUG_23 + 6 * DAY, path: "busy", value: 3 }));
    await rollAndTruncate();

    const found = valuesByKey(await snapshotAt(AUG_23 + 7 * DAY));
    assert.equal(found.get(`${SELF}|quiet`), 1);
    assert.equal(found.get(`${SELF}|busy`), 3);
  });

  it("omits a path that has no row before the instant", async () => {
    record(sample({ ts: AUG_23 + DAY, path: "later", value: 1 }));
    record(sample({ ts: AUG_23, path: "earlier", value: 2 }));

    const found = valuesByKey(await snapshotAt(AUG_23 + 1000));
    assert.equal(found.get(`${SELF}|later`), undefined);
    assert.equal(found.get(`${SELF}|earlier`), 2);
  });

  it("resolves a superseded path from the day before the instant", async () => {
    // The sidecar's row for `p` is newer than the instant, so it cannot answer
    // this one and the tree is read. The row that does answer it is one day
    // back, which is inside the scan window.
    record(sample({ ts: AUG_23, path: "p", value: 1 }));
    record(sample({ ts: AUG_23 + 2 * DAY, path: "p", value: 2 }));
    await rollAndTruncate();

    const found = valuesByKey(await snapshotAt(AUG_23 + DAY + 1000));
    assert.equal(found.get(`${SELF}|p`), 1);
  });

  it("omits a superseded path whose earlier row is past the scan bound", async () => {
    // The stated rule, asserted: the sidecar's row for `p` is newer than the
    // instant, and the row that would answer it is four days back — outside the
    // window a snapshot reads. It is absent rather than searched for.
    record(sample({ ts: AUG_23, path: "p", value: 1 }));
    record(sample({ ts: AUG_23 + 6 * DAY, path: "p", value: 2 }));
    await rollAndTruncate();

    const found = valuesByKey(await snapshotAt(AUG_23 + 4 * DAY));
    assert.equal(found.get(`${SELF}|p`), undefined);
  });

  it("answers with nothing when the instant is not a time", async () => {
    record(sample({ ts: AUG_23 }));
    assert.deepEqual(await snapshotAt(Number.NaN), []);
  });
});

describe("streamHistory", { skip: NO_BUNDLED_EXTENSION }, () => {
  it("replays a window in order, across chunk boundaries", async () => {
    // Three samples in three different 60-second chunks, so the replay only
    // completes if the cursor advances past an empty window as well as a full
    // one.
    record(
      sample({ ts: AUG_23, path: "a", value: 1 }),
      sample({ ts: AUG_23 + 30_000, path: "a", value: 2 }),
      sample({ ts: AUG_23 + 150_000, path: "a", value: 3 }),
    );

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(
        () => client.writes.length >= 3,
        "three deltas to be replayed",
      );
      assert.deepEqual(replayed(client.writes), [
        ["a", 1],
        ["a", 2],
        ["a", 3],
      ]);
      assert.deepEqual(
        client.writes.map((delta) => delta.updates[0].timestamp),
        [
          new Date(AUG_23).toISOString(),
          new Date(AUG_23 + 30_000).toISOString(),
          new Date(AUG_23 + 150_000).toISOString(),
        ],
      );
    } finally {
      stop();
    }
  });

  it("replays every context, not just the own vessel", async () => {
    const other = "vessels.urn:mrn:imo:mmsi:200000001";
    record(
      sample({ ts: AUG_23, path: "a", value: 1 }),
      sample({ ts: AUG_23 + 1000, context: other, path: "a", value: 2 }),
    );

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(() => client.writes.length >= 2, "both vessels");
      assert.deepEqual(
        client.writes.map((delta) => delta.context),
        [SELF, other],
      );
    } finally {
      stop();
    }
  });

  it("gives each source its own update rather than one $source for both", async () => {
    record(
      sample({ ts: AUG_23, path: "a", source: "n2k.0", value: 1 }),
      sample({ ts: AUG_23, path: "a", source: "gps.1", value: 2 }),
    );

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(
        () => client.writes.length >= 2,
        "one update per source",
      );
      assert.deepEqual(
        client.writes.map((delta) => delta.updates[0].$source).sort(),
        ["gps.1", "n2k.0"],
      );
    } finally {
      stop();
    }
  });

  it("labels a vessel from before the window, then replays its data", async () => {
    // The name was recorded days before the playback starts, which is the
    // normal case: identity reports repeat on their own cadence and the
    // recorder writes only changes. Without the injected label the target
    // appears on a plotter unnamed.
    const other = "vessels.urn:mrn:imo:mmsi:200000001";
    record(
      sample({
        ts: AUG_23 - 2 * DAY,
        context: other,
        path: "name",
        kind: "identity",
        value: "Sirius",
      }),
    );
    await rollAndTruncate();
    record(sample({ ts: AUG_23, context: other, path: "a", value: 1 }));

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(() => client.writes.length >= 2, "a label and a value");
      assert.deepEqual(replayed(client.writes), [
        ["", { name: "Sirius" }],
        ["a", 1],
      ]);
      assert.deepEqual(
        client.writes.map((delta) => delta.context),
        [other, other],
      );
    } finally {
      stop();
    }
  });

  it("replays a name recorded inside the window as the delta it was", async () => {
    record(
      sample({ ts: AUG_23, path: "name", kind: "identity", value: "Kaikki" }),
    );

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(() => client.writes.length >= 1, "the name delta");
      // The label and the row are the same value, so the label is suppressed
      // by nothing and both may appear; what matters is the shape.
      assert.deepEqual(replayed(client.writes)[0], ["", { name: "Kaikki" }]);
    } finally {
      stop();
    }
  });

  it("replays a boolean as a boolean", async () => {
    record(
      sample({ ts: AUG_23, path: "b", kind: "boolean", value: "false" }),
      sample({ ts: AUG_23 + 1000, path: "t", kind: "string", value: "true" }),
    );

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(() => client.writes.length >= 2, "both values");
      assert.deepEqual(replayed(client.writes), [
        ["b", false],
        ["t", "true"],
      ]);
    } finally {
      stop();
    }
  });

  it("drains a window that holds more rows than one read returns", async () => {
    // A live install already reaches ~6k rows in a 60-second window. Advancing
    // to the end of the window after a truncated read would drop the rest of it
    // silently, so the cursor resumes inside the window instead.
    const rows: Sample[] = [];
    for (let i = 0; i < 10_500; i += 1) {
      rows.push(sample({ ts: AUG_23 + i, path: "a", value: i }));
    }
    record(...rows);

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(
        () => replayed(client.writes).length >= 10_500,
        "the whole window",
        30_000,
      );
      const values = replayed(client.writes).map(([, value]) => value);
      // Every sample arrives, and in order. A resumed read may repeat the
      // millisecond it resumed at, which is harmless on replay, so this checks
      // coverage rather than an exact count.
      assert.equal(new Set(values).size, 10_500);
      assert.equal(values[0], 0);
    } finally {
      stop();
    }
  });

  it("keeps one query service for the whole session", async () => {
    // The measurement the unit asks for: playback reads a window per chunk, and
    // every one of them goes to the engine that is already running. A process
    // per chunk was the alternative Unit 4a measured and rejected.
    record(
      sample({ ts: AUG_23, path: "a", value: 1 }),
      sample({ ts: AUG_23 + 61_000, path: "a", value: 2 }),
      sample({ ts: AUG_23 + 121_000, path: "a", value: 3 }),
    );

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(() => client.writes.length >= 3, "three chunks read");
      assert.equal(spawns, 1);
    } finally {
      stop();
    }
  });

  it("falls back to real time for a rate that is not a number", async () => {
    // The server passes `spark.query.playbackRate || 1` straight through, and a
    // query string is text. Without the coercion this replays every window with
    // no delay between them — the whole recording, as fast as the service can
    // answer, because `setTimeout(fn, NaN)` fires immediately.
    record(
      sample({ ts: AUG_23, path: "a", value: 1 }),
      sample({ ts: AUG_23 + 61_000, path: "a", value: 2 }),
    );

    const client = fakeSpark();
    const stop = history.streamHistory(
      client.spark,
      {
        startTime: new Date(AUG_23),
        playbackRate: "fast" as unknown as number,
      },
      () => {},
    );
    try {
      await eventually(() => client.writes.length >= 1, "the first window");
      await new Promise((resolve) => setTimeout(resolve, 100));
      // At rate 1 the next window is a minute away, not now.
      assert.equal(client.writes.length, 1);
    } finally {
      stop();
    }
  });

  it("stops replaying when the client disconnects", async () => {
    record(
      sample({ ts: AUG_23, path: "a", value: 1 }),
      sample({ ts: AUG_23 + 61_000, path: "a", value: 2 }),
    );

    const client = fakeSpark();
    history.streamHistory(client.spark, ask(AUG_23), () => {});
    await eventually(() => client.writes.length >= 1, "the first delta");
    client.end();

    const seen = client.writes.length;
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(client.writes.length, seen);
  });

  it("resumes where a disconnected client left off", async () => {
    record(
      sample({ ts: AUG_23, path: "a", value: 1 }),
      sample({ ts: AUG_23 + 61_000, path: "a", value: 2 }),
    );

    const first = fakeSpark();
    const stopFirst = history.streamHistory(first.spark, ask(AUG_23), () => {});
    await eventually(() => first.writes.length >= 1, "the first delta");
    stopFirst();

    const resumed = fakeSpark();
    const stop = history.streamHistory(
      resumed.spark,
      ask(AUG_23 + 61_000),
      () => {},
    );
    try {
      await eventually(() => resumed.writes.length >= 1, "the second delta");
      assert.deepEqual(replayed(resumed.writes), [["a", 2]]);
    } finally {
      stop();
    }
  });

  it("retries the window it could not read rather than skipping it", async () => {
    record(sample({ ts: AUG_23, path: "a", value: 1 }));
    // A store the engine cannot open. The window must be read again once it
    // can, because skipping it hands the client a gap it has no way to notice.
    const storePath = writerPaths(dir).store;
    store.close();
    renameSync(storePath, `${storePath}.away`);
    mkdirSync(storePath);

    const client = fakeSpark();
    const stop = history.streamHistory(client.spark, ask(AUG_23), () => {});
    try {
      await eventually(
        () => debugLines.some((line) => line.startsWith("streamHistory error")),
        "the failure to be reported",
      );
      assert.equal(client.writes.length, 0);

      rmSync(storePath, { recursive: true });
      renameSync(`${storePath}.away`, storePath);

      // The same window, read again and delivered — not the next one.
      await eventually(() => client.writes.length >= 1, "the retried window");
      assert.deepEqual(replayed(client.writes), [["a", 1]]);
    } finally {
      stop();
    }
  });

  it("waits for a window that has not happened yet", async () => {
    // Otherwise a replay that catches up with real time never stops: the
    // window ahead of now is empty because it is in the future, the cursor
    // advances past it, and the next one is further ahead still — a query
    // every 100 ms per client, for ever, against the one shared service.
    const now = Date.now();
    const started = now - CHUNK_SECONDS * 1000 - 5000;
    record(sample({ ts: started + 1000, path: "a", value: 1 }));

    const counted = countingRunner();
    const provider = createHistoryProviderV1(counted.runner, SELF, (line) =>
      debugLines.push(line),
    );

    const client = fakeSpark();
    const stop = provider.streamHistory(client.spark, ask(started), () => {});
    try {
      // The first window is over, so it is read and replayed. The second ends
      // about 55 seconds from now, so it is not read at all.
      await eventually(() => client.writes.length >= 1, "the complete window");
      const after = counted.runs();
      await new Promise((resolve) => setTimeout(resolve, 300));
      assert.equal(counted.runs(), after, "the future window was read anyway");
      assert.equal(client.writes.length, 1);
    } finally {
      stop();
    }
  });

  it("does not replay from a start time that is not a time", async () => {
    record(sample({ ts: AUG_23, path: "a", value: 1 }));

    const counted = countingRunner();
    const provider = createHistoryProviderV1(counted.runner, SELF, (line) =>
      debugLines.push(line),
    );

    const client = fakeSpark();
    const stop = provider.streamHistory(
      client.spark,
      { startTime: new Date("not a date"), playbackRate: 1 },
      () => {},
    );
    try {
      await new Promise((resolve) => setTimeout(resolve, 100));
      // The queries, not the log: starting the engine takes the better part of
      // a second, so a request that is going to be refused has not been refused
      // yet at the moment a log assertion would run — and the test would pass
      // whether or not the guard exists. Without it every read is refused and
      // every refusal reschedules, which is a query a second for as long as the
      // client stays connected.
      assert.equal(counted.runs(), 0, "a replay was started anyway");
      assert.equal(client.writes.length, 0);
    } finally {
      stop();
    }
  });
});
