import { describe, it } from "node:test";
import assert from "node:assert";
import { Recorder, SELF_CONTEXT } from "../recorder.js";
import type { BusValue } from "../recorder.js";
import { normalizeConfig } from "../config/schema.js";
import type { StoredConfig } from "../config/schema.js";
import type { Sample } from "../writer/protocol.js";

const SELF = "vessels.urn:mrn:signalk:uuid:self";
const OTHER = "vessels.urn:mrn:imo:mmsi:244813000";

/** A recorder plus the samples it emitted and the lines it logged. */
function build(config: StoredConfig = {}, startAt = 0) {
  const samples: Sample[] = [];
  const lines: string[] = [];
  let clock = startAt;
  const recorder = new Recorder({
    config: normalizeConfig(config),
    selfContext: SELF,
    emit: (sample) => samples.push(sample),
    now: () => clock,
    log: (line) => lines.push(line),
  });
  return {
    recorder,
    samples,
    lines,
    at(ms: number) {
      clock = ms;
    },
    feed(over: Partial<BusValue> = {}) {
      recorder.handle({
        context: SELF,
        path: "environment.depth.belowKeel",
        value: 4.2,
        $source: "n2k.0",
        ...over,
      });
    },
  };
}

describe("what reaches the writer", () => {
  it("records a scalar with the context shortened to self", () => {
    const t = build({ defaultSamplingRate: 0 });
    t.feed();

    assert.deepStrictEqual(t.samples, [
      {
        ts: 0,
        context: SELF_CONTEXT,
        path: "environment.depth.belowKeel",
        source: "n2k.0",
        kind: "number",
        value: 4.2,
      },
    ]);
  });

  it("stamps the server's receive time, not the delta's own", () => {
    // A boat is a set of independent clocks -- GPS time, RTC-less devices,
    // gateway latencies -- and per-source timestamps make commits land out of
    // order. Every commit being a pure append is what keeps the write cost
    // down, and a device with a broken clock even gets more accurate history.
    const t = build({ defaultSamplingRate: 0 }, 1_700_000_000_000);
    t.feed();

    assert.strictEqual(t.samples[0].ts, 1_700_000_000_000);
  });

  it("records a boolean as text under the boolean kind", () => {
    const t = build({ defaultSamplingRate: 0 });
    t.feed({ path: "electrical.switches.anchorLight.state", value: true });

    assert.strictEqual(t.samples[0].kind, "boolean");
    assert.strictEqual(t.samples[0].value, "true");
  });

  it("records navigation.position as a position", () => {
    const t = build({ defaultSamplingRate: 0 });
    t.feed({
      path: "navigation.position",
      value: { latitude: 60.16, longitude: 24.94 },
    });

    assert.deepStrictEqual(t.samples[0].value, {
      latitude: 60.16,
      longitude: 24.94,
    });
    assert.strictEqual(t.samples[0].kind, "position");
  });

  it("records an object's scalar leaves under their own paths", () => {
    const t = build({ defaultSamplingRate: 0 });
    t.feed({
      path: "navigation.attitude",
      value: { roll: 0.02, yaw: 1.57, nested: { no: 1 } },
    });

    assert.deepStrictEqual(
      t.samples.map((s) => [s.path, s.value]),
      [
        ["navigation.attitude.roll", 0.02],
        ["navigation.attitude.yaw", 1.57],
      ],
    );
  });

  it("keeps a missing source null rather than inventing one", () => {
    const t = build({ defaultSamplingRate: 0 });
    t.feed({ $source: undefined });

    assert.strictEqual(t.samples[0].source, null);
  });

  it("records nothing for a null or undefined value", () => {
    const t = build({ defaultSamplingRate: 0 });
    t.feed({ value: null });
    t.feed({ value: undefined });

    assert.deepStrictEqual(t.samples, []);
  });
});

describe("vessel identity", () => {
  it("records an empty-path name delta under the identity kind", () => {
    const t = build({ recordOthers: true });
    t.recorder.handle({
      context: OTHER,
      path: "",
      value: { name: "SEA BREEZE", mmsi: "244813000" },
      $source: "ais.0",
    });

    assert.deepStrictEqual(t.samples, [
      {
        ts: 0,
        context: OTHER,
        path: "name",
        source: "ais.0",
        kind: "identity",
        value: "SEA BREEZE",
      },
    ]);
  });

  it("records a name once per context until it changes", () => {
    // AIS static data repeats every few minutes for every target in range.
    // Without the dedupe that is a row per target per repeat, forever.
    const t = build({ recordOthers: true });
    const identity = (name: string) => ({
      context: OTHER,
      path: "",
      value: { name },
      $source: "ais.0",
    });

    t.recorder.handle(identity("SEA BREEZE"));
    t.recorder.handle(identity("SEA BREEZE"));
    t.recorder.handle(identity("SEA BREEZE II"));

    assert.deepStrictEqual(
      t.samples.map((s) => s.value),
      ["SEA BREEZE", "SEA BREEZE II"],
    );
  });

  it("skips a name for a vessel whose data is not recorded", () => {
    // Measured on a device: with the default recordOthers off, 16 contexts
    // held nothing but an identity row each -- AIS vessels, meteo stations,
    // navigation aids -- and every one becomes a partition in the roll for one
    // or two rows. A name for a vessel nothing else is recorded for answers no
    // query, and recording it contradicts the setting the operator chose.
    const t = build({ recordOthers: false });
    t.recorder.handle({
      context: OTHER,
      path: "",
      value: { name: "SEA BREEZE" },
    });
    t.recorder.handle({
      context: "atons.urn:mrn:imo:mmsi:992306506",
      path: "",
      value: { name: "KUSTAANMIEKKA" },
    });

    assert.deepStrictEqual(t.samples, []);
    assert.strictEqual(t.recorder.stats.contexts, 0, "no partition was opened");
  });

  it("records the own vessel's name whatever recordOthers says", () => {
    const t = build({ recordOthers: false });
    t.recorder.handle({
      context: SELF,
      path: "",
      value: { name: "OWN VESSEL" },
    });

    assert.strictEqual(t.samples.length, 1);
    assert.strictEqual(t.samples[0].context, SELF_CONTEXT);
  });

  it("records identity even when an include filter names other paths", () => {
    // Identity is not a data stream. An include-mode filter listing only data
    // paths would otherwise silently disable vessel names, and Freeboard reads
    // names from nowhere else.
    const t = build({
      recordOthers: true,
      pathFilter: { mode: "include", paths: ["navigation.*"] },
    });
    t.recorder.handle({
      context: OTHER,
      path: "",
      value: { name: "SEA BREEZE" },
    });

    assert.strictEqual(t.samples.length, 1);
    assert.strictEqual(t.samples[0].kind, "identity");
  });
});

describe("which vessels are recorded", () => {
  it("skips other vessels by default", () => {
    // Off by default because AIS adds a context per target, and the roll holds
    // one Parquet writer per partition.
    const t = build({ defaultSamplingRate: 0 });
    t.feed({ context: OTHER });

    assert.deepStrictEqual(t.samples, []);
  });

  it("records other vessels under their own context when asked", () => {
    const t = build({ defaultSamplingRate: 0, recordOthers: true });
    t.feed({ context: OTHER });

    assert.strictEqual(t.samples[0].context, OTHER);
  });

  it("skips the own vessel when recordSelf is off", () => {
    const t = build({ defaultSamplingRate: 0, recordSelf: false });
    t.feed();

    assert.deepStrictEqual(t.samples, []);
  });
});

describe("the path filter", () => {
  it("excludes what it names, in exclude mode", () => {
    const t = build({
      defaultSamplingRate: 0,
      pathFilter: { mode: "exclude", paths: ["notifications.*"] },
    });
    t.feed({ path: "notifications.mob" });
    t.feed({ path: "navigation.speedOverGround", value: 3.1 });

    assert.deepStrictEqual(
      t.samples.map((s) => s.path),
      ["navigation.speedOverGround"],
    );
  });

  it("admits only what it names, in include mode", () => {
    const t = build({
      defaultSamplingRate: 0,
      pathFilter: { mode: "include", paths: ["navigation.*"] },
    });
    t.feed({ path: "notifications.mob" });
    t.feed({ path: "navigation.speedOverGround", value: 3.1 });

    assert.deepStrictEqual(
      t.samples.map((s) => s.path),
      ["navigation.speedOverGround"],
    );
  });

  it("gates an object's leaves, not the object's own path", () => {
    // An include filter naming only a leaf would otherwise drop the parent
    // before any leaf was seen.
    const t = build({
      defaultSamplingRate: 0,
      pathFilter: { mode: "include", paths: ["navigation.attitude.roll"] },
    });
    t.feed({ path: "navigation.attitude", value: { roll: 0.02, yaw: 1.57 } });

    assert.deepStrictEqual(
      t.samples.map((s) => s.path),
      ["navigation.attitude.roll"],
    );
  });
});

describe("the rate cap", () => {
  it("drops a repeat inside the sampling interval", () => {
    const t = build({ defaultSamplingRate: 2000 }, 1_000_000);
    t.feed();
    t.at(1_001_999);
    t.feed();
    t.at(1_002_000);
    t.feed();

    assert.strictEqual(t.samples.length, 2);
  });

  it("caps each vessel's stream, not every target's together", () => {
    // Keyed on path AND context. Keying on path alone would let one busy AIS
    // target consume the whole allowance and starve the boat's own instrument.
    const t = build(
      { defaultSamplingRate: 2000, recordOthers: true },
      1_000_000,
    );
    t.feed();
    t.feed({ context: OTHER });

    assert.deepStrictEqual(
      t.samples.map((s) => s.context),
      [SELF_CONTEXT, OTHER],
    );
  });

  it("takes a per-path override over the default", () => {
    const t = build(
      {
        defaultSamplingRate: 60_000,
        samplingRates: { "environment.wind.*": 100 },
      },
      1_000_000,
    );
    t.feed({ path: "environment.wind.speedApparent", value: 5 });
    t.at(1_000_100);
    t.feed({ path: "environment.wind.speedApparent", value: 5.5 });

    assert.strictEqual(t.samples.length, 2);
  });
});

describe("the cardinality cap", () => {
  it("stops recording new paths at the cap and keeps the ones it has", () => {
    // Bounded by not recording, not by rolling: the roll holds one Parquet
    // writer per partition, and that sets its memory peak.
    const t = build({ defaultSamplingRate: 0, maxRecordedPaths: 2 });
    t.feed({ path: "a.one", value: 1 });
    t.feed({ path: "a.two", value: 2 });
    t.feed({ path: "a.three", value: 3 });
    t.feed({ path: "a.one", value: 4 });

    assert.deepStrictEqual(
      t.samples.map((s) => s.path),
      ["a.one", "a.two", "a.one"],
    );
    assert.strictEqual(t.recorder.stats.pathsOverCap, 1);
    assert.strictEqual(t.recorder.stats.paths, 2);
  });

  it("does not spend a context slot on a vessel whose paths are all filtered", () => {
    // The cap bounds partition count, and a context that produces no row
    // produces no partition. Spending the budget on vessels that record
    // nothing means the one that would have recorded is refused.
    const t = build({
      defaultSamplingRate: 0,
      recordOthers: true,
      maxRecordedContexts: 1,
      pathFilter: { mode: "include", paths: ["navigation.position"] },
    });

    // A target broadcasting only excluded paths.
    t.feed({
      context: OTHER,
      path: "environment.wind.speedApparent",
      value: 5,
    });
    assert.strictEqual(t.recorder.stats.contexts, 0, "a slot was spent");

    // The vessel that does broadcast a recorded path still gets the slot.
    t.feed({
      context: "vessels.urn:mrn:imo:mmsi:111111111",
      path: "navigation.position",
      value: { latitude: 60.16, longitude: 24.94 },
    });
    assert.strictEqual(t.samples.length, 1);
    assert.strictEqual(t.recorder.stats.contexts, 1);
  });

  it("stops recording new contexts at the cap", () => {
    const t = build({
      defaultSamplingRate: 0,
      recordOthers: true,
      maxRecordedContexts: 1,
    });
    t.feed();
    t.feed({ context: OTHER });

    assert.deepStrictEqual(
      t.samples.map((s) => s.context),
      [SELF_CONTEXT],
    );
    assert.strictEqual(t.recorder.stats.contextsOverCap, 1);
  });

  it("counts what it dropped instead of dropping it silently", () => {
    const t = build({ defaultSamplingRate: 0, maxRecordedPaths: 1 });
    t.feed({ path: "a.one", value: 1 });
    for (let i = 0; i < 50; i++) t.feed({ path: `flood.${i}`, value: i });

    assert.strictEqual(t.recorder.stats.pathsOverCap, 50);
    assert.ok(
      t.lines.length > 0 && t.lines.length <= 6,
      `expected a few named lines, got ${t.lines.length}`,
    );
    assert.match(t.lines[0], /over the cardinality cap/);
    assert.match(
      t.lines[t.lines.length - 1],
      /counted in the plugin status rather than logged/,
    );
  });

  it("bounds identity rows by the context cap too", () => {
    // Otherwise a flood of AIS names walks straight past the bound the cap
    // exists to enforce, since identity skips the filter and the rate cap.
    const t = build({ recordOthers: true, maxRecordedContexts: 1 });
    t.feed();
    for (let i = 0; i < 10; i++) {
      t.recorder.handle({
        context: `vessels.urn:mrn:imo:mmsi:${i}`,
        path: "",
        value: { name: `TARGET ${i}` },
      });
    }

    assert.deepStrictEqual(
      t.samples.map((s) => s.kind),
      ["number"],
    );
    assert.strictEqual(t.recorder.stats.contextsOverCap, 10);
  });
});
