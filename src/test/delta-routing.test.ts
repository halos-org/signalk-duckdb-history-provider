import { describe, it } from "node:test";
import assert from "node:assert";
import {
  extractVesselName,
  flattenObjectValue,
  routeDeltaValue,
} from "../delta-routing.js";

describe("routeDeltaValue", () => {
  it("routes numbers to the number kind", () => {
    assert.strictEqual(
      routeDeltaValue("environment.depth.belowKeel", 4.2),
      "number",
    );
  });

  it("routes booleans to the string table", () => {
    // Switch/relay, pump and valve states, autopilot flags: these used to
    // fall through to null and be dropped without a trace
    // (dirkwa/signalk-questdb#79).
    assert.equal(
      routeDeltaValue("watermaker.brineomatic.high_pressure_pump_on", true),
      "boolean",
    );
    assert.equal(
      routeDeltaValue("electrical.switches.bilgePump.state", false),
      "boolean",
    );
  });

  it("routes strings to the string table", () => {
    assert.strictEqual(
      routeDeltaValue("navigation.state", "anchored"),
      "string",
    );
  });

  it("routes navigation.position to the position table", () => {
    assert.strictEqual(
      routeDeltaValue("navigation.position", {
        latitude: 52.5,
        longitude: 13.4,
      }),
      "position",
    );
  });

  it("does NOT route other lat/lon-object paths to the position table", () => {
    // navigation.anchor.position is re-emitted on every fix by anchor plugins
    // while watching; letting it into the path-less signalk_position table
    // interleaves it with the real vessel track. They flatten instead, so the
    // anchor's coordinates are still recorded — as their own dotted paths,
    // where they cannot be confused with the vessel track
    // (dirkwa/signalk-questdb#128).
    for (const path of [
      "navigation.anchor.position",
      "navigation.courseGreatCircle.nextPoint.position",
      "steering.autopilot.target.position",
    ]) {
      assert.strictEqual(
        routeDeltaValue(path, { latitude: 12.05, longitude: -61.75 }),
        "flatten",
        path,
      );
    }
  });

  it("flattens objects that are not a usable position", () => {
    // A boolean is a recordable value, not an absence.
    // A half-position is not a track point, but its scalar leaves are still
    // real readings, so they are recorded as dotted paths like anything else.
    assert.strictEqual(
      routeDeltaValue("navigation.position", { latitude: 1 }),
      "flatten",
    );
    assert.strictEqual(
      routeDeltaValue("navigation.attitude", { roll: 0.1, pitch: 0 }),
      "flatten",
    );
    assert.strictEqual(routeDeltaValue("navigation.position", null), null);
  });

  it("keeps non-finite or non-numeric coordinates out of the position kind", () => {
    // A NaN latitude is not a track point. The object still flattens, so the
    // usable leaf beside it survives — and flattenObjectValue drops the
    // non-finite one, so no coordinate that means nothing gets stored.
    for (const value of [
      { latitude: NaN, longitude: 13.4 },
      { latitude: "52.5", longitude: 13.4 },
      { latitude: 52.5, longitude: Infinity },
    ]) {
      // Asserts "flatten", not merely "not position": the weaker form would
      // also pass if these were dropped entirely, which is the outcome that
      // matters.
      assert.strictEqual(
        routeDeltaValue("navigation.position", value),
        "flatten",
        JSON.stringify(value),
      );
    }
  });

  it("refuses non-finite top-level numbers", () => {
    // The storage will not catch these: SQLite holds NaN and ±Infinity in a
    // REAL column and Parquet holds them in a DOUBLE, so recording one
    // poisons every aggregate over that path. A source reporting NaN is
    // reporting "no reading", and this rule is the only thing dropping it.
    for (const value of [NaN, Infinity, -Infinity]) {
      assert.strictEqual(
        routeDeltaValue("environment.depth.belowKeel", value),
        null,
        String(value),
      );
    }
    assert.strictEqual(
      routeDeltaValue("environment.depth.belowKeel", 4.2),
      "number",
      "finite numbers are unaffected",
    );
  });

  it("does not flatten arrays", () => {
    // Array indices are not stable identities, so `foo.0` would mean a
    // different thing from one delta to the next.
    assert.strictEqual(routeDeltaValue("some.list", [1, 2, 3]), null);
    assert.strictEqual(routeDeltaValue("some.list", []), null);
  });
});

describe("flattenObjectValue (dirkwa/signalk-questdb#128)", () => {
  it("pulls each scalar leaf out as its own dotted path", () => {
    const leaves = flattenObjectValue("navigation.attitude", {
      roll: 0.02,
      pitch: -0.01,
      yaw: 1.57,
    });

    assert.deepStrictEqual(leaves, [
      { path: "navigation.attitude.roll", value: 0.02 },
      { path: "navigation.attitude.pitch", value: -0.01 },
      { path: "navigation.attitude.yaw", value: 1.57 },
    ]);
  });

  it("keeps string and boolean leaves, not just numbers", () => {
    const leaves = flattenObjectValue("some.thing", {
      count: 3,
      label: "port",
      active: true,
    });

    assert.deepStrictEqual(leaves, [
      { path: "some.thing.count", value: 3 },
      { path: "some.thing.label", value: "port" },
      { path: "some.thing.active", value: true },
    ]);
  });

  it("does not write non-finite numbers", () => {
    // A non-finite leaf is "no reading", same rule as the top-level path.
    const leaves = flattenObjectValue("sensor.x", {
      good: 1.5,
      bad: NaN,
      worse: Infinity,
    });

    assert.deepStrictEqual(leaves, [{ path: "sensor.x.good", value: 1.5 }]);
  });

  it("does not descend into nested objects", () => {
    // One level deep deliberately: a recursive walk would write out whole
    // notification and resource payloads nobody asked to record.
    const leaves = flattenObjectValue("a.b", {
      flat: 1,
      nested: { deep: 2 },
      list: [1, 2],
    });

    assert.deepStrictEqual(leaves, [{ path: "a.b.flat", value: 1 }]);
  });

  it("drops null and undefined leaves", () => {
    const leaves = flattenObjectValue("a.b", {
      present: 1,
      empty: null,
      missing: undefined,
    });

    assert.deepStrictEqual(leaves, [{ path: "a.b.present", value: 1 }]);
  });

  it("yields nothing for a non-object, an array or an empty object", () => {
    for (const value of [null, 42, "x", [1, 2], {}]) {
      const leaves = flattenObjectValue("a.b", value);
      assert.deepStrictEqual(leaves, [], JSON.stringify(value));
    }
  });

  it("yields nothing for an empty parent path", () => {
    // Would otherwise build ".name" — a leading-dot path matching no Signal K
    // path and no filter pattern. Empty-path deltas are identity reports,
    // handled by extractVesselName; this function must not depend on the
    // caller guarding that.
    const leaves = flattenObjectValue("", {
      name: "SEA BREEZE",
      mmsi: "244813000",
    });

    assert.deepStrictEqual(leaves, []);
  });

  it("records the anchor position's coordinates under its own path", () => {
    // The case that must NOT become a position row: the leaves are recorded
    // where they cannot be confused with the vessel track.
    const leaves = flattenObjectValue("navigation.anchor.position", {
      latitude: 12.05,
      longitude: -61.75,
    });

    assert.deepStrictEqual(leaves, [
      { path: "navigation.anchor.position.latitude", value: 12.05 },
      { path: "navigation.anchor.position.longitude", value: -61.75 },
    ]);
  });
});

describe("extractVesselName", () => {
  it("extracts the name from an empty-path object delta", () => {
    assert.strictEqual(
      extractVesselName("", { name: "Sea Breeze" }),
      "Sea Breeze",
    );
    // Identity reports carry siblings alongside the name.
    assert.strictEqual(
      extractVesselName("", { name: "Sea Breeze", mmsi: "244813000" }),
      "Sea Breeze",
    );
  });

  it("ignores non-empty paths — a data path named name stays data", () => {
    assert.strictEqual(extractVesselName("name", "Sea Breeze"), null);
    assert.strictEqual(
      extractVesselName("navigation.state", { name: "x" }),
      null,
    );
  });

  it("ignores empty-path deltas without a usable name", () => {
    assert.strictEqual(extractVesselName("", { mmsi: "244813000" }), null);
    assert.strictEqual(extractVesselName("", { name: "" }), null);
    assert.strictEqual(extractVesselName("", { name: "   " }), null);
    assert.strictEqual(extractVesselName("", { name: 42 }), null);
    assert.strictEqual(extractVesselName("", null), null);
    assert.strictEqual(extractVesselName("", "just a string"), null);
  });
});
