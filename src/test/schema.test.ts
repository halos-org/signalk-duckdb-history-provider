import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  CONFIG_DEFAULTS,
  Config,
  ConfigSchema,
  normalizeConfig,
} from "../config/schema.js";

// A configuration that came out of the Admin UI form, with every key present.
const complete: Config = {
  pathFilter: { mode: "include", paths: ["navigation.*"] },
  defaultSamplingRate: 500,
  samplingRates: { "environment.wind.*": 200 },
  recordSelf: true,
  recordOthers: true,
  maxRecordedPaths: 50,
  maxRecordedContexts: 5,
  flushIntervalMs: 2000,
  flushBatchSize: 500,
  maxBufferMB: 4,
  dataDir: "/var/lib/history",
  retentionDays: 30,
  rollIntervalMinutes: 15,
};

/**
 * The default hourly compaction depends on, asserted as a literal rather than
 * through `CONFIG_DEFAULTS`. Compaction always runs, so the interval is the
 * only default that decides whether any merge happens: at 60 minutes or more
 * `compactClosedHour` returns before spawning anything, so reverting the
 * default to 60 leaves an hour holding one file and nothing to merge.
 */
describe("the default hourly compaction depends on", () => {
  it("rolls every five minutes, which is what makes a merge worth doing", () => {
    assert.equal(CONFIG_DEFAULTS.rollIntervalMinutes, 5);
    assert.equal(
      (
        ConfigSchema.properties.rollIntervalMinutes as unknown as {
          default: number;
        }
      ).default,
      5,
    );
  });
});

describe("ConfigSchema", () => {
  it("renders every option the plugin is configured with", () => {
    // The Admin UI form IS this schema, so a missing property is a missing
    // form field the operator can never set.
    const properties = Object.keys(ConfigSchema.properties);
    assert.deepEqual(properties.sort(), [
      "dataDir",
      "defaultSamplingRate",
      "flushBatchSize",
      "flushIntervalMs",
      "maxBufferMB",
      "maxRecordedContexts",
      "maxRecordedPaths",
      "pathFilter",
      "recordOthers",
      "recordSelf",
      "retentionDays",
      "rollIntervalMinutes",
      "samplingRates",
    ]);
  });

  it("carries a default for every option", () => {
    for (const [name, property] of Object.entries(ConfigSchema.properties)) {
      if (name === "pathFilter") {
        for (const [sub, subProperty] of Object.entries(
          (property as any).properties,
        )) {
          assert.notEqual(
            (subProperty as any).default,
            undefined,
            `pathFilter.${sub} has no default`,
          );
        }
        continue;
      }
      assert.notEqual(
        (property as any).default,
        undefined,
        `${name} has no default`,
      );
    }
  });

  it("agrees with CONFIG_DEFAULTS, which claims to be the only source", () => {
    // The two are maintained by hand and the numbers appear twice. Change one
    // and not the other, and the Admin UI form offers one default while a
    // saved config normalises to another — with every existing test green.
    const declared: Record<string, unknown> = {
      pathFilterMode: (ConfigSchema.properties.pathFilter as any).properties
        .mode.default,
      pathFilterPaths: (ConfigSchema.properties.pathFilter as any).properties
        .paths.default,
      defaultSamplingRate: (ConfigSchema.properties.defaultSamplingRate as any)
        .default,
      samplingRates: (ConfigSchema.properties.samplingRates as any).default,
      recordSelf: (ConfigSchema.properties.recordSelf as any).default,
      recordOthers: (ConfigSchema.properties.recordOthers as any).default,
      flushIntervalMs: (ConfigSchema.properties.flushIntervalMs as any).default,
      flushBatchSize: (ConfigSchema.properties.flushBatchSize as any).default,
      maxBufferMB: (ConfigSchema.properties.maxBufferMB as any).default,
      maxRecordedPaths: (ConfigSchema.properties.maxRecordedPaths as any)
        .default,
      maxRecordedContexts: (ConfigSchema.properties.maxRecordedContexts as any)
        .default,
      dataDir: (ConfigSchema.properties.dataDir as any).default,
      retentionDays: (ConfigSchema.properties.retentionDays as any).default,
      rollIntervalMinutes: (ConfigSchema.properties.rollIntervalMinutes as any)
        .default,
    };
    assert.deepEqual(declared, { ...CONFIG_DEFAULTS });
  });

  it("defaults other vessels off", () => {
    // Every AIS target is a context, and the roll holds a Parquet writer per
    // partition — so this default is what bounds the roll's memory peak on a
    // busy waterway, not a preference.
    assert.equal((ConfigSchema.properties.recordOthers as any).default, false);
    assert.equal((ConfigSchema.properties.recordSelf as any).default, true);
  });
});

describe("normalizeConfig", () => {
  it("replaces a value of the wrong shape rather than passing it on", () => {
    // A hand-edited `"paths": "navigation.*"` is a string, and PathMatcher
    // iterates a string character by character — the `*` compiles to a glob
    // matching every path, which in exclude mode records nothing at all.
    const normalized = normalizeConfig({
      pathFilter: { mode: "invert" as any, paths: "navigation.*" as any },
      samplingRates: [1, 2] as any,
      dataDir: 42 as any,
    });
    assert.deepEqual(normalized.pathFilter.paths, []);
    assert.equal(normalized.pathFilter.mode, "exclude");
    assert.deepEqual(normalized.samplingRates, {});
    assert.equal(normalized.dataDir, "");
  });

  it("drops individual entries that are not usable", () => {
    const normalized = normalizeConfig({
      pathFilter: { mode: "include", paths: ["a.b", 7 as any, "c.d"] },
      samplingRates: { "a.*": 200, "b.*": "fast" as any, "c.*": Number.NaN },
    });
    assert.deepEqual(normalized.pathFilter.paths, ["a.b", "c.d"]);
    assert.deepEqual(normalized.samplingRates, { "a.*": 200 });
  });

  it("drops a per-path rate that is not an interval", () => {
    // The field is a minimum interval between samples, so 0 and negatives are
    // not values it can honour. Dropping them falls back to the default rate,
    // which is what the rate matcher does with them anyway.
    const normalized = normalizeConfig({
      samplingRates: { "a.*": -1, "b.*": 0, "c.*": 250 },
    });
    assert.deepEqual(normalized.samplingRates, { "c.*": 250 });
  });

  it("leaves a complete configuration alone", () => {
    assert.deepEqual(normalizeConfig(complete), complete);
  });

  it("fills in a configuration saved before an option existed", () => {
    const saved = {
      pathFilter: { mode: "exclude", paths: [] },
      defaultSamplingRate: 2000,
      samplingRates: {},
      recordSelf: true,
      recordOthers: false,
    } as unknown as Config;
    const normalized = normalizeConfig(saved);
    assert.equal(normalized.retentionDays, CONFIG_DEFAULTS.retentionDays);
    assert.equal(
      normalized.rollIntervalMinutes,
      CONFIG_DEFAULTS.rollIntervalMinutes,
    );
    assert.equal(normalized.maxRecordedPaths, CONFIG_DEFAULTS.maxRecordedPaths);
    assert.equal(
      normalized.maxRecordedContexts,
      CONFIG_DEFAULTS.maxRecordedContexts,
    );
    assert.equal(normalized.dataDir, "");
  });

  it("fills in a wholly empty configuration", () => {
    const normalized = normalizeConfig({} as Config);
    assert.deepEqual(normalized.pathFilter, { mode: "exclude", paths: [] });
    assert.deepEqual(normalized.samplingRates, {});
    assert.equal(normalized.recordSelf, true);
    assert.equal(normalized.recordOthers, false);
    assert.equal(
      normalized.defaultSamplingRate,
      CONFIG_DEFAULTS.defaultSamplingRate,
    );
  });

  it("honours an explicit false on the context toggles", () => {
    const normalized = normalizeConfig({
      ...complete,
      recordSelf: false,
      recordOthers: false,
    });
    assert.equal(normalized.recordSelf, false);
    assert.equal(normalized.recordOthers, false);
  });

  it("keeps zero where zero means something", () => {
    // 0 is "record every update" and "keep forever", not "unset".
    const normalized = normalizeConfig({
      ...complete,
      defaultSamplingRate: 0,
      retentionDays: 0,
    });
    assert.equal(normalized.defaultSamplingRate, 0);
    assert.equal(normalized.retentionDays, 0);
  });

  it("rejects values that would mean roll continuously or record nothing", () => {
    // A hand-edited zero here is not a setting the plugin can honour, and the
    // failure would be a roll process spawning without pause.
    const normalized = normalizeConfig({
      ...complete,
      rollIntervalMinutes: 0,
      maxRecordedPaths: 0,
      maxRecordedContexts: -1,
    });
    assert.equal(
      normalized.rollIntervalMinutes,
      CONFIG_DEFAULTS.rollIntervalMinutes,
    );
    assert.equal(normalized.maxRecordedPaths, CONFIG_DEFAULTS.maxRecordedPaths);
    assert.equal(
      normalized.maxRecordedContexts,
      CONFIG_DEFAULTS.maxRecordedContexts,
    );
  });

  it("rounds retention down to whole days", () => {
    // Expiry drops whole UTC days, so half a day is not a boundary it has. The
    // Admin UI's number field accepts 0.5 quite happily.
    assert.equal(
      normalizeConfig({ ...complete, retentionDays: 7.9 }).retentionDays,
      7,
    );
    assert.equal(
      normalizeConfig({ ...complete, retentionDays: 0.5 }).retentionDays,
      0,
    );
  });

  it("rejects a non-finite number rather than propagating it", () => {
    const normalized = normalizeConfig({
      ...complete,
      defaultSamplingRate: Number.NaN,
      retentionDays: Number.POSITIVE_INFINITY,
    });
    assert.equal(
      normalized.defaultSamplingRate,
      CONFIG_DEFAULTS.defaultSamplingRate,
    );
    assert.equal(normalized.retentionDays, CONFIG_DEFAULTS.retentionDays);
  });
});

describe("count-shaped options are whole numbers", () => {
  it("floors a fractional batch size instead of stalling the buffer", () => {
    // splice() applies ToIntegerOrInfinity to its delete count, so a batch
    // size of 0.5 removes nothing while isDue still reports a full batch: the
    // buffer never drains, grows to its ceiling and starts evicting, with the
    // plugin reporting "Recording" throughout. A number field accepts 0.5.
    assert.equal(normalizeConfig({ flushBatchSize: 0.5 }).flushBatchSize, 1);
    assert.equal(normalizeConfig({ flushBatchSize: 7.9 }).flushBatchSize, 7);
    assert.equal(
      normalizeConfig({ maxRecordedPaths: 2.5 }).maxRecordedPaths,
      2,
    );
    assert.equal(
      normalizeConfig({ maxRecordedContexts: 0.2 }).maxRecordedContexts,
      1,
    );
  });

  it("leaves the interval and the buffer size fractional, where it is harmless", () => {
    // These are milliseconds and megabytes, compared rather than counted.
    assert.equal(
      normalizeConfig({ flushIntervalMs: 1500.5 }).flushIntervalMs,
      1500.5,
    );
    assert.equal(normalizeConfig({ maxBufferMB: 0.5 }).maxBufferMB, 0.5);
  });
});

describe("the roll interval has to describe a schedule", () => {
  it("keeps an interval that divides the day", () => {
    for (const minutes of [1, 5, 15, 30, 60, 120, 240, 720, 1440]) {
      assert.equal(
        normalizeConfig({ rollIntervalMinutes: minutes }).rollIntervalMinutes,
        minutes,
      );
    }
  });

  it("falls back when the interval does not divide the day", () => {
    // "Aligned to UTC midnight" has no meaning for an interval that does not
    // divide 1,440 minutes: the schedule either drifts or jumps at midnight,
    // and which one it does is an accident of the arithmetic. The rows
    // themselves are safe either way — each lands in the date directory its
    // own timestamp names — so this is about the schedule, not the tree.
    for (const minutes of [7, 100, 1441, 0.5]) {
      assert.equal(
        normalizeConfig({ rollIntervalMinutes: minutes }).rollIntervalMinutes,
        CONFIG_DEFAULTS.rollIntervalMinutes,
        `${minutes} does not divide 1,440`,
      );
    }
  });
});
