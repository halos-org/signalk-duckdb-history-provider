import { Type, Static } from "typebox";
import { dividesTheDay } from "../roll/schedule.js";

/**
 * The single source of truth for both the JSON schema the Signal K Admin UI
 * renders and the `Config` TypeScript type. Add options here, never in two
 * places.
 */
export const ConfigSchema = Type.Object({
  pathFilter: Type.Object({
    mode: Type.Union([Type.Literal("exclude"), Type.Literal("include")], {
      default: "exclude",
      title: "Filter mode",
    }),
    paths: Type.Array(Type.String(), {
      default: [],
      title: "Path patterns (glob supported)",
      description: 'e.g. "notifications.*", "environment.wind.*"',
    }),
  }),

  defaultSamplingRate: Type.Number({
    default: 2000,
    title: "Default sampling rate (ms)",
    description:
      "Minimum ms between recorded samples for any path (0 = record every update). Lower it for individual paths with the per-path overrides below.",
  }),

  samplingRates: Type.Record(Type.String(), Type.Number(), {
    default: {},
    title: "Per-path sampling rates (ms)",
    description:
      'Override the default rate for specific paths. e.g. { "environment.wind.*": 200, "tanks.*": 10000 }',
  }),

  recordSelf: Type.Boolean({
    default: true,
    title: "Record own vessel",
  }),
  recordOthers: Type.Boolean({
    default: false,
    title: "Record other vessels",
    description:
      "Off by default. AIS targets add a context per vessel, and the roll holds one Parquet writer per partition — so this setting, more than data volume, is what sets the roll's memory peak.",
  }),

  maxRecordedPaths: Type.Number({
    default: 2000,
    title: "Maximum distinct recorded paths",
    description:
      "Paths beyond this many are ignored, and the count is reported in the plugin status. A misbehaving source emitting unbounded paths would otherwise inflate the partition count without limit.",
  }),
  maxRecordedContexts: Type.Number({
    default: 100,
    title: "Maximum distinct recorded contexts",
    description:
      "The same bound for vessel contexts. Only relevant when other vessels are recorded.",
  }),

  flushIntervalMs: Type.Number({
    default: 5000,
    title: "Flush interval (ms)",
    description:
      "No sample waits longer than this before being sent to the writer. This is also the crash-loss window: a hard power cut loses at most this much.",
  }),
  flushBatchSize: Type.Number({
    default: 1000,
    title: "Flush batch size (samples)",
    description:
      "Samples per write. Reaching it flushes early, whatever the interval says. Each batch is one SQLite transaction.",
  }),
  maxBufferMB: Type.Number({
    default: 8,
    title: "Buffer ceiling while the writer is unreachable (MB)",
    description:
      "Memory held for samples that could not be sent. When it is full the oldest are dropped, and the count is reported in the plugin status.",
  }),

  dataDir: Type.String({
    default: "",
    title: "Data directory",
    description:
      "Where the hot store and the Parquet tree live. Empty means the plugin's own directory under the Signal K data directory.",
  }),

  retentionDays: Type.Number({
    default: 0,
    title: "Retention (days, 0 = keep forever)",
    description:
      "A bound on what is stored, not a promise that everything older is deleted. Whole UTC days are dropped once the window has passed them, so the oldest sample kept can be up to a day older than the boundary. Applied after each roll.",
  }),

  rollIntervalMinutes: Type.Number({
    default: 5,
    title: "Roll interval (minutes)",
    description:
      "How often the hot store is rolled into the Parquet tree and truncated. Shorter keeps the hot store small and costs more Parquet files; longer does the reverse. Must divide 1440 — the schedule runs every N minutes from UTC midnight — and anything else falls back to the default.",
  }),

  compactHourly: Type.Boolean({
    default: true,
    title: "Compact each completed hour",
    description:
      "Merge an hour's roll files into one file sorted by path, once the hour is complete. Costs a short-lived process an hour and gives back both storage and query time. It does nothing at a roll interval of 60 minutes or more, where an hour already holds one file. Turning it off stops future merges and does not undo past ones.",
  }),
});

export type Config = Static<typeof ConfigSchema>;

/** Every default in one place, so normalization and the schema cannot drift. */
export const CONFIG_DEFAULTS = {
  pathFilterMode: "exclude" as const,
  pathFilterPaths: [] as string[],
  defaultSamplingRate: 2000,
  samplingRates: {} as Record<string, number>,
  recordSelf: true,
  recordOthers: false,
  maxRecordedPaths: 2000,
  maxRecordedContexts: 100,
  flushIntervalMs: 5000,
  flushBatchSize: 1000,
  maxBufferMB: 8,
  dataDir: "",
  retentionDays: 0,
  rollIntervalMinutes: 5,
  compactHourly: true,
};

/**
 * What Signal K actually hands `start()`: whatever JSON is on disk, which may
 * predate any given option or have been edited by hand. Typing it as `Config`
 * would let a later unit accept a raw stored config and dereference a key that
 * is not there — the failure that silently stopped recording in the sibling
 * provider.
 */
export type StoredConfig = {
  [K in keyof Config]?: K extends "pathFilter"
    ? Partial<Config["pathFilter"]>
    : Config[K];
};

/**
 * Fill in config keys that a saved configuration may lack. Signal K hands the
 * plugin its stored configuration verbatim — TypeBox defaults are applied by
 * the Admin UI form only, so a config saved before an option existed (or
 * hand-edited) simply misses the key, and the per-delta path would dereference
 * it on every delta. Normalize once at the boundary instead of guarding every
 * consumer.
 *
 * Numeric fields are also range-checked here rather than trusted: a
 * hand-edited zero or negative roll interval would otherwise mean "roll
 * continuously".
 */
export function normalizeConfig(config: StoredConfig): Config {
  return {
    ...config,
    pathFilter: {
      mode:
        config.pathFilter?.mode === "include" ||
        config.pathFilter?.mode === "exclude"
          ? config.pathFilter.mode
          : CONFIG_DEFAULTS.pathFilterMode,
      // Shape-checked, not just null-coalesced. A hand-edited `"paths":
      // "navigation.*"` is a string, and PathMatcher iterates a string
      // character by character -- the `*` compiles to a glob matching every
      // path, which in exclude mode records nothing at all.
      paths: stringArray(config.pathFilter?.paths),
    },
    samplingRates: numberRecord(config.samplingRates),
    defaultSamplingRate: nonNegative(
      config.defaultSamplingRate,
      CONFIG_DEFAULTS.defaultSamplingRate,
    ),
    // A missing toggle takes the schema default; an explicit false is honoured.
    recordSelf: config.recordSelf ?? CONFIG_DEFAULTS.recordSelf,
    recordOthers: config.recordOthers ?? CONFIG_DEFAULTS.recordOthers,
    compactHourly: config.compactHourly ?? CONFIG_DEFAULTS.compactHourly,
    maxRecordedPaths: positiveInteger(
      config.maxRecordedPaths,
      CONFIG_DEFAULTS.maxRecordedPaths,
    ),
    maxRecordedContexts: positiveInteger(
      config.maxRecordedContexts,
      CONFIG_DEFAULTS.maxRecordedContexts,
    ),
    flushIntervalMs: positive(
      config.flushIntervalMs,
      CONFIG_DEFAULTS.flushIntervalMs,
    ),
    flushBatchSize: positiveInteger(
      config.flushBatchSize,
      CONFIG_DEFAULTS.flushBatchSize,
    ),
    maxBufferMB: positive(config.maxBufferMB, CONFIG_DEFAULTS.maxBufferMB),
    dataDir:
      typeof config.dataDir === "string"
        ? config.dataDir
        : CONFIG_DEFAULTS.dataDir,
    // Floored, because expiry drops whole UTC days and half a day is not one
    // of them. A number field in the Admin UI accepts 0.5 quite happily, and
    // the rounding has to be visible in one place rather than inside the roll.
    retentionDays: Math.floor(
      nonNegative(config.retentionDays, CONFIG_DEFAULTS.retentionDays),
    ),
    rollIntervalMinutes: dayDivisor(
      config.rollIntervalMinutes,
      CONFIG_DEFAULTS.rollIntervalMinutes,
    ),
  };
}

/**
 * A whole number of minutes that divides the day.
 *
 * The schedule is "every N minutes from UTC midnight", which only describes a
 * cadence when N divides 1,440 — at 100 minutes the last slot before midnight
 * is 40 minutes long, and whether the schedule drifts or jumps there is an
 * accident of the arithmetic rather than anything an operator chose.
 *
 * It is not what keeps a roll inside one date directory. The roll places each
 * row by its own timestamp, so a window spanning midnight writes into two
 * directories and stays correct.
 */
function dayDivisor(value: number | undefined, fallback: number): number {
  const positiveValue = positive(value, fallback);
  return dividesTheDay(positiveValue) ? positiveValue : fallback;
}

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [...CONFIG_DEFAULTS.pathFilterPaths];
  return value.filter((entry): entry is string => typeof entry === "string");
}

function numberRecord(value: unknown): Record<string, number> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ...CONFIG_DEFAULTS.samplingRates };
  }
  // Non-positive overrides are dropped rather than carried through. The field
  // is a minimum interval between samples, so 0 and negatives are not values
  // it can honour; dropping them falls back to the default rate, which is what
  // the rate matcher does with them anyway.
  return Object.fromEntries(
    Object.entries(value).filter(
      (entry): entry is [string, number] =>
        typeof entry[1] === "number" &&
        Number.isFinite(entry[1]) &&
        entry[1] > 0,
    ),
  );
}

/**
 * A count, rounded down, never below one.
 *
 * `Array.prototype.splice` applies ToIntegerOrInfinity to its delete count, so
 * a batch size of 0.5 removes nothing while `isDue` still reports a full
 * batch: the buffer never drains, grows to its ceiling and starts evicting,
 * with the plugin reporting "Recording" throughout. A number field in the
 * Admin UI accepts 0.5 quite happily.
 */
function positiveInteger(value: number | undefined, fallback: number): number {
  const positiveValue = positive(value, fallback);
  return Math.max(1, Math.floor(positiveValue));
}

function positive(value: number | undefined, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? value
    : fallback;
}

// Distinct from `positive` because 0 is meaningful for these two: it means
// "no sampling cap" and "keep forever" respectively.
function nonNegative(value: number | undefined, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : fallback;
}
