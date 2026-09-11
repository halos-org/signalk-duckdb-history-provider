import {
  extractVesselName,
  flattenObjectValue,
  routeDeltaValue,
} from "./delta-routing.js";
import { PathMatcher, RateMatcher, Throttle } from "./path-matcher.js";
import type { Config } from "./config/schema.js";
import type { Sample } from "./writer/protocol.js";

/**
 * Everything the Signal K process does with a delta.
 *
 * Filter, rate-cap, bound the cardinality, and hand a sample onwards. No
 * storage work, no serialisation beyond what the buffer's accounting needs,
 * and nothing that can block. The sibling's equivalent measures +0.15 CPU
 * points for this shape of work, which is the budget.
 */

/** One value off `app.streambundle.getBus()`, already normalised by the server. */
export interface BusValue {
  path: string;
  value: unknown;
  context: string;
  $source?: unknown;
}

export interface RecorderOptions {
  config: Config;
  /** `app.selfContext`, so own-vessel values can be told from AIS targets. */
  selfContext: string;
  emit: (sample: Sample) => void;
  now?: () => number;
  log?: (message: string) => void;
}

export interface RecorderStats {
  /** Samples handed onwards. */
  recorded: number;
  /** Distinct paths being recorded. */
  paths: number;
  /** Distinct contexts being recorded. */
  contexts: number;
  /** Values dropped because the path cap was already full. */
  pathsOverCap: number;
  /** Values dropped because the context cap was already full. */
  contextsOverCap: number;
}

/** How many over-cap names to log before saying only how many there were. */
const NAMED_OVER_CAP = 5;

/**
 * The context a row carries for the own vessel.
 *
 * `self`, not the full `vessels.urn:mrn:...` string: it is shorter in every
 * row, and it is what the sibling's history surfaces expect, which Unit 4c
 * reproduces.
 */
export const SELF_CONTEXT = "self";

export class Recorder {
  private readonly config: Config;
  private readonly selfContext: string;
  private readonly emit: (sample: Sample) => void;
  private readonly now: () => number;
  private readonly log: (message: string) => void;

  private readonly pathFilter: PathMatcher;
  private readonly rates: RateMatcher;
  private readonly throttle = new Throttle();

  private readonly recordedPaths = new Set<string>();
  private readonly recordedContexts = new Set<string>();
  private readonly lastNameByContext = new Map<string, string>();

  private recorded = 0;
  private pathsOverCap = 0;
  private contextsOverCap = 0;
  private namedOverCap = 0;

  constructor(options: RecorderOptions) {
    this.config = options.config;
    this.selfContext = options.selfContext;
    this.emit = options.emit;
    this.now = options.now ?? (() => Date.now());
    this.log = options.log ?? (() => {});
    this.pathFilter = new PathMatcher(options.config.pathFilter.paths);
    this.rates = new RateMatcher(options.config.samplingRates);
  }

  get stats(): RecorderStats {
    return {
      recorded: this.recorded,
      paths: this.recordedPaths.size,
      contexts: this.recordedContexts.size,
      pathsOverCap: this.pathsOverCap,
      contextsOverCap: this.contextsOverCap,
    };
  }

  handle(item: BusValue): void {
    // The delta's sourceRef, kept so interleaved multi-source streams (two GPS
    // receivers) can be told apart afterwards and filtered through the history
    // API's `path|sourceRef` syntax.
    const source = typeof item.$source === "string" ? item.$source : null;
    const isSelf = item.context === this.selfContext;
    const context = isSelf ? SELF_CONTEXT : item.context;

    // Static vessel identity arrives as an empty-path object delta, which the
    // path guard below would drop. Deliberately ahead of the path filter and
    // the rate cap: this is identity, not a data stream, so an include-mode
    // filter would silently disable it and the shared throttle would drop most
    // of the opening burst when many vessels appear at once. The per-context
    // dedupe is what bounds it instead.
    //
    // It is NOT ahead of the record-self and record-others gates, which is
    // where this differs from the sibling. Measured on a device with the
    // default `recordOthers: false`: 16 contexts existed holding nothing but
    // an identity row each -- AIS vessels, meteo stations, navigation aids --
    // and each one becomes a partition in the roll for one or two rows. A name
    // for a vessel whose data is not recorded answers no query and costs a
    // partition, and recording it contradicts the setting the operator chose.
    const name = extractVesselName(item.path, item.value);
    if (name !== null) {
      if (isSelf && !this.config.recordSelf) return;
      if (!isSelf && !this.config.recordOthers) return;
      if (this.lastNameByContext.get(context) === name) return;
      if (!this.admitContext(context)) return;
      this.lastNameByContext.set(context, name);
      this.record({
        ts: this.now(),
        context,
        path: "name",
        source,
        kind: "identity",
        value: name,
      });
      return;
    }

    if (!item.path || item.value === undefined || item.value === null) return;
    if (isSelf && !this.config.recordSelf) return;
    if (!isSelf && !this.config.recordOthers) return;

    // Routed once, before the gates, because an object value is gated on its
    // leaf paths instead. Applying the parent's gates to those would be wrong
    // twice over: an include-filter naming only `navigation.attitude.roll`
    // would drop the parent before any leaf was seen, and the parent would
    // consume a throttle slot the leaves then wait out again, halving the
    // effective sampling rate.
    const route = routeDeltaValue(item.path, item.value);
    if (route === null) return;

    const ts = this.now();
    if (route === "flatten") {
      for (const leaf of flattenObjectValue(item.path, item.value)) {
        this.recordScalar(ts, context, leaf.path, source, leaf.value);
      }
      return;
    }
    if (route === "position") {
      if (!this.admit(item.path, context)) return;
      this.record({
        ts,
        context,
        path: item.path,
        source,
        kind: "position",
        value: item.value as { latitude: number; longitude: number },
      });
      return;
    }
    this.recordScalar(
      ts,
      context,
      item.path,
      source,
      item.value as number | string | boolean,
    );
  }

  private recordScalar(
    ts: number,
    context: string,
    path: string,
    source: string | null,
    value: number | string | boolean,
  ): void {
    if (!this.admit(path, context)) return;
    if (typeof value === "number") {
      this.record({ ts, context, path, source, kind: "number", value });
    } else if (typeof value === "boolean") {
      // The text, under the boolean kind. A 1.0 double cannot be told apart
      // from a real numeric channel on the way back out; the kind can.
      this.record({
        ts,
        context,
        path,
        source,
        kind: "boolean",
        value: value ? "true" : "false",
      });
    } else {
      this.record({ ts, context, path, source, kind: "string", value });
    }
  }

  private record(sample: Sample): void {
    this.recorded++;
    this.emit(sample);
  }

  /** Path filter, cardinality caps and rate cap, in the order that costs least. */
  private admit(path: string, context: string): boolean {
    if (!this.passesFilter(path)) return false;
    // Both caps sit behind the filter, so a context whose every path is
    // excluded neither consumes a slot nor is counted as "being recorded". The
    // cap exists to bound partition count, and a context that produces no row
    // produces no partition — the same reasoning the identity branch applies
    // one gate earlier.
    if (!this.admitContext(context)) return false;
    if (!this.admitPath(path)) return false;
    // Throttled per path AND context, so the sampling rate bounds each
    // vessel's stream rather than every target's. Deliberately not per source:
    // the rate bounds the per-path row volume, and keying per source would
    // multiply it by the number of receivers.
    const rate = this.rates.rateFor(path) ?? this.config.defaultSamplingRate;
    return !this.throttle.shouldDrop(path, context, rate, this.now());
  }

  private passesFilter(path: string): boolean {
    if (this.pathFilter.isEmpty) return true;
    const matches = this.pathFilter.matches(path);
    return this.config.pathFilter.mode === "exclude" ? !matches : matches;
  }

  /**
   * The cardinality cap, enforced here rather than at the roll.
   *
   * Partition count is bounded by not recording, not by rolling: the roll
   * holds one Parquet writer per partition and that, more than data volume,
   * is what sets its memory peak — 187 partitions already peak near 231 MB.
   *
   * Over the cap a value is dropped rather than the plugin refusing to record.
   * Refusing would turn one misbehaving source into a total history outage,
   * which is worse than a bounded subset. Dropping silently would be worse
   * still, so the count is carried in the plugin's status line and the first
   * few names reach the log.
   */
  private admitPath(path: string): boolean {
    if (this.recordedPaths.has(path)) return true;
    if (this.recordedPaths.size >= this.config.maxRecordedPaths) {
      this.pathsOverCap++;
      this.nameOverCap(`path ${path}`);
      return false;
    }
    this.recordedPaths.add(path);
    return true;
  }

  private admitContext(context: string): boolean {
    if (this.recordedContexts.has(context)) return true;
    if (this.recordedContexts.size >= this.config.maxRecordedContexts) {
      this.contextsOverCap++;
      this.nameOverCap(`context ${context}`);
      return false;
    }
    this.recordedContexts.add(context);
    return true;
  }

  private nameOverCap(what: string): void {
    if (this.namedOverCap >= NAMED_OVER_CAP) return;
    this.namedOverCap++;
    this.log(
      `over the cardinality cap, not recording ${what}` +
        (this.namedOverCap === NAMED_OVER_CAP
          ? "; further names are counted in the plugin status rather than logged"
          : ""),
    );
  }
}
