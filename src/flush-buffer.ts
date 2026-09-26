import { MAX_FRAME_BYTES } from "./writer/protocol.js";
import type { Sample } from "./writer/protocol.js";

/** Room for `{"type":"batch","seq":N,"samples":[…]}` around the samples. */
const FRAME_ENVELOPE_BYTES = 128;

/**
 * The comma between two samples in the `samples` array.
 *
 * Charged to every entry rather than to every entry but the first, so one
 * accounting model serves both limits: the memory budget over-counts by a
 * single byte per batch, and the frame budget can never under-count, which is
 * the direction that matters against a hard protocol limit.
 */
const SEPARATOR_BYTES = 1;

/**
 * What the plugin holds between flushes.
 *
 * This is the whole of the Signal K process's storage involvement: append,
 * bound, and hand a batch to the socket. It owns no timer and reads no clock —
 * the caller passes the time in — so the flush policy is testable without
 * waiting for one.
 *
 * **The unit is a delta, not a sample.** An object delta arrives as one sample
 * per field, and the writer commits one transaction per batch — so a batch
 * boundary inside a delta would let a query between the two commits see half
 * an object, and dropping one of its samples would store half of it for good.
 * Every limit here therefore takes, evicts or refuses a delta whole.
 */
export interface FlushBufferOptions {
  /** No sample waits longer than this before being sent. */
  flushIntervalMs: number;
  /** Samples per frame. Reaching it flushes early, whatever the clock says. */
  batchSize: number;
  /**
   * Memory ceiling while the writer is unreachable, in bytes of frame payload.
   *
   * A byte budget rather than an element count on purpose. sqhp's 100,000-line
   * cap was a byte budget in disguise — ~80-byte ILP lines, so ~8 MB — and a
   * structured sample here is several times larger than a line, so carrying
   * that number across as elements would carry several times the memory it was
   * chosen to bound.
   */
  maxBytes: number;
}

/**
 * The bytes this sample will occupy in a frame.
 *
 * Serialised rather than estimated from field lengths. JSON escaping expands a
 * control character to six bytes and a quote to two, so a raw-byte estimate
 * reports a fraction of the truth for values carrying them — and an estimate
 * below the truth makes the ceiling fictional in exactly the case it exists
 * for. One `JSON.stringify` of a small object per recorded sample is the cost;
 * sqhp formats a line per sample on the same path and measured +0.15 CPU
 * points in total.
 */
export function sampleBytes(sample: Sample): number {
  return Buffer.byteLength(JSON.stringify(sample), "utf8");
}

/** The samples one delta produced, sharing one `ts`. */
export type DeltaSamples = readonly Sample[];

interface Entry {
  delta: DeltaSamples;
  bytes: number;
}

function entryOf(delta: DeltaSamples): Entry {
  let bytes = 0;
  for (const sample of delta) bytes += sampleBytes(sample) + SEPARATOR_BYTES;
  return { delta, bytes };
}

export class FlushBuffer {
  private readonly options: FlushBufferOptions;
  private entries: Entry[] = [];
  /** Samples held, across every entry. */
  private samples = 0;
  private bytes = 0;
  /** When the oldest waiting sample started waiting. Null while empty. */
  private waitingSince: number | null = null;
  /** Set by a requeue: a retry has already served its interval once. */
  private retryPending = false;
  private droppedSamples = 0;

  /** The largest delta that could ever be sent, whichever limit binds first. */
  private readonly admissionCeiling: number;

  constructor(options: FlushBufferOptions) {
    this.options = options;
    // The envelope leaves room for the batch's own JSON scaffolding, so a
    // single admitted delta always leaves a frame that fits.
    this.admissionCeiling = Math.min(
      options.maxBytes,
      MAX_FRAME_BYTES - FRAME_ENVELOPE_BYTES,
    );
  }

  /** Samples held, not deltas: `batchSize` counts samples. */
  get length(): number {
    return this.samples;
  }

  get byteSize(): number {
    return this.bytes;
  }

  /** Samples discarded to stay under the ceiling, since construction. */
  get dropped(): number {
    return this.droppedSamples;
  }

  add(delta: DeltaSamples, now: number): void {
    if (delta.length === 0) return;
    const entry = entryOf(delta);
    // Refused rather than admitted, against whichever ceiling is lower.
    //
    // The buffer's own ceiling is the obvious one: admitting a sample bigger
    // than the whole budget would leave it permanently over, and "drop the
    // oldest" would then evict every later sample forever to make room for
    // something that never fits.
    //
    // The frame ceiling is the one that was missing. maxBytes defaults to 8 MB
    // and MAX_FRAME_BYTES is 4 MiB, so the buffer used to admit samples that
    // could never be sent -- they reached the socket, encodeFrame refused
    // them, and the whole batch around them was discarded.
    if (entry.bytes > this.admissionCeiling) {
      this.droppedSamples += delta.length;
      return;
    }
    if (this.entries.length === 0) this.waitingSince = now;
    this.entries.push(entry);
    this.samples += delta.length;
    this.bytes += entry.bytes;
    this.evictOldest();
  }

  isDue(now: number): boolean {
    if (this.entries.length === 0) return false;
    if (this.retryPending) return true;
    if (this.samples >= this.options.batchSize) return true;
    return (
      this.waitingSince !== null &&
      now - this.waitingSince >= this.options.flushIntervalMs
    );
  }

  /**
   * Removes and returns up to one batch of whole deltas, oldest first.
   *
   * Bounded by bytes as well as by count: `batchSize` is operator-editable, so
   * count alone lets a batch grow past what a frame can carry, and the whole
   * batch is then refused at the socket. The first delta is always taken,
   * however large, or a delta over `batchSize` would never drain.
   */
  take(now: number): DeltaSamples[] {
    const budget = MAX_FRAME_BYTES - FRAME_ENVELOPE_BYTES;
    let count = 0;
    let samples = 0;
    let bytes = 0;
    while (count < this.entries.length) {
      const entry = this.entries[count];
      // Every entry already carries its separators, so this is a plain sum.
      // Ignoring separators is not a rounding error at these batch sizes: a
      // batch of ~54,000 small samples summed to 4,194,138 bytes and framed to
      // 4,247,945, so encodeFrame refused the batch this method produced.
      const nextBytes = bytes + entry.bytes;
      const nextSamples = samples + entry.delta.length;
      if (
        count > 0 &&
        (nextBytes > budget || nextSamples > this.options.batchSize)
      )
        break;
      bytes = nextBytes;
      samples = nextSamples;
      count++;
    }
    const taken = this.entries.splice(0, count);
    this.samples -= samples;
    this.bytes -= bytes;
    this.retryPending = false;
    // Whatever is left starts waiting again from here. Measuring its wait from
    // when it was added would make every flush after a backlog instantly due,
    // and the batching would disappear exactly when it matters most.
    this.waitingSince = this.entries.length > 0 ? now : null;
    return taken.map((entry) => entry.delta);
  }

  /**
   * Puts a batch that failed to send back at the front.
   *
   * At the front because these are the oldest samples present, and the hot
   * store reads better in order. The ceiling still applies afterwards, so a
   * buffer that filled while the writer was unreachable drops the retry rather
   * than the fresh samples — for a live feed the newest are the ones worth
   * keeping.
   */
  requeue(deltas: readonly DeltaSamples[]): void {
    const restored = deltas.map(entryOf);
    this.entries.unshift(...restored);
    for (const entry of restored) {
      this.samples += entry.delta.length;
      this.bytes += entry.bytes;
    }
    this.retryPending = true;
    this.evictOldest();
  }

  private evictOldest(): void {
    while (this.bytes > this.options.maxBytes && this.entries.length > 0) {
      const dropped = this.entries.shift();
      if (dropped === undefined) break;
      this.samples -= dropped.delta.length;
      this.bytes -= dropped.bytes;
      this.droppedSamples += dropped.delta.length;
    }
    if (this.entries.length === 0) this.waitingSince = null;
  }
}
