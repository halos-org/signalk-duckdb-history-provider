import { describe, it } from "node:test";
import assert from "node:assert";
import { fat, sample } from "./fixtures.js";
import { FlushBuffer, sampleBytes } from "../flush-buffer.js";
import type { FlushBufferOptions } from "../flush-buffer.js";
import { MAX_FRAME_BYTES, encodeFrame } from "../writer/protocol.js";

function buffer(over: Partial<FlushBufferOptions> = {}) {
  return new FlushBuffer({
    flushIntervalMs: 5000,
    batchSize: 1000,
    maxBytes: 8 * 1024 * 1024,
    ...over,
  });
}

describe("flushing on whichever comes first", () => {
  it("is due once the batch threshold is reached, before any time passes", () => {
    const buf = buffer({ batchSize: 3 });

    buf.add(sample(), 1000);
    buf.add(sample(), 1000);
    assert.strictEqual(buf.isDue(1000), false, "two of three is not a batch");
    buf.add(sample(), 1000);
    assert.strictEqual(buf.isDue(1000), true);
  });

  it("is due once the interval has elapsed, below the threshold", () => {
    const buf = buffer({ batchSize: 1000, flushIntervalMs: 5000 });
    buf.add(sample(), 0);

    assert.strictEqual(buf.isDue(4999), false);
    assert.strictEqual(buf.isDue(5000), true);
  });

  it("is never due while empty, however long it waits", () => {
    // Otherwise an idle device sends an empty frame every interval forever.
    const buf = buffer();
    assert.strictEqual(buf.isDue(0), false);
    assert.strictEqual(buf.isDue(1_000_000), false);
  });

  it("measures the interval from the oldest waiting sample, not from construction", () => {
    // The guarantee is per sample: none waits longer than the interval. A
    // clock anchored at construction would make the first flush after a long
    // idle period fire the instant a sample arrived.
    const buf = buffer({ flushIntervalMs: 5000 });
    buf.add(sample(), 100_000);

    assert.strictEqual(buf.isDue(104_999), false);
    assert.strictEqual(buf.isDue(105_000), true);
  });

  it("restarts the interval for what is left behind after a take", () => {
    // Three samples against a batch of two, so what is left behind is short of
    // a batch. With a remainder that is itself a full batch, isDue would be
    // true for the threshold reason and say nothing about the clock.
    const buf = buffer({ flushIntervalMs: 5000, batchSize: 2 });
    buf.add(sample({ ts: 1 }), 0);
    buf.add(sample({ ts: 2 }), 0);
    buf.add(sample({ ts: 3 }), 0);

    assert.deepStrictEqual(
      buf.take(1000).map((s) => s.ts),
      [1, 2],
    );
    assert.strictEqual(buf.length, 1);
    // The remaining sample's wait is measured from the take, not from when it
    // was added — otherwise a backlog makes every subsequent flush instantly
    // due and the batching disappears exactly when it matters most.
    assert.strictEqual(buf.isDue(5999), false);
    assert.strictEqual(buf.isDue(6000), true);
  });

  it("takes at most one batch and leaves the rest buffered", () => {
    const buf = buffer({ batchSize: 2 });
    for (let i = 0; i < 5; i++) buf.add(sample({ ts: i }), 0);

    assert.deepStrictEqual(
      buf.take(0).map((s) => s.ts),
      [0, 1],
      "oldest first",
    );
    assert.strictEqual(buf.length, 3);
    assert.strictEqual(buf.isDue(0), true, "a full batch still remains");
  });
});

describe("the ceiling is a byte budget", () => {
  it("drops the oldest samples when the budget is exceeded", () => {
    const buf = buffer({ maxBytes: 4 * 200, batchSize: 1000 });
    for (let i = 0; i < 10; i++) buf.add(fat(100, { ts: i }), 0);

    const kept = buf.take(0).map((s) => s.ts);
    assert.ok(kept.length < 10, "some samples must have been dropped");
    assert.deepStrictEqual(
      kept,
      kept.slice().sort((a, b) => a - b),
      "survivors stay in order",
    );
    assert.strictEqual(
      kept[kept.length - 1],
      9,
      "the newest sample is always kept",
    );
    assert.strictEqual(buf.dropped, 10 - kept.length);
  });

  it("bounds bytes, not element count", () => {
    // sqhp's 100k-line cap was a byte budget expressed in ~80-byte ILP lines.
    // Structured samples are several times larger each, so a cap counted in
    // elements would hold several times the memory it was chosen for.
    const buf = buffer({ maxBytes: 10_000, batchSize: 1_000_000 });
    for (let i = 0; i < 50; i++) buf.add(fat(1000), 0);

    assert.ok(
      buf.byteSize <= 10_000,
      `byteSize ${buf.byteSize} exceeded the budget`,
    );
    assert.ok(buf.length < 50, "an element-counted cap would have kept all 50");
  });

  it("keeps a sample larger than the whole budget out of the buffer", () => {
    // Admitting it would leave the buffer permanently over budget holding one
    // element, and "drop the oldest" would then evict every later sample
    // forever to make room for something that never fits.
    const buf = buffer({ maxBytes: 500 });
    buf.add(fat(5000), 0);

    assert.strictEqual(buf.length, 0);
    assert.strictEqual(buf.dropped, 1);
    assert.strictEqual(buf.byteSize, 0);
  });

  it("tracks byteSize as the sum of what it holds", () => {
    const buf = buffer();
    const one = sample({ ts: 1 });
    const two = sample({ kind: "string", value: "moored", ts: 2 });

    buf.add(one, 0);
    buf.add(two, 0);

    // Each entry carries its own separator, so the buffer accounts for what a
    // frame would cost rather than for the samples alone.
    assert.strictEqual(buf.byteSize, sampleBytes(one) + sampleBytes(two) + 2);
    buf.take(0);
    assert.strictEqual(buf.byteSize, 0);
  });
});

describe("the frame limit binds as well as the budget", () => {
  it("refuses a sample no frame could ever carry, whatever the budget says", () => {
    // maxBytes defaults to 8 MB and MAX_FRAME_BYTES is 4 MiB, so a budget
    // larger than the frame limit used to admit samples that could never be
    // sent: they reached the socket, encodeFrame refused them, and the whole
    // batch around them was discarded.
    const buf = buffer({ maxBytes: 8 * 1024 * 1024 });
    buf.add(fat(5 * 1024 * 1024), 0);

    assert.strictEqual(buf.length, 0);
    assert.strictEqual(buf.dropped, 1);
  });

  it("stops a batch at the frame budget rather than at the element count", () => {
    // batchSize is operator-editable with no upper bound, so count alone lets
    // a batch grow past what a frame can carry.
    const buf = buffer({ maxBytes: 64 * 1024 * 1024, batchSize: 1000 });
    for (let i = 0; i < 8; i++) buf.add(fat(1024 * 1024, { ts: i }), 0);

    const taken = buf.take(0);
    assert.ok(taken.length < 8, "the whole backlog was handed over at once");
    // Measured by framing it, not by summing sampleBytes. Summing re-derives
    // the accounting under test, so it agreed with a take() that ignored the
    // commas between array elements and let a 4,247,945-byte frame through.
    assert.doesNotThrow(() =>
      encodeFrame({ type: "batch", seq: 1, samples: taken }),
    );
    assert.strictEqual(
      buf.length,
      8 - taken.length,
      "the rest stayed buffered",
    );
  });

  it("frames a batch of many small samples, separators included", () => {
    // The case the byte sum misses: one comma per sample beyond the first. At
    // ~54,000 small samples that is ~53 KB, enough on its own to push a batch
    // that summed under the limit past it.
    const buf = buffer({ maxBytes: 64 * 1024 * 1024, batchSize: 1_000_000 });
    const per = sampleBytes(sample());
    const enough = Math.ceil((MAX_FRAME_BYTES - 128) / per) + 10;
    for (let i = 0; i < enough; i++) buf.add(sample(), 0);

    const taken = buf.take(0);
    assert.ok(taken.length > 1000, "not enough samples to expose separators");
    assert.doesNotThrow(
      () => encodeFrame({ type: "batch", seq: 1, samples: taken }),
      "take() produced a batch encodeFrame refuses",
    );
  });

  it("still hands over a single sample that fills the budget on its own", () => {
    // The byte bound must not stall: one sample at the ceiling has to go, or
    // nothing ever drains.
    const buf = buffer({ maxBytes: 8 * 1024 * 1024, batchSize: 1000 });
    buf.add(fat(3 * 1024 * 1024), 0);
    buf.add(fat(3 * 1024 * 1024), 0);

    assert.strictEqual(buf.take(0).length, 1);
    assert.strictEqual(buf.length, 1);
  });
});

describe("sampleBytes", () => {
  it("counts UTF-8 bytes, not string length", () => {
    const ascii = sampleBytes(sample({ kind: "string", value: "aaa" }));
    const wide = sampleBytes(sample({ kind: "string", value: "⛵⛵⛵" }));

    assert.strictEqual(wide - ascii, 6, "each sail is 3 bytes, not 1");
  });

  it("counts the escaping JSON adds, not the raw characters", () => {
    // The case a byteLength-based estimate gets wrong, and the reason this is
    // measured rather than approximated: JSON expands a control character to
    // six bytes and a quote to two, so a raw-byte count of a value full of
    // them reports a fraction of what the frame will actually carry.
    const plain = sampleBytes(sample({ kind: "string", value: "aaaa" }));
    const quoted = sampleBytes(sample({ kind: "string", value: '""""' }));
    const control = sampleBytes(sample({ kind: "string", value: "\x01\x02" }));
    const rawControl = sampleBytes(sample({ kind: "string", value: "ab" }));

    assert.strictEqual(quoted - plain, 4, "each quote costs a backslash");
    assert.strictEqual(
      control - rawControl,
      2 * 5,
      "each control character becomes a six-byte \\u escape",
    );
  });

  it("agrees with what the frame will carry", () => {
    for (const s of [
      sample(),
      sample({ source: null }),
      sample({ kind: "identity", path: "name", value: "Øresund ⛵" }),
      sample({
        kind: "position",
        path: "navigation.position",
        value: { latitude: -60.166_666_7, longitude: 24.943_333_3 },
      }),
      sample({ ts: Number.MAX_SAFE_INTEGER, value: -1.234_567_890_123e-7 }),
    ]) {
      assert.strictEqual(
        sampleBytes(s),
        Buffer.byteLength(JSON.stringify(s), "utf8"),
        JSON.stringify(s).slice(0, 80),
      );
    }
  });
});

describe("a batch that failed to send goes back where it came from", () => {
  it("requeues at the front, so ordering survives a failed send", () => {
    const buf = buffer({ batchSize: 2 });
    for (let i = 0; i < 4; i++) buf.add(sample({ ts: i }), 0);

    buf.requeue(buf.take(0));

    assert.strictEqual(buf.length, 4);
    assert.deepStrictEqual(
      buf.take(0).map((s) => s.ts),
      [0, 1],
      "the retried batch is sent again before the newer samples",
    );
  });

  it("restores the byte accounting it removed", () => {
    const buf = buffer({ batchSize: 2 });
    for (let i = 0; i < 4; i++) buf.add(sample({ ts: i }), 0);
    const before = buf.byteSize;

    buf.requeue(buf.take(0));

    assert.strictEqual(buf.byteSize, before);
  });

  it("is due immediately, without waiting out another interval", () => {
    // A retry has already waited its interval once. Making it wait again
    // would double the crash-loss window every time the writer blips.
    const buf = buffer({ flushIntervalMs: 5000, batchSize: 10 });
    buf.add(sample(), 0);

    buf.requeue(buf.take(5000));

    assert.strictEqual(buf.isDue(5000), true);
  });

  it("still honours the ceiling, dropping the oldest of what results", () => {
    // The requeued batch is the oldest data present, so a buffer that filled
    // while the writer was unreachable drops the retry rather than the fresh
    // samples. For a live feed the newest are the ones worth keeping.
    const buf = buffer({ maxBytes: 3 * 200, batchSize: 2 });
    buf.add(fat(100, { ts: 0 }), 0);
    buf.add(fat(100, { ts: 1 }), 0);
    const taken = buf.take(0);

    buf.add(fat(100, { ts: 2 }), 0);
    buf.add(fat(100, { ts: 3 }), 0);
    buf.requeue(taken);

    assert.ok(buf.byteSize <= 3 * 200);
    const kept = buf.take(0).map((s) => s.ts);
    assert.ok(!kept.includes(0), "the oldest retried sample was dropped first");
    assert.ok(buf.dropped > 0);
  });
});
