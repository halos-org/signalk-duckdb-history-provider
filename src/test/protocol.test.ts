import { describe, it } from "node:test";
import assert from "node:assert";
import { sample } from "./fixtures.js";
import {
  FrameDecoder,
  HEADER_BYTES,
  MAX_FRAME_BYTES,
  MAX_SESSION_BYTES,
  ProtocolError,
  encodeFrame,
} from "../writer/protocol.js";
import type { Message } from "../writer/protocol.js";

function decodeAll(bytes: Buffer): Message[] {
  return new FrameDecoder().push(bytes);
}

/** A header declaring `length` bytes of body, with no body behind it. */
function header(length: number): Buffer {
  const buf = Buffer.alloc(HEADER_BYTES);
  buf.writeUInt32BE(length, 0);
  return buf;
}

describe("frame round-trip", () => {
  it("returns the batch it was given", () => {
    const batch: Message = {
      type: "batch",
      seq: 7,
      samples: [
        sample(),
        sample({ kind: "string", value: "moored", path: "navigation.state" }),
        sample({ kind: "boolean", value: "true", path: "electrical.switch.1" }),
        sample({
          kind: "position",
          path: "navigation.position",
          value: { latitude: 60.16, longitude: 24.94 },
        }),
        sample({ kind: "identity", path: "name", value: "SEA BREEZE" }),
      ],
    };

    assert.deepStrictEqual(decodeAll(encodeFrame(batch)), [batch]);
  });

  it("returns a hello and a welcome", () => {
    const hello: Message = { type: "hello", session: "b3f1c0" };
    const welcome: Message = {
      type: "welcome",
      session: "b3f1c0",
      lastSeq: 41,
    };

    assert.deepStrictEqual(decodeAll(encodeFrame(hello)), [hello]);
    assert.deepStrictEqual(decodeAll(encodeFrame(welcome)), [welcome]);
  });

  it("returns an ack and an error message", () => {
    const ack: Message = { type: "ack", seq: 7, stored: 5 };
    const err: Message = {
      type: "error",
      seq: 8,
      message: "hot store is locked by another writer",
    };

    assert.deepStrictEqual(decodeAll(encodeFrame(ack)), [ack]);
    assert.deepStrictEqual(decodeAll(encodeFrame(err)), [err]);
  });

  it("sizes the header by UTF-8 bytes, not by string length", () => {
    // The classic framing bug: `payload.length` on a JS string counts UTF-16
    // code units, so any multi-byte character makes the declared length short
    // and every subsequent frame in the stream is misaligned. Signal K paths
    // and vessel names both carry non-ASCII in the field.
    const batch: Message = {
      type: "batch",
      seq: 1,
      samples: [
        sample({ kind: "identity", path: "name", value: "Øresund ⛵" }),
      ],
    };
    const frame = encodeFrame(batch);

    assert.strictEqual(
      frame.readUInt32BE(0),
      frame.length - HEADER_BYTES,
      "declared length must equal the actual body length",
    );
    // Two frames back to back: a short header would leave the decoder reading
    // the tail of the first as the header of the second.
    assert.deepStrictEqual(decodeAll(Buffer.concat([frame, frame])), [
      batch,
      batch,
    ]);
  });
});

describe("stream reassembly", () => {
  it("yields nothing until a frame is complete, then yields it once", () => {
    const batch: Message = { type: "batch", seq: 2, samples: [sample()] };
    const frame = encodeFrame(batch);
    const decoder = new FrameDecoder();

    for (let i = 0; i < frame.length - 1; i++) {
      assert.deepStrictEqual(
        decoder.push(frame.subarray(i, i + 1)),
        [],
        `byte ${i} completed a frame early`,
      );
    }
    assert.deepStrictEqual(decoder.push(frame.subarray(frame.length - 1)), [
      batch,
    ]);
    // And the decoder does not re-emit it on the next, empty read.
    assert.deepStrictEqual(decoder.push(Buffer.alloc(0)), []);
  });

  it("decodes several frames arriving in one chunk", () => {
    const one: Message = { type: "ack", seq: 1, stored: 1 };
    const two: Message = { type: "ack", seq: 2, stored: 2 };
    const three: Message = { type: "ack", seq: 3, stored: 3 };

    assert.deepStrictEqual(
      decodeAll(
        Buffer.concat([encodeFrame(one), encodeFrame(two), encodeFrame(three)]),
      ),
      [one, two, three],
    );
  });

  it("carries a partial frame across pushes without losing the one before it", () => {
    const first: Message = { type: "ack", seq: 1, stored: 1 };
    const second: Message = { type: "ack", seq: 2, stored: 2 };
    const frames = Buffer.concat([encodeFrame(first), encodeFrame(second)]);
    const split = encodeFrame(first).length + 3;
    const decoder = new FrameDecoder();

    assert.deepStrictEqual(decoder.push(frames.subarray(0, split)), [first]);
    assert.deepStrictEqual(decoder.push(frames.subarray(split)), [second]);
  });
});

describe("a frame that declares too much is refused before it is allocated", () => {
  it("rejects an oversized declared length from the header alone", () => {
    // The header is all the decoder has here — no body follows. If it only
    // noticed the size after buffering the body, this call would return []
    // and wait for 4 GB of it.
    const decoder = new FrameDecoder();
    assert.throws(
      () => decoder.push(header(MAX_FRAME_BYTES + 1)),
      (err: unknown) =>
        err instanceof ProtocolError && /frame of \d+ bytes/.test(err.message),
    );
  });

  it("rejects the largest length the header can express", () => {
    assert.throws(
      () => new FrameDecoder().push(header(0xffffffff)),
      ProtocolError,
    );
  });

  it("rejects a zero-length frame", () => {
    assert.throws(() => new FrameDecoder().push(header(0)), ProtocolError);
  });

  it("refuses to encode a message that would exceed the maximum", () => {
    // The sender is held to the same limit, so a bug on this side surfaces
    // here rather than as a dropped connection on the other.
    const huge: Message = {
      type: "batch",
      seq: 1,
      samples: [sample({ kind: "string", value: "x".repeat(MAX_FRAME_BYTES) })],
    };
    assert.throws(() => encodeFrame(huge), ProtocolError);
  });
});

describe("a malformed frame is refused, not crashed on", () => {
  function badBody(body: string): Buffer {
    const payload = Buffer.from(body, "utf8");
    return Buffer.concat([header(payload.length), payload]);
  }

  it("rejects a body that is not JSON", () => {
    assert.throws(() => decodeAll(badBody("{not json")), ProtocolError);
  });

  it("rejects JSON that is not an object", () => {
    for (const body of ["null", "42", '"batch"', "[]"]) {
      assert.throws(() => decodeAll(badBody(body)), ProtocolError, body);
    }
  });

  it("rejects an unknown message type", () => {
    assert.throws(
      () => decodeAll(badBody('{"type":"shutdown"}')),
      ProtocolError,
    );
  });

  it("rejects a batch whose samples are not an array", () => {
    assert.throws(
      () => decodeAll(badBody('{"type":"batch","seq":1,"samples":{}}')),
      ProtocolError,
    );
  });

  it("rejects a sample whose kind does not match its value", () => {
    const cases: Record<string, unknown>[] = [
      { ...sample(), kind: "number", value: "4.2" },
      { ...sample(), kind: "string", value: 4.2 },
      { ...sample(), kind: "boolean", value: true },
      { ...sample(), kind: "position", value: { latitude: 60.16 } },
      {
        ...sample(),
        kind: "position",
        value: { latitude: "60", longitude: 1 },
      },
      { ...sample(), kind: "elephant", value: 1 },
    ];
    for (const bad of cases) {
      assert.throws(
        () =>
          decodeAll(
            badBody(JSON.stringify({ type: "batch", seq: 1, samples: [bad] })),
          ),
        ProtocolError,
        JSON.stringify(bad),
      );
    }
  });

  it("rejects a sample carrying a non-finite number", () => {
    // JSON has no NaN literal, so this arrives as null and must not become 0.
    assert.throws(
      () =>
        decodeAll(
          badBody(
            '{"type":"batch","seq":1,"samples":[{"ts":1,"context":"vessels.self","path":"a.b","source":null,"kind":"number","value":null}]}',
          ),
        ),
      ProtocolError,
    );
  });

  it("rejects a sample missing a required field", () => {
    for (const drop of ["ts", "context", "path", "kind", "value"]) {
      const bad: Record<string, unknown> = { ...sample() };
      delete bad[drop];
      assert.throws(
        () =>
          decodeAll(
            badBody(JSON.stringify({ type: "batch", seq: 1, samples: [bad] })),
          ),
        ProtocolError,
        `missing ${drop} was accepted`,
      );
    }
  });

  it("rejects a session that is empty, absent or over-long", () => {
    // The writer stores the session alongside the sequence number it applied,
    // so an unbounded string here would be an unbounded string in the store.
    for (const body of [
      '{"type":"hello"}',
      '{"type":"hello","session":""}',
      '{"type":"hello","session":42}',
      `{"type":"hello","session":"${"s".repeat(MAX_SESSION_BYTES + 1)}"}`,
    ]) {
      assert.throws(() => decodeAll(badBody(body)), ProtocolError, body);
    }
    assert.doesNotThrow(() =>
      decodeAll(
        badBody(
          `{"type":"hello","session":"${"s".repeat(MAX_SESSION_BYTES)}"}`,
        ),
      ),
    );
  });

  it("rejects an empty context or path", () => {
    // Distinct from the missing case above: an empty path would become a
    // partition directory with no name, and an empty context names no vessel.
    for (const over of [{ path: "" }, { context: "" }]) {
      assert.throws(
        () =>
          decodeAll(
            badBody(
              JSON.stringify({
                type: "batch",
                seq: 1,
                samples: [{ ...sample(), ...over }],
              }),
            ),
          ),
        ProtocolError,
        JSON.stringify(over),
      );
    }
  });

  it("accepts a null source, which is what an unattributed delta has", () => {
    const batch: Message = {
      type: "batch",
      seq: 1,
      samples: [sample({ source: null })],
    };
    assert.deepStrictEqual(decodeAll(encodeFrame(batch)), [batch]);
  });
});
