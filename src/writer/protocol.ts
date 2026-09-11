/**
 * The wire format between the plugin and the writer process.
 *
 * Length-prefixed JSON over a Unix domain socket. JSON because the encode cost
 * lands on the Signal K event loop, which the design budgets at roughly sqhp's
 * measured +0.15 CPU points, and one `JSON.stringify` per flush window is far
 * below that — a batch is built every few seconds, not per delta. If the
 * measurement in Unit 7 says otherwise, the framing here is what changes and
 * nothing else has to.
 *
 * The reader is hostile-input code: it runs in the process that owns the hot
 * store, and the socket's mode is the only thing standing between it and any
 * other process on the device. So it declares its limits, checks them against
 * the header before it holds a body, and rejects rather than repairs.
 */

/** Bytes of length prefix in front of every body. Big-endian unsigned. */
export const HEADER_BYTES = 4;

/**
 * The largest body this protocol will send or accept.
 *
 * A legitimate frame is one flush: at most the configured batch size in
 * samples, each a few hundred bytes, so a full 1000-sample batch of unusually
 * long paths is still under a megabyte. Four leaves room for that without
 * leaving room for a frame that is really an allocation request — the header
 * can express 4 GiB, and the decoder must never be talked into holding it.
 */
export const MAX_FRAME_BYTES = 4 * 1024 * 1024;

/**
 * The kinds a recorded value can have — the single declaration of the set.
 *
 * `Sample`'s variants and the hot store's CHECK constraint are both built from
 * this, so adding a kind is one edit that fails the build wherever it was not
 * handled. Spelled out separately in each place, the copy carrying the name
 * was the one nothing consulted, and a new kind would have surfaced as a
 * constraint failure inside a transaction on a device.
 */
export const VALUE_KINDS = [
  "number",
  "string",
  "boolean",
  "position",
  "identity",
] as const;

export type ValueKind = (typeof VALUE_KINDS)[number];

export interface Position {
  latitude: number;
  longitude: number;
}

interface SampleBase {
  /** Epoch milliseconds, stamped by the recorder when the delta arrived. */
  ts: number;
  context: string;
  path: string;
  /** Null when the delta carried no source attribution. */
  source: string | null;
}

/**
 * One recorded value.
 *
 * A discriminated union rather than a bag of optional columns, so a `number`
 * sample cannot be constructed carrying a string — the case the hot store's
 * `value_kind` exists to keep distinguishable on the way back out. Booleans
 * travel as the text `"true"`/`"false"`; the kind, not the shape, is what
 * makes them booleans again.
 */
export type Sample =
  | (SampleBase & { kind: Extract<ValueKind, "number">; value: number })
  | (SampleBase & {
      kind: Extract<ValueKind, "string" | "boolean" | "identity">;
      value: string;
    })
  | (SampleBase & { kind: Extract<ValueKind, "position">; value: Position });

/**
 * Identifies one run of the plugin, so the writer knows whose sequence
 * numbers it is looking at.
 *
 * The writer skips a batch whose sequence number it has already committed,
 * which is what makes a resend after a lost acknowledgement idempotent. That
 * comparison is only meaningful within a single client run: the plugin's
 * counter restarts at 1 when the plugin does. Normally the writer restarts
 * with it, but a writer that outlives its plugin would otherwise discard the
 * new plugin's batches as duplicates of the old one's, acknowledge them, and
 * record nothing while reporting healthy.
 */
export type Message =
  | { type: "hello"; session: string }
  | { type: "welcome"; session: string; lastSeq: number }
  | { type: "batch"; seq: number; samples: Sample[] }
  | { type: "ack"; seq: number; stored: number }
  | { type: "error"; seq: number | null; message: string };

/** Bounds the session id so a hello cannot be used to store an arbitrary string. */
export const MAX_SESSION_BYTES = 64;

/**
 * How much peer-controlled text may appear in an error message.
 *
 * These messages reach the Signal K log. Unbounded, a single frame carrying a
 * 200,000-character `kind` produced one ~200 KB log line, and at
 * `MAX_FRAME_BYTES` one frame could push ~4 MB into it — a disk-fill primitive
 * against the server the plugin lives inside.
 */
const MAX_ECHOED_BYTES = 48;

/**
 * A fragment of peer-controlled text, safe to put in a log line.
 *
 * Truncated, and stripped of the control characters that would otherwise let a
 * peer forge a line: V8's own JSON parse error embeds raw bytes from the body,
 * newlines included.
 */
function echo(value: unknown): string {
  const text =
    typeof value === "string"
      ? value
      : (JSON.stringify(value) ?? String(value));
  const clean = text.replace(/[\p{Cc}\p{Cf}]/gu, "\uFFFD");
  return clean.length > MAX_ECHOED_BYTES
    ? `${clean.slice(0, MAX_ECHOED_BYTES)}…`
    : clean;
}

/** A frame that cannot be trusted. The caller logs it and drops the connection. */
export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

export function encodeFrame(message: Message): Buffer {
  const body = Buffer.from(JSON.stringify(message), "utf8");
  // Held to the same limit as the reader, so an oversized batch fails here —
  // where the stack names the sender — instead of as a dropped connection with
  // the cause on the other side of the socket.
  if (body.length > MAX_FRAME_BYTES) {
    throw new ProtocolError(
      `refusing to send a frame of ${body.length} bytes; the maximum is ${MAX_FRAME_BYTES}`,
    );
  }
  const frame = Buffer.allocUnsafe(HEADER_BYTES + body.length);
  frame.writeUInt32BE(body.length, 0);
  body.copy(frame, HEADER_BYTES);
  return frame;
}

/**
 * Reassembles frames from a byte stream.
 *
 * One decoder per connection: it holds the tail of a partial frame between
 * reads, so sharing one across sockets would splice two streams together.
 * Every `push` either returns the messages that completed or throws, and a
 * throw means the connection is no longer interpretable — there is no
 * resynchronisation point in a length-prefixed stream once the length is in
 * doubt.
 */
export class FrameDecoder {
  private pending: Buffer = Buffer.alloc(0);

  push(chunk: Buffer): Message[] {
    this.pending =
      this.pending.length === 0 ? chunk : Buffer.concat([this.pending, chunk]);

    const messages: Message[] = [];
    for (;;) {
      if (this.pending.length < HEADER_BYTES) break;
      const length = this.pending.readUInt32BE(0);
      // Checked against the header, before the body exists. Reading the length
      // and then waiting for that many bytes would turn a four-byte write from
      // any process that can open the socket into a demand to buffer gigabytes.
      if (length === 0) {
        throw new ProtocolError("frame of 0 bytes carries no message");
      }
      if (length > MAX_FRAME_BYTES) {
        throw new ProtocolError(
          `frame of ${length} bytes declared; the maximum is ${MAX_FRAME_BYTES}`,
        );
      }
      if (this.pending.length < HEADER_BYTES + length) break;

      const body = this.pending.subarray(HEADER_BYTES, HEADER_BYTES + length);
      messages.push(parseMessage(body));
      this.pending = this.pending.subarray(HEADER_BYTES + length);
    }
    return messages;
  }
}

function parseMessage(body: Buffer): Message {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body.toString("utf8"));
  } catch (err) {
    // The class and the size, not V8's message: that message embeds raw bytes
    // from the body, newlines included, so a peer could forge a log line.
    throw new ProtocolError(
      `frame body of ${body.length} bytes is not JSON (${err instanceof Error ? err.name : "Error"})`,
    );
  }
  if (!isRecord(parsed)) {
    throw new ProtocolError("frame body is not a JSON object");
  }

  switch (parsed.type) {
    case "hello":
      return { type: "hello", session: requireSession(parsed.session) };
    case "welcome": {
      if (!isCount(parsed.lastSeq)) {
        throw new ProtocolError("welcome.lastSeq is not a sequence number");
      }
      return {
        type: "welcome",
        session: requireSession(parsed.session),
        lastSeq: parsed.lastSeq,
      };
    }
    case "batch": {
      const seq = requireSeq(parsed.seq);
      if (!Array.isArray(parsed.samples)) {
        throw new ProtocolError("batch.samples is not an array");
      }
      return { type: "batch", seq, samples: parsed.samples.map(parseSample) };
    }
    case "ack": {
      const seq = requireSeq(parsed.seq);
      if (!isCount(parsed.stored)) {
        throw new ProtocolError("ack.stored is not a count");
      }
      return { type: "ack", seq, stored: parsed.stored };
    }
    case "error": {
      if (typeof parsed.message !== "string") {
        throw new ProtocolError("error.message is not a string");
      }
      const seq = parsed.seq === null ? null : requireSeq(parsed.seq);
      return { type: "error", seq, message: parsed.message };
    }
    default:
      throw new ProtocolError(`unknown message type ${echo(parsed.type)}`);
  }
}

/**
 * Rebuilds a sample field by field rather than validating in place, so a frame
 * carrying extra keys cannot smuggle them into the hot store's insert.
 */
function parseSample(raw: unknown): Sample {
  if (!isRecord(raw)) throw new ProtocolError("sample is not an object");

  const ts = raw.ts;
  if (!isCount(ts)) throw new ProtocolError("sample.ts is not a timestamp");

  const context = raw.context;
  if (typeof context !== "string" || context === "") {
    throw new ProtocolError("sample.context is not a non-empty string");
  }

  const path = raw.path;
  if (typeof path !== "string" || path === "") {
    throw new ProtocolError("sample.path is not a non-empty string");
  }

  const source = raw.source;
  if (source !== null && typeof source !== "string") {
    throw new ProtocolError("sample.source is neither a string nor null");
  }

  const base: SampleBase = { ts, context, path, source };
  switch (raw.kind) {
    case "number":
      // JSON has no NaN or Infinity literal, so a non-finite reading arrives
      // as null and must be refused rather than coerced to 0.
      if (typeof raw.value !== "number" || !Number.isFinite(raw.value)) {
        throw new ProtocolError("number sample carries no finite value");
      }
      return { ...base, kind: "number", value: raw.value };
    case "string":
    case "boolean":
    case "identity":
      if (typeof raw.value !== "string") {
        throw new ProtocolError(`${raw.kind} sample carries no string value`);
      }
      return { ...base, kind: raw.kind, value: raw.value };
    case "position": {
      const value = raw.value;
      if (
        !isRecord(value) ||
        typeof value.latitude !== "number" ||
        !Number.isFinite(value.latitude) ||
        typeof value.longitude !== "number" ||
        !Number.isFinite(value.longitude)
      ) {
        throw new ProtocolError(
          "position sample carries no finite coordinates",
        );
      }
      return {
        ...base,
        kind: "position",
        value: { latitude: value.latitude, longitude: value.longitude },
      };
    }
    default:
      throw new ProtocolError(`unknown value kind ${echo(raw.kind)}`);
  }
}

function requireSession(value: unknown): string {
  if (typeof value !== "string" || value === "") {
    throw new ProtocolError("session is not a non-empty string");
  }
  if (Buffer.byteLength(value, "utf8") > MAX_SESSION_BYTES) {
    throw new ProtocolError(
      `session is longer than ${MAX_SESSION_BYTES} bytes`,
    );
  }
  return value;
}

function requireSeq(value: unknown): number {
  if (!isCount(value)) throw new ProtocolError("seq is not a sequence number");
  return value;
}

/** A non-negative integer that JSON can round-trip exactly. */
function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
