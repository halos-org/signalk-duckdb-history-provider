import { Socket } from "node:net";
import { FrameDecoder, ProtocolError, encodeFrame } from "./protocol.js";
import type { Message, Sample } from "./protocol.js";
import type { FlushBuffer } from "../flush-buffer.js";

/**
 * The plugin's side of the socket.
 *
 * Follows `signalk-questdb-history-provider/src/ilp-writer.ts`, which learned
 * these rules against a real QuestDB on a real boat. The one that matters most
 * is that `close` is the single source of truth for a connection ending: Node
 * emits it after a failed connect and after a live socket drops, so all
 * backoff, flap accounting and rescheduling live there. A connect-failure path
 * that also reschedules double-counts flaps and runs two timers.
 */

const DEFAULTS = {
  // Short, because the writer is a child process the plugin has just spawned
  // and the first connect routinely beats its listen by a few hundred
  // milliseconds. A second of backoff there is a second of every start spent
  // buffering for no reason. It doubles from here, so a writer that is really
  // gone still backs off to the maximum quickly.
  initialReconnectDelayMs: 250,
  maxReconnectDelayMs: 30_000,
  /**
   * A connection that closes sooner than this never carried useful data. It is
   * counted as a failed attempt so the backoff grows, rather than resetting the
   * delay and reconnecting roughly once a second forever.
   */
  stableConnectionMs: 5000,
  /** After this many instant drops the plugin says so instead of staying green. */
  unhealthyAfterFlaps: 5,
  /**
   * How often the time-based flush is checked. Only the interval trigger needs
   * it: reaching the batch size flushes from `add` directly, so this timer
   * carries a partial batch that has 5000 ms of slack.
   */
  pumpIntervalMs: 1000,
  /**
   * How long a batch may sit unacknowledged before the connection is treated
   * as dead. Generous against the writer's real work -- a 1000-row transaction
   * plus a WAL checkpoint on a slow card -- and far short of the operator
   * noticing by hand.
   */
  ackTimeoutMs: 30_000,
};

export interface WriterClientOptions {
  socketPath: string;
  /** Identifies this run of the plugin to the writer. */
  session: string;
  buffer: FlushBuffer;
  log?: (message: string) => void;
  onUnhealthy?: (message: string) => void;
  onHealthy?: () => void;
  /** Fired once the writer has welcomed this session. */
  onConnected?: () => void;
  now?: () => number;
  timing?: Partial<typeof DEFAULTS>;
}

export interface WriterClientStats {
  connected: boolean;
  /** Batches the writer confirmed. */
  acked: number;
  /** Samples the writer confirmed. */
  stored: number;
  /** Samples the buffer discarded to stay under its ceiling. */
  dropped: number;
  consecutiveFlaps: number;
}

export class WriterClient {
  private readonly options: Required<Omit<WriterClientOptions, "timing">> & {
    timing: typeof DEFAULTS;
  };
  private socket: Socket | null = null;
  private decoder = new FrameDecoder();
  private connected = false;
  private welcomed = false;
  private stopped = true;
  private connectedAt = 0;
  private stableTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private pumpTimer: NodeJS.Timeout | null = null;
  private reconnectDelay: number;
  private consecutiveFlaps = 0;
  private unhealthy = false;
  private seq = 0;
  /** The batch on the wire, kept until the writer confirms it. */
  private inFlight: { seq: number; samples: Sample[] } | null = null;
  private acked = 0;
  private stored = 0;
  /** Samples thrown away because they could not be framed. Counted, not silent. */
  private discarded = 0;
  private unhealthyReason: string | undefined;
  private ackTimer: NodeJS.Timeout | null = null;

  constructor(options: WriterClientOptions) {
    this.options = {
      socketPath: options.socketPath,
      session: options.session,
      buffer: options.buffer,
      log: options.log ?? (() => {}),
      onUnhealthy: options.onUnhealthy ?? (() => {}),
      onHealthy: options.onHealthy ?? (() => {}),
      onConnected: options.onConnected ?? (() => {}),
      now: options.now ?? (() => Date.now()),
      timing: { ...DEFAULTS, ...options.timing },
    };
    this.reconnectDelay = this.options.timing.initialReconnectDelayMs;
  }

  get stats(): WriterClientStats {
    return {
      connected: this.connected && this.welcomed,
      acked: this.acked,
      stored: this.stored,
      dropped: this.options.buffer.dropped + this.discarded,
      consecutiveFlaps: this.consecutiveFlaps,
    };
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.pumpTimer = setInterval(
      () => this.pump(),
      this.options.timing.pumpIntervalMs,
    );
    this.pumpTimer.unref();
    this.openSocket();
  }

  /** Buffers a sample. Flushes at once if that completed a batch. */
  add(sample: Sample): void {
    const now = this.options.now();
    this.options.buffer.add(sample, now);
    if (this.options.buffer.isDue(now)) this.pump();
  }

  /**
   * Sends what is buffered, up to a deadline.
   *
   * A graceful stop used to throw the buffer away: at the measured rate that is
   * several hundred samples on every config save, and `flushIntervalMs` is
   * documented as the window a *power cut* loses. Bounded, because a writer
   * that has stopped answering must not hold the server's shutdown open.
   */
  async drain(deadlineMs = 2000): Promise<void> {
    const until = this.options.now() + deadlineMs;
    while (this.options.now() < until) {
      if (!this.connected || !this.welcomed) break;
      if (this.inFlight === null && this.options.buffer.length === 0) return;
      // Force the pump regardless of the flush interval: this is the last
      // chance these samples get.
      if (this.inFlight === null && this.options.buffer.length > 0) {
        this.forcePump();
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  private forcePump(): void {
    const now = this.options.now();
    const samples = this.options.buffer.take(now);
    if (samples.length === 0) return;
    this.seq++;
    this.inFlight = { seq: this.seq, samples };
    this.send({ type: "batch", seq: this.seq, samples });
    this.armAckTimer();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.clearTimer("pumpTimer");
    this.clearTimer("reconnectTimer");
    this.clearTimer("stableTimer");
    this.clearAckTimer();
    const socket = this.socket;
    this.socket = null;
    if (socket === null) return;
    await new Promise<void>((resolve) => {
      socket.once("close", () => resolve());
      socket.destroy();
    });
  }

  private openSocket(): void {
    if (this.stopped || this.socket !== null) return;

    const socket = new Socket();
    this.socket = socket;
    this.decoder = new FrameDecoder();
    this.welcomed = false;

    socket.on("connect", () => {
      this.connected = true;
      this.connectedAt = this.options.now();
      // Deliberately no backoff reset here. Accepting a connection is not
      // proof the connection is usable; only surviving stableConnectionMs is.
      this.startStabilityTimer();
      this.send({ type: "hello", session: this.options.session });
    });

    socket.on("data", (chunk: Buffer) => {
      let messages: Message[];
      try {
        messages = this.decoder.push(chunk);
      } catch (err) {
        this.options.log(
          `dropping the writer connection: ${err instanceof ProtocolError ? err.message : String(err)}`,
        );
        socket.destroy();
        return;
      }
      for (const message of messages) this.receive(message);
    });

    // Logged but not acted on: `close` always follows, and handling both would
    // double-count the flap and schedule two reconnects.
    socket.on("error", (err) => this.options.log(`writer socket: ${err.name}`));

    socket.on("close", () => {
      const wasStable = this.connected && this.stableTimer === null;
      // A connect that never succeeded is not a flap: nothing was established
      // to drop. Both still grow the backoff and both still count towards the
      // unhealthy report -- a writer that never appears is the case most worth
      // surfacing -- but calling an unreachable socket a flap sends whoever
      // reads the log looking for the wrong fault.
      const everConnected = this.connected;
      const upForMs = this.connectedAt
        ? this.options.now() - this.connectedAt
        : 0;
      this.connected = false;
      this.welcomed = false;
      this.connectedAt = 0;
      this.clearTimer("stableTimer");
      this.clearAckTimer();
      this.socket = null;
      if (this.stopped) return;

      if (!wasStable) {
        this.consecutiveFlaps++;
        this.reconnectDelay = Math.min(
          this.reconnectDelay * 2,
          this.options.timing.maxReconnectDelayMs,
        );
        this.options.log(
          everConnected
            ? `writer connection dropped after ${upForMs}ms (drop ${this.consecutiveFlaps}), retrying in ${this.reconnectDelay}ms`
            : `writer not reachable at ${this.options.socketPath} (attempt ${this.consecutiveFlaps}), retrying in ${this.reconnectDelay}ms`,
        );
        if (this.consecutiveFlaps >= this.options.timing.unhealthyAfterFlaps) {
          this.markUnhealthy();
        }
      }
      this.scheduleReconnect();
    });

    socket.connect(this.options.socketPath);
  }

  private receive(message: Message): void {
    switch (message.type) {
      case "welcome": {
        // The writer's counter is authoritative on reconnect -- it may have
        // committed a batch whose acknowledgement never arrived -- but only up
        // to what this client could actually have sent. A higher value cannot
        // be true, and trusting it would count never-written samples as stored
        // and push the counter past MAX_SAFE_INTEGER, after which the writer's
        // own validator rejects every batch and the client reconnect-loops
        // forever.
        if (message.lastSeq > this.seq) {
          this.options.log(
            `writer reported seq ${message.lastSeq}, past the ${this.seq} this run has sent`,
          );
          this.socket?.destroy();
          return;
        }
        this.settleInFlight(message.lastSeq);
        this.welcomed = true;
        // Reported rather than left for the next status tick: a plugin that
        // says "writer unreachable" for ten seconds after every enable is
        // reporting a problem that lasted milliseconds.
        this.options.onConnected();
        this.pump();
        return;
      }
      case "ack": {
        if (this.inFlight?.seq === message.seq) {
          this.acked++;
          this.stored += message.stored;
          this.inFlight = null;
          this.clearAckTimer();
          // Recovery is an ack, not a connection that stayed up. The stability
          // timer fires once, five seconds after connect, so a fault later in
          // a long-lived connection used to latch the plugin red for the rest
          // of that connection and suppress every later, different fault.
          if (this.unhealthy) {
            this.unhealthy = false;
            this.options.onHealthy();
          }
        }
        this.pump();
        return;
      }
      case "error": {
        this.options.log(`writer refused a batch: ${message.message}`);
        if (this.inFlight !== null && this.inFlight.seq === message.seq) {
          // Back to the buffer rather than discarded: the writer said it did
          // not store this, so losing it here would be loss the plugin chose.
          this.options.buffer.requeue(this.inFlight.samples);
          this.inFlight = null;
        }
        this.markUnhealthy(message.message);
        return;
      }
      default:
        this.options.log(`unexpected ${message.type} from the writer`);
        this.socket?.destroy();
    }
  }

  /**
   * Decides the fate of a batch that was on the wire when the connection ended.
   *
   * The writer's last committed sequence number is what separates the two
   * cases: at or below it, the batch landed and its acknowledgement was simply
   * lost, so resending it would duplicate rows. Above it, the writer never had
   * it, so dropping it would lose them.
   */
  private settleInFlight(writerLastSeq: number): void {
    const pending = this.inFlight;
    if (pending === null) return;
    this.inFlight = null;
    if (pending.seq <= writerLastSeq) {
      this.acked++;
      this.stored += pending.samples.length;
      this.options.log(`batch ${pending.seq} had landed before the drop`);
      return;
    }
    this.options.buffer.requeue(pending.samples);
  }

  private pump(): void {
    if (!this.connected || !this.welcomed || this.inFlight !== null) return;
    const now = this.options.now();
    if (!this.options.buffer.isDue(now)) return;

    const samples = this.options.buffer.take(now);
    if (samples.length === 0) return;
    this.seq++;
    this.inFlight = { seq: this.seq, samples };
    this.send({ type: "batch", seq: this.seq, samples });
    this.armAckTimer();
  }

  private send(message: Message): void {
    const socket = this.socket;
    if (socket === null || socket.destroyed) return;
    try {
      socket.write(encodeFrame(message));
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      this.options.log(`could not send to the writer: ${reason}`);
      if (message.type !== "batch" || this.inFlight?.seq !== message.seq)
        return;
      // These samples are already out of the buffer -- pump() spliced them --
      // so doing nothing here loses them with no drop count and no change to
      // the status line, which keeps saying "Recording".
      //
      // Requeueing would retry the same refusal forever, because encodeFrame
      // is deterministic: the batch that did not fit will never fit. So they
      // are discarded and counted, which is the honest half of the trade, and
      // the plugin is told.
      const lost = this.inFlight.samples.length;
      this.inFlight = null;
      this.discarded += lost;
      this.markUnhealthy(
        `${lost} samples could not be framed and were dropped: ${reason}`,
      );
    }
  }

  /**
   * Gives an unacknowledged batch a deadline.
   *
   * Without one, a writer that is connected but has stopped reading -- blocked
   * in a long synchronous SQLite call, or stalled on the card -- holds the
   * socket open forever. `close` never fires, so the batch is never settled, no
   * flap is counted, and `connected` stays true while the status line says
   * "Recording" and the buffer quietly fills. Destroying the socket routes the
   * failure into the path that already handles it.
   */
  private armAckTimer(): void {
    this.clearAckTimer();
    this.ackTimer = setTimeout(() => {
      this.ackTimer = null;
      if (this.inFlight === null) return;
      const waited = this.options.timing.ackTimeoutMs;
      this.markUnhealthy(
        `the writer has not acknowledged batch ${this.inFlight.seq} in ${waited}ms`,
      );
      this.socket?.destroy();
    }, this.options.timing.ackTimeoutMs);
    this.ackTimer.unref();
  }

  private clearAckTimer(): void {
    if (this.ackTimer === null) return;
    clearTimeout(this.ackTimer);
    this.ackTimer = null;
  }

  private startStabilityTimer(): void {
    this.clearTimer("stableTimer");
    this.stableTimer = setTimeout(() => {
      this.stableTimer = null;
      this.reconnectDelay = this.options.timing.initialReconnectDelayMs;
      this.consecutiveFlaps = 0;
      if (this.unhealthy) {
        this.unhealthy = false;
        this.options.onHealthy();
      }
    }, this.options.timing.stableConnectionMs);
    this.stableTimer.unref();
  }

  private scheduleReconnect(): void {
    this.clearTimer("reconnectTimer");
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.openSocket();
    }, this.reconnectDelay);
    this.reconnectTimer.unref();
  }

  private markUnhealthy(reason?: string): void {
    // Suppress a repeat of the same reason, not a new one: a second, different
    // fault is information the operator does not otherwise get.
    if (this.unhealthy && reason === this.unhealthyReason) return;
    this.unhealthy = true;
    this.unhealthyReason = reason;
    const dropped = this.options.buffer.dropped;
    this.options.onUnhealthy(
      reason ??
        `the writer connection has failed ${this.consecutiveFlaps} times in a row` +
          (dropped > 0 ? `; ${dropped} samples dropped so far` : ""),
    );
  }

  private clearTimer(
    which: "stableTimer" | "reconnectTimer" | "pumpTimer",
  ): void {
    const timer = this[which];
    if (timer === null) return;
    clearTimeout(timer);
    clearInterval(timer);
    this[which] = null;
  }
}
