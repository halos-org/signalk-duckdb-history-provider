import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert";
import { eventually, sample } from "./fixtures.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect, createServer } from "node:net";
import { mkdirSync } from "node:fs";
import { FrameDecoder, encodeFrame } from "../writer/protocol.js";
import { FlushBuffer } from "../flush-buffer.js";
import { HotStore } from "../writer/hot-store.js";
import { WriterServer } from "../writer/server.js";
import { WriterClient } from "../writer/client.js";

/**
 * Forwards between a client and the writer, swallowing the first
 * acknowledgement and killing the connection — a commit whose reply never
 * arrives, which is otherwise not reproducible against a real server.
 */
async function lossyProxy(
  frontPath: string,
  backPath: string,
): Promise<{ swallowedAck: boolean; close: () => Promise<void> }> {
  const state = { swallowedAck: false, close: async () => {} };
  const server = createServer((front) => {
    const back = connect(backPath);
    const decoder = new FrameDecoder();
    front.on("data", (chunk: Buffer) => back.write(chunk));
    back.on("data", (chunk: Buffer) => {
      for (const message of decoder.push(chunk)) {
        if (message.type === "ack" && !state.swallowedAck) {
          state.swallowedAck = true;
          front.destroy();
          back.destroy();
          return;
        }
        front.write(encodeFrame(message));
      }
    });
    front.on("close", () => back.destroy());
    back.on("close", () => front.destroy());
    front.on("error", () => back.destroy());
    back.on("error", () => front.destroy());
  });
  await new Promise<void>((resolve) => server.listen(frontPath, resolve));
  state.close = () =>
    new Promise<void>((resolve) => server.close(() => resolve()));
  return state;
}

let dir: string;
let store: HotStore;
let server: WriterServer | null = null;
let client: WriterClient | null = null;
let socketPath: string;
let proxy: { swallowedAck: boolean; close: () => Promise<void> } | null = null;

function newBuffer(
  over: Partial<ConstructorParameters<typeof FlushBuffer>[0]> = {},
) {
  return new FlushBuffer({
    flushIntervalMs: 50,
    batchSize: 3,
    maxBytes: 1024 * 1024,
    ...over,
  });
}

async function startServer(): Promise<void> {
  server = await WriterServer.listen({ socketPath, store });
}

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "writer-client-"));
  socketPath = join(dir, "run", "writer.sock");
  store = HotStore.open(join(dir, "hot.sqlite"));
});

afterEach(async () => {
  await client?.stop();
  client = null;
  await server?.close();
  server = null;
  await proxy?.close();
  proxy = null;
  try {
    store.close();
  } catch {
    // Closed by the test.
  }
  rmSync(dir, { recursive: true, force: true });
});

describe("delivery", () => {
  it("greets, then delivers a full batch without waiting for the interval", async () => {
    await startServer();
    const buffer = newBuffer({ flushIntervalMs: 60_000, batchSize: 3 });
    client = new WriterClient({ socketPath, session: "s1", buffer });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");

    client.add(sample({ ts: 1 }));
    client.add(sample({ ts: 2 }));
    assert.strictEqual(store.rowCount(), 0, "two of three is not a batch");
    client.add(sample({ ts: 3 }));

    await eventually(() => store.rowCount() === 3, "the batch to land");
    // A second wait, because these two counters move on the acknowledgement
    // and the rows land a round-trip before it arrives. Asserting them off the
    // row count alone failed once in a full-suite run and never in isolation.
    await eventually(() => client!.stats.acked === 1, "the acknowledgement");
    assert.strictEqual(client.stats.stored, 3);
  });

  it("delivers a partial batch once the interval elapses", async () => {
    await startServer();
    const buffer = newBuffer({ flushIntervalMs: 40, batchSize: 1000 });
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      timing: { pumpIntervalMs: 10 },
    });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");

    client.add(sample());
    await eventually(() => store.rowCount() === 1, "the interval flush");
  });

  it("sends one batch at a time and keeps them in order", async () => {
    await startServer();
    const buffer = newBuffer({ flushIntervalMs: 20, batchSize: 2 });
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      timing: { pumpIntervalMs: 5 },
    });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");

    for (let i = 1; i <= 6; i++) client.add(sample({ ts: i }));
    await eventually(() => store.rowCount() === 6, "all six samples");
    // Same round-trip gap as above: the counter is waited for, not read off
    // the row count.
    await eventually(
      () => client!.stats.acked === 3,
      "the third acknowledgement",
    );
    assert.strictEqual(client.stats.acked, 3, "three batches of two");
  });
});

describe("when the writer is not there", () => {
  it("buffers rather than losing samples, and delivers them once it appears", async () => {
    // No server yet: the client's first connect fails.
    const buffer = newBuffer({ flushIntervalMs: 20, batchSize: 2 });
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      timing: { initialReconnectDelayMs: 20, pumpIntervalMs: 5 },
    });
    client.start();

    for (let i = 1; i <= 4; i++) client.add(sample({ ts: i }));
    assert.strictEqual(buffer.length, 4, "held, not sent and not dropped");

    await startServer();
    await eventually(() => store.rowCount() === 4, "the backlog to drain");
  });

  it("does not call an unreachable writer a dropped connection", async () => {
    // Nothing was established, so nothing dropped. Both grow the backoff and
    // both count towards the unhealthy report, but the log line has to send
    // whoever reads it after the right fault.
    const lines: string[] = [];
    const buffer = newBuffer();
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      log: (line) => lines.push(line),
      timing: { initialReconnectDelayMs: 5, maxReconnectDelayMs: 20 },
    });
    client.start();

    await eventually(
      () => lines.some((line) => /not reachable at/.test(line)),
      "an unreachable report",
    );
    assert.ok(
      !lines.some((line) => /connection dropped/.test(line)),
      `nothing was connected, so nothing dropped: ${JSON.stringify(lines)}`,
    );
  });

  it("says so after repeated instant drops instead of staying green", async () => {
    const unhealthy: string[] = [];
    const buffer = newBuffer();
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      onUnhealthy: (message) => unhealthy.push(message),
      timing: {
        initialReconnectDelayMs: 5,
        maxReconnectDelayMs: 20,
        unhealthyAfterFlaps: 3,
      },
    });
    client.start();

    await eventually(() => unhealthy.length > 0, "an unhealthy report");
    assert.match(unhealthy[0], /failed \d+ times in a row/);
    assert.ok(client.stats.consecutiveFlaps >= 3);
  });

  it("goes quiet again once a connection survives", async () => {
    const unhealthy: string[] = [];
    let healthy = 0;
    const buffer = newBuffer();
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      onUnhealthy: (message) => unhealthy.push(message),
      onHealthy: () => healthy++,
      timing: {
        initialReconnectDelayMs: 5,
        maxReconnectDelayMs: 20,
        unhealthyAfterFlaps: 2,
        stableConnectionMs: 30,
      },
    });
    client.start();
    await eventually(() => unhealthy.length > 0, "an unhealthy report");

    await startServer();
    await eventually(() => healthy > 0, "recovery to be reported");
    assert.strictEqual(client.stats.consecutiveFlaps, 0);
  });
});

describe("a batch that cannot be framed", () => {
  it("is counted as dropped and reported, not lost in silence", async () => {
    // take() now bounds a batch by bytes, so this is a backstop rather than a
    // path normal operation reaches -- driven here through a stubbed take so
    // the backstop is not left unexercised. pump() has already spliced the
    // samples out of the buffer, so doing nothing loses them with no drop
    // count while the status line still says "Recording". Requeueing would
    // retry the same refusal forever: encodeFrame is deterministic, and a
    // batch that did not fit never will.
    await startServer();
    const unhealthy: string[] = [];
    const buffer = newBuffer({ flushIntervalMs: 60_000, batchSize: 1 });
    const oversized = Array.from({ length: 5 }, (_, i) =>
      sample({ ts: i, kind: "string", value: "x".repeat(1024 * 1024) }),
    );
    let handedOut = false;
    const realTake = buffer.take.bind(buffer);
    buffer.take = (now: number) => {
      if (handedOut) return realTake(now);
      handedOut = true;
      return oversized;
    };

    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      onUnhealthy: (message) => unhealthy.push(message),
      timing: { pumpIntervalMs: 5 },
    });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");
    client.add(sample());

    await eventually(() => unhealthy.length > 0, "an unhealthy report");
    assert.match(unhealthy[0], /could not be framed/);
    assert.strictEqual(client.stats.dropped, 5, "the loss was not counted");
    assert.strictEqual(store.rowCount(), 0);
  });
});

describe("a writer that stops answering", () => {
  it("is noticed rather than left holding the connection open", async () => {
    // A connected writer that has stopped reading holds the socket open
    // forever: close never fires, the batch is never settled, no flap is
    // counted, and connected stays true while the buffer fills.
    const unhealthy: string[] = [];
    const silent = createServer((socket) => {
      const decoder = new FrameDecoder();
      socket.on("data", (chunk: Buffer) => {
        for (const message of decoder.push(chunk)) {
          // Welcome it, then never acknowledge anything.
          if (message.type === "hello") {
            socket.write(
              encodeFrame({
                type: "welcome",
                session: message.session,
                lastSeq: 0,
              }),
            );
          }
        }
      });
      socket.on("error", () => {});
    });
    const silentPath = join(dir, "run", "silent.sock");
    mkdirSync(join(dir, "run"), { recursive: true });
    await new Promise<void>((resolve) => silent.listen(silentPath, resolve));

    try {
      const buffer = newBuffer({ flushIntervalMs: 20, batchSize: 1 });
      client = new WriterClient({
        socketPath: silentPath,
        session: "s1",
        buffer,
        onUnhealthy: (message) => unhealthy.push(message),
        timing: {
          pumpIntervalMs: 5,
          ackTimeoutMs: 150,
          initialReconnectDelayMs: 5,
          maxReconnectDelayMs: 20,
        },
      });
      client.start();
      await eventually(() => client!.stats.connected, "the handshake");
      client.add(sample());

      await eventually(
        () => unhealthy.some((m) => /has not acknowledged/.test(m)),
        `the silence to be reported (saw ${JSON.stringify(unhealthy)})`,
      );
    } finally {
      // Before closing the server: close() waits for active sockets, and the
      // client's reconnect loop would keep handing it new ones until the outer
      // teardown runs -- which cannot run until this block returns.
      await client?.stop();
      client = null;
      await new Promise<void>((resolve) => silent.close(() => resolve()));
    }
  });
});

describe("recovery", () => {
  it("clears the unhealthy latch on an acknowledgement, not on uptime", async () => {
    // The stability timer fires once, five seconds after connect. A fault
    // later in a long-lived connection used to latch the plugin red for the
    // rest of that connection and suppress every later, different fault.
    await startServer();
    const unhealthy: string[] = [];
    let healthy = 0;
    const buffer = newBuffer({ flushIntervalMs: 20, batchSize: 1 });
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      onUnhealthy: (message) => unhealthy.push(message),
      onHealthy: () => healthy++,
      timing: { pumpIntervalMs: 5, stableConnectionMs: 60_000 },
    });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");

    // A framing failure marks it unhealthy without touching the connection.
    const realTake = buffer.take.bind(buffer);
    let handedOut = false;
    buffer.take = (now: number) => {
      if (handedOut) return realTake(now);
      handedOut = true;
      return Array.from({ length: 5 }, (_, i) =>
        sample({ ts: i, kind: "string", value: "x".repeat(1024 * 1024) }),
      );
    };
    client.add(sample());
    await eventually(() => unhealthy.length > 0, "an unhealthy report");
    buffer.take = realTake;

    // The stability timer will not fire for another minute, so only an ack
    // can clear this.
    client.add(sample({ ts: 2 }));
    await eventually(() => healthy > 0, "recovery on the next acknowledgement");
    // The count is not the claim here -- the sample buffered alongside the
    // stubbed batch rides along on the same flush. What matters is that an
    // acknowledgement cleared the latch while the stability timer was still a
    // minute away.
    assert.ok(store.rowCount() > 0, "nothing was stored, so nothing was acked");
  });
});

describe("a batch on the wire when the connection ends", () => {
  it("is not stored twice when the writer had already committed it", async () => {
    // The case the session handshake exists for. Arranging it needs the
    // acknowledgement genuinely lost, so a proxy sits between: it forwards the
    // welcome, lets the batch through to be committed, then swallows the ack
    // and kills the connection. Closing the server after a normal ack would
    // prove nothing -- there would be no batch in flight to mishandle.
    await startServer();
    const frontPath = join(dir, "run", "front.sock");
    proxy = await lossyProxy(frontPath, socketPath);

    const buffer = newBuffer({ flushIntervalMs: 60_000, batchSize: 2 });
    client = new WriterClient({
      socketPath: frontPath,
      session: "s1",
      buffer,
      timing: { initialReconnectDelayMs: 10, pumpIntervalMs: 5 },
    });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");

    client.add(sample({ ts: 1 }));
    client.add(sample({ ts: 2 }));
    await eventually(() => store.rowCount() === 2, "the batch to be committed");
    await eventually(() => proxy!.swallowedAck, "the ack to be swallowed");

    // Reconnecting, the client learns the writer's last committed sequence
    // number and must recognise its in-flight batch in it.
    await eventually(() => client!.stats.connected, "the reconnection");
    await eventually(
      () => client!.stats.acked === 1,
      "the in-flight batch to be settled",
    );
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.strictEqual(store.rowCount(), 2, "the committed batch stayed once");
  });

  it("is sent again when the writer never had it", async () => {
    await startServer();
    const buffer = newBuffer({ flushIntervalMs: 60_000, batchSize: 2 });
    client = new WriterClient({
      socketPath,
      session: "s1",
      buffer,
      timing: { initialReconnectDelayMs: 10, pumpIntervalMs: 5 },
    });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");

    // Kill the server first, so the batch is written into a dead socket.
    await server!.close();
    server = null;
    client.add(sample({ ts: 1 }));
    client.add(sample({ ts: 2 }));

    await startServer();
    await eventually(() => store.rowCount() === 2, "the batch to be resent");
  });
});

describe("a session of its own", () => {
  it("starts at sequence one against a store the previous run left behind", async () => {
    // The plugin restarting. The store already holds a high sequence number
    // from the previous run; treating this run's numbers as duplicates would
    // acknowledge every batch and store none of them.
    store.beginSession("previous-run");
    store.insertBatch(40_000, [sample({ ts: 0 })]);
    await startServer();

    const buffer = newBuffer({ flushIntervalMs: 20, batchSize: 1 });
    client = new WriterClient({
      socketPath,
      session: "this-run",
      buffer,
      timing: { pumpIntervalMs: 5 },
    });
    client.start();
    await eventually(() => client!.stats.connected, "the handshake");

    client.add(sample({ ts: 1 }));
    await eventually(() => store.rowCount() === 2, "the new run's sample");
  });
});
