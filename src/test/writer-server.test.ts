import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert";
import { sample } from "./fixtures.js";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { connect } from "node:net";
import type { Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HotStore } from "../writer/hot-store.js";
import {
  StoreLockedError,
  WriterServer,
  probeLiveWriter,
} from "../writer/server.js";
import { FrameDecoder, encodeFrame } from "../writer/protocol.js";
import type { Message } from "../writer/protocol.js";

let dir: string;
let store: HotStore;
let server: WriterServer | null;
let socketPath: string;
const openSockets: Socket[] = [];

beforeEach(async () => {
  dir = mkdtempSync(join(tmpdir(), "writer-server-"));
  socketPath = join(dir, "run", "writer.sock");
  store = HotStore.open(join(dir, "hot.sqlite"));
  server = await WriterServer.listen({ socketPath, store });
});

afterEach(async () => {
  for (const socket of openSockets.splice(0)) socket.destroy();
  await server?.close();
  server = null;
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

/** A client that collects decoded messages as they arrive. */
function client(): { socket: Socket; messages: Message[] } {
  const decoder = new FrameDecoder();
  const messages: Message[] = [];
  const socket = connect(socketPath);
  socket.on("data", (chunk: Buffer) => messages.push(...decoder.push(chunk)));
  openSockets.push(socket);
  return { socket, messages };
}

/** Resolves once `messages` holds at least `count`, or rejects on timeout. */
function until(messages: Message[], count: number): Promise<Message[]> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + 5000;
    const poll = setInterval(() => {
      if (messages.length >= count) {
        clearInterval(poll);
        resolve(messages);
      } else if (Date.now() > deadline) {
        clearInterval(poll);
        reject(new Error(`only ${messages.length} of ${count} messages`));
      }
    }, 5);
  });
}

describe("the socket is reachable only by its owner", () => {
  it("puts the socket in a directory nobody else may enter", () => {
    // The directory is what actually gates access: reaching a socket needs
    // execute permission on the directory holding it, checked before the
    // socket's own mode. Both are set because either alone is one mistake from
    // being world-reachable.
    assert.strictEqual(statSync(join(dir, "run")).mode & 0o777, 0o700);
    assert.strictEqual(statSync(socketPath).mode & 0o777, 0o600);
  });

  it("is a socket, not a TCP port", () => {
    assert.ok(statSync(socketPath).isSocket());
  });
});

describe("the store is held against a second writer", () => {
  it("refuses to start while another writer is listening", async () => {
    // The first server is already up from beforeEach.
    await assert.rejects(
      () => WriterServer.listen({ socketPath, store }),
      StoreLockedError,
    );
  });

  it("takes over a socket file whose writer was killed", async () => {
    // What a container restart leaves behind. A graceful close would not do:
    // Node unlinks the socket on close, so there would be nothing stale to
    // take over. Only an abrupt death leaves the file with no listener.
    //
    // A pid-file lock got this wrong on a real device: the pid it held came
    // from the previous PID namespace and named a live but unrelated process,
    // so every later writer refused to start, permanently.
    await server!.close();
    server = null;
    const stalePath = join(dir, "run", "stale.sock");
    const holder = spawn(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `import { createServer } from "node:net";
         createServer().listen(${JSON.stringify(stalePath)}, () =>
           process.stdout.write("listening\\n"));`,
      ],
      { stdio: ["ignore", "pipe", "inherit"] },
    );
    await new Promise<void>((resolve, reject) => {
      holder.stdout.on("data", (chunk: Buffer) => {
        if (chunk.toString().includes("listening")) resolve();
      });
      holder.on("error", reject);
    });
    assert.strictEqual(await probeLiveWriter(stalePath), true);

    holder.kill("SIGKILL");
    await new Promise<void>((resolve) => holder.on("exit", () => resolve()));
    assert.ok(statSync(stalePath).isSocket(), "the file outlives the process");
    assert.strictEqual(
      await probeLiveWriter(stalePath),
      false,
      "nothing answers on a dead writer's socket",
    );

    server = await WriterServer.listen({ socketPath: stalePath, store });
    assert.ok(statSync(stalePath).isSocket());
  });

  it("reports nothing live when the socket file does not exist", async () => {
    assert.strictEqual(
      await probeLiveWriter(join(dir, "run", "absent.sock")),
      false,
    );
  });

  it("reports a live writer while one is listening", async () => {
    assert.strictEqual(await probeLiveWriter(socketPath), true);
  });
});

describe("the handshake", () => {
  it("welcomes a new session at sequence zero", async () => {
    const { socket, messages } = client();
    socket.write(encodeFrame({ type: "hello", session: "s1" }));

    assert.deepStrictEqual((await until(messages, 1))[0], {
      type: "welcome",
      session: "s1",
      lastSeq: 0,
    });
  });

  it("welcomes a returning session at what it committed", async () => {
    const first = client();
    first.socket.write(encodeFrame({ type: "hello", session: "s1" }));
    await until(first.messages, 1);
    first.socket.write(
      encodeFrame({ type: "batch", seq: 9, samples: [sample()] }),
    );
    await until(first.messages, 2);
    first.socket.destroy();

    const second = client();
    second.socket.write(encodeFrame({ type: "hello", session: "s1" }));
    assert.deepStrictEqual((await until(second.messages, 1))[0], {
      type: "welcome",
      session: "s1",
      lastSeq: 9,
    });
  });

  it("refuses a batch that arrives before a hello", async () => {
    // Otherwise the batch is written against whatever session the store last
    // saw, which after a plugin restart is the previous run's.
    const { socket, messages } = client();
    socket.write(encodeFrame({ type: "batch", seq: 1, samples: [sample()] }));

    const [reply] = await until(messages, 1);
    assert.strictEqual(reply.type, "error");
    assert.strictEqual(store.rowCount(), 0);
  });
});

describe("batches", () => {
  async function greeted() {
    const c = client();
    c.socket.write(encodeFrame({ type: "hello", session: "s1" }));
    await until(c.messages, 1);
    return c;
  }

  it("acknowledges what it stored", async () => {
    const { socket, messages } = await greeted();
    socket.write(
      encodeFrame({
        type: "batch",
        seq: 1,
        samples: [sample(), sample(), sample()],
      }),
    );

    assert.deepStrictEqual((await until(messages, 2))[1], {
      type: "ack",
      seq: 1,
      stored: 3,
    });
    assert.strictEqual(store.rowCount(), 3);
  });

  it("acknowledges a resend without storing it twice", async () => {
    const { socket, messages } = await greeted();
    const batch: Message = { type: "batch", seq: 1, samples: [sample()] };

    socket.write(encodeFrame(batch));
    await until(messages, 2);
    socket.write(encodeFrame(batch));

    assert.deepStrictEqual((await until(messages, 3))[2], {
      type: "ack",
      seq: 1,
      stored: 0,
    });
    assert.strictEqual(store.rowCount(), 1);
  });
});

describe("what a peer can put in the log", () => {
  it("bounds and sanitises the text it echoes", async () => {
    // These lines reach the Signal K log. Unbounded, one frame carrying a
    // 200,000-character kind produced a single ~200 KB line, and V8's own JSON
    // parse error embeds raw bytes from the body -- newlines included -- so a
    // peer could forge a line.
    const lines: string[] = [];
    await server?.close();
    server = await WriterServer.listen({
      socketPath,
      store,
      log: (line) => lines.push(line),
    });

    const send = (body: string) => {
      const c = client();
      const payload = Buffer.from(body, "utf8");
      const head = Buffer.alloc(4);
      head.writeUInt32BE(payload.length, 0);
      c.socket.write(Buffer.concat([head, payload]));
      return new Promise<void>((resolve) =>
        c.socket.on("close", () => resolve()),
      );
    };

    await send(
      JSON.stringify({
        type: "batch",
        seq: 1,
        samples: [{ ...sample(), kind: "z".repeat(200_000) }],
      }),
    );
    await send('{"a":\n[writer] FORGED LINE\nmore');

    assert.ok(lines.length >= 2, `expected two drops, got ${lines.length}`);
    for (const line of lines) {
      assert.ok(line.length < 400, `a ${line.length}-byte log line`);
      assert.ok(!/[\r\n]/.test(line), `a newline reached the log: ${line}`);
      assert.ok(!line.includes("FORGED"), `raw body bytes reached the log`);
    }
  });
});

describe("one client at a time", () => {
  async function greet(session: string) {
    const c = client();
    c.socket.write(encodeFrame({ type: "hello", session }));
    await until(c.messages, 1);
    return c;
  }

  it("does not let a second connection move the first one's sequence counter", async () => {
    // Reproduced before this guard existed: the store's session and counter are
    // process-global, so one hello plus one batch at a huge sequence number
    // from a second connection made every later legitimate batch skip as a
    // duplicate -- acknowledged, never written, for as long as the writer ran,
    // with the plugin reporting "Recording" throughout.
    const legit = await greet("legit-run");
    legit.socket.write(
      encodeFrame({ type: "batch", seq: 1, samples: [sample()] }),
    );
    await until(legit.messages, 2);
    assert.strictEqual(store.rowCount(), 1);

    const intruder = await greet("second-connection");
    intruder.socket.write(
      encodeFrame({
        type: "batch",
        seq: 9_007_199_254_740_000,
        samples: [sample()],
      }),
    );
    await until(intruder.messages, 2);

    // The incumbent was closed rather than left to write under a counter it
    // does not know about.
    await new Promise<void>((resolve) =>
      legit.socket.destroyed
        ? resolve()
        : legit.socket.on("close", () => resolve()),
    );

    // A fresh session behaves normally, which is what a reconnecting plugin does.
    const rejoined = await greet("legit-run");
    rejoined.socket.write(
      encodeFrame({ type: "batch", seq: 1, samples: [sample()] }),
    );
    const [, ack] = await until(rejoined.messages, 2);
    assert.strictEqual(ack.type, "ack");
    assert.strictEqual(
      (ack as { stored: number }).stored,
      1,
      "the rejoining client's batch was skipped as a duplicate",
    );
  });

  it("refuses a second hello on a connection that already has a session", async () => {
    const c = await greet("s1");
    c.socket.write(encodeFrame({ type: "hello", session: "s2" }));

    const [, reply] = await until(c.messages, 2);
    assert.strictEqual(reply.type, "error");
    assert.match(
      (reply as { message: string }).message,
      /already has a session/,
    );
  });
});

describe("a store failure during the handshake", () => {
  it("is answered, not thrown out of the socket handler", async () => {
    // beginSession runs SELECT, INSERT and UPDATE. Unguarded, a throw here
    // escapes the data handler, is not covered by main()'s catch, and kills
    // the writer -- which nothing respawns.
    const lines: string[] = [];
    await server?.close();
    store.close();
    server = await WriterServer.listen({
      socketPath,
      store,
      log: (line) => lines.push(line),
    });

    const c = client();
    c.socket.write(encodeFrame({ type: "hello", session: "s1" }));

    const [reply] = await until(c.messages, 1);
    assert.strictEqual(reply.type, "error");
    assert.match(
      (reply as { message: string }).message,
      /starting the session failed/,
    );
    // The writer is still serving, which is the point.
    assert.strictEqual(server!.connectionCount >= 1, true);

    store = HotStore.open(join(dir, "hot.sqlite"));
  });

  it('names the SQLite cause rather than the constant "Error"', async () => {
    // Every node:sqlite failure is a plain Error, so the name alone makes a
    // full disk and a corrupt store indistinguishable to an operator.
    const lines: string[] = [];
    await server?.close();
    store.close();
    store = HotStore.open(join(dir, "hot.sqlite"));
    server = await WriterServer.listen({
      socketPath,
      store,
      log: (line) => lines.push(line),
    });

    const c = client();
    c.socket.write(encodeFrame({ type: "hello", session: "s1" }));
    await until(c.messages, 1);
    store.close();
    c.socket.write(encodeFrame({ type: "batch", seq: 1, samples: [sample()] }));

    const [, reply] = await until(c.messages, 2);
    assert.strictEqual(reply.type, "error");
    assert.match(
      (reply as { message: string }).message,
      /ERR_/,
      "the reply names no SQLite code",
    );
    assert.ok(
      lines.some((line) => /ERR_/.test(line)),
      `the log names no SQLite code: ${JSON.stringify(lines)}`,
    );

    store = HotStore.open(join(dir, "hot.sqlite"));
  });
});

describe("a frame it cannot trust ends the connection", () => {
  it("drops the connection on a malformed frame and stays up for the next one", async () => {
    const lines: string[] = [];
    await server?.close();
    server = await WriterServer.listen({
      socketPath,
      store,
      log: (line) => lines.push(line),
    });

    const bad = client();
    const body = Buffer.from("{not json", "utf8");
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length, 0);
    bad.socket.write(Buffer.concat([header, body]));

    await new Promise<void>((resolve) =>
      bad.socket.on("close", () => resolve()),
    );
    assert.ok(
      lines.some((line) => line.includes("dropping the connection")),
      `expected a log line about the drop, got ${JSON.stringify(lines)}`,
    );

    // The writer itself survives: a new client is served normally.
    const good = client();
    good.socket.write(encodeFrame({ type: "hello", session: "s2" }));
    assert.strictEqual((await until(good.messages, 1))[0].type, "welcome");
  });

  it("never writes a recorded value into the log", async () => {
    // These lines reach the Signal K log and ship in support bundles. A
    // position or a tank level is the boat's, not the log's.
    const lines: string[] = [];
    await server?.close();
    server = await WriterServer.listen({
      socketPath,
      store,
      log: (line) => lines.push(line),
    });

    const c = client();
    c.socket.write(encodeFrame({ type: "hello", session: "s1" }));
    await until(c.messages, 1);

    // Closing the store makes the insert throw with the batch in hand, which
    // is the moment the server has both a failure to report and a sample to
    // leak. A malformed frame would not do: the decoder rejects it before the
    // server ever sees a value.
    store.close();
    const marker = "60.1667,24.9433";
    c.socket.write(
      encodeFrame({
        type: "batch",
        seq: 1,
        samples: [sample({ kind: "string", value: marker })],
      }),
    );

    // The failure path really ran -- without this the assertion below passes
    // on an empty log.
    const [, reply] = await until(c.messages, 2);
    assert.strictEqual(reply.type, "error");
    assert.ok(
      lines.some((line) => line.includes("batch 1 of 1 samples failed")),
      `expected the failure to be logged, got ${JSON.stringify(lines)}`,
    );
    for (const line of lines) {
      assert.ok(
        !line.includes(marker),
        `a recorded value reached the log: ${line}`,
      );
    }
    assert.ok(
      !JSON.stringify(reply).includes(marker),
      "a recorded value reached the error reply",
    );

    // Reopened so afterEach's close has something to close.
    store = HotStore.open(join(dir, "hot.sqlite"));
  });
});
