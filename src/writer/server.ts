import { chmodSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { connect, createServer } from "node:net";
import type { Server, Socket } from "node:net";
import { dirname } from "node:path";
import { FrameDecoder, ProtocolError, encodeFrame } from "./protocol.js";
import type { Message } from "./protocol.js";
import type { HotStore } from "./hot-store.js";

/**
 * The writer's side of the socket.
 *
 * Two things here exist to stop something rather than to do something: the
 * store is held under an explicit lock so a second writer cannot interleave
 * rows into it, and the socket sits in a directory only its owner can enter so
 * no other account on the device can send it frames.
 */

/** How long the probe waits before deciding it cannot tell. */
const PROBE_TIMEOUT_MS = 2000;

/** Raised when another writer is already serving the hot store. */
export class StoreLockedError extends Error {
  constructor(socketPath: string) {
    super(
      `a writer is already listening on ${socketPath}; refusing to open a second one`,
    );
    this.name = "StoreLockedError";
  }
}

/**
 * Whether a live writer is serving this socket.
 *
 * The socket is the claim on the hot store, and connecting to it is the test.
 * SQLite will not keep a second writer out on its own — two processes can both
 * open the file and both write to it, taking per-transaction locks rather than
 * per-handle ones — and `PRAGMA locking_mode = EXCLUSIVE` is unavailable
 * because it also blocks readers, which is the reason the store is SQLite at
 * all.
 *
 * A pid file was the obvious alternative and is wrong here. This runs inside a
 * container, so a pid is only meaningful within one PID namespace; a restart
 * gives a new namespace where the dead writer's pid is some unrelated live
 * process. Measured on a device: a stale lock naming pid 31 made every
 * subsequent writer refuse to start, permanently, while the plugin buffered
 * and reported an unreachable writer. A socket carries no such ambiguity —
 * nothing answers on a dead writer's socket, in any namespace.
 *
 * The residual race is two writers starting within milliseconds of each other:
 * both see a refused connection, both unlink, both bind, and the loser serves
 * an unlinked inode while both hold the store. The plugin spawns exactly one
 * writer per start and Signal K serialises starts, so the case this has to
 * survive is the one above — an old writer alive across a restart — which the
 * probe answers correctly.
 */
export function probeLiveWriter(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    if (!existsSync(socketPath)) {
      resolve(false);
      return;
    }
    const socket = connect(socketPath);
    let settled = false;
    const settle = (live: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      resolve(live);
    };
    // A probe that neither connects nor errors would leave `listen` unresolved
    // forever, so the writer would hang without listening and without exiting
    // while the plugin buffered.
    const timer = setTimeout(() => settle(true), PROBE_TIMEOUT_MS);
    timer.unref();

    socket.once("connect", () => settle(true));
    socket.once("error", (err) => {
      // Only "nothing is listening" means dead. EMFILE on the writer, EAGAIN
      // against a saturated backlog, EACCES — all of those come from a writer
      // that is alive and merely unable to accept right now, and reading them
      // as dead would unlink a live writer's socket and bind a second one
      // over it. Both would then hold the store, which is what the claim
      // exists to prevent, with the loser being the running writer rather
      // than a starting one.
      const code = (err as NodeJS.ErrnoException).code;
      settle(!(code === "ECONNREFUSED" || code === "ENOENT"));
    });
  });
}

/** The `code` node:sqlite actually sets, when there is one. */
function errorCode(err: unknown): string | null {
  const code = (err as { code?: unknown })?.code;
  return typeof code === "string" ? code : null;
}

/**
 * What went wrong, in terms an operator can act on.
 *
 * node:sqlite throws plain `Error`, so the name is always "Error" and says
 * nothing. `code` separates a SQLite failure from a misused handle, and
 * `errstr` names the condition -- "disk is full", "attempt to write a readonly
 * database", "database disk image is malformed". Neither can contain a
 * recorded value.
 */
function describeError(err: unknown): string {
  const name = err instanceof Error ? err.name : "Error";
  const code = errorCode(err);
  const errstr = (err as { errstr?: unknown })?.errstr;
  const detail = typeof errstr === "string" ? `: ${errstr}` : "";
  return `${code ?? name}${detail}`;
}

export interface WriterServerOptions {
  socketPath: string;
  store: HotStore;
  /** Counts, path names and error classes only — never a recorded value. */
  log?: (message: string) => void;
}

export class WriterServer {
  private readonly server: Server;
  private readonly store: HotStore;
  private readonly log: (message: string) => void;
  private readonly sockets = new Set<Socket>();

  private constructor(server: Server, options: WriterServerOptions) {
    this.server = server;
    this.store = options.store;
    this.log = options.log ?? (() => {});
  }

  static listen(options: WriterServerOptions): Promise<WriterServer> {
    const { socketPath } = options;
    const server = createServer();
    const instance = new WriterServer(server, options);
    server.on("connection", (socket) => instance.handle(socket));

    // A Unix domain socket, never a TCP port — not even on loopback, which is
    // shared with every local process and every container in the namespace.
    // The directory is the real gate: on Linux, reaching a socket requires
    // execute permission on the directory holding it, and that check happens
    // before the socket's own mode. Node exposes no way to read peer
    // credentials (SO_PEERCRED) without a native addon, which this package
    // exists to avoid, so filesystem permission is the enforcement.
    mkdirSync(dirname(socketPath), { recursive: true, mode: 0o700 });
    chmodSync(dirname(socketPath), 0o700);

    return probeLiveWriter(socketPath).then((live) => {
      // "Cannot tell" resolves as live: refusing to start is recoverable and
      // visible, displacing a running writer is neither.
      if (live) throw new StoreLockedError(socketPath);
      // Nothing answered, so the file is a leftover rather than a claim.
      rmSync(socketPath, { force: true });
      return instance.bind(socketPath);
    });
  }

  private bind(socketPath: string): Promise<WriterServer> {
    return new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(socketPath, () => {
        this.server.removeListener("error", reject);
        // Belt as well as braces: the 0700 directory is what actually gates
        // access, since reaching a socket needs execute permission on the
        // directory holding it.
        chmodSync(socketPath, 0o600);
        resolve(this);
      });
    });
  }

  get connectionCount(): number {
    return this.sockets.size;
  }

  private handle(socket: Socket): void {
    // One decoder per connection: it holds the tail of a partial frame, so
    // sharing one would splice two streams together.
    const decoder = new FrameDecoder();
    let session: string | null = null;
    this.sockets.add(socket);

    socket.on("data", (chunk: Buffer) => {
      let messages: Message[];
      try {
        messages = decoder.push(chunk);
      } catch (err) {
        // There is no resynchronisation point in a length-prefixed stream once
        // the length is in doubt, so the connection ends here.
        this.log(
          `dropping the connection: ${err instanceof ProtocolError ? err.message : String(err)}`,
        );
        socket.destroy();
        return;
      }
      for (const message of messages) {
        session = this.dispatch(socket, message, session);
      }
    });

    socket.on("error", (err) => {
      this.log(`connection error: ${err.name}`);
      socket.destroy();
    });
    socket.on("close", () => this.sockets.delete(socket));
  }

  private dispatch(
    socket: Socket,
    message: Message,
    session: string | null,
  ): string | null {
    switch (message.type) {
      case "hello": {
        if (session !== null) {
          // A connection gets one session. Without this a second hello on an
          // established connection re-points the store's counter underneath
          // the batches already in flight on it.
          this.send(socket, {
            type: "error",
            seq: null,
            message: "this connection already has a session",
          });
          return session;
        }
        // The store's session and sequence counter are process-global, so a
        // second connection moving them is enough to make every batch on the
        // first one skip as a duplicate: acknowledged, never written, for as
        // long as the writer runs. Reproduced before this guard existed --
        // one hello and one batch at seq 9007199254740000 from a second
        // connection, and every later legitimate batch came back
        // {"stored":0} while the plugin reported "Recording".
        //
        // The design says there is exactly one client, so enforce it: the
        // newcomer wins and the incumbent is closed. Refusing the newcomer
        // instead would let a stuck connection lock out the live plugin.
        for (const other of this.sockets) {
          if (other !== socket) {
            this.log("closing an earlier connection: a new session arrived");
            other.destroy();
          }
        }
        let lastSeq: number;
        try {
          lastSeq = this.store.beginSession(message.session);
        } catch (err) {
          // beginSession runs SELECT, INSERT and UPDATE, any of which can fail
          // on a full disk or a busy store. Unguarded, the throw escapes the
          // socket's data handler, is not covered by main()'s catch, and kills
          // the writer -- which nothing respawns.
          const name = err instanceof Error ? err.name : "Error";
          const code = errorCode(err);
          this.log(
            `session ${message.session} could not start: ${code ?? name}`,
          );
          this.send(socket, {
            type: "error",
            seq: null,
            message: `starting the session failed: ${code ?? name}`,
          });
          return session;
        }
        this.send(socket, {
          type: "welcome",
          session: message.session,
          lastSeq,
        });
        this.log(`session ${message.session} resumed at seq ${lastSeq}`);
        return message.session;
      }
      case "batch": {
        if (session === null) {
          this.send(socket, {
            type: "error",
            seq: message.seq,
            message: "batch before hello",
          });
          return session;
        }
        try {
          const result = this.store.insertBatch(message.seq, message.samples);
          this.send(socket, {
            type: "ack",
            seq: message.seq,
            stored: result.stored,
          });
        } catch (err) {
          // The cause and the batch's size, never a sample's value: this line
          // goes to the Signal K log, which ships in support bundles.
          //
          // `err.name` alone is useless here: every node:sqlite failure is a
          // plain Error, so a full disk and a corrupt store both read as
          // "Error". Measured, the cause is in `code` (ERR_SQLITE_ERROR,
          // ERR_INVALID_STATE) and `errstr` ("attempt to write a readonly
          // database"). Neither carries a recorded value.
          const cause = describeError(err);
          this.log(
            `batch ${message.seq} of ${message.samples.length} samples failed: ${cause}`,
          );
          this.send(socket, {
            type: "error",
            seq: message.seq,
            message: `writing the batch failed: ${cause}`,
          });
        }
        return session;
      }
      default:
        // welcome, ack and error travel the other way. Receiving one means the
        // peer is not a plugin.
        this.log(`unexpected ${message.type} from the client`);
        socket.destroy();
        return session;
    }
  }

  private send(socket: Socket, message: Message): void {
    if (socket.destroyed) return;
    socket.write(encodeFrame(message));
  }

  close(): Promise<void> {
    for (const socket of this.sockets) socket.destroy();
    this.sockets.clear();
    return new Promise((resolve) => this.server.close(() => resolve()));
  }
}
