import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import createPlugin from "../index.js";
import { writerPaths } from "../writer/contract.js";
import type { BusValue } from "../recorder.js";

/**
 * The whole path, with a real writer process on the end of it.
 *
 * Everything else in the suite tests one piece against stubs. This is the only
 * place that proves the pieces are wired to each other: a value handed to the
 * bus becomes a row in a SQLite file written by a different process.
 */

const SELF = "vessels.urn:mrn:signalk:uuid:self";

function stubApp(dataDirPath: string) {
  const calls = {
    status: [] as string[],
    errors: [] as string[],
    logged: [] as unknown[],
    debug: [] as string[],
  };
  let handler: ((value: BusValue) => void) | null = null;
  return {
    calls,
    feed: (value: Partial<BusValue>) =>
      handler?.({
        context: SELF,
        path: "environment.depth.belowKeel",
        value: 4.2,
        $source: "n2k.0",
        ...value,
      }),
    app: {
      debug: (...args: unknown[]) => calls.debug.push(args.join(" ")),
      error: (...args: unknown[]) => calls.logged.push(...args),
      setPluginStatus: (message: string) => calls.status.push(message),
      setPluginError: (message: string) => calls.errors.push(message),
      getDataDirPath: () => dataDirPath,
      selfContext: SELF,
      streambundle: {
        getBus: () => ({
          onValue: (fn: (value: BusValue) => void) => {
            handler = fn;
            return () => {
              handler = null;
            };
          },
        }),
      },
    },
  };
}

/** Reads the hot store while the writer still holds it, as the roll will. */
function storedRows(storePath: string): Record<string, unknown>[] {
  if (!existsSync(storePath)) return [];
  const db = new DatabaseSync(storePath, { readOnly: true });
  try {
    return db.prepare("SELECT * FROM sample ORDER BY rowid").all() as Record<
      string,
      unknown
    >[];
  } catch {
    // The writer may not have created the table yet.
    return [];
  } finally {
    db.close();
  }
}

async function eventually(
  check: () => boolean,
  what: string,
  timeoutMs = 20_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (check()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

describe("a delta reaching the writer's store", () => {
  it("is recorded by a separate process, with its kind intact", async () => {
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-e2e-"));
    const paths = writerPaths(base);
    const { app, feed, calls } = stubApp(base);
    const plugin = createPlugin(app);
    try {
      plugin.start({
        defaultSamplingRate: 0,
        flushIntervalMs: 50,
        flushBatchSize: 3,
      });

      feed({ path: "environment.depth.belowKeel", value: 4.25 });
      feed({ path: "navigation.state", value: "moored" });
      feed({ path: "electrical.switches.anchorLight.state", value: true });

      await eventually(
        () => storedRows(paths.store).length >= 3,
        `three rows in ${paths.store} (errors: ${JSON.stringify(calls.errors)})`,
      );

      const rows = storedRows(paths.store);
      assert.deepEqual(
        rows.map((r) => [r.path, r.value_kind, r.value_num ?? r.value_str]),
        [
          ["environment.depth.belowKeel", "number", 4.25],
          ["navigation.state", "string", "moored"],
          ["electrical.switches.anchorLight.state", "boolean", "true"],
        ],
      );
      // The context is shortened, and the source survives for the history
      // API's `path|sourceRef` filtering.
      assert.equal(rows[0].context, "self");
      assert.equal(rows[0].source, "n2k.0");
      assert.deepEqual(calls.errors, []);
    } finally {
      await plugin.stop();
      // The writer removes its pid file on SIGTERM. The claim on the store is
      // the socket, not this file, but a writer that never got its stop signal
      // would leave it behind and that is worth noticing.
      await eventually(
        () => !existsSync(paths.pidFile),
        "the writer to clean up after itself",
        10_000,
      ).catch(() => {});
      rmSync(base, { recursive: true, force: true });
    }
  });

  /**
   * Every one of these crosses a process boundary as a command-line flag, so a
   * name the two sides spell differently would mean rolling on the default,
   * keeping everything for ever, or compacting a tree an operator asked to
   * leave alone — with nothing failing anywhere. The writer reports back what
   * it was actually given, which is the only place the two spellings meet.
   */
  for (const { compactHourly, interval, expected } of [
    {
      compactHourly: true,
      interval: 30,
      expected: "compacting each completed hour",
    },
    {
      compactHourly: false,
      interval: 30,
      expected: "not compacting: turned off",
    },
    // The flag is on and the merge still cannot run: an hour holds one roll at
    // this interval. Reporting the flag here would tell exactly the devices
    // that get no merge that they are being merged.
    {
      compactHourly: true,
      interval: 60,
      expected: "not compacting: a 60-minute roll already fills an hour",
    },
  ]) {
    it(`reports the writer's effective compaction at ${interval} minutes with the flag ${compactHourly ? "on" : "off"}`, async () => {
      const base = mkdtempSync(join(tmpdir(), "sk-parquet-e2e-"));
      const paths = writerPaths(base);
      const { app, calls } = stubApp(base);
      const plugin = createPlugin(app);
      try {
        plugin.start({
          rollIntervalMinutes: interval,
          retentionDays: 14,
          compactHourly,
        });

        // The writer prints its ready line and the effective configuration
        // in one write, but the plugin relays each stdout chunk as its own
        // debug call, so wait for the whole pattern rather than for the
        // ready line alone.
        const effective = new RegExp(
          `rolling every ${interval} minutes, keeping 14 days, ` +
            expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
        );
        await eventually(
          () => effective.test(calls.debug.join("\n")),
          `the writer to report "${expected}" (errors: ${JSON.stringify(calls.errors)})`,
        );
      } finally {
        await plugin.stop();
        await eventually(
          () => !existsSync(paths.pidFile),
          "the writer to clean up after itself",
          10_000,
        ).catch(() => {});
        rmSync(base, { recursive: true, force: true });
      }
    });
  }

  it("survives a writer that cannot be spawned at all", async () => {
    // A ChildProcess with no `error` listener rethrows the event, and the throw
    // lands a tick after start() returned, so start()'s catch cannot see it.
    // Measured before this was fixed: the process exits with code 1. Inside
    // Signal K that is the whole server, for a failure that should cost one
    // plugin its recording.
    //
    // Driven in a child because the only way to make spawn fail is to make the
    // executable unspawnable, and the plugin spawns process.execPath. The child
    // reports whether it was still alive after the event had to have fired.
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-e2e-"));
    try {
      const script = `
        import createPlugin from ${JSON.stringify(
          new URL("../index.js", import.meta.url).href,
        )};
        const errors = [];
        const app = {
          debug: () => {},
          error: () => {},
          setPluginStatus: () => {},
          setPluginError: (m) => errors.push(m),
          getDataDirPath: () => ${JSON.stringify(base)},
          selfContext: "vessels.self",
          streambundle: { getBus: () => ({ onValue: () => () => {} }) },
        };
        // Nothing can be spawned from here on.
        process.execPath = "/nonexistent/for/this/test/node";
        createPlugin(app).start({});
        setTimeout(() => {
          process.stdout.write("alive " + JSON.stringify(errors) + "\\n");
          process.exit(0);
        }, 500);
      `;
      const child = spawn(
        process.execPath,
        [
          "--disable-warning=ExperimentalWarning",
          "--input-type=module",
          "-e",
          script,
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let out = "";
      let err = "";
      child.stdout.on("data", (c: Buffer) => (out += c.toString()));
      child.stderr.on("data", (c: Buffer) => (err += c.toString()));
      const code = await new Promise<number | null>((resolve) =>
        child.on("exit", (value: number | null) => resolve(value)),
      );

      assert.equal(code, 0, `the plugin took the process down: ${err}`);
      assert.match(out, /^alive /, `child did not survive: ${out} ${err}`);
      assert.match(
        out,
        /could not be started/,
        "the failure was survived but never reported",
      );
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("creates the whole data layout private, from the first moment", async () => {
    // mkdir's mode is masked by umask and does nothing for a directory that
    // already exists, so the chmod is the enforcement. Before both, the
    // directories were created at 0755 and only the socket's parent was ever
    // tightened -- so the hot store sat world-readable for as long as the
    // writer took to boot, and the Parquet tree stayed readable permanently.
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-e2e-"));
    const paths = writerPaths(base);
    const { app, calls } = stubApp(base);
    const plugin = createPlugin(app);
    try {
      plugin.start({});
      await eventually(() => existsSync(paths.store), "the store to appear");

      for (const dir of [base, join(base, "hot"), join(base, "parquet")]) {
        assert.equal(
          statSync(dir).mode & 0o777,
          0o700,
          `${dir} is not owner-only`,
        );
      }
      assert.deepEqual(calls.errors, []);
    } finally {
      await plugin.stop();
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("sends what is buffered before it stops, and waits for the writer", async () => {
    // A graceful stop used to throw the buffer away. At the measured rate that
    // is several hundred samples on every config save, while the configuration
    // describes the flush interval as the window a *power cut* loses. Not
    // waiting for the writer also left the next start racing it, which the
    // plugin then latched as "another writer holds the store".
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-e2e-"));
    const paths = writerPaths(base);
    const { app, feed, calls } = stubApp(base);
    const plugin = createPlugin(app);
    try {
      // A flush interval far longer than the test, so nothing drains by itself.
      plugin.start({
        defaultSamplingRate: 0,
        flushIntervalMs: 600_000,
        flushBatchSize: 1000,
      });
      await eventually(
        () => calls.status.some((s) => /Recording/.test(s)),
        "the handshake",
      );
      for (let i = 1; i <= 4; i++) feed({ value: i });
      assert.equal(storedRows(paths.store).length, 0, "it flushed too early");

      await plugin.stop();

      assert.equal(
        storedRows(paths.store).length,
        4,
        "the buffer was discarded rather than sent",
      );
      assert.equal(
        existsSync(paths.pidFile),
        false,
        "the writer outlived stop",
      );
      assert.deepEqual(calls.errors, []);
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("starts again straight after a stop, without latching on a held store", async () => {
    // Signal K restarts a plugin on every config save. Before stop() waited,
    // the incoming writer could find the outgoing one still listening, exit
    // EXIT_LOCKED, and the plugin latched that as fatal -- for a condition
    // that resolves itself in milliseconds.
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-e2e-"));
    const paths = writerPaths(base);
    const { app, feed, calls } = stubApp(base);
    try {
      for (let run = 0; run < 3; run++) {
        const plugin = createPlugin(app);
        plugin.start({ defaultSamplingRate: 0, flushIntervalMs: 50 });
        await eventually(
          () => calls.status.some((s) => /Recording/.test(s)),
          `run ${run} to connect (errors: ${JSON.stringify(calls.errors)})`,
        );
        feed({ value: run });
        await plugin.stop();
        calls.status.length = 0;
      }
      assert.deepEqual(
        calls.errors.filter((e) => /holds the hot store/.test(e)),
        [],
        "a restart latched on a held store",
      );
      assert.ok(storedRows(paths.store).length >= 3);
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("says so when the writer dies, instead of staying green", async () => {
    // The writer is a separate process, so it can die without taking the
    // server with it. What must not happen is the plugin carrying on showing
    // "Recording" while nothing lands -- the failure this whole design is
    // meant to make impossible.
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-e2e-"));
    const paths = writerPaths(base);
    const { app, feed, calls } = stubApp(base);
    const plugin = createPlugin(app);
    try {
      plugin.start({
        defaultSamplingRate: 0,
        flushIntervalMs: 50,
        flushBatchSize: 1,
      });
      feed({ value: 1 });
      await eventually(
        () => storedRows(paths.store).length >= 1,
        "the first row",
      );
      await eventually(
        () => /Recording/.test(calls.status[calls.status.length - 1] ?? ""),
        "the status to say it is recording",
      );

      // The pid file exists for exactly this: finding the writer from outside.
      // Nothing in the plugin decides anything from it.
      await eventually(
        () => existsSync(paths.pidFile),
        "the writer's pid file",
      );
      const pid = Number.parseInt(
        readFileSync(paths.pidFile, "utf8").trim(),
        10,
      );
      assert.ok(
        Number.isInteger(pid) && pid > 0,
        `bad pid in ${paths.pidFile}`,
      );
      process.kill(pid, "SIGKILL");

      await eventually(
        () => calls.errors.some((line) => /writer process exited/.test(line)),
        `the death to be reported (errors: ${JSON.stringify(calls.errors)})`,
      );
    } finally {
      await plugin.stop();
      rmSync(base, { recursive: true, force: true });
    }
  });
});
