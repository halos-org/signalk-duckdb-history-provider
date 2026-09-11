import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import createPlugin from "../index.js";
import { ConfigSchema } from "../config/schema.js";
import { DATA_LAYOUT } from "../data-dir.js";
import { PLUGIN_ID } from "../plugin-id.js";
import type { BusValue } from "../recorder.js";

/**
 * The plugin is the only runtime behaviour this build ships, and its catch
 * turns every startup failure into a status string and returns normally — so
 * a mis-wired error path fails nothing anywhere else.
 */
function stubApp(dataDirPath: string) {
  const calls = {
    status: [] as string[],
    errors: [] as string[],
    logged: [] as unknown[],
    busValues: [] as ((value: BusValue) => void)[],
    unsubscribed: 0,
    registered: [] as unknown[],
    registeredV1: [] as unknown[],
  };
  return {
    calls,
    app: {
      debug: () => {},
      error: (...args: unknown[]) => calls.logged.push(...args),
      setPluginStatus: (message: string) => calls.status.push(message),
      setPluginError: (message: string) => calls.errors.push(message),
      getDataDirPath: () => dataDirPath,
      selfContext: "vessels.self",
      registerHistoryApiProvider: (provider: unknown) =>
        calls.registered.push(provider),
      registerHistoryProvider: (provider: unknown) =>
        calls.registeredV1.push(provider),
      streambundle: {
        getBus: () => ({
          onValue: (fn: (value: BusValue) => void) => {
            calls.busValues.push(fn);
            return () => {
              calls.unsubscribed++;
            };
          },
        }),
      },
    },
  };
}

describe("the plugin", () => {
  it("declares the id and schema the server reads", () => {
    const { app } = stubApp("/nonexistent");
    const plugin = createPlugin(app);
    assert.equal(plugin.id, PLUGIN_ID);
    assert.equal(plugin.schema, ConfigSchema);
    assert.equal(typeof plugin.start, "function");
    assert.equal(typeof plugin.stop, "function");
  });

  it("creates the storage layout under the resolved data directory", () => {
    // Unit 2's writer will assume these exist. Nothing else asserts that
    // DATA_LAYOUT is used rather than merely declared.
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-plugin-"));
    try {
      const { app, calls } = stubApp(base);
      const plugin = createPlugin(app);
      plugin.start({});
      for (const sub of Object.values(DATA_LAYOUT)) {
        assert.ok(existsSync(join(base, sub)), `${sub} was not created`);
      }
      assert.equal(calls.errors.length, 0);
      assert.match(calls.status[0], /paths across/);
      plugin.stop();
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("subscribes to the delta bus and unsubscribes on stop", () => {
    // A subscription that outlives the plugin keeps recording into a client
    // that has stopped, and Signal K reuses the bus across enable/disable.
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-plugin-"));
    try {
      const { app, calls } = stubApp(base);
      const plugin = createPlugin(app);
      plugin.start({});
      assert.equal(calls.busValues.length, 1, "never subscribed");

      plugin.stop();
      assert.equal(calls.unsubscribed, 1, "left the bus subscribed");
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("honours a configured data directory over the plugin's own", () => {
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-plugin-"));
    try {
      const { app } = stubApp(join(base, "plugin"));
      const plugin = createPlugin(app);
      plugin.start({ dataDir: join(base, "elsewhere") });
      assert.ok(existsSync(join(base, "elsewhere", DATA_LAYOUT.hotStore)));
      assert.ok(!existsSync(join(base, "plugin", DATA_LAYOUT.hotStore)));
      plugin.stop();
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("reports a startup failure to the log as well as the status line", () => {
    // setPluginError writes to a field nobody sees unless they open the Admin
    // UI. The server log is what survives a reboot and ships in a support
    // bundle, and it is the only one that gets the stack.
    //
    // The failure is forced with a directory path that runs through a regular
    // file, which is ENOTDIR on every platform. An earlier version used a path
    // under /proc, which is Linux-specific and hung the whole test file there.
    const base = mkdtempSync(join(tmpdir(), "sk-parquet-plugin-"));
    try {
      const file = join(base, "a-file");
      writeFileSync(file, "not a directory");
      const { app, calls } = stubApp(base);
      createPlugin(app).start({ dataDir: join(file, "below") });

      assert.equal(calls.status.length, 0, "reported success on failure");
      assert.equal(calls.errors.length, 1);
      assert.match(calls.errors[0], /Startup failed/);
      assert.equal(
        calls.logged.length,
        1,
        "the failure never reached the server log",
      );
      assert.ok(calls.logged[0] instanceof Error, "the log lost the stack");
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it("registers both history surfaces", async () => {
    const work = mkdtempSync(join(tmpdir(), "sk-parquet-history-"));
    const { app, calls } = stubApp(work);
    const plugin = createPlugin(app);
    try {
      plugin.start({});

      assert.equal(calls.registered.length, 1, "no v2 provider was registered");
      const v2 = calls.registered[0] as Record<string, unknown>;
      for (const method of ["getValues", "getPaths", "getContexts"]) {
        assert.equal(
          typeof v2[method],
          "function",
          `the registered v2 provider has no ${method}`,
        );
      }

      assert.equal(calls.registeredV1.length, 1, "no v1 provider registered");
      const v1 = calls.registeredV1[0] as Record<string, unknown>;
      // All three, not just playback: the snapshot route calls `getHistory` and
      // the WebSocket handler calls `hasAnyData` before it offers playback at
      // all.
      for (const method of ["hasAnyData", "streamHistory", "getHistory"]) {
        assert.equal(
          typeof v1[method],
          "function",
          `the registered v1 provider has no ${method}`,
        );
      }

      // Nothing unregisters here: the server queues that itself when a provider
      // is registered, and runs it before `stop()` is entered.
      await plugin.stop();
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  });

  it("records even on a server with no history registry", async () => {
    // The plugin declares no server version floor. Recording is the half with
    // no alternative, so an older server must not lose it over a surface it
    // cannot serve.
    const work = mkdtempSync(join(tmpdir(), "sk-parquet-noreg-"));
    const { app, calls } = stubApp(work);
    const older = { ...app } as Record<string, unknown>;
    delete older.registerHistoryApiProvider;
    delete older.registerHistoryProvider;
    const plugin = createPlugin(older as typeof app);
    try {
      plugin.start({});
      assert.deepEqual(calls.errors, []);
      assert.equal(calls.busValues.length, 1, "the plugin did not subscribe");
      await plugin.stop();
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  });

  it("registers the surface a server does have", async () => {
    // The two registries arrived in different server releases, so they are
    // asked for separately: half a history API is what such a server can serve,
    // and it is better than none.
    const work = mkdtempSync(join(tmpdir(), "sk-parquet-v2only-"));
    const { app, calls } = stubApp(work);
    const older = { ...app } as Record<string, unknown>;
    delete older.registerHistoryProvider;
    const plugin = createPlugin(older as typeof app);
    try {
      plugin.start({});
      assert.deepEqual(calls.errors, []);
      assert.equal(calls.registered.length, 1, "the v2 surface was lost too");
      assert.equal(calls.registeredV1.length, 0);
      await plugin.stop();
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  });

  it("stops without throwing, whether or not it started", () => {
    const { app } = stubApp("/nonexistent");
    const plugin = createPlugin(app);
    assert.doesNotThrow(() => plugin.stop());
  });
});

describe("the history v1 slot", () => {
  /** Starts the plugin against a server whose settings and slot are given. */
  async function startWith(over: Record<string, unknown>) {
    const work = mkdtempSync(join(tmpdir(), "sk-parquet-v1slot-"));
    const { app, calls } = stubApp(work);
    const plugin = createPlugin({ ...app, ...over } as typeof app);
    try {
      plugin.start({});
      await plugin.stop();
      return { calls, said: calls.logged.map(String).join("\n") };
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  }

  it("takes it when no default provider is configured", async () => {
    // The common case: nothing writes that setting except the Admin UI, so a
    // device where nobody picked has no key at all. Declining here would mean
    // no playback and no snapshots on a device this plugin is alone on.
    const { calls, said } = await startWith({});
    assert.equal(calls.registeredV1.length, 1);
    assert.equal(said, "");
  });

  it("takes it when it is the configured default", async () => {
    const { calls } = await startWith({
      config: { settings: { historyApi: { defaultProvider: PLUGIN_ID } } },
    });
    assert.equal(calls.registeredV1.length, 1);
  });

  it("stands aside for another configured provider, and says so", async () => {
    const { calls, said } = await startWith({
      config: {
        settings: {
          historyApi: { defaultProvider: "signalk-questdb-history-provider" },
        },
      },
    });
    assert.equal(calls.registeredV1.length, 0, "the slot was taken anyway");
    assert.match(said, /not registering the history v1 provider/);
    assert.match(said, /signalk-questdb-history-provider/);
    // v2 is unaffected: its registry keys by plugin id and two providers
    // coexist there without either one silently winning.
    assert.equal(calls.registered.length, 1, "the v2 surface was lost too");
  });

  it("reports a slot another provider already held", async () => {
    const { calls, said } = await startWith({
      historyProvider: { streamHistory: () => {} },
    });
    assert.equal(calls.registeredV1.length, 1, "it still takes the slot");
    assert.match(said, /already holds the v1 slot/);
    assert.match(said, /unsupported/);
  });

  it("does not report a takeover it is not performing", async () => {
    const { calls, said } = await startWith({
      historyProvider: { streamHistory: () => {} },
      config: {
        settings: {
          historyApi: { defaultProvider: "signalk-questdb-history-provider" },
        },
      },
    });
    assert.equal(calls.registeredV1.length, 0);
    assert.doesNotMatch(said, /already holds the v1 slot/);
    assert.match(said, /not registering the history v1 provider/);
  });
});
