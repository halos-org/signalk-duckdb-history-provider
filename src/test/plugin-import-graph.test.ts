import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

/**
 * The plugin loads inside the Signal K process. The premise of the whole
 * design is that the process pays for path filtering, rate capping and a
 * socket write, and for no storage engine at all — `@duckdb/node-api` carries
 * a native addon of about 100 MB, and the writer, the roll and every query
 * run in separate processes precisely so the server never maps it.
 *
 * One `import` in the wrong file undoes that with no test failing anywhere
 * else, so this walks the compiled entry point's import graph and then checks
 * what a real process actually loads.
 */
const DIST = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ENTRY = join(DIST, "index.js");

/**
 * Matched as a pattern, not as one exact string.
 *
 * `@duckdb/node-api` is the wrapper; the ~100 MB native addon and libduckdb
 * live in `@duckdb/node-bindings-<platform>`. A deep subpath, the bindings
 * package directly, or a future rename all carry the same cost and none of
 * them equals the wrapper's name.
 */
const FORBIDDEN = /(^|\/)@?duckdb/i;

function importedSpecifiers(source: string): string[] {
  const specifiers: string[] = [];
  const patterns = [
    /(?:^|\n)\s*import\s+[^"';]*?from\s*["']([^"']+)["']/g,
    /(?:^|\n)\s*import\s*["']([^"']+)["']/g,
    /(?:^|\n)\s*export\s+[^"';]*?from\s*["']([^"']+)["']/g,
    /\bimport\s*\(\s*["']([^"']+)["']\s*\)/g,
    // createRequire is the documented way to reach CommonJS from ESM, and it
    // is how a lazily-loaded engine would most naturally arrive.
    /\brequire\s*\(\s*["']([^"']+)["']\s*\)/g,
  ];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) specifiers.push(match[1]);
  }
  return specifiers;
}

function walk(entry: string): {
  bare: Set<string>;
  builtins: Set<string>;
  files: string[];
} {
  const bare = new Set<string>();
  const builtins = new Set<string>();
  const files: string[] = [];
  const queue = [entry];
  const seen = new Set<string>();

  while (queue.length > 0) {
    const file = queue.pop() as string;
    if (seen.has(file)) continue;
    seen.add(file);
    files.push(file);
    for (const specifier of importedSpecifiers(readFileSync(file, "utf8"))) {
      if (specifier.startsWith(".")) {
        queue.push(resolve(dirname(file), specifier));
      } else if (specifier.startsWith("node:")) {
        builtins.add(specifier);
      } else {
        bare.add(specifier);
      }
    }
  }
  return { bare, builtins, files };
}

describe("the plugin's import graph", () => {
  it("never reaches the DuckDB engine", () => {
    assert.ok(existsSync(ENTRY), "build first: dist/index.js is missing");
    const { bare, files } = walk(ENTRY);
    assert.ok(files.length > 1, "expected the entry point to import something");
    const offenders = [...bare].filter((specifier) =>
      FORBIDDEN.test(specifier),
    );
    assert.deepEqual(
      offenders,
      [],
      `dist/index.js reaches ${offenders.join(", ")}. The Signal K process ` +
        `must not map the engine: spawn a process for the work instead.`,
    );
  });

  it("never reaches SQLite either", () => {
    // The same rule as the engine, one layer down and easier to trip over:
    // node:sqlite is a native module, and importing it here would put a
    // database handle and its WAL in the server's heap. The hot store is the
    // writer process's, and the writer is spawned rather than imported --
    // which is only true as long as nothing on this side pulls in hot-store.ts
    // for a constant.
    const { builtins } = walk(ENTRY);
    assert.ok(
      !builtins.has("node:sqlite"),
      "dist/index.js reaches node:sqlite. The hot store belongs to the " +
        "writer process; import from writer/contract.js, not writer/main.js.",
    );
  });

  it("keeps the engine out of the writer as well", () => {
    // The writer is long-lived, so an import here would make the ~100 MB
    // addon resident for as long as recording runs — the same cost as
    // mapping it into the server, one process over. It reaches the roll by
    // spawning it, which is the only reason the roll's memory is transient.
    const writer = join(DIST, "writer", "main.js");
    assert.ok(
      existsSync(writer),
      "build first: dist/writer/main.js is missing",
    );
    const { bare, builtins, files } = walk(writer);
    // The positive control the plugin's own check has: without it, a walk that
    // resolved nothing at all passes, and the writer could be importing the
    // engine while this stays green.
    assert.ok(files.length > 1, "expected the writer to import something");
    assert.ok(
      builtins.has("node:sqlite"),
      "the writer must reach node:sqlite through hot-store.js; if it does " +
        "not, this walk did not traverse and proves nothing",
    );
    assert.deepEqual(
      [...bare].filter((specifier) => FORBIDDEN.test(specifier)),
      [],
    );
  });

  it("loads no DuckDB library when a real process starts and runs it", () => {
    // The static walk covers this package's own files. This covers everything
    // else — a dependency that pulls the addon in transitively would map it
    // just the same.
    // Importing the module is not enough: the factory it exports is what the
    // server calls, and a lazily-loaded engine inside start() would map
    // nothing until then. So this runs the plugin, against a throwaway data
    // directory, exactly as the server would.
    const work = mkdtempSync(join(tmpdir(), "sk-parquet-probe-"));
    const probe = [
      `const factory = (await import(${JSON.stringify(pathToFileURL(ENTRY).href)})).default;`,
      `const failures = [];`,
      `let handler = null;`,
      `const app = {`,
      `  debug() {}, error(...a) { failures.push(String(a[0])); },`,
      `  setPluginStatus() {}, setPluginError(m) { failures.push(m); },`,
      `  getDataDirPath: () => ${JSON.stringify(work)},`,
      `  selfContext: "vessels.self",`,
      // The stub the plugin actually needs. Without it start() threw at
      // app.streambundle.getBus(), index.ts swallowed the throw into these
      // no-op reporters, and the probe then measured a plugin that had bailed
      // before doing anything -- passing for the wrong reason.
      `  streambundle: { getBus: () => ({ onValue: (fn) => { handler = fn; return () => {}; } }) },`,
      `};`,
      `const plugin = factory(app);`,
      // Awaited: if start() ever becomes async and loads the engine lazily --
      // the case this test exists for -- an un-awaited call leaves the import
      // pending when the report is taken, and the probe reports nothing.
      `await plugin.start({ defaultSamplingRate: 0 });`,
      // Drive a delta through the real handler, so a lazy load on the
      // per-sample path is reached too.
      `handler?.({ context: "vessels.self", path: "a.b", value: 1, $source: "x" });`,
      `await plugin.stop?.();`,
      // DuckDB only. node:sqlite is compiled into the Node binary rather than
      // loaded as a shared object, and the host's own libsqlite3 is mapped by
      // every process on macOS -- so this report cannot tell our use from the
      // operating system's. The static walk above is what covers node:sqlite.
      `const loaded = process.report.getReport().sharedObjects`,
      `  .filter((p) => /duckdb/i.test(p));`,
      `console.log(JSON.stringify({ loaded, failures, subscribed: handler !== null }));`,
    ].join("\n");
    try {
      const output = execFileSync(
        process.execPath,
        ["--input-type=module", "-e", probe],
        // Bounded: a child that never exits would hang the whole suite with no
        // diagnostic, which is exactly how a /proc deadlock in another test
        // cost a CI run.
        { encoding: "utf8", timeout: 30_000 },
      );
      const result = JSON.parse(output.trim()) as {
        loaded: string[];
        failures: string[];
        subscribed: boolean;
      };
      // Assert the plugin got far enough to be worth measuring, before
      // measuring. Without this the whole check passes on a start that threw.
      assert.deepEqual(
        result.failures,
        [],
        "the plugin reported a failure, so the probe measured an aborted start",
      );
      assert.equal(
        result.subscribed,
        true,
        "the plugin never subscribed, so nothing after that line ran",
      );
      assert.deepEqual(result.loaded, []);
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  });
});
