import { createHash, randomUUID } from "node:crypto";
import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  statSync,
  unlinkSync,
  writeSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { syncDirectory } from "../durable-write.js";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import {
  SQLITE_SCANNER,
  bundledExtensionRelPath,
  currentDuckdbPlatform,
  duckdbVersionFromPackageVersion,
} from "./duckdb-version.js";
import { sqlLiteral } from "./sql.js";

/**
 * Base DuckDB settings for every instance this package creates.
 *
 * Both flags are security settings, not conveniences: with autoload on, a
 * query naming an unknown function makes DuckDB fetch a binary from the
 * internet and run it, at query time, in a process that holds the vessel's
 * history. `lockDownFileAccess` below is the other half of that, and runs
 * once everything legitimate is open.
 */
export const BASE_DUCKDB_CONFIG: Readonly<Record<string, string>> =
  Object.freeze({
    autoinstall_known_extensions: "false",
    autoload_known_extensions: "false",
  });

export interface PlatformEntry {
  /** Of the compressed file as shipped. */
  sha256: string;
  bytes: number;
  /** Of the expanded binary, so a truncated cache entry is detectable without
   * hashing 27 MB on every spawned process. */
  expandedBytes: number;
}

export interface ExtensionManifest {
  duckdbVersion: string;
  platforms: Record<string, PlatformEntry>;
}

/** The installed package's root, from this module's own location. */
export function packageRoot(): string {
  // dist/duckdb/extension.js -> dist/duckdb -> dist -> package root
  return resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
}

/**
 * The DuckDB version this package is pinned to, read from the dependency that
 * supplies the engine. `package.json` pins it exactly for this reason: a range
 * would let an install move the engine away from the bundled extension, which
 * only shows up as a `LOAD` failure on the device.
 */
export function pinnedDuckdbVersion(root: string = packageRoot()): string {
  const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as {
    dependencies?: Record<string, string>;
  };
  const spec = pkg.dependencies?.["@duckdb/node-api"];
  if (!spec) {
    throw new Error("package.json does not depend on @duckdb/node-api");
  }
  return duckdbVersionFromPackageVersion(spec);
}

export function readManifest(root: string = packageRoot()): ExtensionManifest {
  const path = join(root, "extensions", "manifest.json");
  if (!existsSync(path)) {
    throw new Error(
      `No bundled DuckDB extensions found at ${path}. ` +
        `Run \`./run fetch-extensions\` in a clone; an installed copy should ` +
        `have shipped them.`,
    );
  }
  // Three readers share this file and none of them owns its shape, so it is
  // checked here rather than cast. An absent `platforms` otherwise surfaces as
  // a TypeError raised while building the message for a different failure.
  const manifest = JSON.parse(
    readFileSync(path, "utf8"),
  ) as Partial<ExtensionManifest>;
  if (
    typeof manifest.duckdbVersion !== "string" ||
    typeof manifest.platforms !== "object" ||
    manifest.platforms === null
  ) {
    throw new Error(`${path} is not a readable extension manifest`);
  }
  // The engine and the extension are shipped together and must agree; a
  // mismatch here means an install moved one of them, and `LOAD` would report
  // it much later with a message naming neither version.
  const pinned = pinnedDuckdbVersion(root);
  if (manifest.duckdbVersion !== pinned) {
    throw new Error(
      `The bundled extensions are for DuckDB ${manifest.duckdbVersion} but ` +
        `this package pins ${pinned}.`,
    );
  }
  return manifest as ExtensionManifest;
}

export interface ResolveOptions {
  /** Where the decompressed binary is cached. The plugin's data directory —
   * the package directory is not reliably writable. */
  cacheDir: string;
  root?: string;
  platform?: string;
}

/**
 * The absolute path of a loadable `sqlite_scanner` for this platform,
 * expanding the bundled copy on first use.
 *
 * The package ships the binary gzipped, because that is 8 MB against 27 MB
 * and npm would compress it anyway. DuckDB's `LOAD '<path>'` needs the
 * decompressed file, so the first caller on a device expands it into the
 * cache and every later one finds it there. Version- and platform-keyed, so
 * a DuckDB upgrade expands a fresh copy rather than loading a stale one.
 *
 * Named for what it does. "resolve" reads as a path computation, and a caller
 * who believes that will call it in a loop.
 */
export function ensureExtensionExtracted(options: ResolveOptions): string {
  const root = options.root ?? packageRoot();
  const platform = options.platform ?? currentDuckdbPlatform();
  const manifest = readManifest(root);
  const version = manifest.duckdbVersion;
  const expected = manifest.platforms[platform];

  // A binary the manifest does not describe is not something to load. The
  // fetch script writes files inside its loop and the manifest only after it,
  // so an interrupted fetch leaves exactly this state — and skipping the
  // checksum for it would mean the integrity check quietly does nothing in
  // the one case where something has already gone wrong.
  if (!expected) {
    const shipped = Object.keys(manifest.platforms).sort().join(", ") || "none";
    throw new Error(
      `This build bundles ${SQLITE_SCANNER} for ${shipped}, not for ` +
        `${platform}. Add it to the published platform set, or run ` +
        `\`./run fetch-extensions ${platform}\` in a clone.`,
    );
  }

  const targetDir = join(options.cacheDir, `v${version}`, platform);
  const target = join(targetDir, `${SQLITE_SCANNER}.duckdb_extension`);
  // Length, not a hash: hashing 27 MB in every spawned query process is a real
  // cost, and a truncated file — what a power cut between the write and the
  // rename leaves behind — is exactly what a length catches. A file of the
  // right length and wrong content is not a failure a boat produces.
  if (existsSync(target) && statSync(target).size === expected.expandedBytes) {
    return target;
  }

  const source = join(root, bundledExtensionRelPath(version, platform));
  if (!existsSync(source)) {
    throw new Error(
      `${source} is missing, though the manifest lists ${platform}. The ` +
        `published tarball did not carry it.`,
    );
  }

  const compressed = readFileSync(source);
  const actual = createHash("sha256").update(compressed).digest("hex");
  if (actual !== expected.sha256) {
    throw new Error(
      `Bundled ${SQLITE_SCANNER} for ${platform} does not match the manifest ` +
        `checksum (${actual} vs ${expected.sha256}).`,
    );
  }

  mkdirSync(targetDir, { recursive: true });

  // Expand through a temporary name and rename into place: several query
  // processes can spawn at once, and a reader must never see a half-written
  // 27 MB binary. The fsync is what makes that survive a power cut — rename is
  // atomic against a concurrent reader and says nothing about what reached
  // the disk, and power loss is the normal way a vessel's Pi shuts down.
  // Unique per call, not per process. Worker threads share a pid, so a
  // pid-named temporary collides between two concurrent extractions in one
  // process: the second `openSync(…, "wx")` fails EEXIST and its catch then
  // unlinks the FIRST caller's live temporary, whose rename fails ENOENT.
  // Both callers fail and the cache is never populated. A unique name means
  // the catch can only ever remove its own file.
  const temp = join(targetDir, `${SQLITE_SCANNER}.${randomUUID()}.tmp`);
  sweepStaleTemporaries(targetDir, temp);
  try {
    const fd = openSync(temp, "wx", 0o644);
    try {
      writeSync(fd, gunzipSync(compressed));
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(temp, target);
    syncDirectory(targetDir);
  } catch (err) {
    try {
      unlinkSync(temp);
    } catch {
      /* the temp file may never have been created */
    }
    throw err;
  }
  return target;
}

/** How long a temporary must sit untouched before it counts as an orphan
 * rather than a live extraction. Expanding 27 MB takes well under a second
 * even on an SD card, so an hour is far past any legitimate write. */
const ORPHAN_AGE_MS = 60 * 60 * 1000;

/**
 * Remove `*.tmp` left by a process that died between the write and the
 * rename. The catch in the caller covers a thrown error; it does not cover
 * SIGKILL, an OOM kill or a power cut, and each orphan is 27 MB on the same
 * card that holds the hot store and the Parquet tree.
 *
 * Age is what makes this safe. Several query processes can extract at once,
 * and unlinking a temporary another process is *currently* writing does not
 * fail — POSIX unlink succeeds on an open file, the writer carries on into an
 * unlinked inode, and its `renameSync` then fails with ENOENT. So a sweep that
 * simply deleted every `.tmp` would break the concurrency this function exists
 * to support.
 */
function sweepStaleTemporaries(directory: string, own: string): void {
  const cutoff = Date.now() - ORPHAN_AGE_MS;
  for (const entry of readdirSync(directory)) {
    if (!entry.startsWith(`${SQLITE_SCANNER}.`) || !entry.endsWith(".tmp")) {
      continue;
    }
    const path = join(directory, entry);
    if (path === own) continue;
    try {
      if (statSync(path).mtimeMs > cutoff) continue;
      unlinkSync(path);
    } catch {
      /* it may have been renamed into place, or swept by another process */
    }
  }
}

/** Minimal shape of what `DuckDBConnection` offers us, so this module stays
 * out of the plugin's import graph — see src/test/plugin-import-graph.test.ts. */
export interface Runnable {
  run(sql: string): Promise<unknown>;
}

/** Load the bundled sqlite_scanner into an open connection. */
export async function loadSqliteScanner(
  connection: Runnable,
  options: ResolveOptions,
): Promise<string> {
  const path = ensureExtensionExtracted(options);
  // A single-quoted literal: the path is ours (package root plus configured
  // cache directory), and DuckDB has no parameter binding for LOAD.
  await connection.run(`LOAD '${sqlLiteral(path)}'`);
  return path;
}

/**
 * Confine the engine to `directories` and freeze its configuration.
 *
 * **`allowed_directories` on its own does nothing.** Measured against DuckDB
 * 1.5.5: with external access left enabled, a file outside the list is read
 * happily. `enable_external_access = false` is what turns the list into an
 * allowlist rather than a decoration — after both, a path outside it fails
 * with "file system operations are disabled by configuration", `INSTALL`
 * cannot reach its extension repository, and an `https://` URL is refused.
 *
 * Directories are prefix-matched, so the data directory covers the tree's
 * dated sub-directories, the sidecar, the hot store's WAL and shm files, the
 * expanded extension cache and the spill directory — including the dated
 * directories that do not exist yet when this runs.
 *
 * **Everything legitimate must already be open.** The extension has to be
 * loaded and the hot store attached before this call; an attached database
 * keeps working afterwards, and a `LOAD` afterwards would not.
 */
export async function lockDownFileAccess(
  connection: Runnable,
  directories: string[],
): Promise<void> {
  const list = directories.map((dir) => `'${sqlLiteral(dir)}'`).join(", ");
  await connection.run(`SET allowed_directories = [${list}]`);
  await connection.run("SET enable_external_access = false");
  await connection.run("SET lock_configuration = true");
}
