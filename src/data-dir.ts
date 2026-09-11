import { isAbsolute, resolve } from "node:path";

/**
 * Where the hot store, the Parquet tree and the expanded DuckDB extension
 * live.
 *
 * The writer, the roll and every query process are spawned separately and
 * must all land on the same directory, so this resolves the configured value
 * once, in the plugin, and the answer is passed to them rather than
 * recomputed. A relative setting resolves against the Signal K plugin data
 * directory, not the current working directory: the spawned processes do not
 * share the server's, so anything cwd-relative would mean different
 * directories in different processes.
 */
export function resolveDataDir(
  configured: string,
  pluginDataDir: string,
): string {
  const trimmed = (configured ?? "").trim();
  if (trimmed === "") return resolve(pluginDataDir);
  if (isAbsolute(trimmed)) return resolve(trimmed);
  return resolve(pluginDataDir, trimmed);
}

/** Sub-directory names under the resolved data directory. Named here so the
 * writer, the roll and the query layer cannot spell them differently. */
/**
 * Mode for the data directory and everything under it.
 *
 * `mkdir`'s mode is masked by umask, so creating at 0700 is not enough on its
 * own — the explicit chmod is the enforcement. Before both, the directories
 * were created at 0755 and only the socket's parent was ever tightened, so the
 * hot store sat world-readable for the few hundred milliseconds it took the
 * writer to boot, and the Parquet tree stayed readable permanently.
 */
export const DATA_DIR_MODE = 0o700;

export const DATA_LAYOUT = {
  /** The SQLite hot store the writer owns. */
  hotStore: "hot",
  /** The Parquet tree the roll writes. */
  tree: "parquet",
  /** Expanded DuckDB extension binaries, keyed by version and platform. */
  extensionCache: "duckdb-extensions",
  /**
   * The cumulative last-value sidecar the roll rewrites.
   *
   * Outside `tree` on purpose: its rows are copies of rows already in the
   * tree, so a reader globbing the tree would count every path's last value
   * twice.
   */
  sidecar: "latest",
  /**
   * Where a roll lets DuckDB spill. Explicit because the engine's default for
   * an in-memory database is relative to the current working directory, which
   * for a spawned process is the Signal K server's.
   */
  scratch: "tmp",
} as const;
