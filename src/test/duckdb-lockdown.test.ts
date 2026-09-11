import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import type { DuckDBConnection } from "@duckdb/node-api";
import { BASE_DUCKDB_CONFIG, lockDownFileAccess } from "../duckdb/extension.js";

/**
 * What the lockdown is claimed to do, asserted against the engine.
 *
 * These are DuckDB's behaviours rather than this package's, and that is why
 * they are pinned here: the security argument for spawning a query process
 * rests on them, the engine is upgraded from time to time, and
 * `allowed_directories` on its own is silently ineffective — it took a
 * measurement to find that `enable_external_access = false` is what makes it
 * mean anything.
 */

let work: string;
let inside: string;
let outside: string;
let connection: DuckDBConnection;
let close: () => void;

before(async () => {
  work = mkdtempSync(join(tmpdir(), "lockdown-"));
  inside = join(work, "tree", "date=2026-08-23", "1.parquet");
  outside = join(tmpdir(), `outside-${process.pid}.csv`);
  writeFileSync(outside, "a\n1\n");

  const instance = await DuckDBInstance.create(":memory:", {
    ...BASE_DUCKDB_CONFIG,
    temp_directory: work,
  });
  connection = await instance.connect();
  close = () => {
    connection.closeSync();
    instance.closeSync();
  };
  // Written before the lockdown, into a directory that does not exist yet when
  // the allowlist is set — which is the tree's own shape: a new dated
  // directory appears every day and no query re-states the allowlist.
  mkdirSync(join(work, "tree", "date=2026-08-23"), { recursive: true });
  await connection.run(`COPY (SELECT 1 AS a) TO '${inside}' (FORMAT parquet)`);
  await lockDownFileAccess(connection, [work]);
});

after(() => {
  close();
  rmSync(work, { recursive: true, force: true });
  rmSync(outside, { force: true });
});

describe("after the file-access lockdown", () => {
  it("reads a file under the allowed directory", async () => {
    const result = await connection.runAndReadAll(
      `SELECT count(*) FROM read_parquet('${inside}')`,
    );
    assert.equal(Number(result.getRowsJS()[0][0]), 1);
  });

  it("refuses a file outside it", async () => {
    await assert.rejects(
      connection.runAndReadAll(`SELECT count(*) FROM read_csv('${outside}')`),
      /file system operations are disabled by configuration/,
    );
  });

  it("refuses a URL, so a crafted path cannot become a download", async () => {
    await assert.rejects(
      connection.runAndReadAll(
        "SELECT count(*) FROM read_csv('https://example.com/x.csv')",
      ),
      /disabled by configuration/,
    );
  });

  it("cannot install an extension", async () => {
    // The other half of `autoinstall_known_extensions = false`: that setting
    // stops a query from fetching a binary implicitly, and this stops a
    // statement from asking for one.
    await assert.rejects(
      connection.run("INSTALL httpfs"),
      /disabled by configuration/,
    );
  });

  it("cannot have its configuration changed back", async () => {
    await assert.rejects(
      connection.run("SET enable_external_access = true"),
      /configuration has been locked/,
    );
  });
});
