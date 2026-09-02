import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { HOUR_MS, coversHour, planCompaction } from "../compact/plan.js";
import { DATA_LAYOUT } from "../data-dir.js";

/**
 * Which roll files a merge folds in, decided from names alone.
 *
 * The tree is written here as empty files: the planner never opens one, and a
 * test that produced real Parquet would be testing DuckDB instead of the rule.
 */

const HOUR = Date.UTC(2026, 8, 2, 11);
let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "compact-plan-"));
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

function tree(date: string, ...names: string[]): void {
  const directory = join(dir, DATA_LAYOUT.tree, `date=${date}`);
  mkdirSync(directory, { recursive: true });
  for (const name of names) writeFileSync(join(directory, name), "");
}

describe("coversHour", () => {
  /**
   * A roll writes the interval that just ended, so the roll at the top of the
   * hour belongs to the hour below it and the one at the top of the previous
   * hour does not.
   */
  it("takes the roll at the closing boundary and not the opening one", () => {
    assert.equal(coversHour(HOUR, HOUR), false);
    assert.equal(coversHour(HOUR + 300_000, HOUR), true);
    assert.equal(coversHour(HOUR + HOUR_MS, HOUR), true);
    assert.equal(coversHour(HOUR + HOUR_MS + 1, HOUR), false);
  });
});

describe("planCompaction", () => {
  it("is empty when there is no tree", () => {
    assert.deepEqual(planCompaction(dir, HOUR), []);
  });

  it("collects the hour's rolls and names the output for the hour", () => {
    tree(
      "2026-09-02",
      `${HOUR + 300_000}.parquet`,
      `${HOUR + 600_000}.parquet`,
      `${HOUR + HOUR_MS}.parquet`,
    );
    const [unit, ...rest] = planCompaction(dir, HOUR);
    assert.equal(rest.length, 0);
    assert.equal(unit.inputs.length, 3);
    assert.equal(basename(unit.output), `hour-${HOUR}.parquet`);
    assert.equal(unit.temp, `${unit.output}.tmp`);
  });

  it("leaves rolls from other hours alone", () => {
    tree(
      "2026-09-02",
      `${HOUR - 300_000}.parquet`,
      `${HOUR + 300_000}.parquet`,
      `${HOUR + 600_000}.parquet`,
      `${HOUR + HOUR_MS + 300_000}.parquet`,
    );
    const [unit] = planCompaction(dir, HOUR);
    assert.deepEqual(
      unit.inputs.map((p) => basename(p)),
      [`${HOUR + 300_000}.parquet`, `${HOUR + 600_000}.parquet`],
    );
  });

  /**
   * A roll spanning midnight writes the same name in two date directories, and
   * each is merged where it lies. Moving rows between directories would break
   * the only pruning this layout has.
   */
  it("produces one unit per date directory a roll reached", () => {
    tree(
      "2026-09-02",
      `${HOUR + 300_000}.parquet`,
      `${HOUR + 600_000}.parquet`,
    );
    tree(
      "2026-09-03",
      `${HOUR + 300_000}.parquet`,
      `${HOUR + 600_000}.parquet`,
    );
    const units = planCompaction(dir, HOUR);
    assert.equal(units.length, 2);
    assert.deepEqual(
      units.map((u) => basename(u.directory)),
      ["date=2026-09-02", "date=2026-09-03"],
    );
  });

  it("skips a directory holding only one of the hour's rolls", () => {
    tree("2026-09-02", `${HOUR + 300_000}.parquet`);
    assert.deepEqual(planCompaction(dir, HOUR), []);
  });

  /**
   * A merge is not a roll, so a second pass must not fold one into another —
   * that would rewrite the same rows every hour for ever.
   */
  it("never reads a file an earlier merge wrote", () => {
    tree(
      "2026-09-02",
      `hour-${HOUR}.parquet`,
      `${HOUR + 300_000}.parquet`,
      `${HOUR + 600_000}.parquet`,
    );
    const [unit] = planCompaction(dir, HOUR);
    assert.equal(
      unit.inputs.some((p) => basename(p).startsWith("hour-")),
      false,
    );
  });

  it("ignores a half-written temp file and anything not a roll", () => {
    tree(
      "2026-09-02",
      `${HOUR + 300_000}.parquet`,
      `${HOUR + 600_000}.parquet`,
      `${HOUR + 900_000}.parquet.tmp`,
      "notes.txt",
    );
    const [unit] = planCompaction(dir, HOUR);
    assert.equal(unit.inputs.length, 2);
  });

  it("ignores an entry that is not a date directory", () => {
    mkdirSync(join(dir, DATA_LAYOUT.tree, "scratch"), { recursive: true });
    tree(
      "2026-09-02",
      `${HOUR + 300_000}.parquet`,
      `${HOUR + 600_000}.parquet`,
    );
    assert.equal(planCompaction(dir, HOUR).length, 1);
  });
});
