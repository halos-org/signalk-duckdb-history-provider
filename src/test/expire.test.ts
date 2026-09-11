import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { expire } from "../retention/expire.js";
import { sidecarFile, treeRoot } from "../roll/tree-path.js";

const DAY_MS = 86_400_000;

/** A day the tests can count from: 2026-08-20T00:00:00Z. */
const DAY_ZERO = Date.UTC(2026, 7, 20);

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "expire-"));
});

afterEach(() => {
  // A test that made a directory unwritable has to give it back, or the
  // cleanup fails and the next run starts in a full tmpdir.
  try {
    chmodSync(treeRoot(dir), 0o700);
  } catch {
    /* the tree was never created */
  }
  rmSync(dir, { recursive: true, force: true });
});

/** A date directory holding one file, as a roll leaves it. */
function tree(...days: number[]): void {
  for (const day of days) {
    const directory = join(treeRoot(dir), `date=${segment(day)}`);
    mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, `${DAY_ZERO + day * DAY_MS}.parquet`), "x");
  }
}

/** The `YYYY-MM-DD` for a day offset from DAY_ZERO. */
function segment(day: number): string {
  return new Date(DAY_ZERO + day * DAY_MS).toISOString().slice(0, 10);
}

/** Which date directories are left, as day offsets from DAY_ZERO. */
function remaining(): number[] {
  return readdirSync(treeRoot(dir))
    .filter((entry) => entry.startsWith("date="))
    .map((entry) => Date.parse(`${entry.slice(5)}T00:00:00.000Z`))
    .map((ms) => (ms - DAY_ZERO) / DAY_MS)
    .sort((a, b) => a - b);
}

/** An instant part-way through a day, which is where a clock usually is. */
function middayOf(day: number): () => number {
  return () => DAY_ZERO + day * DAY_MS + DAY_MS / 2;
}

describe("expire", () => {
  it("removes the days the window has passed and keeps the rest", () => {
    tree(0, 1, 2, 3, 4, 5);
    const result = expire({
      dataDir: dir,
      retentionDays: 2,
      now: middayOf(5),
    });

    // The reference is midday on day 5, so the window opens at midday on
    // day 3. Day 3 straddles that and survives whole — which is why two days
    // of retention leave three directories.
    assert.deepEqual(remaining(), [3, 4, 5]);
    assert.deepEqual(result.removed, [segment(0), segment(1), segment(2)]);
    assert.deepEqual(result.failures, []);
  });

  it("keeps everything when retention is zero", () => {
    tree(0, 1, 2, 3, 4, 5);
    const result = expire({
      dataDir: dir,
      retentionDays: 0,
      now: middayOf(5),
    });

    assert.deepEqual(remaining(), [0, 1, 2, 3, 4, 5]);
    assert.deepEqual(result.removed, []);
  });

  it("keeps a day that straddles the boundary, whole", () => {
    tree(0, 1, 2);
    // Midday on day 2 with a one-day window puts the boundary at midday on
    // day 1, inside that directory. Retention is a storage bound, so the
    // samples before midday on day 1 stay.
    expire({ dataDir: dir, retentionDays: 1, now: middayOf(2) });

    assert.deepEqual(remaining(), [1, 2]);
  });

  it("never removes the newest date directory", () => {
    tree(0);
    expire({ dataDir: dir, retentionDays: 1, now: middayOf(400) });

    assert.deepEqual(remaining(), [0]);
  });

  it("caps the reference at the newest day, so a clock jump loses nothing", () => {
    tree(0, 1, 2, 3);
    // The clock says a year later — a device whose RTC read garbage before
    // NTP. Wall-clock retention would delete the whole tree.
    expire({ dataDir: dir, retentionDays: 2, now: middayOf(365) });

    assert.deepEqual(remaining(), [1, 2, 3]);
  });

  it("caps the reference at the clock, so a future directory loses nothing", () => {
    tree(0, 1, 2, 300);
    expire({ dataDir: dir, retentionDays: 2, now: middayOf(2) });

    assert.deepEqual(remaining(), [0, 1, 2, 300]);
  });

  it("leaves entries that are not date directories alone", () => {
    tree(0, 5);
    mkdirSync(join(treeRoot(dir), "date=2026-08-32"));
    mkdirSync(join(treeRoot(dir), "notes"));
    writeFileSync(join(treeRoot(dir), "README"), "x");

    const result = expire({
      dataDir: dir,
      retentionDays: 1,
      now: middayOf(5),
    });

    assert.deepEqual(result.removed, [segment(0)]);
    assert.ok(existsSync(join(treeRoot(dir), "date=2026-08-32")));
    assert.ok(existsSync(join(treeRoot(dir), "notes")));
    assert.ok(existsSync(join(treeRoot(dir), "README")));
  });

  it("leaves the sidecar alone", () => {
    tree(0, 5);
    mkdirSync(dirname(sidecarFile(dir)), { recursive: true });
    writeFileSync(sidecarFile(dir), "x");

    expire({ dataDir: dir, retentionDays: 1, now: middayOf(5) });

    assert.ok(existsSync(sidecarFile(dir)));
  });

  it("holds the tree steady: a day arrives, a day goes, indefinitely", () => {
    // The verification the unit asks for, at day granularity and without
    // waiting forty days for it. One directory a day for forty days, expired
    // after each: the count has to stop rising, not merely rise slower.
    const counts: number[] = [];
    for (let day = 0; day < 40; day += 1) {
      tree(day);
      expire({ dataDir: dir, retentionDays: 5, now: middayOf(day) });
      counts.push(remaining().length);
      assert.equal(remaining().at(-1), day, `day ${day} survives its own roll`);
    }

    // Six, not five: the day the boundary falls in survives whole, which is
    // the storage bound this documents rather than a deletion guarantee.
    assert.deepEqual(counts.slice(0, 6), [1, 2, 3, 4, 5, 6]);
    assert.deepEqual(new Set(counts.slice(6)), new Set([6]));
  });

  it("does nothing, and says nothing, when there is no tree", () => {
    const result = expire({
      dataDir: dir,
      retentionDays: 1,
      now: middayOf(5),
    });

    assert.deepEqual(result, { removed: [], failures: [], treeError: null });
  });

  it("reports a tree it cannot list, rather than reading as a clean run", (t) => {
    if (process.getuid?.() === 0) {
      t.skip("root ignores directory permissions");
      return;
    }
    tree(0, 5);
    // 0100: the directory can be traversed but not listed. An empty result
    // here is indistinguishable from a device that has never rolled, and the
    // scheduler would report a clean roll while the tree grows without bound.
    chmodSync(treeRoot(dir), 0o100);

    const result = expire({
      dataDir: dir,
      retentionDays: 1,
      now: middayOf(5),
    });

    assert.equal(result.treeError, "EACCES");
    assert.deepEqual(result.removed, []);
  });

  it("reports a directory it could not remove and expires the rest", (t) => {
    if (process.getuid?.() === 0) {
      t.skip("root ignores directory permissions");
      return;
    }
    tree(0, 1, 5);
    // 0500: the entry can be listed but not unlinked from its parent.
    chmodSync(treeRoot(dir), 0o500);

    const result = expire({
      dataDir: dir,
      retentionDays: 1,
      now: middayOf(5),
    });

    assert.deepEqual(result.removed, []);
    assert.deepEqual(
      result.failures.map((failure) => failure.date),
      [segment(0), segment(1)],
    );
    // A code, not a message. A filesystem error's message carries the whole
    // path it failed on, and these lines ship in support bundles.
    assert.equal(result.failures[0].why, "EACCES");
    assert.ok(!result.failures[0].why.includes(dir));
    chmodSync(treeRoot(dir), 0o700);
    assert.deepEqual(remaining(), [0, 1, 5]);
  });

  it("removes the oldest first, so a failure part-way leaves the newest", () => {
    tree(0, 1, 2, 10);
    const result = expire({
      dataDir: dir,
      retentionDays: 1,
      now: middayOf(10),
    });

    assert.deepEqual(result.removed, [segment(0), segment(1), segment(2)]);
  });

  it("treats a retention under one day, or no number at all, as keep-forever", () => {
    for (const retentionDays of [
      0.5,
      -3,
      Number.NaN,
      Number.POSITIVE_INFINITY,
    ]) {
      rmSync(treeRoot(dir), { recursive: true, force: true });
      tree(0, 5);
      const result = expire({ dataDir: dir, retentionDays, now: middayOf(5) });
      assert.deepEqual(result.removed, [], `retentionDays ${retentionDays}`);
      assert.deepEqual(remaining(), [0, 5], `retentionDays ${retentionDays}`);
    }
  });

  it("rounds a fractional retention down to whole days", () => {
    tree(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    // 3.9 is 3, not 3.9. Carrying the fraction into the boundary would put it
    // 0.9 of a day earlier and take day 6 with it.
    expire({ dataDir: dir, retentionDays: 3.9, now: middayOf(10) });

    assert.deepEqual(remaining(), [7, 8, 9, 10]);
  });
});
