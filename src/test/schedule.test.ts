import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { delayToNextRoll, nextRollAt } from "../roll/schedule.js";

const iso = (ms: number) => new Date(ms).toISOString();

describe("the roll schedule", () => {
  it("lands on the hour for an hourly interval", () => {
    assert.equal(
      iso(nextRollAt(Date.UTC(2026, 7, 23, 14, 37, 12), 60)),
      "2026-08-23T15:00:00.000Z",
    );
  });

  it("moves past a boundary rather than repeating it", () => {
    // On the boundary the roll for that slot has just run. Returning it again
    // fires a second roll with nothing new in it, once per restart at :00.
    assert.equal(
      iso(nextRollAt(Date.UTC(2026, 7, 23, 15, 0, 0), 60)),
      "2026-08-23T16:00:00.000Z",
    );
  });

  it("aligns to UTC midnight, not to whenever the writer started", () => {
    for (const minutes of [1, 5, 15, 60, 240, 720, 1440]) {
      const at = nextRollAt(Date.UTC(2026, 7, 23, 23, 59, 59), minutes);
      assert.equal(at % (minutes * 60_000), 0, `${minutes}`);
    }
    assert.equal(
      iso(nextRollAt(Date.UTC(2026, 7, 23, 23, 59, 59), 1440)),
      "2026-08-24T00:00:00.000Z",
    );
  });

  it("crosses midnight without a short or a long slot", () => {
    const before = nextRollAt(Date.UTC(2026, 7, 23, 22, 30), 240);
    const after = nextRollAt(before, 240);
    assert.equal(iso(before), "2026-08-24T00:00:00.000Z");
    assert.equal(after - before, 240 * 60_000);
  });

  it("reports a delay that never goes backwards", () => {
    const now = Date.UTC(2026, 7, 23, 14, 37, 12);
    assert.equal(delayToNextRoll(now, 60), nextRollAt(now, 60) - now);
    assert.ok(delayToNextRoll(now, 15) > 0);
  });

  it("refuses an interval that names no schedule", () => {
    for (const minutes of [0, -1, 7, 100, 1441, 1.5]) {
      assert.throws(
        () => nextRollAt(Date.UTC(2026, 7, 23), minutes),
        /divide the day/,
        `${minutes}`,
      );
    }
  });
});
