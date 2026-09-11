import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { measureOneShot, ownPeakBytes } from "../bench/one-shot.js";

const onLinux = process.platform === "linux";

describe("measuring a process that does not stay still", () => {
  it("reports what the subject said about itself, exactly", async () => {
    const result = await measureOneShot({
      command: process.execPath,
      args: ["-e", `console.log(JSON.stringify({ peakRssBytes: 123456789 }))`],
      selfReportedPeak: (stdout) =>
        (JSON.parse(stdout.trim()) as { peakRssBytes: number }).peakRssBytes,
    });

    assert.equal(result.exitCode, 0);
    assert.equal(result.peakBytes, 123_456_789);
    assert.equal(result.peakSource, "self-reported");
    // The cross-check is the point of keeping both: a reported figure with no
    // independent reading beside it is a number nobody can audit.
    if (onLinux) {
      assert.ok(result.sampledPeakBytes > 0, "the poll must still have run");
    }
  });

  it("falls back to the poll when the subject says nothing", async () => {
    const result = await measureOneShot({
      command: process.execPath,
      // Long enough to be polled, and it allocates something worth reading.
      args: [
        "-e",
        `const b = Buffer.alloc(64 * 1024 * 1024, 1); setTimeout(() => console.log(b.length), 120)`,
      ],
      sampleIntervalMs: 5,
    });

    assert.equal(result.exitCode, 0);
    assert.equal(result.peakSource, "sampled");
    if (onLinux) {
      assert.ok(result.samples > 0, "expected /proc to have been read");
      assert.ok(
        result.peakBytes > 64 * 1024 * 1024,
        `peak ${result.peakBytes} should exceed the 64 MB the child allocated`,
      );
    } else {
      // No /proc: the result says so rather than pretending to a number.
      assert.equal(result.samples, 0);
      assert.equal(result.peakBytes, 0);
    }
  });

  it("reports a failure rather than throwing", async () => {
    const result = await measureOneShot({
      command: process.execPath,
      args: ["-e", `console.error("no"); process.exit(4)`],
    });
    assert.equal(result.exitCode, 4);
    assert.match(result.stderr, /no/);
  });

  it("reports a command that cannot be spawned at all", async () => {
    const result = await measureOneShot({
      command: "/nonexistent/command",
      args: [],
    });
    assert.equal(result.exitCode, null);
  });
});

describe("a process reading its own high-water mark", () => {
  it("gives a figure on Linux and null anywhere else", () => {
    const peak = ownPeakBytes();
    if (onLinux) {
      assert.ok(peak !== null && peak > 0, `${peak}`);
    } else {
      assert.equal(peak, null);
    }
  });
});
