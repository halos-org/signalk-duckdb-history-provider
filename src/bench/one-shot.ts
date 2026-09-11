import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { parseProcStatusMemory } from "./proc.js";

/**
 * Measuring a process that does not stay still long enough to be sampled.
 *
 * The rest of this harness measures a running subject: settle, then equal
 * windows, differencing counters for rates. A roll lives between one and
 * fifteen seconds, has no steady state, and its whole cost is a transient —
 * so none of that applies, and the subject is gone before a window closes.
 *
 * What is left is the kernel's own high-water mark. `VmHWM` is a maximum the
 * kernel already tracked, not an instantaneous reading, so a poll every few
 * milliseconds cannot miss a peak that happened between two polls — only one
 * that happens after the last poll and before the process exits. That window
 * is why the measured process is asked to report its own figure as well: when
 * it does, that number is exact and the polled one is kept beside it as a
 * cross-check.
 */

export interface OneShotResult {
  /** The figure to report: the process's own if it gave one, else the poll's. */
  peakBytes: number;
  /** Where `peakBytes` came from. A poll can only under-report. */
  peakSource: "self-reported" | "sampled";
  /** The highest `VmHWM` this process read from outside. */
  sampledPeakBytes: number;
  /** How many times /proc was read. Zero means the process was too quick. */
  samples: number;
  wallMs: number;
  exitCode: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
}

export interface OneShotOptions {
  command: string;
  args: string[];
  /**
   * How often to read `VmHWM`. Cheap — one small file — and only the interval
   * between the last read and the exit is unmeasured.
   */
  sampleIntervalMs?: number;
  /**
   * Pulls the subject's own peak out of what it printed. Given the whole of
   * stdout; returns bytes, or null when the output carries no such figure.
   */
  selfReportedPeak?: (stdout: string) => number | null;
}

const DEFAULT_SAMPLE_INTERVAL_MS = 10;

export async function measureOneShot(
  options: OneShotOptions,
): Promise<OneShotResult> {
  const started = performance.now();
  const child = spawn(options.command, options.args, {
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk: Buffer) => (stdout += chunk.toString()));
  child.stderr?.on("data", (chunk: Buffer) => (stderr += chunk.toString()));
  // An EventEmitter with no error listener rethrows the event, which would
  // take the measuring process down for a failure in the measured one.
  child.stdout?.on("error", () => {});
  child.stderr?.on("error", () => {});

  let sampledPeakBytes = 0;
  let samples = 0;
  const sampler = setInterval(() => {
    if (child.pid === undefined) return;
    try {
      const status = readFileSync(`/proc/${child.pid}/status`, "utf8");
      sampledPeakBytes = Math.max(
        sampledPeakBytes,
        parseProcStatusMemory(status).peakBytes,
      );
      samples += 1;
    } catch {
      // The process is gone, or this is not Linux. Both are reported by what
      // the result says rather than by throwing here.
    }
  }, options.sampleIntervalMs ?? DEFAULT_SAMPLE_INTERVAL_MS);

  const { exitCode, signal } = await new Promise<{
    exitCode: number | null;
    signal: NodeJS.Signals | null;
  }>((resolve) => {
    child.on("error", () => resolve({ exitCode: null, signal: null }));
    // `close`, not `exit`: exit can fire while the subject's last line is
    // still in the pipe, and that line is the figure being measured.
    child.on("close", (code, sig) => resolve({ exitCode: code, signal: sig }));
  });
  clearInterval(sampler);

  const selfReported = options.selfReportedPeak?.(stdout) ?? null;
  return {
    peakBytes: selfReported ?? sampledPeakBytes,
    peakSource: selfReported === null ? "sampled" : "self-reported",
    sampledPeakBytes,
    samples,
    wallMs: performance.now() - started,
    exitCode,
    signal,
    stdout,
    stderr,
  };
}

/**
 * This process's own high-water mark, for a subject that reports it.
 *
 * `null` off Linux, where there is no `/proc/self/status` and the harness's
 * numbers do not mean anything anyway.
 */
export function ownPeakBytes(): number | null {
  try {
    return parseProcStatusMemory(readFileSync("/proc/self/status", "utf8"))
      .peakBytes;
  } catch {
    return null;
  }
}

/**
 * This process's resident size now.
 *
 * The peak is what a process that exits is judged on; a process that stays
 * needs the current figure beside it, because the two diverge — DuckDB does
 * not return what a large query allocated, so a query service settles near its
 * own high-water mark rather than back at its idle cost.
 */
export function ownResidentBytes(): number | null {
  try {
    return parseProcStatusMemory(readFileSync("/proc/self/status", "utf8"))
      .rssBytes;
  } catch {
    return null;
  }
}
