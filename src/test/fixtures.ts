import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Position, Sample } from "../writer/protocol.js";

/**
 * Why a suite that needs the real DuckDB extension may not run.
 *
 * `null` when it can. The binaries are not committed — `./run
 * fetch-extensions` downloads them — so a fresh clone has none and the tests
 * that spawn a real roll would fail for a reason that is not a defect. CI
 * fetches them before the suite, so a skip there means the fetch step failed,
 * and that step fails the job on its own.
 */
export const NO_BUNDLED_EXTENSION: string | false = existsSync(
  join(
    resolve(dirname(fileURLToPath(import.meta.url)), "..", ".."),
    "extensions",
    "manifest.json",
  ),
)
  ? false
  : "no bundled DuckDB extension; run `./run fetch-extensions`";

/**
 * Shared test fixtures.
 *
 * One copy, because five had already drifted: three used
 * `context: "vessels.self"` and two used `"self"`, so three of them described
 * a sample the recorder never produces. Every copy also ended in `as Sample`,
 * which switches the discriminated union off — `sample({kind: "string", value:
 * 4.2})` compiled in all five, which is exactly the mismatch `value_kind`
 * exists to make impossible.
 */

interface Common {
  ts?: number;
  /** As the recorder writes it: `self` for the own vessel. */
  context?: string;
  path?: string;
  source?: string | null;
}

const BASE = {
  ts: 1_700_000_000_000,
  context: "self",
  path: "environment.depth.belowKeel",
  source: "n2k.0" as string | null,
};

/**
 * A sample, typed per kind so the compiler still checks the pairing.
 *
 * Overloaded rather than cast: a wrong value for a kind is a build error here,
 * where five hand-written copies used to accept one silently.
 */
export function sample(
  over?: Common & { kind?: "number"; value?: number },
): Sample;
export function sample(
  over: Common & { kind: "string" | "boolean" | "identity"; value?: string },
): Sample;
export function sample(
  over: Common & { kind: "position"; value?: Position },
): Sample;
export function sample(
  over: Common & { kind?: Sample["kind"]; value?: unknown } = {},
): Sample {
  const { kind = "number", value, ...rest } = over;
  const base = { ...BASE, ...rest };
  switch (kind) {
    case "number":
      return { ...base, kind, value: (value as number) ?? 4.2 };
    case "position":
      return {
        ...base,
        kind,
        value: (value as Position) ?? { latitude: 60.16, longitude: 24.94 },
      };
    default:
      return { ...base, kind, value: (value as string) ?? "moored" };
  }
}

/** A string sample whose JSON is at least `bytes` long, for filling a ceiling. */
export function fat(bytes: number, over: Common = {}): Sample {
  return sample({ ...over, kind: "string", value: "x".repeat(bytes) });
}

/**
 * Polls until `check` holds, so a test never depends on a fixed sleep.
 *
 * Rejects rather than resolving on timeout: a helper that gave up quietly
 * would turn every timing assertion into one that cannot fail.
 *
 * The deadline bounds a failing test; it does not time a passing one. The
 * longest waits here are for a query service to start, which takes 2-4 s on
 * an idle machine and several times that beside other suites.
 */
export async function eventually(
  check: () => boolean,
  what: string,
  timeoutMs = 30_000,
  pollMs = 5,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (check()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}
