/**
 * Timing a history query the way a dashboard issues it.
 *
 * The rest of this harness measures processes. This measures a round trip:
 * request to parsed answer, over the Signal K v2 history route, addressed to
 * one provider by plugin id. That is the only surface on which three providers
 * loaded at once can be compared — each answers the same question from its own
 * store, and the figure includes the plugin's own work assembling the result,
 * which a query straight to the backend leaves out.
 *
 * Run it on the device. A round trip measured across a network measures the
 * network.
 *
 * `../query/duck.ts` and the `query` subcommand stay: they measure the DuckDB
 * engine alone, which is a different question and still worth asking.
 */

import { summarize, type Dispersion } from "./statistics.js";

/** Where the v2 history routes live under a Signal K server's base URL. */
const HISTORY_BASE = "/signalk/v2/api/history";

export interface HttpQuerySpec {
  /** The server's origin, e.g. `http://localhost:3000`. */
  baseUrl: string;
  /** Plugin id of the provider to address, e.g. `signalk-questdb-history-provider`. */
  provider: string;
  /** ISO instants, as the route takes them. */
  from: string;
  to: string;
  paths: string[];
  /** Bucket width in seconds. Omitted means the route's own default. */
  resolution?: number;
  context?: string;
}

export interface HttpQueryRun {
  wallMs: number;
  /** Rows in the answer's `data` array. Disagreement between providers on this
   * is a result, not a detail. */
  rows: number;
  status: number;
}

export interface HttpQueryResult {
  provider: string;
  url: string;
  /** The first request. It pays for whatever the provider starts, and it is
   * reported rather than discarded. */
  cold: HttpQueryRun;
  /** Every request after the first. */
  warm: HttpQueryRun[];
  /** Spread across the warm runs. `null` when there were none. */
  warmDispersion: Dispersion | null;
}

export interface HttpQueryOptions {
  /** Total requests, cold included. */
  repeat?: number;
  /** Abandons a request that hangs, so a run cannot stall a campaign. */
  timeoutMs?: number;
}

const DEFAULT_REPEAT = 4;
const DEFAULT_TIMEOUT_MS = 60_000;

export function valuesUrl(spec: HttpQuerySpec): string {
  const url = new URL(`${HISTORY_BASE}/values`, spec.baseUrl);
  url.searchParams.set("from", spec.from);
  url.searchParams.set("to", spec.to);
  url.searchParams.set("paths", spec.paths.join(","));
  if (spec.resolution !== undefined) {
    url.searchParams.set("resolution", String(spec.resolution));
  }
  if (spec.context !== undefined) url.searchParams.set("context", spec.context);
  url.searchParams.set("provider", spec.provider);
  return url.toString();
}

/**
 * The plugin ids the server will answer for.
 *
 * Today the route rejects an unknown id with a 400, so this check is a second
 * lock on the same door. It is here because the first lock is in a repo we do
 * not own, and the failure it prevents — every figure in a condition attributed
 * to the wrong provider — leaves no trace in the numbers.
 */
export async function registeredProviders(
  baseUrl: string,
  timeoutMs = DEFAULT_TIMEOUT_MS,
): Promise<string[]> {
  const url = new URL(`${HISTORY_BASE}/_providers`, baseUrl).toString();
  const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
  if (!response.ok) {
    throw new Error(
      `Could not list history providers: ${url} answered ${response.status}`,
    );
  }
  return Object.keys((await response.json()) as Record<string, unknown>);
}

async function runOnce(url: string, timeoutMs: number): Promise<HttpQueryRun> {
  const started = performance.now();
  const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
  const body = await response.text();
  // After the body, not after the headers: a dashboard waits for the whole
  // answer, and so does this.
  const wallMs = performance.now() - started;

  if (!response.ok) {
    throw new Error(
      `${url} answered ${response.status}: ${body.slice(0, 300)}`,
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    // Described, not quoted. A history answer cut off in transit is also a body
    // that will not parse, and its bytes are recorded vessel values. The size
    // and the content type separate the two cases a reader has to tell apart —
    // a proxy's HTML page from a truncated answer — without carrying any of it.
    const contentType =
      response.headers.get("content-type") ?? "no content type";
    throw new Error(
      `${url} answered ${response.status} with ${body.length} bytes of ${contentType} that did not parse as JSON`,
    );
  }
  const data = (parsed as { data?: unknown }).data;
  if (!Array.isArray(data)) {
    throw new Error(`${url} answered without a "data" array`);
  }

  return { wallMs, rows: data.length, status: response.status };
}

export async function measureHttpQuery(
  spec: HttpQuerySpec,
  options: HttpQueryOptions = {},
): Promise<HttpQueryResult> {
  const repeat = options.repeat ?? DEFAULT_REPEAT;
  // Not `repeat < 1`: NaN fails that comparison, runs the loop zero times, and
  // leaves the result carrying an undefined cold run as if it were measured.
  if (!Number.isInteger(repeat) || repeat < 1) {
    throw new Error(`repeat must be a positive integer, not ${repeat}`);
  }
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  const available = await registeredProviders(spec.baseUrl, timeoutMs);
  if (!available.includes(spec.provider)) {
    throw new Error(
      `The server has no history provider "${spec.provider}". It registered: ${available.join(", ") || "none"}`,
    );
  }

  const url = valuesUrl(spec);
  const runs: HttpQueryRun[] = [];
  for (let attempt = 0; attempt < repeat; attempt += 1) {
    runs.push(await runOnce(url, timeoutMs));
  }

  const [cold, ...warm] = runs;
  return {
    provider: spec.provider,
    url,
    cold,
    warm,
    warmDispersion:
      warm.length > 0 ? summarize(warm.map((r) => r.wallMs)) : null,
  };
}
