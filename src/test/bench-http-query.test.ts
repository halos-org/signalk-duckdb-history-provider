import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { measureHttpQuery } from "../bench/http-query.js";

/**
 * A stand-in for the Signal K v2 history routes.
 *
 * It answers `_providers` and `values` the way the server does, and lets each
 * test say what those two should return. The point of measuring through HTTP
 * is that the provider is chosen by a query parameter, so the stub records
 * which one each request asked for.
 */
interface Stub {
  base: string;
  /** Every `provider` query parameter the stub was asked for, in order. */
  asked: string[];
  providers: Record<string, { isDefault: boolean }>;
  /** Rows returned per provider id. */
  rowsFor: Record<string, number>;
  /** When set, `values` answers with this status and body instead. */
  fail: { status: number; body: string } | null;
}

let server: Server;
let stub: Stub;

before(async () => {
  stub = {
    base: "",
    asked: [],
    providers: {
      "provider-a": { isDefault: true },
      "provider-b": { isDefault: false },
    },
    rowsFor: { "provider-a": 60, "provider-b": 42 },
    fail: null,
  };

  server = createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://stub");
    if (url.pathname.endsWith("/history/_providers")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(stub.providers));
      return;
    }
    if (url.pathname.endsWith("/history/values")) {
      const provider = url.searchParams.get("provider") ?? "";
      stub.asked.push(provider);
      if (stub.fail) {
        res.writeHead(stub.fail.status, { "content-type": "application/json" });
        res.end(stub.fail.body);
        return;
      }
      const rows = stub.rowsFor[provider] ?? 0;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          range: {
            from: url.searchParams.get("from"),
            to: url.searchParams.get("to"),
          },
          data: Array.from({ length: rows }, (_, i) => [`t${i}`, i]),
        }),
      );
      return;
    }
    res.writeHead(404).end();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  stub.base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

after(() => server.close());

function spec(provider: string) {
  return {
    baseUrl: stub.base,
    provider,
    from: "2026-09-08T06:00:00Z",
    to: "2026-09-08T07:00:00Z",
    paths: ["navigation.speedOverGround"],
    resolution: 60,
  };
}

describe("timing a history query through the v2 route", () => {
  it("reports the row count and one wall time per repeat", async () => {
    stub.fail = null;
    const result = await measureHttpQuery(spec("provider-a"), { repeat: 4 });

    assert.equal(result.provider, "provider-a");
    assert.equal(result.cold.rows, 60);
    assert.equal(
      result.warm.length,
      3,
      "four repeats are one cold and three warm",
    );
    for (const run of [result.cold, ...result.warm]) {
      assert.equal(run.rows, 60);
      assert.equal(run.status, 200);
      assert.ok(run.wallMs >= 0, "a run carries its own wall time");
    }
  });

  it("keeps the cold run out of the warm figure", async () => {
    stub.fail = null;
    const result = await measureHttpQuery(spec("provider-a"), { repeat: 4 });

    // The first request pays to start whatever the provider starts. Folding it
    // into a mean describes neither it nor the runs after it.
    assert.equal(result.warmDispersion?.n, 3);
    assert.ok(
      !result.warm.includes(result.cold),
      "the cold run must not also appear among the warm ones",
    );
  });

  it("has no warm figure at all for a single repeat", async () => {
    stub.fail = null;
    const result = await measureHttpQuery(spec("provider-a"), { repeat: 1 });

    assert.equal(result.warm.length, 0);
    assert.equal(result.warmDispersion, null);
  });

  it("reports no standard deviation below three warm runs", async () => {
    stub.fail = null;
    const result = await measureHttpQuery(spec("provider-a"), { repeat: 3 });

    assert.equal(result.warmDispersion?.n, 2);
    assert.equal(
      result.warmDispersion?.sd,
      null,
      "two values say nothing a min and a max do not",
    );
  });

  it("addresses the provider it was given, not the default", async () => {
    stub.fail = null;
    stub.asked.length = 0;

    const a = await measureHttpQuery(spec("provider-a"), { repeat: 1 });
    const b = await measureHttpQuery(spec("provider-b"), { repeat: 1 });

    assert.deepEqual(stub.asked, ["provider-a", "provider-b"]);
    assert.equal(a.cold.rows, 60);
    assert.equal(b.cold.rows, 42);
  });

  it("refuses a provider the server has not registered, before timing anything", async () => {
    stub.fail = null;
    stub.asked.length = 0;

    await assert.rejects(
      () => measureHttpQuery(spec("provider-missing"), { repeat: 3 }),
      (error: Error) => {
        assert.match(error.message, /provider-missing/);
        return true;
      },
    );
    assert.deepEqual(
      stub.asked,
      [],
      "nothing may be measured against an absent provider",
    );
  });

  it("fails on a non-200 rather than recording a fast query", async () => {
    stub.fail = {
      status: 400,
      body: JSON.stringify({
        error: "Requested provider not found! (provider-a)",
      }),
    };

    await assert.rejects(
      () => measureHttpQuery(spec("provider-a"), { repeat: 2 }),
      (error: Error) => {
        assert.match(error.message, /400/);
        // The server's own words, not a bare status: a 400 whose reason is
        // thrown away is a debugging session on the device.
        assert.match(error.message, /Requested provider not found/);
        return true;
      },
    );
  });

  it("fails on a body it cannot read as a history answer", async () => {
    stub.fail = { status: 200, body: "<!DOCTYPE html><html>not json</html>" };

    await assert.rejects(
      () => measureHttpQuery(spec("provider-a"), { repeat: 1 }),
      (error: Error) => {
        assert.match(error.message, /parse|JSON|data/i);
        return true;
      },
    );
  });
});
