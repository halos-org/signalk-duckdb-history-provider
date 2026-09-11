import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { renderComparison } from "../bench/report.js";
import {
  MetricResult,
  RESULT_FORMAT_VERSION,
  RunResult,
} from "../bench/run.js";
import { summarize } from "../bench/statistics.js";

function metric(
  unit: string,
  values: number[],
  extra: Partial<MetricResult> = {},
): MetricResult {
  return {
    unit,
    perWindow: values,
    dispersion: summarize(values),
    halfWindowDrift: values.map(() => 0),
    steady: true,
    ...extra,
  };
}

function run(
  label: string,
  cpu: number[],
  extra: Partial<RunResult> = {},
): RunResult {
  return {
    formatVersion: RESULT_FORMAT_VERSION,
    harnessVersion: "0.1.0",
    label,
    startedAt: "2026-08-23T00:00:00.000Z",
    host: "test-host",
    windows: 3,
    windowSeconds: 300,
    measuredWindowSeconds: summarize([300, 300, 300]),
    settleSeconds: 180,
    tolerance: 0.1,
    devices: ["mmcblk0"],
    notes: [],
    subjects: [
      {
        name: "signalk",
        kind: "pid",
        target: "pid 1",
        metrics: {
          cpuPercentOfCore: metric("% of one core", cpu),
          memoryMb: metric("MB", [87, 88, 87], {
            peak: 218,
            steady: null,
            halfWindowDrift: null,
          }),
        },
      },
    ],
    ...extra,
  };
}

describe("renderComparison", () => {
  it("puts the conditions side by side with their spread", () => {
    const table = renderComparison([
      run("control", [23.1, 23.2, 23.3]),
      run("sqhp", [25.3, 25.5, 25.6]),
    ]);
    assert.match(table, /\| control\s+\| sqhp\s+\|/);
    assert.match(table, /signalk CPU \(% of one core\)/);
    assert.match(table, /23\.20 \(23\.10–23\.30\)/);
  });

  it("marks a metric whose window halves disagreed", () => {
    const drifting = run("parquet", [10, 20, 30]);
    drifting.subjects[0].metrics.cpuPercentOfCore.steady = false;
    const table = renderComparison([drifting]);
    assert.match(table, /†/);
    assert.match(table, /covered a transition/);
  });

  it("leaves a metric with no half-window split unmarked, and says so once", () => {
    // `steady: null` is "never checked", not "checked and agreed". It renders
    // without the marker — but the legend must not imply the metric passed.
    const table = renderComparison([run("parquet", [1, 1, 1])]);
    const memoryRow = table
      .split("\n")
      .find((line) => line.includes("signalk memory"));
    assert.ok(memoryRow, "expected a memory row");
    assert.ok(!memoryRow.includes("†"), memoryRow);
  });

  it("reports gauge peaks apart from the means, in the gauge's own unit", () => {
    const table = renderComparison([run("parquet", [1, 1, 1])]);
    assert.match(table, /peak 218\.0 MB/);
    assert.match(table, /different quantities/);
  });

  it("carries a peak for any gauge, not only for memory", () => {
    // `peak` is documented as "gauges only", so filtering the section by the
    // metric name would silently drop a gauge a later unit adds.
    const withFds = run("parquet", [1, 1, 1]);
    withFds.subjects[0].metrics.openFiles = metric("count", [12, 13, 12], {
      peak: 400,
      steady: null,
      halfWindowDrift: null,
    });
    assert.match(renderComparison([withFds]), /peak 400\.0 count/);
  });

  it("refuses to render one measurement reported in two units", () => {
    // The row header takes whichever unit came first and every later cell is
    // printed beneath it, so this would render a table that looks right and
    // is out by a factor of 1000.
    const kb = run("control", [1, 1, 1]);
    const mb = run("parquet", [1, 1, 1]);
    mb.subjects[0].metrics.cpuPercentOfCore.unit = "% of four cores";
    assert.throws(() => renderComparison([kb, mb]), /cannot share a row/);
  });

  it("says so when the runs do not share a method", () => {
    // Comparing a 300-second window against a 30-second one is the sort of
    // thing that reads fine in a table and means nothing.
    const table = renderComparison([
      run("control", [1, 1, 1]),
      run("parquet", [2, 2, 2], { windowSeconds: 30 }),
    ]);
    assert.match(table, /do not share a method and are not comparable/);
  });

  it("counts a differing tolerance as a differing method", () => {
    // Tolerance decides what the † marker means, so two runs that disagree on
    // it carry markers that are not the same claim.
    const table = renderComparison([
      run("control", [1, 1, 1]),
      run("parquet", [2, 2, 2], { tolerance: 0.9 }),
    ]);
    assert.match(table, /do not share a method and are not comparable/);
  });

  it("fills a cell a condition does not have", () => {
    const other = run("parquet", [1, 1, 1]);
    other.subjects[0].name = "writer";
    const table = renderComparison([run("control", [1, 1, 1]), other]);
    assert.match(table, /—/);
  });

  it("carries the conditions' notes", () => {
    const annotated = run("parquet", [1, 1, 1], {
      notes: ["1000 ms cap, self only"],
    });
    assert.match(renderComparison([annotated]), /1000 ms cap, self only/);
  });

  it("names the disks the system rows were measured against", () => {
    // A mistyped --device sums to zero, which renders as a legitimate 0.00.
    // Naming the device set is what lets a reader tell those apart.
    const table = renderComparison([run("control", [1, 1, 1])]);
    assert.match(table, /mmcblk0/);
  });

  it("escapes a pipe in a label so it cannot forge a column", () => {
    // --label is free text. An unescaped pipe adds a column and shifts every
    // cell in that row under the wrong condition.
    const table = renderComparison([run("a|b", [1, 1, 1])]);
    const header = table.split("\n").find((l) => l.startsWith("| measurement"));
    assert.ok(header, "expected a header row");
    // Counted on unescaped pipes only — a `\|` is a literal pipe in a cell,
    // not a column boundary.
    const columns = header.split(/(?<!\\)\|/);
    assert.equal(columns.length, 4, `the label forged a column: ${header}`);
    assert.match(table, /a\\\|b/);
  });

  it("keeps a newline in a label from forging Markdown structure", () => {
    // The table was escaped; the Conditions list, the peaks list and the
    // method line interpolated the same free text raw, so a newline there
    // started a new block and the report described something else.
    const sneaky = run(
      "real\n\n## Injected heading\n\n- forged bullet",
      [1, 1, 1],
      {
        notes: ["note\nwith a break"],
      },
    );
    const table = renderComparison([sneaky]);
    // The text survives — it is the operator's label. What must not survive is
    // its position: nothing from inside a value may begin a line, because that
    // is what makes it Markdown structure rather than content.
    const lines = table.split("\n");
    assert.ok(
      !lines.some((line) => line.startsWith("#")),
      `a label started a heading:\n${table}`,
    );
    assert.ok(
      !lines.some((line) => line.startsWith("- forged bullet")),
      `a label forged a list item:\n${table}`,
    );
    // The text itself survives, on one line, where it belongs.
    const conditions = table
      .split("\n")
      .find((line) => line.startsWith("- **real"));
    assert.ok(conditions, `expected a Conditions bullet, got:\n${table}`);
    assert.match(conditions, /note with a break/);
  });

  it("keeps emphasis characters from closing the construct they sit in", () => {
    // The Conditions bullet wraps the label in **…**; an unescaped asterisk
    // would close it early and re-style the rest of the line.
    const table = renderComparison([
      run("a*b_c`d", [1, 1, 1], { notes: ["n"] }),
    ]);
    assert.match(table, /- \*\*a\\\*b\\_c\\`d\*\* —/);
  });

  it("refuses to compare nothing", () => {
    assert.throws(() => renderComparison([]));
  });

  it("survives the round trip through JSON that every real run takes", () => {
    // `bench run -o file.json` then `bench compare file.json` is how these
    // numbers reach a human; nothing else in the suite crosses that boundary.
    const original = renderComparison([run("control", [23.1, 23.2, 23.3])]);
    const roundTripped = renderComparison([
      JSON.parse(JSON.stringify(run("control", [23.1, 23.2, 23.3]))),
    ]);
    assert.equal(roundTripped, original);
  });
});
