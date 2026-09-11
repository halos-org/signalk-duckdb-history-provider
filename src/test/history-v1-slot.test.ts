import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  decideHistoryV1Slot,
  describeDeclinedV1Slot,
  describeOccupiedV1Slot,
} from "../history-v1-slot.js";
import { PLUGIN_ID } from "../plugin-id.js";

const OTHER = "signalk-questdb-history-provider";

describe("who may take the history v1 slot", () => {
  it("claims it when nobody is configured", () => {
    // The case the gate exists to NOT break. Nothing writes
    // `historyApi.defaultProvider` except the Admin UI, so on a device where
    // the operator never picked, the key is absent for ever — and declining
    // here would mean no playback and no snapshots on a fresh install.
    for (const settings of [
      undefined,
      {},
      { defaultProvider: undefined },
      { defaultProvider: "" },
      { defaultProvider: "   " },
    ]) {
      assert.deepEqual(
        decideHistoryV1Slot(settings),
        { claim: true, because: "unconfigured" },
        JSON.stringify(settings),
      );
    }
  });

  it("claims it when this plugin is the configured default", () => {
    assert.deepEqual(decideHistoryV1Slot({ defaultProvider: PLUGIN_ID }), {
      claim: true,
      because: "configured",
    });
  });

  it("declines when the key names somebody else", () => {
    assert.deepEqual(decideHistoryV1Slot({ defaultProvider: OTHER }), {
      claim: false,
      configured: OTHER,
    });
  });
});

describe("what the operator is told", () => {
  it("names who holds it and how to change it, when declining", () => {
    const message = describeDeclinedV1Slot({ claim: false, configured: OTHER });
    assert.match(message, new RegExp(OTHER));
    assert.match(message, new RegExp(PLUGIN_ID));
    // Actionable, not merely descriptive: an operator reading this in the log
    // has to learn where the setting lives.
    assert.match(message, /Apps & Plugins/);
  });

  it("says nothing about an occupied slot it is not taking", () => {
    // Declining is already reported on its own. Saying both would describe a
    // takeover that is not happening.
    assert.equal(
      describeOccupiedV1Slot({ claim: false, configured: OTHER }, true),
      null,
    );
  });

  it("says nothing when the slot was free", () => {
    assert.equal(
      describeOccupiedV1Slot({ claim: true, because: "unconfigured" }, false),
      null,
    );
  });

  it("reports a takeover as unsupported rather than resolving it", () => {
    const message = describeOccupiedV1Slot(
      { claim: true, because: "configured" },
      true,
    );
    assert.ok(message !== null);
    assert.match(message, /unsupported/);
    assert.match(message, /Disable one of them/);
  });
});
