import { PLUGIN_ID } from "./plugin-id.js";

/**
 * Whether this plugin may claim the history v1 slot.
 *
 * **v1 has no registry.** `signalk-server/src/index.ts:316` is
 * `app.registerHistoryProvider = (provider) => { app.historyProvider = provider }`
 * — one global field, last registration wins — and `unregisterHistoryProvider`
 * ignores its argument, so whichever history plugin stops first removes
 * whoever holds the slot. Neither can be changed from inside a plugin, so what
 * is left is declining to take a slot that belongs to somebody else.
 *
 * v2 needs none of this. Its registry keys providers by plugin id and its
 * default falls back to the first registered provider, so two of them coexist
 * without either one silently winning.
 */

/** What the server's `historyApi` settings hold, of what matters here. */
export interface HistoryApiSettings {
  defaultProvider?: string;
}

export type SlotDecision =
  | { claim: true; because: "configured" | "unconfigured" }
  | { claim: false; configured: string };

/**
 * Claim the slot when this plugin is the configured default **or when nothing
 * is configured**; decline only when the key names somebody else.
 *
 * The second condition is not a convenience. Nothing writes
 * `historyApi.defaultProvider` except the Admin UI:
 * `registerHistoryApiProvider` sets no setting, and `saveConfiguredProvider`
 * is reached only from the `PUT` route behind Apps & Plugins →
 * Configuration. So on a device where nobody ever picked, the key is absent
 * for ever — and a plugin that declined on an absent key would never serve
 * v1 at all, including as the only history provider installed. That is the
 * whole of playback and snapshots, gone, on a fresh device.
 *
 * Declining is therefore reserved for the case it exists for: the operator
 * has chosen another provider, and this one must not take the slot from it.
 */
export function decideHistoryV1Slot(
  settings: HistoryApiSettings | undefined,
): SlotDecision {
  const configured = settings?.defaultProvider;
  if (typeof configured !== "string" || configured.trim() === "") {
    return { claim: true, because: "unconfigured" };
  }
  if (configured === PLUGIN_ID) return { claim: true, because: "configured" };
  return { claim: false, configured };
}

/**
 * What to tell the operator when this plugin stands aside.
 *
 * A decline is reported at error level rather than debug. It means playback
 * and snapshots are being served by another plugin — the thing an operator
 * would otherwise discover by finding this one apparently installed, enabled
 * and not answering.
 */
export function describeDeclinedV1Slot(decision: {
  claim: false;
  configured: string;
}): string {
  return (
    `not registering the history v1 provider: ${decision.configured} is the ` +
    `configured default history provider, and v1 has a single global slot ` +
    `that the last plugin to register takes. Playback and snapshots are ` +
    `served by ${decision.configured}. To serve them from ${PLUGIN_ID}, ` +
    `select it under Apps & Plugins → Configuration and restart the server.`
  );
}

/**
 * What to tell the operator when another provider already holds the slot.
 *
 * Running two history providers on one device is unsupported, so this names
 * the state rather than resolving it — a precedence rule invented here would
 * be one somebody has to maintain for ever, and it would still not survive
 * the other plugin registering second.
 *
 * **It only sees a provider that registered before this plugin started.** The
 * server hands each plugin a shallow copy of the app object, so a provider
 * that registers afterwards is invisible from here and quietly takes the slot.
 * That direction needs a registry, which is a change to signalk-server.
 */
export function describeOccupiedV1Slot(
  decision: SlotDecision,
  occupied: boolean,
): string | null {
  if (!occupied || !decision.claim) return null;
  return (
    `another history provider already holds the v1 slot and ${PLUGIN_ID} is ` +
    `taking it over. Two history providers on one device is unsupported: ` +
    `they record the same deltas twice and the v1 slot belongs to whichever ` +
    `registered last. Disable one of them.`
  );
}
