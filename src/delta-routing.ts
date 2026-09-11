/**
 * Decides what kind of value a delta carries, and therefore how the hot store
 * records it.
 *
 * Copied from `signalk-questdb-history-provider` with its suite. The routing
 * **behaviour is deliberately identical**, because Unit 4c has to reproduce
 * that provider's history contract; only the comments are retargeted from its
 * three QuestDB tables to this store's `value_kind` column. Fix bugs in both.
 *
 * The kinds are `number`, `string`, `boolean`, `position` and `identity`.
 * `flatten` is not a kind — it means the value is an object whose scalar
 * leaves are recorded as ordinary paths of their own.
 */
export type DeltaRoute =
  "number" | "string" | "boolean" | "position" | "flatten" | null;

/** One scalar leaf pulled out of an object value, ready to record. */
export interface FlattenedLeaf {
  /** Parent path plus the key, e.g. `navigation.attitude.roll`. */
  path: string;
  value: number | string | boolean;
}

/**
 * Static vessel identity arrives as EMPTY-path object deltas —
 * `{path: "", value: {name: "..."}}` is how a vessel identity report reaches
 * the server, and the exact shape Freeboard reads names from. These never make
 * it past the recorder's path guard, so vessel names were absent from history
 * (dirkwa/signalk-questdb#91). Returns the name when the delta carries a usable
 * one, null otherwise.
 *
 * The recorder stores it under the synthetic path `name` tagged
 * `value_kind = 'identity'`, which is the shape the v1 surface queries for.
 */
export function extractVesselName(path: string, value: unknown): string | null {
  if (path !== "") return null;
  if (value === null || typeof value !== "object") return null;
  const name = (value as { name?: unknown }).name;
  return typeof name === "string" && name.trim() !== "" ? name : null;
}

export function routeDeltaValue(path: string, value: unknown): DeltaRoute {
  // A non-finite number is a source reporting "no reading", and recording
  // nothing is what that means. SQLite and Parquet can both hold NaN and
  // ±Infinity, unlike the line protocol this rule was first written for — so
  // the storage no longer forces the drop and the semantics have to. Keeping
  // it also holds the two copies to the same answer. flattenObjectValue
  // applies the same rule to leaves; the top-level path has to agree.
  if (typeof value === "number")
    return Number.isFinite(value) ? "number" : null;
  if (typeof value === "string") return "string";
  // Booleans are everywhere in Signal K — switch and relay states, pump and
  // valve states, autopilot flags — and are a class of history worth keeping
  // rather than dropping for want of a column. They are recorded as
  // the text "true"/"false" under `value_kind = 'boolean'` rather than as 1
  // and 0: a 1.0 double cannot be told apart from a real numeric channel, and
  // the kind tag is what lets a replayed boolean come back a boolean instead
  // of the word.
  if (typeof value === "boolean") return "boolean";
  // Only `navigation.position` is a position. Other lat/lon-object paths —
  // navigation.anchor.position, which anchor plugins re-emit on every fix
  // while watching — record their latitude and longitude as ordinary scalar
  // leaves instead.
  //
  // In the sibling the reason is structural: its position table has no path
  // column, so a second position path would interleave with the vessel track.
  // Here rows carry their path and that cannot happen. The rule is kept
  // anyway, because it decides what the history surfaces return for a
  // position query, and Unit 4c reproduces that contract.
  if (
    path === "navigation.position" &&
    value !== null &&
    typeof value === "object" &&
    "latitude" in value &&
    "longitude" in value &&
    Number.isFinite((value as { latitude: unknown }).latitude) &&
    Number.isFinite((value as { longitude: unknown }).longitude)
  )
    return "position";
  // Any other non-null, non-array object: record its scalar leaves
  // individually. Arrays are excluded deliberately — their indices are not
  // stable identities, so `foo.0` would silently mean a different thing from
  // one delta to the next.
  if (value !== null && typeof value === "object" && !Array.isArray(value))
    return "flatten";
  return null;
}

/**
 * Scalar leaves of an object value, as dotted paths.
 *
 *   navigation.attitude {roll: 0.02, yaw: 1.57}
 *     -> navigation.attitude.roll  0.02
 *     -> navigation.attitude.yaw   1.57
 *
 * **One level deep, deliberately.** That covers attitude and effectively every
 * real Signal K object, while a recursive walk would happily write out whole
 * nested payloads (notifications, resource documents) that nobody asked to
 * record. A nested object is therefore skipped, not descended into.
 *
 * The leaves are ordinary scalar paths, which is what makes this cheap: no
 * schema change, and sampling, path filtering, the cardinality cap, retention
 * and both history surfaces apply to them unchanged.
 */
export function flattenObjectValue(
  path: string,
  value: unknown,
): FlattenedLeaf[] {
  const leaves: FlattenedLeaf[] = [];
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return leaves;
  }
  // An empty parent would build ".name" — a leading-dot path that matches no
  // Signal K path and no filter pattern. The recorder already returns before
  // reaching here for path "" (that shape is the vessel identity report,
  // handled by extractVesselName), so this is belt-and-braces: the function
  // must not depend on a caller's guard to avoid emitting a malformed path.
  if (path === "") return leaves;
  for (const [key, leaf] of Object.entries(value as Record<string, unknown>)) {
    const leafPath = `${path}.${key}`;
    if (typeof leaf === "number") {
      // A non-finite leaf is "no reading", same rule as the top-level path.
      if (Number.isFinite(leaf)) leaves.push({ path: leafPath, value: leaf });
    } else if (typeof leaf === "string" || typeof leaf === "boolean") {
      leaves.push({ path: leafPath, value: leaf });
    }
    // Anything else — nested object, array, null, undefined — has no column.
  }
  return leaves;
}
