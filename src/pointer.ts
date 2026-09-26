/**
 * How an object's fields are named in storage: `<path>#/<key>`, the key an
 * RFC 6901 JSON pointer token. `#` never occurs in a Signal K path, so a field
 * cannot be mistaken for a scalar path of its own, and the name splits back
 * into path and key unambiguously.
 *
 * The same encoding as `signalk-questdb-history-provider/src/storage/pointer.ts`.
 * Engine-free: the plugin, the writer's recorder and the query service all
 * read it.
 */

/** Separates the object's path from the field's pointer token. */
export const POINTER = "#/";

/** The stored name of field `key` of the object at `path`. */
export function pointerPath(path: string, key: string): string {
  // `~` before `/`, or the `~` that `/` becomes would be escaped again.
  return `${path}${POINTER}${key.replaceAll("~", "~0").replaceAll("/", "~1")}`;
}

/** A pointer token back to the key it escapes: `~1` first, then `~0`. */
export function fieldKey(token: string): string {
  return token.replaceAll("~1", "/").replaceAll("~0", "~");
}

/** The object path and field key of a stored name, or null for a scalar path. */
export function splitPointerPath(
  name: string,
): { path: string; key: string } | null {
  const at = name.indexOf(POINTER);
  if (at < 0) return null;
  return {
    path: name.slice(0, at),
    key: fieldKey(name.slice(at + POINTER.length)),
  };
}

/** An object value being put back together from its stored fields. */
export type ObjectValue = Record<string, number | string | boolean>;

/**
 * Sets one field of an object read back from storage.
 *
 * A number is never replaced by text: two deltas from one source in one
 * millisecond read as one, and a field recorded as a number in one of them
 * keeps the number. Defined rather than assigned, so a stored key
 * `__proto__` stays a field instead of replacing the prototype.
 */
export function setField(
  fields: ObjectValue,
  key: string,
  value: number | string | boolean,
): void {
  const held = Object.hasOwn(fields, key) ? fields[key] : undefined;
  if (typeof value !== "number" && typeof held === "number") return;
  Object.defineProperty(fields, key, {
    value,
    enumerable: true,
    writable: true,
    configurable: true,
  });
}
