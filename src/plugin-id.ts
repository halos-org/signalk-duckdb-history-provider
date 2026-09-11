/**
 * The plugin's identity, in ONE place.
 *
 * Signal K derives almost nothing from the package name: `plugin.id` is
 * declared in code, and the id then reappears as the sender argument to
 * `app.handleMessage`, in the plugin's config filename, and as the name a
 * history provider registers under. A mismatch at any of those sites fails at
 * runtime, not at build time, so every site reads it from here.
 *
 * **It is deliberately not the package name.** The package is
 * `@halos-org/signalk-duckdb-history-provider`, and this is not, because the
 * id becomes a filename — `plugin-config-data/<id>.json` — and a scope would
 * put a slash in it. `historyApi.defaultProvider` holds this value too, since
 * the server's registry keys on `plugin.id` rather than on the package
 * (`signalk-server/src/interfaces/plugins.ts:965`), so an operator typing the
 * package name there would name a provider that never registers.
 * `@halos-org/skip-freeboard-panel` is the same shape: a scoped package
 * declaring the unscoped id `skip-plotter-panel`.
 */
export const PLUGIN_ID = "signalk-duckdb-history-provider";
