# AGENTS.md

Guidance for AI assistants working in this repository.

## Commands

- `npm run build` — `tsc` into `dist/`, then the bundled-extension gate
- `npm test` — the Node test runner against **compiled** output
  (`dist/test/**/*.test.js`). Build first; `npm run build:all` does both.
  Three test files run at a time: several of them spawn a process that maps
  DuckDB's ~100 MB addon, and at the runner's default of one file per core
  the suite starves its own children when other suites run beside it.
- `npm run format` — prettier write + `eslint --fix`
- `npm run ci-lint` — what CI checks
- Single test file: `node --test dist/test/bench-run.test.js` after a build

Node ≥ 22.13.0. That floor is `node:sqlite`, not a preference: it needs
`--experimental-sqlite` below 22.13.0, and the writer imports it unflagged. The
API is identical on 22, 24 and 26 — checked on all three rather than assumed.

`./run` lists everything with descriptions.

## What this is

A Signal K plugin that serves the history APIs out of Parquet files rather
than a database server. It is a peer alternative to
`signalk-questdb-history-provider`, selected per device. **Nothing in that
repository changes because of this one**, and no shared package is extracted —
the two history surfaces are copied and their query construction rewritten,
which is deliberate and recorded in
[halos-org/halos#152](https://github.com/halos-org/halos/issues/152).

The plan is that issue; the units are its sub-issues. Read the unit before
implementing it.

## The rule the design rests on

**The Signal K process does path filtering, rate capping and a socket write,
and no storage work whatsoever.** Everything else runs in its own process: the
writer owns the SQLite hot store, the roll is short-lived because DuckDB's
allocator does not return memory in-process, and queries are answered by a
service process that holds the engine so the server never has to.

So `src/index.ts` and everything it imports must never reach
`@duckdb/node-api` — nor `node:sqlite`, which is a native module and would
put a database handle and its WAL in the server's heap. One import undoes the
plugin's entire reason to exist by mapping a ~100 MB native addon into the
server, and nothing else would fail.
`src/test/plugin-import-graph.test.ts` matches any `@duckdb/*` specifier — the
native addon lives in `@duckdb/node-bindings-<platform>`, not in the wrapper —
including `require()` and `createRequire` forms, and then starts the plugin in
a real process and checks what it mapped. The Signal K integration workflow
checks `/proc/<pid>/maps` on a running server. Do not weaken any of the three:
a lazily-loaded engine inside a query handler is the shape that would slip past
a check on module evaluation alone.

## Layout

- `src/index.ts` — the plugin the server loads. Subscribes to the delta bus,
  spawns the writer process, and reports what is happening in the status line.
- `src/recorder.ts` — the whole of the Signal K process's involvement: path
  filter, rate cap, cardinality cap, and a sample handed onwards.
- `src/flush-buffer.ts` — what is held between flushes. Bounded in **bytes**,
  not elements, and the bytes are measured by serialising rather than
  estimated, because JSON escaping can expand a value sixfold and an estimate
  below the truth makes the ceiling fictional.
- `src/writer/` — the writer process and the socket to it. `main.ts` is its
  entry point and must never be imported by the plugin; `contract.ts` holds the
  exit codes and paths both sides need, which is why it exists at all.
- `src/config/schema.ts` — TypeBox. One source for both the Admin UI's JSON
  schema and the `Config` type. Add options here, and add a README row.
- `src/plugin-id.ts` — the id, in one place. It reappears as `plugin.id`, as
  the `handleMessage` sender and as the config filename; none of those
  mismatches fails at build time.
- `src/data-dir.ts` — resolves the configured directory once, in the plugin,
  because the spawned processes do not share the server's working directory.
- `src/delta-routing.ts` — copied from `signalk-questdb-history-provider` with
  its suite. Routing behaviour is identical and must stay that way: it decides
  what a position query returns, and Unit 4c reproduces that provider's history
  contract. Only the comments were retargeted, from its three QuestDB tables to
  this store's `value_kind` column. Fix bugs in both.
- `src/path-matcher.ts`, `src/time-range.ts` — copied from
  `signalk-questdb-history-provider` with their suites. Fix bugs in both.
  They currently **differ** from the sibling by three fixes: `resolveTimeRange`
  treats a zero duration as a value rather than as absent, `Throttle` tests a
  pair for presence rather than using a `0` sentinel, and `Throttle` treats a
  backwards clock step as a discontinuity rather than as a sample arriving too
  soon — without that, a device whose clock is stepped at boot stops recording
  for the length of the step. The sibling is tracked
  at
  [signalk-questdb-history-provider#23](https://github.com/halos-org/signalk-questdb-history-provider/issues/23).
- `src/duckdb/` — version and platform naming, the bundled-extension resolver,
  and the standalone offline check. The resolver only finds and expands the
  engine's binary and does not import it; `check-extension.ts` does import it,
  and may, because it is a CLI that exits.
- `src/roll/` — the roll. `main.ts` is the process the writer spawns, `roll.ts`
  the work it does, `schedule.ts` the every-N-minutes-from-UTC-midnight grid,
  `tree-path.ts` the tree's paths and the hour rule the merge, the reader and
  the roll all share. **The rule is not a file list and not "one directory owns
  the engine": `@duckdb/node-api` may never be reachable from `src/index.ts` or
  from `src/writer/`, both of which run for as long as recording does.**
  Everything that does import it runs in its own process: `roll/roll.ts`,
  `compact/compact.ts` and `duckdb/check-extension.ts` in one that exits,
  `query/reader.ts` in the long-lived query service.
  `src/test/plugin-import-graph.test.ts` checks the invariant against
  the compiled plugin and writer; nothing checks a count, which is why the
  count is not stated here.
- `src/compact/` — the hourly merge. `main.ts` is the process the writer
  spawns at a slot that closes an hour, whether or not that slot had rows to
  roll; `compact.ts` the work it does; `plan.ts` the pure rule choosing which
  files it reads and what it writes. Its preconditions live in `compactHour`,
  not in its callers: the scheduler and a hand-run backfill reach the same
  function and only one of them knows the schedule. **A merged hour is only
  ever trusted after it has been read back** — an existing `hour-<H>.parquet`
  licenses the deletion of the rolls beside it only once those rolls' rows have
  been shown to be in it.
- `src/retention/expire.ts` — dropping whole date directories the retention
  window has passed. It imports no engine and is called at the end of a roll,
  after the roll's own files are in the tree. Read "Retention" below before
  changing the boundary: what it deletes is a bound on storage rather than a
  deletion guarantee, and that distinction is documented to operators.
- `src/query/` — reading. `duck.ts` is the side that runs inside the Signal K
  process: it starts the query service, queues requests, enforces the deadline
  and restarts what dies, and it imports no engine. `main.ts` is the service —
  one process, many requests — and `reader.ts` the work it does: file
  selection, the seam, and one statement per request.
- `src/history-v1.ts` — the history v1 WebSocket surface: `hasAnyData`,
  `streamHistory` and `getHistory`, all three registered by the plugin. The
  chunk/resume machine and the delta grouping are copied from
  `signalk-questdb-history-provider/src/history-v1.ts`; the queries are not.
  Fix contract bugs in both. It **differs** from the sibling by three: the own
  vessel's context is resolved in `getHistory` as well as in `streamHistory`
  ([#32](https://github.com/halos-org/signalk-questdb-history-provider/issues/32)),
  a `playbackRate` that is not a number falls back to real time instead of
  replaying every window with no delay
  ([#33](https://github.com/halos-org/signalk-questdb-history-provider/issues/33)),
  and a window that has not happened yet is not read
  ([#34](https://github.com/halos-org/signalk-questdb-history-provider/issues/34)).
- `src/history-v1-slot.ts` — who may take the v1 slot, and what the operator is
  told when this plugin stands aside or takes it from somebody. Pure and
  engine-free; the plugin supplies the settings and the observed slot. See "The
  history surface" for why declining on an absent setting would be wrong.
- `src/history-v2.ts` — the history v2 REST surface, registered by the plugin.
  Mostly contract behaviour copied from
  `signalk-questdb-history-provider/src/history-v2.ts` — the moving averages,
  the context normalisation, the timestamp union and the column assembly —
  with only the query construction rewritten. Fix contract bugs in both.
- `src/durable-write.ts` — fsync, rename, fsync the directory. Shared so the
  order exists once.

- `src/bench/` — the measurement harness. Every unit that reports a number
  reports it through this, so figures stay comparable.
- `docs/layout-decision.md` — what the roll writes, how often, and why: one
  Parquet file per roll under a dated directory, no path partitioning, an
  hourly compaction pass, plus a last-value sidecar. Read it before touching
  the roll or the reader; the measurements behind each choice are in it, and so
  is what reopened the compaction decision after it had been rejected once.
- `docs/query-layer.md` — what a query costs, measured through the shipped
  reader: the 336–375 ms an engine takes to start, what a warm query costs
  against it, what the service holds while it waits, and the layout decision
  re-checked against all three. Read it before changing anything about how a
  query is executed.

## The writer

One writer process per plugin run, spawned by the plugin. **Its listening
socket is the claim on the hot store**: a writer that finds something answering
on the socket path refuses to start and exits `EXIT_LOCKED`, and a socket file
nothing answers on is a leftover to take over.

The claim matters because SQLite will not make it: SQLite takes per-transaction
locks rather than per-handle ones, so two writers would both open the file and
interleave their rows and their sequence numbers. `PRAGMA locking_mode =
EXCLUSIVE` would prevent that and is unavailable, because it also blocks
readers, and the roll reading the store while the writer holds it is why the
store is SQLite at all.

A pid file is the obvious alternative and is wrong here: a pid means nothing
across the PID namespaces a container restart creates, so a stale lock names an
unrelated live process and refuses every later writer, permanently.
`writer.pid` is still written, for whoever is reading the device, and nothing
decides anything from it.

The socket is a Unix domain socket in a `0700` directory, mode `0600` — never
a TCP port, not even on loopback, which is shared with every local process and
container in the namespace. Node exposes no way to read peer credentials
without a native addon, so filesystem permission is the enforcement.

The server serves one client at a time. The store's session and sequence
counter are process-global, so a second connection moving them is enough to
make every batch on the first skip as a duplicate — acknowledged, never
written. A new session therefore closes the incumbent, and a second `hello` on
an established connection is refused.

A batch carries a sequence number, and the writer skips one it has already
committed. That is what makes a resend after a lost acknowledgement
idempotent, and it is scoped by a session id from the handshake: the plugin's
counter restarts at 1 when the plugin does, so without the session a writer
outliving its plugin would discard the new run's batches as duplicates of the
old run's, acknowledge them, and record nothing while reporting healthy.

## The roll

One roll process per roll, spawned by **the writer** — not by the plugin. The
reason is ownership: the roll opens the hot store `READ_ONLY`, so the delete
that follows a successful roll can only happen in the process that owns the
store. Scheduling it anywhere else would mean a protocol message asking the
owner to do what the owner already knows how to do.

`src/writer/roll-scheduler.ts` holds the timer, spawns `src/roll/main.ts`, and
deletes only on exit 0. Nothing in the writer's import graph reaches DuckDB;
`src/test/plugin-import-graph.test.ts` checks that against the compiled writer
as well as the compiled plugin, because a long-lived writer that imported the
engine would hold the ~100 MB addon for as long as recording runs.

**The set a roll covers is `rowid <= maxRowid`, read by the writer before the
roll starts.** Not a time window: the recorder stamps `ts` when the delta
arrives and the flush buffer holds it for up to a flush interval, so a sample
older than any window routinely reaches the store _after_ the roll has read it.
Truncating by timestamp would delete that sample unrolled. The bound is read,
used and dropped inside one roll — SQLite restarts the rowid sequence at 1 once
the table has been emptied, so a bound carried across two rolls could name rows
it never covered.

**A roll that fails keeps its id.** A roll that wrote its Parquet and then died
leaves those rows in the tree and in the store; reusing the id makes the retry
overwrite what the first attempt wrote instead of adding a second copy under a
new name.

Inside the roll: one streaming `COPY` per UTC date, never `PARTITION_BY`, and
no `ORDER BY`. Both of those are measurements rather than preferences —
`docs/layout-decision.md` has the numbers, and the short version is that a
partitioned write's peak rises with the row count and runs out of memory at
this package's path cardinality, while a sort costs three times the memory of
the whole rest of the roll. Each file is fsynced under a `.tmp` name and
renamed; the suffix is the whole mechanism that keeps a killed roll from
leaving something a `*.parquet` glob reads as finished.

## Retention

**A storage bound, not a deletion guarantee, and the difference is what an
operator has to be told.** A date directory is the finest thing this layout can
drop: a roll file is named for the roll that wrote it and holds whatever rows of
that day the roll covered, so no path names a narrower boundary. A directory
therefore survives until its whole day is behind the window, and the oldest
sample kept can be up to a day older than the instant "now minus retentionDays"
names. Somebody setting retention for privacy is setting it for the wrong thing;
the config description, the README and `expire.ts` all say so, and any change to
the boundary has to keep saying it.

The window is measured back from `min(clock, the end of the newest date
directory)`. That is QuestDB's own TTL formula with the directory standing in
for its table's latest timestamp, and both caps earn their place in opposite
directions: a device whose RTC reads a year ahead before NTP corrects it would
otherwise lose its whole tree to the first roll, and one delta stamped in the far
future would otherwise take every real day with it. The consequence to keep in
mind is that expiry follows the data rather than the calendar — a device that
stops recording stops expiring, which is harmless because the tree stops growing
with it.

Expiry runs inside the roll process, after the files and the sidecar are
committed and after the engine is closed. Three reasons, in order: the roll
already holds the claim on the data directory, so nothing else can be writing a
directory this deletes; the boundary is measured from the newest directory, which
this roll may have just created; and an unlink on a failing card should not be
holding 160 MB of engine. It never throws — a directory it cannot remove is
reported, and the roll still exits 0, because a roll that fails is a roll whose
rows are never truncated from the hot store. Costing recording to save disk is
the wrong trade.

Its two failures are reported apart, because they are different states with
different remedies: a tree that could not be listed means nothing was even
considered, and a directory that could not be unlinked means the rest of the
expiry ran. A _missing_ tree is neither — that is every device's first hour, and
the only case that stays silent. What is reported is an error **code**, never a
message: a filesystem error's message carries the whole path it failed on, these
lines reach the Signal K log and ship in support bundles, and the date already
names the directory an operator would act on.

A query that has already listed a file expiry then unlinks fails, and nothing
retries it. The window is between `treeFilesInRange`'s readdir and DuckDB
opening the file, it needs a request whose range reaches the oldest day in the
tree, and it can happen at most once per roll. Costing one request an hour in
the worst case is cheaper than a retry that would also swallow real IO errors.

**The sidecar is not expired.** Its rows are copies of rows the tree may no
longer hold, and that is deliberate: it answers "the last value of everything",
and pruning it to the boundary would drop every path that went quiet inside the
window — the one question this storage has no index for. It is bounded by key
count rather than by time, so retention is not what keeps it small.

## The query

**One service, not one process per query.** The plugin starts `query/main.js`
on the first history request and keeps it. Starting an engine costs 336–375 ms
on the device — ~220 ms of it mapping the addon — which is more than an
ordinary request costs to answer, so a process per query spent more on starting
than on working.

What that costs is memory the service does not give back: 92 MB idle, ~165 MB
after a dozen ordinary queries, and the high-water mark of the largest shape it
has served (317 MB after an 82,000-row answer). It converges rather than
leaking, and bounding it is
[halos-org/halos#178](https://github.com/halos-org/halos/issues/178). Do not
add a second concurrent engine without reading that issue.

**One request compiles to one statement.** The sibling provider issues a query
per pathSpec, which is free against a running server and is not free here.

The service answers one request at a time; eight may queue and the rest are
refused. A query that fails costs the request — the engine is worth more than
one answer — and a query that overruns its deadline costs the service, because
the engine cannot be interrupted from the plugin's side.

A query subtracts the rows a completed roll has put in the tree and the writer
has not yet deleted, rather than deduplicating the answer. It lists the tree's
files first and the pending-roll record second, and excludes only the days
whose file for that roll is on disk — the order is what keeps the race from
turning a duplicate into a gap.

The hot store is attached per request (0.2–1.2 ms) rather than held. Holding it
also works — a held attachment sees rows written after it, and the writer can
still truncate its WAL underneath it, both measured across processes — but
attaching per request also picks up a store that did not exist when the service
started.

The engine is confined before any statement runs: `lockDownFileAccess` sets
`allowed_directories` to the data directory, turns external access off — which
is what makes the allowlist mean anything, and was not obvious — and locks the
configuration. Everything else must be loaded first; attaching inside the
allowed directory still works afterwards.

`docs/query-layer.md` has the measurements behind every number here.

## Names

**The package is `@halos-org/signalk-duckdb-history-provider`; the plugin id
is `signalk-duckdb-history-provider`.** They differ on purpose and must not be
made to match. The id becomes a filename — `plugin-config-data/<id>.json` — so
a scope would put a slash in it, and it is also what
`historyApi.defaultProvider` holds, because the server keys its registry on
`plugin.id` and carries the package name separately as `plugin.packageName`
(`signalk-server/src/interfaces/plugins.ts:1222`).
`@halos-org/skip-freeboard-panel` is the same shape.
`src/test/package-e2e.test.ts` pins the relation that is left: the id is the
package name with the scope stripped, so neither can be renamed alone.

The scope is why `publishConfig.access` is `public` — a scoped package
publishes restricted by default, and that failure would land at install time on
a boat rather than in CI. Workflows read the name from `package.json` rather
than spelling it, with one exception: `main.yml`'s `package-name` is a _Debian_
name, which takes neither `@` nor `/`.

## The history surface

Two of them, registered separately because the server's two registries arrived
in different releases: a server carrying one and not the other serves the half
it has. **The v1 registry is a single global slot** —
`signalk-server/src/index.ts:316`, last registration wins — so two history
plugins on one device silently pick a winner.

`src/history-v1-slot.ts` decides whether this plugin may take that slot: yes
when it is the configured `historyApi.defaultProvider`, yes when nothing is
configured, no when the key names somebody else. **The second condition is not
a convenience and must not be removed.** Nothing writes that setting except the
Admin UI — `registerHistoryApiProvider` sets none, and `saveConfiguredProvider`
is reached only from the `PUT` route — so on a device where nobody ever picked,
the key is absent for ever, and gating on it alone would leave playback and
snapshots dead even for the only history provider installed. v2 needs no gate:
its registry keys by plugin id and its default falls back to the first
registered provider.

Co-residency is reported rather than resolved, and the report is one-sided: the
app object a plugin receives is a shallow copy taken when it loads, so a
provider registering _earlier_ is visible and one registering _later_ is not. A
precedence rule invented here would still lose to that second case. Running two
history providers on one device is unsupported.

The plugin registers the v2 provider on start and does not unregister it: the
server's plugin wrapper queues that when the provider is registered and runs it
before `stop()` is entered, so the provider is gone before the plugin stops the
query service and asks the writer to close the store — a query holds a read on
it. A server with no registry gets recording and a debug line, because
recording is the half with no alternative.

**A request is buckets, and this side lays them out.** The sibling provider
fabricates a row per bucket inside QuestDB with `FILL(NULL)`, because it issues
one query per pathSpec and needs the timestamps to line up. Here one statement
answers every series and the matrix is assembled here anyway, so the engine
returns only buckets that hold something and the gaps are filled during
assembly. Same response, without fabricated rows crossing a pipe. The timeline
spans the data rather than the request, which is what `FILL(NULL)` does.

A million buckets per request is the ceiling, counted as buckets × series
before anything is queried. Resolutions clamp up to one second, as they do in
the sibling — this storage could serve 0.5 s and deliberately does not, because
two providers rendering the same chart is what the surface is judged on.

`average` over a `units: rad` path is an arithmetic mean, so 359° and 1° average
to 180°. That is wrong, it is wrong in the same way in the sibling provider, and
fixing it needs server metadata the query service does not have. It belongs with
the resolution ladder (#173), where that metadata has to arrive anyway.

**The snapshot is the one question this storage has no index for.** "Every
path's last value at T" is a backward scan whose depth is the retention window
rather than the request, because a path that stopped reporting days before T
still has a last value — QuestDB answers it from an index and the sibling
provider still records that timing out past 30 seconds on a real install. Here
the sidecar answers it instead: it holds one row per `(context, path)`, so any
key whose newest rolled row is at or before T is resolved exactly, however long
ago that row was written. **A snapshot of the present therefore reads the
sidecar and the hot store and no tree file at all.** Only a key the sidecar has
already moved past needs the tree, and that read is bounded to
`SNAPSHOT_SCAN_DAYS` date directories ending at T's own — a path whose last row
before T is older than that is absent from the snapshot rather than searched
for. `query/reader.ts` states the rule; changing the bound changes what a
historical snapshot holds.

`getHistory` returns one row per `(context, path)` and not per source, and it
labels the own vessel `vessels.<id>` rather than the `self` the store spells.
That second part is not cosmetic: the snapshot route maps `self` in the URL to
the vessel's id _before_ walking the tree it builds from these deltas, so a
delta labelled `self` builds a tree the route cannot find. The sibling provider
does not do this, which is
[signalk-questdb-history-provider#32](https://github.com/halos-org/signalk-questdb-history-provider/issues/32).

**Playback runs on the query service like everything else.** A session reads a
60-second window per chunk and holds no engine between them, so the queue can
only be as deep as there are clients — one outstanding request each, by
construction. **A window that has not happened yet is not read at all.** An
empty window advances the cursor, so without that rule a replay which catches
up with real time reads one future window every 100 ms, for ever, against the
service every other history request shares. The sibling provider does exactly
that, which is
[signalk-questdb-history-provider#34](https://github.com/halos-org/signalk-questdb-history-provider/issues/34). Nothing caps the number of sessions: past `MAX_QUEUED_QUERIES` a
request is refused, which reaches a session as an error and becomes the same
backoff as any other failure, and the v1 interface gives a provider no channel
to refuse a session on. A cap enforced through `hasAnyData` would tell the
client its vessel has no history, which is what that answer means to
`interfaces/ws.ts`.

## Conventions

- TypeScript strict; do not loosen `tsconfig.json`.
- **Relative imports carry the `.js` extension** (`./config/schema.js`, from a
  `.ts` source). Node's ESM rule, enforced by `moduleResolution: "nodenext"`.
  Omitting it fails the build, which is the point — `"bundler"` resolution
  would emit unresolvable specifiers instead.
- The package is the unscoped `typebox` (1.x), not `@sinclair/typebox` (which
  stopped at 0.34). TypeBox 1 was published under a new name rather than a
  major bump, so `npm outdated` never points at it.
- `@duckdb/node-api` is pinned exactly, not by range. The bundled extension
  binary is built for one DuckDB version; a range lets an install move the
  engine out from under it, and the failure lands at `LOAD` on a device.
- `.npmignore` excludes by exact filename, so renaming a config file silently
  publishes it. It must never exclude `extensions/`.

## The bundled extension

`sqlite_scanner` is not statically linked into DuckDB and is not committed
here. `./run fetch-extensions` downloads it; `prepublishOnly` does the same
before packing. `tools/check-bundled-extensions.mjs` fails the build when the
bundle and the pinned engine disagree, when a binary is present that the
manifest does not describe, and — with `--strict` — when the published platform
set is incomplete. The scripts in `tools/` read the version, path and platform
rules from `dist/`, so they need a build first — that is on purpose, so those
rules exist once and are unit-tested.

`./run fetch-extensions` compiles with `tsc` directly rather than through
`npm run build`, which also runs that gate. The gate fails on a stale or
mismatched `extensions/` directory — the state the command exists to repair —
so routing through it would refuse to run the fix and then name the fix as the
remedy.

## Measurement

Numbers get reported with their spread, never as point estimates, and a
transient peak is never added to a steady-state figure. Rates divide by the
interval the clock measured, not the one that was requested. `src/bench/`
enforces the method; `./run bench selftest` compares the harness against a load
generator's own accounting of itself and exits non-zero when they disagree.
