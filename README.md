# @halos-org/signalk-duckdb-history-provider

A Signal K history provider that stores data in Parquet files instead of a
database server. Deltas go to a SQLite hot store owned by a separate writer
process; a short-lived roll turns that store into a Parquet tree; queries run
in a spawned DuckDB that reads the tree and the hot store together.

It is an alternative to
[`signalk-questdb-history-provider`](https://github.com/halos-org/signalk-questdb-history-provider),
chosen per device. The two are not meant to run on one device, and this plugin
changes nothing about that one. The reason to want it is the resource floor:
QuestDB costs a JVM at about 366 MB resident, 24 hours a day, whether or not
anyone queries it.

## Status

The plugin records, rolls and answers. It filters, rate-caps and buffers; a
separate writer process owns a SQLite hot store and rolls it into a Parquet tree
on a schedule; and a query service holds one DuckDB engine that serves both
history API surfaces — the v2 REST API, and v1 playback and snapshots — reading
the tree and the hot store as one.

The resolution ladder, packaging and on-device verification are still open.
Progress is tracked in
[halos-org/halos#152](https://github.com/halos-org/halos/issues/152).

## The tree

    <data directory>/
      hot/hot.sqlite            the writer's store, truncated after each roll
      parquet/date=YYYY-MM-DD/  <slot>.parquet     one file per roll
                                hour-<start>.parquet  one completed hour, merged
      latest/latest.parquet     every path's last value, cumulative

`context` and `path` are columns, never directories, and each row lands under
the date its own timestamp names — so a roll spanning midnight writes two
files. Once an hour is complete its rolls are merged into one file sorted by
path, which a device measured at 1.81 bytes per row against 4.94, and a
single-path range over a day at 3.4 ms against 42.2. A reader answers each row
once throughout: a roll stops being read the instant a merged file covering it
exists. Turn it off with `compactHourly` to leave the tree exactly as the rolls
wrote it. Why it is shaped this way, with the measurements behind it, is
`docs/layout-decision.md`.

## Retention

**Retention is a bound on what is stored. It is not a promise that everything
older has been deleted.** A date directory is the finest thing this layout can
drop, so a directory survives until its whole day is behind the window: with
seven days configured, a sample recorded seven days and twenty hours ago can
still be there. Anyone setting it for privacy rather than for disk has to read
it that way.

Expiry runs at the end of each roll, so a changed setting takes effect at the
next one. The window is measured back from whichever is earlier, the clock or
the newest day in the tree. Both caps matter on a device without a real-time
clock: an RTC reading a year ahead would otherwise delete the whole tree at the
first roll, and one delta stamped in the far future would take every real day
with it. It also means a device that records nothing expires nothing — the tree
is not growing then either.

`latest/latest.parquet` is not expired. It holds one row per `(context, path)`
and answers "the last value of everything", which is the one question this
storage has no index for; pruning it to the boundary would make a path that
went quiet inside the window disappear from a snapshot of the present. It is
bounded by how many paths a vessel has, not by how long it has been recording.

## Installation

From the Signal K app store, or with
`npm install @halos-org/signalk-duckdb-history-provider` into the server's
plugin directory. On HaLOS Marine it is baked into the image and needs no
installation.

**The plugin id is `signalk-duckdb-history-provider`, without the scope.** It
is what names the config file and what `historyApi.defaultProvider` holds; the
scoped name is the package only.

## Choosing this provider

A Signal K server exposes two history APIs, and they behave differently when
more than one provider is installed.

The **v2 REST API** has a real registry keyed by plugin id. It serves the
provider named by `historyApi.defaultProvider`, and falls back to the first one
registered when that names nothing or names a provider that is not running.
This plugin always registers there.

The **v1 WebSocket API** — playback and snapshots — has no registry. The server
keeps one provider in a single global field and the last plugin to register
takes it, so this plugin claims it only when `historyApi.defaultProvider` names
this plugin or names nothing at all. If it names another provider, this one
stands aside and says so in the log. Choose the provider under **Apps & Plugins
→ Configuration**, then restart the server: the v1 slot is taken at plugin
start, so a change made while the server is running reaches v2 immediately and
v1 only after a restart.

**Running two history providers on one device is unsupported.** They record the
same deltas twice, and which one answers v1 depends on plugin load order. The
plugin reports the state when it can see it — it cannot see a provider that
registers after it does — but it does not try to resolve it. Disable one.

## Configuration

Every option is rendered in the Signal K Admin UI from the plugin's own schema
(`src/config/schema.ts`), which is also the source of the `Config` type.

| Option                                              | Default               | What it does                                                                                                                                                                                                                                                                                                                        |
| --------------------------------------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Filter mode                                         | `exclude`             | Whether the path patterns below name what to skip or what to keep.                                                                                                                                                                                                                                                                  |
| Path patterns (glob supported)                      | none                  | Globs over Signal K paths, e.g. `notifications.*`.                                                                                                                                                                                                                                                                                  |
| Default sampling rate (ms)                          | `2000`                | Minimum interval between recorded samples for a path. `0` records every update.                                                                                                                                                                                                                                                     |
| Per-path sampling rates (ms)                        | none                  | Overrides for individual paths or globs.                                                                                                                                                                                                                                                                                            |
| Record own vessel                                   | on                    | Whether `vessels.self` is recorded.                                                                                                                                                                                                                                                                                                 |
| Record other vessels                                | **off**               | Whether AIS targets and other vessels are recorded. Off by default because every vessel is a context, and the roll holds one Parquet writer per partition — this setting, more than data volume, sets the roll's memory peak.                                                                                                       |
| Maximum distinct recorded paths                     | `2000`                | Paths beyond this are ignored, so a misbehaving source cannot inflate the partition count without limit.                                                                                                                                                                                                                            |
| Maximum distinct recorded contexts                  | `100`                 | The same bound for vessel contexts.                                                                                                                                                                                                                                                                                                 |
| Flush interval (ms)                                 | `5000`                | No sample waits longer than this before reaching the writer. Also the crash-loss window: a hard power cut loses at most this much.                                                                                                                                                                                                  |
| Flush batch size (samples)                          | `1000`                | Samples per write, whichever comes first with the interval. Each batch is one SQLite transaction.                                                                                                                                                                                                                                   |
| Buffer ceiling while the writer is unreachable (MB) | `8`                   | Memory held for samples that could not be sent. When full the oldest are dropped and the count is reported in the plugin status.                                                                                                                                                                                                    |
| Data directory                                      | plugin data directory | Where the hot store and the Parquet tree live. A relative value resolves against the plugin's own directory.                                                                                                                                                                                                                        |
| Retention (days, 0 = keep forever)                  | `0`                   | A bound on what is stored, not a promise that everything older is deleted. Whole UTC days are dropped once the window has passed them, so the oldest sample kept can be up to a day older than the boundary. Applied after each roll.                                                                                               |
| Roll interval (minutes)                             | `5`                   | How often the hot store becomes Parquet and is truncated. Shorter keeps the hot store small at the cost of more Parquet files, and the hot store's size is what every query touching recent time pays for. Must divide 1440 — the schedule runs every N minutes from UTC midnight — and anything else falls back to 5.              |
| Compact each completed hour                         | on                    | Merge an hour's roll files into one file sorted by path, once the hour is complete. Costs a short-lived process an hour and gives back both storage and query time. It does nothing at a roll interval of 60 minutes or more, where an hour already holds one file. Turning it off stops future merges and does not undo past ones. |

**Upgrading from a build before hourly compaction.** The roll interval's
default moved from 60 minutes to 5, and compaction is on. What that means
depends on what the device has stored:

- **No interval stored** — it starts writing 288 files a day instead of 24,
  merged back to 24 an hour behind. The tree's shape changes and nothing about
  it needs attention.
- **60 minutes or more stored** — nothing changes at all. An hour holds one
  roll there, so no merge runs and the writer's log line says so.
- **Under 60 minutes stored** (15 or 30, say) — the interval is kept and
  compaction starts merging the tree.

Past hours are not merged retroactively. Compaction only ever runs on the hour
a roll has just closed, so hours that pass while the writer is stopped are
never merged either; `dist/compact/main.js --data-dir <dir> --hour <ms>` merges
one by hand.

**Merging is one way.** Turning `compactHourly` off stops future merges and
does not undo past ones — hours already merged stay merged and their rolls are
gone. A tree that has been merged must not be read by a build older than this
one: those builds read every file in a date directory, and between a merge's
rename and the removal of its inputs that counts the hour twice.

## The bundled DuckDB extension

DuckDB links `parquet` and `json` in statically but not `sqlite_scanner`, and
without that extension it cannot read the hot store at all. A device may have
no network, so the published package carries the binary rather than letting
DuckDB download one — autoinstall and autoload are both disabled, which also
keeps a query from fetching and running a binary from the internet.

The binaries are **not committed**: they are about 8 MB each and would land in
the history again on every DuckDB bump. `./run fetch-extensions` downloads
them into `extensions/` for development, and `prepublishOnly` does the same
before packing, so the npm tarball always has them. The published set is
`linux_arm64` (the device) and `linux_amd64` (CI and x86 development);
anything else is one `./run fetch-extensions <triple>` away.

An extension binary is built for exactly one DuckDB version and one platform,
and a mismatch fails at `LOAD` rather than at install. Three things keep that
from reaching a device: `@duckdb/node-api` is pinned exactly rather than by
range, `npm run build` refuses a bundle whose manifest names a different
version, and CI loads the real binary on both architectures inside a container
with no network.

To check an installation on the machine it runs on:

```bash
./run check-extension          # in a clone
node dist/duckdb/check-extension.js   # in an installed copy on a device
```

It creates a DuckDB, loads the bundled extension, attaches a SQLite file,
writes rows and reads them back.

## The measurement harness

Every unit that reports a number reports it through `src/bench/`, so the
figures are comparable across units and against the QuestDB baseline. The
method: settle, then N windows, each differenced end to end for its rate and
split in half to check that it measured a steady state rather than a
transition. Rates divide by the interval the clock actually measured, never by
the requested window length — those differ under load, and by more in the
condition being tested than in the control it is compared against.

Memory is sampled through the window instead, and its peak is reported apart
from its mean: a transient peak and a 24-hour cost are different quantities,
and adding them produces a number that describes nothing. The peak comes from
the kernel's own high-water mark (`VmHWM`, or cgroup `memory.peak`) rather than
from the samples, because the roll process is short-lived by design and its
peak is exactly what a sampling interval misses.

That method needs a subject that is still running, which a roll is not: it
lives seconds, has no steady state, and is gone before a window closes. `roll`
measures one instead, by asking the roll process for the high-water mark the
kernel kept for it and polling `/proc` from outside as a cross-check. **Point
it at a copy of a data directory** — a roll writes into the tree and does not
truncate the hot store, so one run beside a live writer puts those rows in the
tree twice. It refuses if anything answers on the writer's socket.

```bash
./run bench run --label sqhp --subject signalk:pid=1234 -o sqhp.json
./run bench compare control.json sqhp.json parquet.json
./run bench selftest
./run bench roll --data-dir /path/to/a/copy --max-rowid 1267241
./run bench query --data-dir /path/to/a/tree --from 1788825600000 --to 1788829200000
./run bench http-query --provider signalk-questdb-history-provider \
  --from 2026-09-08T06:00:00Z --to 2026-09-08T07:00:00Z \
  --path navigation.speedOverGround --resolution 60
```

`query` and `http-query` time different things. `query` runs a request through
this plugin's own DuckDB reader against a tree on disk, which is the engine's
cost with no server in the way. `http-query` times a round trip over the Signal
K v2 history route, addressed to one provider by plugin id — so it works against
any provider the server has registered, not only this one, and it includes the
plugin's own assembly of the answer. That route is the only surface on which
providers backed by different engines answer the same question, which makes it
the one to compare them on. Run it on the device: a round trip measured across a
network measures the network. It checks the server's `_providers` list before
timing anything, so a misspelled plugin id fails rather than quietly measuring
whichever provider is the default.

`selftest` measures a load generator with a known duty cycle and compares the
harness's numbers against the generator's own accounting — it counted the bytes
it fsynced and asked the kernel for its own CPU time, so those are ground truth
and a disagreement beyond 15% fails the command. It reads `/proc` and cgroup
counters, so it only runs on Linux.

## Development

```bash
./run            # list the commands
./run test       # build, then run the suites
./run lint
./run ci         # what CI runs, in CI's order
```

Requires Node 22 or newer.

## License

MIT. Copyright (c) 2026 Hat Labs Oy.
