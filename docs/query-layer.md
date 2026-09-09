# What a query costs

What a history query costs on the device, measured through the shipped reader
rather than through an ad-hoc statement, and what follows from the figures.
`docs/layout-decision.md` is its counterpart for what the roll writes; the plan
both belong to is
[halos-org/halos#152](https://github.com/halos-org/halos/issues/152).

## The shape

One query service, started on the first history request and kept until the
plugin stops. It answers one request at a time; the plugin queues the rest.

It is a separate process because `@duckdb/node-api` maps a ~100 MB native addon
and DuckDB does not return what a query allocates — neither belongs in the
process serving the vessel's data. It stays alive because starting it costs
more than answering anything.

One request compiles to one statement: the tree files whose date directory
intersects the range, unioned with the unrolled remainder of the hot store.
Nothing prunes on `path` — the tree carries time as directories and everything
else as columns — so a range is a directory selection plus a `ts` filter.

## The floor, paid once

Three runs each, on a HALPI2 (4 GB, aarch64, DuckDB 1.5.5, Node 22.23.2):

| up to and including         | ms            |
| --------------------------- | ------------- |
| node starts                 | 26, 26, 29    |
| `import @duckdb/node-api`   | 251, 255, 218 |
| instance created, connected | 265, 266, 269 |
| `LOAD sqlite_scanner`       | 375, 353, 336 |

**Mapping the engine's native addon is ~220 ms of it**, and nothing in this
design avoids that. Loading the extension is another ~80 ms. Together they are
why the service exists: a process per query paid all of it before every
request, and an ordinary request costs less than that to answer.

The hot store is attached per query rather than at startup, which costs
0.2–1.2 ms against the live hot store and buys two things — a store that did
not exist when the service started is picked up, and no SQLite handle is held
between queries. Neither was strictly necessary: measured across processes, a
held attachment still sees rows written after it, and the writer's
`wal_checkpoint(TRUNCATE)` still truncates to zero with `busy: 0` while it is
held. Nor does it slow the statement that follows: alternating five held-attach
runs with five per-query-attach runs, twice, gives 104–137 ms either way. It is
a millisecond for one less thing to reason about.

## What a query costs

Against the live data directory — 23 roll files across two dates, and a hot
store file of 271 MB — driven through `./run bench query`, which uses the same
client the plugin does. Wall clock is request to answer.

| query                       | first request | later requests | rows   |
| --------------------------- | ------------- | -------------- | ------ |
| paths in the day            | 499 ms        | 93–101 ms      | 524    |
| one path, last hour         | 548 ms        | 127–174 ms     | 1,664  |
| one path, one day           | 736 ms        | 353–408 ms     | 12,849 |
| every path, last 10 minutes | 1,647 ms      | 1,226–1,289 ms | 81,381 |

**These figures move with how much data the device holds**, which is why they
are dated rather than fixed: the same shapes measured a day earlier, against a
tree of 10 files and half as many rows in the day, ran roughly twice as fast.
Compare them against each other, not against a run from another day.

## Where the time goes

In-process and end-to-end for the same request, **alternated inside one run**,
because the device keeps recording and rolling: two tables taken minutes apart
describe two datasets, and subtracting one from the other measures the clock as
much as the code. `plan` is `planSources` plus the `ATTACH`, `sql` is the
statement, `shape` is `getRowsJS`, the BigInt pass and the `DETACH`. Medians of
four pairs.

| query               | rows   | plan | sql | shape | in-process | end-to-end | difference |
| ------------------- | ------ | ---- | --- | ----- | ---------- | ---------- | ---------- |
| paths in the day    | 524    | 2.4  | 158 | 4     | 168 ms     | 164 ms     | −4 ms      |
| one path, last hour | 1,655  | 4.4  | 174 | 17    | 192 ms     | 219 ms     | +27 ms     |
| one path, one day   | 12,849 | 1.6  | 295 | 126   | 415 ms     | 482 ms     | +67 ms     |
| every path, 10 min  | 81,455 | 2.0  | 240 | 771   | 1,017 ms   | 1,227 ms   | +209 ms    |

These totals run higher than the table above them because they were taken later
against more data, and because two engines share the process while the pair is
being measured. Read the columns against each other, not against another run.

**The difference between the two columns is the pipe and the plugin's own
handling.** It grows with the size of the answer and stays a small share of the
whole: at most a sixth of what a request takes, and inside the noise for the
smallest one.

**A small answer is its statement; a large one is its rows.** `plan` never
exceeds 5 ms. For a recent range the statement is dominated by the full scan of
the hot store through `sqlite_scanner`, which `docs/layout-decision.md`
measured at ~1.9 ms per MB and predicted would put an hourly store's
recent-query floor near 100 ms. The lever on that is the roll interval, which
is the hot store's ceiling — not the process model, and not the transport.

## What this is not comparable to

The plan's criterion asks for a query "within sqhp's range (~34 ms for an
hour-long request)". **Nothing here is a like-for-like against that**, and the
numbers above should not be read as one:

- Those sqhp figures are Signal K history API requests for **one hour at
  60-second resolution** — about 60 rows, aggregated inside QuestDB before
  anything crosses its HTTP connection. Every figure here is a raw range, and
  this provider returns raw rows.
- They are measured through the server's HTTP API; these are measured at the
  plugin's client boundary, with no HTTP layer.
- The archived sqhp measurements on this hardware span 14.2, 18.8, 27 and
  38.8 ms across four runs, moving with QuestDB's tuning. `~34 ms` is a point
  inside that spread rather than a measured constant.

**The cost of an aggregated answer is unmeasured.** No query of that shape has
been run here, and nothing in the table above stands in for one.

Nothing here changes the memory case the design was chosen for: QuestDB's
standing cost is ~366 MB.

## What the service holds

It reports its own resident size with every answer, because a process that
stays is judged on what it holds as well as on what it takes.

| after                                     | service RSS |
| ----------------------------------------- | ----------- |
| started, nothing asked                    | 92 MB       |
| one single-path hour                      | 117 MB      |
| fifteen of them                           | 164 MB      |
| forty of them                             | 170 MB      |
| one all-paths ten-minute query (82k rows) | 317 MB      |

So repeated work converges rather than leaking — 117 to 164 MB over fifteen
queries, then 6 MB over the next twenty-five. What it does not do is come back
down: the service settles at the high-water mark of the largest shape it has
been asked for. With the writer at 87 MB, a service that has answered a few
queries puts the plugin's standing cost near 260 MB, against the 150 MB this
project set itself and QuestDB's ~366 MB.

Bounding that is
[halos-org/halos#178](https://github.com/halos-org/halos/issues/178): recycle
the service on an idle timeout, an RSS ceiling, or both. Until then it is one
process for the life of the plugin, and this is what that costs.

## The layout decision, re-checked

`docs/layout-decision.md` chose one file per roll, no path partitioning and no
_daily_ compaction, and named what would reopen it: a multi-day query dominated
by per-file cost rather than by startup. What reopened it in the end was the
roll interval rather than the query — see the closing paragraph of this
section.

Both trees below are one day of real data hard-linked into 30 dated
directories, with no hot store. The rows therefore repeat, and a 30-day range
returns 30× the rows — so these numbers overstate the row cost of a real 30-day
window and measure the file cost honestly. Each figure includes one engine
start: they were taken before the service existed, with a process per query.

| tree                   | files | one path, 30 days           | paths, 30 days      | one path, one day |
| ---------------------- | ----- | --------------------------- | ------------------- | ----------------- |
| roll files (8 per day) | 240   | 2,195–2,244 (e 1,836–1,892) | 661–727 (e 381–438) | 360–435           |
| compacted (1 per day)  | 30    | 1,916–2,062 (e 1,579–1,746) | 601–631 (e 339–367) | 375–444           |

**Per-file planning cost is about 1 ms.** Collapsing 240 files into 30 saves
~230 ms of a 2,200 ms query and ~60 ms of a 660 ms one. The 30-day query is
dominated by its 82,590 rows, not by its files, so the condition for reopening
compaction is not met. Compacting one day cost 639 ms in a process measured
there at 344–484 MB — the largest transient in the design — to save 230 ms on a
query nobody has to make.

**A date-scoped query does not care how large the tree is.** 1 file, 8 files
and 240 files all answer a one-day single-path range in 360–444 ms. That is the
property the layout was chosen for, and it is the reason a long-range query
stays avoidable rather than fast.

So the decision stood at the time of these measurements: no compaction pass, no
path partitioning. What would reopen it is a _real_ thirty-day tree — 720 files
at hourly rolls — where the same per-file slope predicts ~720 ms of planning.

**That threshold has since been crossed, by the roll interval rather than by
time.** The default is now 5 minutes, so thirty days is 8,640 files, not 720,
and the ~1 ms per file measured above prices that at several seconds of
planning. Path partitioning is still rejected. Hourly compaction is not: it
brings thirty days back to 720 files, and on a device it took a single-path
range over one day's files from 42.2 ms to 3.4 ms. `docs/layout-decision.md`,
_Compaction: yes, hourly_, has the numbers and the safety argument.

No path partitioning, still.

`getPaths` is a scan rather than a directory listing, and the flat layout is
why. The cumulative sidecar could answer "every path ever" from one 11 kB file,
but not "every path with data in this range", which is what the history API
asks. Nothing reads the sidecar.

## The snapshot

Every other query names a time range, and the tree's directories are cut on
time — so a range is a directory selection and a `ts` filter, and the numbers
above are what that costs. The v1 snapshot API asks a different question:
**every path's last value at an instant**. Nothing in this layout answers that
by pruning, because a path that stopped reporting a week before T still has a
last value, so the honest form of the question is a backward scan whose depth is
the retention window rather than the request.

The sidecar is what makes it affordable. It holds one row per
`(context, path)` — the newest that has ever been rolled — so a key whose
sidecar row is at or before T is answered by that row exactly, however long ago
it was written. Only a key the sidecar has already moved past needs anything
else, and one statement over the sidecar tells the reader whether any such key
exists before it opens a file:

| snapshot of                         | reads                                 |
| ----------------------------------- | ------------------------------------- |
| the present                         | the sidecar and the hot store         |
| an instant the tree has rolled past | those, plus `SNAPSHOT_SCAN_DAYS` days |

The second row is the bounded case, and the bound is the rule the API answers
under: date directories ending at T's own, newest data first, and a path whose
last row before T is older than that window is **absent from the snapshot**
rather than searched for. Two days is the shipped value — a day resolves every
path still reporting at T, and the day before it resolves one that went quiet
overnight. It is a constant in `query/reader.ts` rather than a setting, because
changing it changes what a historical snapshot holds and not just what it costs.

Retention does not prune the sidecar, so the first row keeps answering for a
path whose tree files have expired: a snapshot of the present still carries its
last value. The second row degrades instead — an instant inside an expired
range finds no date directory to scan, and the answer for a key the sidecar has
moved past is empty rather than wrong.

Both branches reduce with one `arg_max` over a struct, grouped by
`(context, path)`: a hash aggregate over the key count — hundreds of groups,
whatever the row count — where `DISTINCT ON` would sort the input. One row per
key and not per source, which is what the sibling provider's
`LATEST ON ts PARTITION BY path, context` returns and what the tree the server
assembles from it can hold.

**Neither branch is measured on the device.** The unbounded case is arithmetic
from the figures above — a bounded snapshot reads whole days, and a day of this
device's data is the 12,849-row shape's tree files without its `ts` filter —
and nothing here stands in for a measurement. What the sibling provider records
for the same request is a `LATEST ON` over an index that still timed out past
30 seconds on a real install.

## The seam

A roll writes its rows to Parquet, and the writer deletes them from the hot
store afterwards. Between those two steps every covered row is in both places,
and after a failed truncate it stays that way until the writer restarts.

The reader subtracts one copy exactly. It lists the tree's files first and
reads the pending-roll record second, then excludes from the hot store the rows
`rowid <= maxRowid` whose own UTC day has a file named for that roll. Anchoring
on the file list rather than on the record is what makes the race safe in the
direction that matters: a record naming a roll whose file has not appeared yet
subtracts nothing, so a duplicate is never turned into a gap. A roll killed
between two date directories is handled by the same rule — the day it wrote is
subtracted and the day it did not is not.

`DISTINCT` over the answer was the alternative. It would cost a sort over every
range and would also collapse two genuinely identical samples, which the
sibling provider's QuestDB tables do by dedup key but this store does not.

## Concurrency, and what can overlap

The service answers one request at a time: two queries on one connection would
interleave their rows on one pipe, and at the time one request takes, a queue
is a better answer than a second engine. Eight may wait; past that a request is refused
rather than queued behind requests that will not be served in time either. The
deadline is 30 seconds and covers the wait as well as the work.

A query that overruns it costs the service — the engine cannot be interrupted
from the plugin's side — and the next request starts a new one. A query that
_fails_ costs only the request; the engine is worth more than one answer.

**A playback session is a client of the same queue, not a process of its own.**
It reads a 60-second window per chunk and holds no engine between chunks, so it
has at most one outstanding request at any moment and N clients make a queue of
N. It also never reads a window that has not happened yet, which is what keeps
a session that has caught up with real time from becoming ten queries a second
against this service for as long as its client stays connected. Past the eight that may wait, a request is refused — which reaches a playback
session as an error and becomes the same backoff as any other failure. Nothing
caps the number of sessions: the v1 provider interface offers no way to decline
one, and the only answer it does have (`hasAnyData` returning false) means "this
vessel has no history" to the server.

**Nothing coordinates a query with a roll.** The roll runs on the writer's
schedule and a query arrives when a client asks, so the worst case is the sum
of peaks measured apart: a service that has served a large query at ~320 MB and
a roll at 163 MB, so under 500 MB. That fits a 4 GB device running the marine
stack, which is why nothing admission-controls the two against each other.

A query returns at most 100,000 rows and says so when it truncated, because the
Signal K process holds the whole answer to serialise it — the row limit is a
ceiling on that process, not on this one.

## What this does not measure

- **Aggregation.** The reader returns raw rows. Bucketed aggregates belong to
  the v2 surface, and nothing here prices them.
- **A snapshot, in either of its branches.** See the section above.
- **A playback session.** A chunk is an ordinary range query of the shapes
  above, but what a session costs over minutes — and what two of them do to the
  queue — is Unit 7's to measure.
- **A concurrent roll and query.** The figure above is arithmetic.
- **A tree with a real retention window in it.** Both aged trees are one day of
  data wearing thirty dates.
- **A service across days rather than minutes.** The plateau above is forty
  queries of one shape; what a week of mixed use settles at is
  [#178](https://github.com/halos-org/halos/issues/178)'s to answer.
- **A file deleted while a query reads it.** The file list is taken before the
  statement runs, so whatever ships expiry has to decide what a reader already
  holding a list should see.
