# Tree layout decision

What the roll writes, how often, whether a compaction pass follows it, and
whether it emits a last-value sidecar. Unit 3a of
[halos-org/halos#152](https://github.com/halos-org/halos/issues/152), settled by
measurement on the device rather than by argument, because the file count that
falls out of these choices is what a later query has to live with and reversing
one after data exists means re-rolling the tree.

## The decision

1. **No path partitioning.** One Parquet file per roll, holding every context
   and every path, with `context` and `path` as columns. The directory tree
   carries time only: `<data-dir>/parquet/date=<YYYY-MM-DD>/<rollStart>.parquet`.
2. **Five-minute rolls by default**, aligned to UTC midnight, and
   configurable. What the interval really has to hold is the hot store's
   ceiling near 50 MB. An hour does that at the rate measured here and not at
   ten times it, and the shorter interval holds it at both — see _Compaction:
   yes, hourly_ below for why the file count it produces is no longer the
   objection it was.
3. **An hourly compaction pass**, merging each completed hour's rolls into one
   file sorted by path. Superseded decision 3 below, which was "no compaction
   pass"; the reasoning that changed it is in _Compaction: yes, hourly_.
4. **A cumulative last-value sidecar per roll**, one row per
   `(context, path)`.

## What was measured

A snapshot of the live hot store on a HALPI2 (4 GB, aarch64, DuckDB 1.5.5,
Node 22.23.2), taken with `VACUUM INTO` while the writer kept ingesting:

|                            |                                          |
| -------------------------- | ---------------------------------------- |
| rows                       | 1,267,241                                |
| span                       | 2 h 48 min                               |
| rate                       | 125.7 rows/second                        |
| distinct `(context, path)` | 552                                      |
| distinct paths             | 525                                      |
| contexts                   | 29, of which `self` holds 1,267,206 rows |
| store on disk              | 129.7 MiB                                |

Half of everything recorded is notifications: `notifications.*` is 144 paths and
621,945 rows. `vhfdata.*` is another 96 paths and 175,875 rows. The 28 non-self
contexts hold one or two rows each — AIS identity records — so any layout that
made `context` a directory would create 28 near-empty directories per roll. That
distribution is a recording-policy question for another unit; here it is the
input.

Every roll and every query ran in its own process at `memory_limit = 256MB`, and
each cell ran three times. Peaks are `VmHWM` from `/proc/self/status`, write
volume is `write_bytes` from `/proc/self/io`, CPU is `process.cpuUsage`. Figures
are median with range.

`src/bench/` did not run these. Its method is three settled windows against a
subject addressed by pid or cgroup, differencing counters for rates and
splitting each window in half to check steadiness — which needs a subject that
is still running. A roll lives between 0.7 and 5 seconds and its quantity is the
high-water mark the kernel already recorded, so there is no window to settle and
no pid to attach to before it exits. The scratch scripts therefore read the same
files with the same expressions `src/bench/proc.ts` parses, and duplicated that
parsing rather than importing it. What the project's measurement rule actually
needs here is for the one-shot peak to become part of the harness, which is
Unit 3b's to do when it has a roll process to point at.

**Every peak below includes the process itself.** Node alone is 75–87 MB
resident; opening DuckDB takes it to 85–96 MB; loading `sqlite_scanner`,
attaching the hot store and counting it reaches 97–99 MB. Subtract that to get
what a roll costs beyond existing.

## Why no path partitioning

Three layouts were rolled from the same window: no partitioning at all,
`(context, top-level path group)` — 38 partitions — and `(context, path)` — 552
partitions.

| layout | rows      | peak RSS                  | wall     | output  | files |
| ------ | --------- | ------------------------- | -------- | ------- | ----- |
| flat   | 38,970    | 109.3 MB (109.1–109.5)    | 1,057 ms | 0.11 MB | 1     |
| flat   | 381,494   | 141.6 MB (140.2–146.1)    | 1,266 ms | 0.97 MB | 1     |
| flat   | 1,267,240 | 146.5 MB (142.8–150.7)    | 1,720 ms | 3.18 MB | 1     |
| group  | 1,267,240 | 470.4 MB (447–491.7)      | 1,783 ms | 2.64 MB | 38    |
| path   | 38,970    | 223.2 MB (222.6–224.2)    | 655 ms   | 0.74 MB | 546   |
| path   | 381,494   | **out of memory, 3 of 3** |          |         |       |
| path   | 1,267,240 | **out of memory, 3 of 3** |          |         |       |

A partitioned write's peak rises with the row count and a plain `COPY`'s rises
far more slowly. Rolling 38,970, 381,494 and 1,267,240 rows costs the group
layout 130.9, 280.6 and 470.4 MB — a 33-fold increase in rows for a 3.6-fold
increase in memory — against the flat layout's 109.3, 141.6 and 146.5 MB, which
is 1.3-fold, and 37 MB of it lands between the first two points. Flat is not
constant; it is bounded, and the bound is measured below at 204–246 MB for
11.4M rows. The partitioned shape — memory rising with input and released only
at the end — is what a write that collects its result set before emitting it
looks like, though this measured the behaviour and not the engine's source. At 552 partitions it exceeds a 256 MB
limit on anything longer than a ten-minute window, and none of the knobs that
look like they should contain it do: `row_group_size` at 2,048, 10,000, 40,000
and 122,880 all fail, `partitioned_write_max_open_files` at 8, 32 and 100 all
fail, and `preserve_insertion_order = false` fails. Only `threads = 1` succeeds,
at 307.7–308.8 MB and 5,208–5,470 ms against the flat roll's 1,720 ms.

The partitioned write is also loud on disk. Writing 2.64 MB of Parquet through
`PARTITION_BY` puts 32.5–45.1 MB through the block layer; the flat `COPY` writes
3.2 MB for 3.18 MB of output.

Then the tree itself. The same 1,267,240 rows, cut the six ways that were built:

| tree                     | files  | Parquet bytes | disk      |
| ------------------------ | ------ | ------------- | --------- |
| one sorted file per day  | 1      | 2.40 MiB      | 2.5 MB    |
| one file per day         | 1      | 3.18 MiB      | 3.2 MB    |
| one file per roll        | 24     | 3.51 MiB      | 3.6 MB    |
| group, one file per day  | 38     | 2.61 MiB      | 3.0 MB    |
| group, one file per roll | 272    | 3.24 MiB      | 4.0 MB    |
| path, one file per day   | 552    | 4.06 MiB      | 7.5 MB    |
| path, one file per roll  | 10,337 | **21.4 MiB**  | **43 MB** |

Per-path files at a per-roll cadence hold six times the Parquet bytes of the
flat tree for identical data and occupy twelve times the disk, because a file
averaging 2.2 kB is mostly header and footer and still takes a 4 kB block.

And they are slower to read, not faster. Against the one-day trees, times are
total wall clock from process start, which is how a spawned query is actually
paid for:

| tree                           | files  | single-path range      | latest value, all paths | peak RSS   |
| ------------------------------ | ------ | ---------------------- | ----------------------- | ---------- |
| flat, per roll                 | 24     | 150 ms (133–182)       | 247 ms (226–248)        | 105–106 MB |
| group, per roll                | 272    | 144 ms (127–145)       | 232 ms (219–244)        | 107–118 MB |
| path, per roll                 | 10,337 | 1,298 ms (1,292–1,474) | 2,059 ms (2,055–2,241)  | 389–512 MB |
| flat, one file                 | 1      | 140 ms (113–144)       | 212 ms (201–227)        | 108 MB     |
| flat, one file, sorted by path | 1      | 104 ms (100–114)       | 157 ms (144–175)        | 98–107 MB  |
| group, per day                 | 38     | 105 ms (104–135)       | 200 ms (186–230)        | 101–111 MB |
| path, per day                  | 552    | 189 ms (176–237)       | 277 ms (270–421)        | 112–123 MB |

**Process startup is 70–110 ms of every one of those totals.** Spawning Node,
opening DuckDB and loading the extension costs more than any well-shaped query
over a day of data. The gap between the best layout here and the flat one is
about 40 ms, against a floor nothing in this design can go below, which is why
40 ms does not buy a partitioning scheme.

Cardinality growth strengthens this rather than reopening it. 552
`(context, path)` pairs is one vessel with non-self contexts almost entirely
filtered out; an installation recording AIS targets can reach thousands. Under a
partitioned layout that is thousands of partitions, and the write already fails
at 552. Under a flat one it is more rows in the same file, which the roll is
bounded against.

There is one more thing the flat layout gets for free. The plan's risk table
carries "delta-supplied path and context strings become directory names",
mitigated by an allowlist and a containment assertion. With only the date as a
directory, and the date generated rather than received, no untrusted string ever
reaches the filesystem. The risk is closed by construction rather than guarded.

### The case against, with its numbers

Partitioning does buy something: the planner skips files without opening them.
It shows up only on a long-range single-path query over an aged tree. Each tree
was aged to 30 days by hard-linking one day's output into 30 dated copies, which
measures planning and metadata cost honestly and understates I/O — 30 links to
one inode share a page-cache entry where 30 real days would not.

| tree                      | files  | whole range                          | one day          |
| ------------------------- | ------ | ------------------------------------ | ---------------- |
| flat, per roll            | 720    | 965 ms (931–1,169), 75.5 MB read     | 117 ms (116–152) |
| group, per roll           | 8,160  | 1,599 ms (1,597–1,604), 28 MB read   | 144 ms (124–154) |
| flat, one per day, sorted | 30     | 327 ms (303–352), 7 MB read          | 103 ms (101–111) |
| group, one per day        | 1,140  | 559 ms (544–574), 19.1 MB read       | 112 ms (100–150) |
| path, one per day         | 16,560 | 3,068 ms (2,864–3,282), 17.1 MB read | 174 ms (170–179) |

So group partitioning is about twice as fast as flat for a single path over
thirty days — but only when it is also compacted to one file per partition per
day. At a per-roll cadence it is slower than flat (1,599 ms against 965 ms),
because eight thousand files cost more to plan than seven hundred. Partitioning
pays only in combination with a compaction pass, and that pass is priced below.

The one-day column is the same query with the date directory pruning it, and
every layout lands within 100–180 ms there. A date directory is what makes
long-range queries avoidable; path partitioning is what makes them slightly
cheaper when they are not.

## Why the interval was hourly, and why it is five minutes

**Superseded in its conclusion, kept for its measurements.** This section
prices the interval trade-off and concluded an hour. The default is now five
minutes: what it holds is the hot store's ceiling near 50 MB, and an hour holds
that at the rate measured here but not at ten times it. The file count that
made an hour attractive below is the cost _Compaction: yes, hourly_ pays off —
288 files a day merged back to 24, an hour behind.

The interval sets three things, and they do not pull in the same direction.

**The hot store's size, and with it the cost of every recent query.** The
snapshot averages 46 MiB of SQLite per hour at 125.7 rows/second; its last
hour is 50 MB and 492,734 rows. The roll truncates the store, so the interval is
the ceiling. Every query that touches recent time reads the unrolled remainder
through `sqlite_scanner`, and that read is a full table scan:

| hot store | rows      | single-path range | latest value, all paths |
| --------- | --------- | ----------------- | ----------------------- |
| 13 MB     | 122,672   | 63 ms (60–64)     | 94 ms (94–100)          |
| 50 MB     | 492,734   | 102 ms (99–117)   | 161 ms (142–175)        |
| 130 MB    | 1,267,241 | 248 ms (236–267)  | 441 ms (350–507)        |

An index on `ts` does not change it — 67, 110 and 246 ms for the same three
stores — so the scan is not avoidable by indexing and the interval is the only
control there is. The three points are close to linear at about 1.9 ms per MB,
which puts an hourly store's recent-query floor near 100 ms and a four-hourly
one's near 250 ms. A daily store would be around 1.2 GB; extrapolating the same
slope gives a two-second scan, which was not measured.

**Roll cost.** An hour here is 381,494 rows at the start of the span and
492,734 at the end; the flat roll writes the first of those in 1,266 ms
(1,220–1,299) at 141.6 MB (140.2–146.1). The roll is indifferent to how much
larger it gets: streaming the snapshot two, four and nine times over, up to
11.4M rows — a day at the observed rate — peaks at 152–194 MB, 232–247 MB and
204–246 MB respectively, while wall time rises linearly to 15.5–15.9 s. Memory
does not decide the interval.

**File count.** At the hourly interval this section prices: 24 files a day, 720
in a thirty-day window, 8,760 in a year. At the shipped five-minute interval it
is 288 a day before compaction and 24 after. The
aged-tree table above says 720 files answer a whole-range single-path query in
931–1,169 ms and a date-scoped one in 116–152 ms.

Fifteen-minute rolls would cut the recent-query floor from ~100 ms to ~63 ms and
raise the file count fourfold, to 2,880 in a thirty-day window. That is a
defensible other answer, and it is the direction the default eventually went:
hourly was chosen here because 24 files a day is already past the point where
file count is doing any harm, and the remaining 40 ms is inside the startup cost
of the process asking for it — an argument that only holds while nothing merges
the files back.

One constraint comes with it: the interval must divide 1,440 minutes, because
"every N minutes from UTC midnight" describes a cadence only when it does. At
100 minutes the last slot before midnight is 40 minutes long. This was written
here as the thing that stops a roll straddling a date directory, and
implementation showed that is not what it does — the roll places each row in
the directory its own timestamp names, so a roll spanning midnight writes two
files and both are correct. The constraint is about the schedule alone.

And the interval has to be configurable, because the rate that produced these
figures — 125.7 rows/second, 552 paths — is one vessel's and the hot store
scales with it. An installation recording ten times as much fills roughly
500 MB an hour, which is four times past the largest store measured here and
puts the recent-query floor near a second by the same slope. The rule that
follows is to hold the hot store's ceiling near 50 MB rather than to hold the
interval at an hour: ten times the rate wants a six-minute roll, at 240 files a
day. Nothing in the flat layout objects to that — the roll cost at 38,970 rows
is 109 MB and 1.0 s — but it is the roll interval, not the layout, that has to
move. An hour is right for a rate near the one measured and wrong above it,
which is why the default moved to five minutes rather than staying at the rate
this table happened to sample.

## Compaction: no, daily

**Superseded — see _Compaction: yes, hourly_ below.** This section priced a
_daily_ pass and rejected it, and the rejection still holds for that shape. The
hourly pass that ships is a different unit with a different peak, and it exists
because a later decision made the roll interval twelve times shorter.

Compaction would merge a day of roll files into one file per partition. Its only
real gain is that a merged file can be sorted by path, which gives row-group
statistics something to prune on:

|                               | single-path range | bytes read | tree     |
| ----------------------------- | ----------------- | ---------- | -------- |
| 24 flat roll files            | 150 ms (133–182)  | 2.5 MB     | 3.51 MiB |
| one flat file, sorted by path | 104 ms (100–114)  | 0.2 MB     | 2.40 MiB |

Roughly 40 ms and 1.1 MiB a day. The cost is a second short-lived process at
466.5–484.6 MB peak that spills 46–51.2 MB to disk sorting 1.27M rows — the
largest transient anywhere in this design, larger than the roll it would follow,
and it arrives with its own scheduling, its own atomicity and its own failure
mode. A day at the real rate is 11M rows, not 1.27M, so that peak is a floor.

Compaction without the sort is cheaper — 344.4–369.4 MB, 704 ms — and buys
almost nothing: 140 ms against 150 ms, 3.18 MiB against 3.51 MiB.

So: no daily compaction pass, and no second unit for one.

**What would reopen it.** If Unit 4a's real reader shows a multi-day query
dominated by per-file cost rather than by startup. The aged-tree row for 720
flat files — 931–1,169 ms and 75.5 MB read for one path over thirty days — is
the warning sign to watch. If that shape turns out to be common, the answer is
group partitioning plus daily compaction (559 ms, 19.1 MB), and it has to be
decided before a year of tree exists.

## Compaction: yes, hourly

**What reopened it was not the query — it was the roll interval.** The default
moved from 60 minutes to 5, because the hot store's ceiling is what the
interval has to hold and 5 minutes holds it at rates an hour does not. That
turns 24 files a day into 288, and 720 files for thirty days into 8,640 — well
past the file count the section above named as the warning sign.

The pass that ships merges **one completed hour**, not a day, and it is the
sort that pays rather than the file count:

|                                         |                  |
| --------------------------------------- | ---------------- |
| twelve 5-minute rolls, one hour         | 936,216 B        |
| one hourly file, sorted by `path`, zstd | 343,746 B        |
| bytes per row, inputs                   | 4.94             |
| bytes per row, unsorted hourly roll     | 3.17             |
| bytes per row, merged and sorted        | 1.81             |
| single-path range over a day's files    | 42.2 ms → 3.4 ms |

The transient is 190k rows at a 202 MB peak — the order the roll beside it
already costs, and less than half the 466.5–484.6 MB the daily pass needed.
That is the whole difference: a day is 1.27M–11M rows and cannot be merged
inside this device's budget; an hour can.

Three properties make it safe to run over the only copy of the data:

- **A reader never sees an hour twice.** The merge renames its output into
  place — atomic for one file — and unlinks its inputs afterwards, at no
  particular moment. `liveTreeFiles` stops returning a roll the instant a
  compacted file covering its id exists, so the window where both are on disk
  answers each row once.
- **An hour is merged once.** `planCompaction` reports an hour whose output
  already exists as already merged, and the only thing done to it is the
  removal of inputs an interrupted run did not get to. Re-merging survivors
  would rename a fraction of the hour over the whole of it.
- **A roll never lands inside a merged hour.** Suppression is by id range, so
  such a roll would be on disk and invisible to every query. `roll.ts` refuses
  the id outright and the rows stay in the hot store, which is this package's
  preferred failure.

Turning it off leaves the tree exactly as the rolls wrote it: `compactHourly`
in the plugin config, `--no-compact` on the writer.

## The sidecar: yes

Each roll writes one extra file holding the last value of every
`(context, path)` seen so far — cumulative, folding the previous sidecar into
the current window. Cumulative is not a refinement: a sidecar holding only its
own roll's paths answers "latest value" wrong for any path that stopped
reporting before the newest roll, which is the case the query exists to serve.

Measured over 24 rolls, three times: 26–28 ms median per roll (15–71 ms across
individual rolls), 11,185 bytes, 552 rows. Answering the all-paths snapshot from
the newest sidecar takes 92–96 ms total, of which 14–18 ms is the query. The
same answer costs 226–248 ms from a one-day tree of roll files, and
2,779–2,841 ms from a thirty-day one. That last number is the one that decides
it: a snapshot query's cost against a tree grows with the retention window and
the sidecar's does not.

It bounds the tree half of that query only. The hot-store half is still a full
scan — 94 ms at 13 MB, 441 ms at 130 MB — which is the roll interval again.

## What the shipped roll measures, on the device

Unit 3b's roll, running on a HALPI2 against a copy of a live hot store —
2,493,029 rows, 552 `(context, path)` pairs — through `./run bench roll`. The
figure is the roll process's own `VmHWM`, with an external poll of `/proc`
beside it as a cross-check; the two agreed to within 1% on every run.

| rows      | peak     | wall  |
| --------- | -------- | ----- |
| 156,000   | 146.1 MB | 4.2 s |
| 623,000   | 161.0 MB | 4.3 s |
| 1,246,000 | 159.6 MB | 4.8 s |
| 2,493,029 | 163.0 MB | 6.1 s |

Sixteen times the rows for 12% more memory. That is the streaming claim this
layout rests on, now measured through the shipped code rather than a scratch
script, and it is better than this document's own scale test (204–246 MB at
11.4M rows) because that test unioned one store with itself nine times and
widened every scan.

**The first roll after a DuckDB version change costs 40–50 MB more**, twice
reproduced at 623,000 rows: 193.9 and 203.7 MB cold against 151.7 and
153.5 MB warm. It
is the bundled `sqlite_scanner` being gunzipped from 8 MB to 27 MB into the
roll's own heap before any query runs. One roll per device per version pays it,
and nothing was added to avoid it.

## Provisional

**Every query figure here is provisional, including the ordering between
layouts.** The reader that will actually run these queries is Unit 4a; every
statement measured here was written for the measurement, so none of these
numbers predicts what Unit 4a will report, and Unit 4a re-measures all of them.

The ordering is what the decision rests on, so state what it would take to
overturn it rather than calling it safe. A real reader would have to make a
10,337-file tree answer a single-path range about eight times faster than the
ad-hoc statement did — 1,298 ms against the flat tree's 150 ms — while not
speeding the flat tree up at all. The bytes and the file count do not move: the
per-path tree holds six times the Parquet and twelve times the disk whatever
reads it. That is the margin, and Unit 4a is where it is checked rather than
asserted.

Two other limits worth stating. The aged trees hold thirty days of _files_ and
one day of _bytes_, so their timestamps repeat and no time pruning is possible
within them; the whole-range rows are therefore honest for a query that really
does span the window and pessimistic for one that does not, which is what the
one-day column brackets. And nothing here measured aggregation, so Unit 5b's
resolution ladder — which runs inside a roll process whose peak this document
reports — is unpriced.

## What the layout requires of whatever reads and writes it

These are properties of the tree, not a work plan; the unit issues carry the
task assignments.

- **Nothing untrusted becomes a path.** Contexts and paths are columns. Only the
  roll's own UTC date is a directory.
- **The interval must divide 1,440 minutes.** Not because a roll would
  otherwise straddle a date directory — rows are placed by their own timestamp,
  so a roll spanning midnight writes two files — but because a schedule of
  "every N minutes from UTC midnight" only describes a cadence when N divides
  the day.
- **The set a roll covers is identified by rowid, not by a time window.** The
  recorder stamps `ts` on arrival and the buffer holds it for up to a flush
  interval, so a sample older than any window routinely reaches the store after
  the roll has read it. What Parquet receives and what the store then deletes
  have to be the same set, and only a monotonic id makes them so.
- **Selection is by date directory and timestamp, not by partition pruning.** A
  reader narrows to files with a directory glob and a `ts` filter; nothing in
  the tree prunes on `path`.
- **Expiry drops whole date directories, and nothing finer.** A roll file is
  named for the roll that wrote it, not for the rows inside it, so the file name
  bounds nothing and the date directory is the finest boundary the tree has.
  That makes retention a **storage bound rather than a deletion guarantee** — a
  directory survives until its whole day is behind the window, so the oldest
  surviving sample can be up to a day older than the boundary. The window is
  measured back from `min(clock, the end of the newest date directory)`, which
  is QuestDB's TTL formula with the directory in place of its latest timestamp.
  Settled by Unit 5a.
- **The sidecar is read as well as written.** Each roll folds the previous one
  in, so it is an input to the roll that produces it. It lives at
  `latest/latest.parquet`, a sibling of the tree rather than a file inside it,
  and expiry does not touch it — so it keeps answering for a path whose tree
  files have gone.
- **A roll's peak needs a measurement the harness does not have.** `src/bench/`
  measures running subjects over settled windows; a roll's only quantity is
  `VmHWM` at exit.
