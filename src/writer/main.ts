import {
  chmodSync,
  mkdirSync,
  rmSync,
  writeFileSync,
  writeSync,
} from "node:fs";
import { join } from "node:path";
import { DATA_DIR_MODE, DATA_LAYOUT } from "../data-dir.js";
import { dividesTheDay } from "../roll/schedule.js";
import { HotStore } from "./hot-store.js";
import { RollScheduler } from "./roll-scheduler.js";
import { StoreLockedError, WriterServer } from "./server.js";
import { EXIT_LOCKED, writerPaths } from "./contract.js";

/**
 * The writer process.
 *
 * Spawned by the plugin, one per plugin run, and the only process that writes
 * the hot store. It exists so that no storage work happens on the Signal K
 * event loop; keeping it a separate process rather than a worker thread is
 * what makes that true of memory as well as CPU.
 *
 *   node dist/writer/main.js --data-dir <path> --roll-interval-minutes <n>
 *     [--retention-days <n>]
 *
 * Exit codes are read by the plugin, which turns them into a status an
 * operator can see:
 *   0  asked to stop, stopped cleanly
 *   3  another writer already holds the hot store
 *   1  anything else
 */

/**
 * Write to stderr and be sure it arrives.
 *
 * `process.stderr.write` is asynchronous when stderr is a pipe — which it
 * always is here, since the parent spawns this with piped stdio — and
 * `process.exit` right after it truncates whatever is still queued. The
 * message this loses is the one the parent logs as the reason.
 */
function writeStderr(line: string): void {
  writeSync(2, line);
}

function argValue(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

async function main(): Promise<void> {
  const dataDir = argValue("--data-dir");
  if (dataDir === undefined || dataDir === "") {
    writeStderr(
      "usage: writer/main.js --data-dir <path> --roll-interval-minutes <n> " +
        "[--retention-days <n>] [--no-compact]\n",
    );
    process.exit(1);
  }
  // Checked here, beside --data-dir, rather than left to the scheduler. An
  // unchecked NaN reached `arm()` only AFTER the store was open, the socket
  // claimed and the pid file written, and the plugin then latched that exit as
  // fatal — recording stopped for a missing argument.
  const rollIntervalMinutes = Number(argValue("--roll-interval-minutes"));
  if (!dividesTheDay(rollIntervalMinutes)) {
    writeStderr(
      `--roll-interval-minutes must be a whole number of minutes dividing 1440, ` +
        `not ${argValue("--roll-interval-minutes") ?? "(missing)"}\n`,
    );
    process.exit(1);
  }

  // Not checked like the interval above, and deliberately: a retention this
  // cannot read means the tree keeps everything, which costs disk, while
  // exiting here would stop recording. The plugin normalizes the value before
  // it gets here, so an unreadable one is a bug in this package rather than
  // something an operator typed.
  const retentionRaw = argValue("--retention-days");
  const retentionDays = Number(retentionRaw ?? 0);
  const retentionUsable =
    Number.isFinite(retentionDays) && retentionDays >= 0
      ? Math.floor(retentionDays)
      : 0;
  if (retentionRaw !== undefined && retentionUsable !== retentionDays) {
    writeStderr(
      `--retention-days ${retentionRaw} is not a whole number of days; ` +
        `keeping ${retentionUsable === 0 ? "everything" : `${retentionUsable} days`}\n`,
    );
  }

  const paths = writerPaths(dataDir);
  const hot = join(dataDir, DATA_LAYOUT.hotStore);
  mkdirSync(hot, { recursive: true, mode: DATA_DIR_MODE });
  // Before the store is opened: HotStore.open creates hot.sqlite and its WAL
  // inside this directory, and until the mode is right they are readable by
  // anyone who can reach it.
  chmodSync(hot, DATA_DIR_MODE);

  const store = HotStore.open(paths.store);
  let server;
  try {
    server = await WriterServer.listen({
      socketPath: paths.socket,
      store,
      log: (line) => process.stdout.write(`${line}\n`),
    });
  } catch (err) {
    if (err instanceof StoreLockedError) {
      // Named separately from any other failure because the plugin's remedy is
      // different: a second writer is a problem to resolve, not to retry.
      writeStderr(`${err.message}\n`);
      process.exit(EXIT_LOCKED);
    }
    throw err;
  }
  // Informational only. Nothing decides anything from it -- see writerPaths.
  writeFileSync(paths.pidFile, `${process.pid}\n`, { mode: 0o600 });
  // The settings this process was actually given, not the ones the Admin UI
  // shows. They arrive as arguments, and a flag the plugin and this side spell
  // differently would otherwise mean silently keeping everything for ever.
  // Present-means-off, like --replace on the roll: the plugin passes the flag
  // only when an operator turned compaction off, so a writer started by an
  // older plugin compacts, which is the default.
  const compactHourly = !process.argv.includes("--no-compact");
  process.stdout.write(
    `writer ready on ${paths.socket}, rolling every ${rollIntervalMinutes} minutes, ` +
      `keeping ${retentionUsable === 0 ? "everything" : `${retentionUsable} days`}, ` +
      `${compactHourly ? "compacting" : "not compacting"} each completed hour\n`,
  );

  // The roll runs here rather than in the plugin because only this process may
  // write to the store: the roll reads it read-only, and the delete that
  // follows a successful roll has nowhere else it could happen.
  const rolls = new RollScheduler({
    store,
    dataDir,
    intervalMinutes: rollIntervalMinutes,
    retentionDays: retentionUsable,
    compactHourly,
    log: (line) => process.stdout.write(`${line}\n`),
    // stderr, because the plugin routes it to app.error while stdout goes to
    // app.debug. A roll failure nobody sees is the shape this whole design
    // exists to make impossible.
    onError: (line) => writeStderr(`${line}\n`),
  });
  rolls.start();

  let stopping = false;
  const stop = async (signal: string): Promise<void> => {
    if (stopping) return;
    stopping = true;
    process.stdout.write(`writer stopping on ${signal}\n`);
    await rolls.stop();
    await server.close();
    store.close();
    rmSync(paths.pidFile, { force: true });
    process.exit(0);
  };
  process.on("SIGTERM", () => void stop("SIGTERM"));
  process.on("SIGINT", () => void stop("SIGINT"));
}

main().catch((err: unknown) => {
  writeStderr(
    `${err instanceof Error ? (err.stack ?? err.message) : String(err)}\n`,
  );
  process.exit(1);
});
