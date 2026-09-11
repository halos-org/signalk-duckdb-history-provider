import { readFileSync } from "node:fs";
import { join } from "node:path";
import { DATA_LAYOUT } from "../data-dir.js";

/**
 * What the plugin and the writer process have to agree on.
 *
 * Kept apart from `main.ts` because the plugin needs these values and
 * importing `main.ts` to get them would run the writer inside the Signal K
 * process — the one thing this design exists to prevent. Nothing here imports
 * a storage engine, so it is safe on both sides.
 */

/**
 * The writer's exit codes. The plugin reads them and turns them into something
 * an operator can see.
 *
 * `EXIT_LOCKED` is named separately from a general failure because the remedy
 * differs: another writer owns the store, and retrying cannot help.
 */
export const EXIT_LOCKED = 3;

/**
 * A roll found its filename already taken and is not a retry of the roll that
 * wrote it.
 *
 * Named separately because the scheduler must treat it as the one failure a
 * retry cannot fix. Every other failure keeps the roll's id so the retry
 * replaces its own half-written output; keeping it for this one turns the
 * refusal into permission at the next slot, and the roll then overwrites an
 * earlier roll's file with a fraction of its rows.
 */
export const EXIT_NAME_TAKEN = 4;

/**
 * The shutdown budget the plugin and the writer have to agree on.
 *
 * `ROLL_KILL_GRACE_MS` is strictly under `WRITER_EXIT_TIMEOUT_MS`: the
 * writer's SIGTERM handler waits for a running roll, and the plugin SIGKILLs
 * the writer when that takes too long. With the grace above the budget, a roll
 * slow to die outlives its writer, keeps writing to the tree with nothing left
 * to kill it, and races the successor's roll for the same filename. The two
 * live here because neither file can see the other's constant.
 */
export const WRITER_EXIT_TIMEOUT_MS = 3000;
export const ROLL_KILL_GRACE_MS = 1500;

/**
 * Where the writer's files live under the resolved data directory.
 *
 * `pidFile` is written for whoever is reading the device, and nothing decides
 * anything from it. The claim on the store is the socket: a pid means nothing
 * across the PID namespaces a container restart creates.
 */
export function writerPaths(dataDir: string): {
  store: string;
  pidFile: string;
  socket: string;
  pendingRoll: string;
  rollSocket: string;
} {
  const hot = join(dataDir, DATA_LAYOUT.hotStore);
  return {
    store: join(hot, "hot.sqlite"),
    pidFile: join(hot, "writer.pid"),
    socket: join(hot, "writer.sock"),
    // The id of a roll that has been started and not yet truncated. Unlike
    // the pid file, something does decide from this one — see
    // roll-scheduler.ts.
    pendingRoll: join(hot, "roll-pending.json"),
    // A roll's claim on the data directory, made the same way the writer
    // claims the store: something answering here is a live roll. A pid would
    // not do — this runs in a container, where a restart makes a dead roll's
    // pid some unrelated live process.
    rollSocket: join(hot, "roll.sock"),
  };
}

/**
 * What one roll attempt was doing, durable across a writer's death.
 *
 * `rolling`: files may exist under this id and nothing is truncated — a retry
 * owns them. `written`: the roll finished and only the truncate is
 * outstanding.
 *
 * Two processes read this and only the writer writes it. The scheduler decides
 * whether a retry may replace a file; a query decides which of the store's
 * rows the tree already holds, and so must not be counted twice.
 */
export interface PendingRoll {
  rollId: number;
  maxRowid: number;
  phase: "rolling" | "written";
}

export function readPendingRoll(dataDir: string): PendingRoll | null {
  try {
    const raw = JSON.parse(
      readFileSync(writerPaths(dataDir).pendingRoll, "utf8"),
    ) as Partial<PendingRoll>;
    // `>= 1`, matching what the roll process accepts. Accepting 0 here made a
    // `{"rollId":0}` file wedge every future roll: the scheduler adopted it
    // and the roll refused it, for ever. `nextRollAt` really can return 0 —
    // for any clock set before the epoch.
    if (
      !Number.isSafeInteger(raw.rollId) ||
      (raw.rollId as number) < 1 ||
      !Number.isSafeInteger(raw.maxRowid) ||
      (raw.maxRowid as number) < 1 ||
      (raw.phase !== "rolling" && raw.phase !== "written")
    ) {
      return null;
    }
    return raw as PendingRoll;
  } catch {
    // Absent is the normal case. Unreadable is treated the same way on
    // purpose: a corrupt file must not be able to stop a device rolling.
    return null;
  }
}
