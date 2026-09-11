import { chmodSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import type { Server } from "node:net";
import { probeLiveWriter } from "./server.js";

/**
 * The exclusive claim on a data directory, taken by every process that
 * rewrites the tree.
 *
 * The roll and the merge both rename files into the tree and unlink others,
 * and the two must never overlap: a roll that lands while a merge of its hour
 * is in flight ends up inside a merged hour, which `liveTreeFiles` suppresses
 * and no query returns. The scheduler serialises its own children, but the
 * merge is also a hand-run process -- the backfill of a tree recorded before
 * compaction existed is a loop over `compact/main.js` -- so serialisation
 * inside one writer is not enough.
 *
 * The pending-roll record cannot carry this. `readPendingRoll` returns null
 * for absent, torn and unreadable alike, deliberately, because "a corrupt file
 * must not be able to stop a device rolling" -- fail-open is right for the
 * writer's liveness and wrong as an interlock over deletion.
 *
 * Nothing here imports a storage engine, so both entry points can take it
 * without either becoming reachable from the plugin.
 */

/**
 * Take the claim, or return null because another process holds it.
 *
 * **Bind first, ask questions second.** The bind is the atomic step: two
 * processes racing cannot both succeed at it, and the loser gets EADDRINUSE.
 * Probing first and then unlinking would let both see nothing listening, both
 * unlink, and both bind -- which is precisely the case this exists for: an
 * orphan left by a killed writer, and the successor that follows it.
 *
 * The unlink only ever removes a socket nothing answers on. The residual race
 * is two processes finding the *same* stale socket in the same instant.
 */
export async function claimTheDataDirectory(
  socketPath: string,
): Promise<Server | null> {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const server = createServer();
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(socketPath, () => resolve());
      });
      chmodSync(socketPath, 0o600);
      // Never hold the process open: the caller's own work decides when it ends.
      server.unref();
      return server;
    } catch (err) {
      server.close();
      if ((err as NodeJS.ErrnoException).code !== "EADDRINUSE") throw err;
      // Something is at that path. Only a live holder may keep it.
      if (await probeLiveWriter(socketPath)) return null;
      rmSync(socketPath, { force: true });
    }
  }
  return null;
}
