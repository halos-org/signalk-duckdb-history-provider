import {
  closeSync,
  constants,
  fchmodSync,
  fchownSync,
  fstatSync,
  fsyncSync,
  openSync,
  renameSync,
} from "node:fs";
import { dirname } from "node:path";

/**
 * Making a file appear, completely or not at all.
 *
 * Nothing here imports a storage engine, so both the extension resolver and
 * the roll can use it. `syncDirectory` is shared by both; `commitFile` is for
 * callers that do not already hold the write handle — the extension resolver
 * does, and fsyncs it inline while it still has it.
 */

/**
 * `fsync` a directory, so a rename into it survives a power cut.
 *
 * Not every platform allows fsync on a directory handle. The device target is
 * Linux, where it works and where it is what makes the rename durable.
 */
export function syncDirectory(directory: string): void {
  try {
    const fd = openSync(directory, "r");
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
  } catch (err) {
    // Only the platform's refusal is expected. Anything else — the directory
    // is gone, or unreadable — means the rename this was meant to make
    // durable is in doubt, and swallowing that would hide it.
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== "EINVAL" && code !== "EACCES" && code !== "EPERM") throw err;
  }
}

/** The uid and gid a committed file should carry, when they are not the
 * committing process's own. `statSync` on the target directory produces it. */
export interface FileOwner {
  uid: number;
  gid: number;
}

/**
 * Publish a file written under a temporary name.
 *
 * The order is the whole point: the file's own bytes reach the disk, then the
 * rename makes it visible, then the directory entry reaches the disk. A
 * reader either sees the previous file or this one, never a partial one —
 * which is what lets the roll write into a tree something else is reading.
 *
 * **`O_NOFOLLOW`, and every mode change through the descriptor.** The temp
 * lives in a directory the writing process does not necessarily own — the
 * merge exists to be runnable by hand, as another user, over a tree Signal K
 * owns. Anyone who can create names in that directory can replace the temp
 * with a symlink between the write and this call, and a `chmod` or `chown` by
 * path would then follow it: a process running as root would hand an arbitrary
 * file to whoever set the link. `O_NOFOLLOW` refuses a symlink outright, and
 * an open descriptor cannot be swapped afterwards.
 *
 * `owner` is for the caller whose uid differs from the tree's. Without it a
 * merge run as root leaves a 0600 root-owned file in a tree the query service
 * reads as another user — unreadable, with the rolls it superseded already
 * unlinked.
 */
export function commitFile(
  temp: string,
  final: string,
  owner?: FileOwner,
): void {
  const fd = openSync(temp, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    if (owner !== undefined) {
      // Skipped when they already match, which is every in-process caller:
      // `fchown` to the values a file already has is permitted for its owner,
      // but asking for it needlessly turns a no-op into an EPERM on any
      // filesystem that refuses the call outright.
      const current = fstatSync(fd);
      if (current.uid !== owner.uid || current.gid !== owner.gid) {
        fchownSync(fd, owner.uid, owner.gid);
      }
    }
    // 0600, like the pid file and the pending-roll record. DuckDB creates its
    // output at 0666 & ~umask, which is 0644 by default — and the tree holds
    // the vessel's position history. The 0700 directory above it is the only
    // other protection, and that does not survive a copy or a filesystem
    // without modes.
    fchmodSync(fd, 0o600);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(temp, final);
  syncDirectory(dirname(final));
}
