import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  existsSync,
  lstatSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { commitFile } from "../durable-write.js";

/**
 * Publishing a file, and refusing to publish through a symlink.
 *
 * The temp a caller commits lives in a directory that caller does not
 * necessarily own -- the merge exists to be runnable by hand, as another user,
 * over a tree Signal K owns. Anyone who can create names there can replace the
 * temp between the write and the commit.
 */

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "durable-"));
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe("commitFile", () => {
  it("publishes the temp under its final name, at 0600", () => {
    const temp = join(dir, "x.tmp");
    const final = join(dir, "x");
    writeFileSync(temp, "rows", { mode: 0o644 });

    commitFile(temp, final);

    assert.equal(existsSync(temp), false);
    assert.equal(readFileSync(final, "utf8"), "rows");
    assert.equal(statSync(final).mode & 0o777, 0o600);
  });

  /**
   * **The reason the mode change goes through a descriptor.** A `chmod` or
   * `chown` by path follows a symlink, so a process running as root would hand
   * an arbitrary file to whoever set the link -- and the window is the whole
   * time between the write and the commit, which for a merge is two full scans
   * of its inputs. `O_NOFOLLOW` refuses the open outright, and a descriptor
   * cannot be swapped afterwards.
   */
  it("refuses a temp that has been replaced by a symlink", () => {
    const victim = join(dir, "victim");
    writeFileSync(victim, "not yours", { mode: 0o644 });
    const temp = join(dir, "x.tmp");
    symlinkSync(victim, temp);

    assert.throws(() => commitFile(temp, join(dir, "x")), /ELOOP|EMLINK/);

    // Untouched: neither its mode nor its contents, and the link still stands.
    assert.equal(statSync(victim).mode & 0o777, 0o644);
    assert.equal(readFileSync(victim, "utf8"), "not yours");
    assert.equal(lstatSync(temp).isSymbolicLink(), true);
  });

  it("gives the published file an owner when one is asked for", () => {
    const temp = join(dir, "x.tmp");
    writeFileSync(temp, "rows");
    const own = statSync(dir);

    // The same uid and gid it already has: the call is a no-op, and asking for
    // it must not fail. A differing owner needs privilege this test does not
    // have, so the branch that changes anything is exercised on a device.
    commitFile(temp, join(dir, "x"), { uid: own.uid, gid: own.gid });

    assert.equal(statSync(join(dir, "x")).uid, own.uid);
  });
});
