# syncthing-backup-tool

Version **0.0.3**. A Linux service that creates full, timestamped ZIP snapshots
of directories and removes older snapshots on an independent retention schedule.
It works with any readable directory; Syncthing is a common pairing, not a dependency.

The service delegates copying to `rsync`, compression to `zip`, and ZIP testing
to `unzip`. It stages data on the backup drive, verifies archives before publication,
and includes a manifest with file checksums and its original retention policy.
Requests for a busy target are skipped rather than building an unlimited backlog.

## Install with APT

The signed APT repository is hosted directly in this GitHub repository's `apt`
branch. Release 0.0.3 provides an **amd64** package for Debian/Ubuntu systems
with Linux **5.6 or newer**, systemd with cgroup v2, and `/proc`. The binary is statically linked
with musl; Rust is not needed on the server.

```bash
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -d -m 0755 /etc/apt/keyrings

curl -fsSL https://raw.githubusercontent.com/dingyisun0101/syncthing-backup-tool/apt/syncthing-backup-tool-archive-keyring.gpg \
  | sudo tee /etc/apt/keyrings/syncthing-backup-tool.gpg >/dev/null
sudo chmod 0644 /etc/apt/keyrings/syncthing-backup-tool.gpg

echo 'deb [arch=amd64 signed-by=/etc/apt/keyrings/syncthing-backup-tool.gpg] https://raw.githubusercontent.com/dingyisun0101/syncthing-backup-tool/apt/ ./' \
  | sudo tee /etc/apt/sources.list.d/syncthing-backup-tool.list >/dev/null

sudo apt-get update
sudo apt-get install syncthing-backup-tool
```

The public signing-key fingerprint is recorded in [release documentation](docs/releases.md).
APT checks signed repository metadata and package checksums; this setup uses
`signed-by`, without disabling authentication. See [APT's authentication documentation](https://manpages.debian.org/bookworm/apt/apt-secure.8.en.html).

Alternatively, download the `.deb` from the
[v0.0.2 release](https://github.com/dingyisun0101/syncthing-backup-tool/releases/tag/v0.0.3)
and install it with `sudo apt-get install ./syncthing-backup-tool_0.0.3_amd64.deb`.

Installation creates the `syncthing-backup` service account, installs an empty
configuration, and leaves the service stopped. Package upgrades preserve the
configuration as a Debian conffile; they do not automatically restart the service.

## Configure and grant access

The default configuration is **`/etc/syncthing-backup-tool/config.json`**.
Override it with `--config /absolute/path/config.json`. All source, destination,
and state paths in the JSON must be absolute.

Start from the installed example:

```bash
sudo cp /usr/share/doc/syncthing-backup-tool/config.example.json \
  /etc/syncthing-backup-tool/config.json
sudo chown root:syncthing-backup /etc/syncthing-backup-tool/config.json
sudo chmod 0640 /etc/syncthing-backup-tool/config.json
sudoedit /etc/syncthing-backup-tool/config.json
```

Replace the example paths with your own. `required_source_mount` and
`required_destination_mount` must identify actual mount points; set either to
`null` for an ordinary directory on an already available filesystem. An absent
required mount prevents capture or writes to the underlying mount directory.

After mounting the drives, create an empty destination owned by the service account:

```bash
sudo install -d -o syncthing-backup -g syncthing-backup -m 0700 \
  /mnt/hdd/backups/photos
```

The service needs read access to source files and read/traversal access to source
directories. It needs traversal access to all parent directories and write access
to its backup/state directories. For a source owned by another account, ACLs
grant the needed access while retaining ownership:

```bash
sudo setfacl -m u:syncthing-backup:x /mnt/ssd /mnt/ssd/syncthing
sudo setfacl -R -P -m u:syncthing-backup:rX /mnt/ssd/syncthing/photos
sudo find /mnt/ssd/syncthing/photos -type d \
  -exec setfacl -m d:u:syncthing-backup:rx {} +
```

Add traversal access to restrictive destination parents as needed. Default ACLs
help new files inherit access, but creation modes and subsequent permission
changes can still restrict it. Verify access to a newly synced file. See
[Linux ACL inheritance](https://man7.org/linux/man-pages/man5/acl.5.html).

Validate the configuration, then test one backup before enabling the service:

```bash
# The example writes audit logs here. Create it before the first one-shot run;
# systemd also creates it when starting the service.
sudo install -d -o syncthing-backup -g syncthing-backup -m 0700 \
  /var/log/syncthing-backup-tool
sudo -u syncthing-backup syncthing-backup-tool validate
sudo -u syncthing-backup syncthing-backup-tool backup --target photos
sudo systemctl enable --now syncthing-backup-tool
sudo syncthing-backup-tool status
```

`validate` checks JSON and policies without creating files. The one-shot backup
also exercises permissions, mount checks, and archive verification. It cannot
run alongside a daemon using the same state directory.

## Write configuration

Here is a minimal target, using defaults for omitted settings:

```json
{
  "config_version": 1,
  "state_dir": "/var/lib/syncthing-backup-tool",
  "targets": [
    {
      "id": "documents",
      "source_dir": "/home/alice/Documents",
      "destination_dir": "/mnt/backup/documents",
      "required_destination_mount": "/mnt/backup",
      "backup_interval_seconds": 21600,
      "retention": {
        "min_snapshots": 2,
        "max_snapshots": 30
      }
    }
  ]
}
```

This captures every six hours and retains up to 30 snapshots. Cleanup runs hourly
by default. Counts, age limits, byte limits, compression, exclusion globs, retries,
worker limits, and memory budgets are documented in the
[configuration reference](docs/configuration.md). The
[full example](packaging/config.example.json) includes every setting.

## Refresh configuration explicitly

Editing JSON **does not change the running service**. It does not watch the file,
poll its modification time, or automatically reload it. Apply an edit with:

```bash
sudo syncthing-backup-tool reload
# Equivalent through systemd:
sudo systemctl reload syncthing-backup-tool
```

The daemon reads its original config path, validates the entire replacement, and
then activates it. An invalid replacement returns an error and leaves the loaded
configuration active. `status` reads daemon state rather than rereading JSON.

Running/queued backups keep their original target settings. Existing archives
are never rewritten, renamed, or moved by a reload. Each retains the retention
policy recorded when it was created. Changing retention creates a new cohort for
future archives; old cohorts continue under their original policy. Consequently,
the total retained count can exceed the newest policy's `max_snapshots`.

Removing/disabling a target or changing its source/destination stops cleanup of
its old location. Queued work for a removed/disabled target is cancelled; an
already running operation can finish under its captured settings. Keep target
IDs and the state directory stable when reusing a destination.

Changing `state_dir` or `memory_limit_bytes` requires a restart. Resource changes
must wait until active backup/retention workers finish. If you change the hard
memory limit or shutdown grace period, generate a matching systemd unit:

```bash
sudo syncthing-backup-tool unit \
  | sudo tee /etc/systemd/system/syncthing-backup-tool.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl restart syncthing-backup-tool
```

## Operation and restore

```bash
sudo syncthing-backup-tool status
sudo journalctl -u syncthing-backup-tool -f
sudo systemctl stop syncthing-backup-tool
```

Archive names contain the capture start time in UTC and a unique job ID, for example
`2026-10-08T03-15-00.123456789Z_<job-id>.zip`. Each ZIP is independent:

```bash
unzip -t /mnt/hdd/backups/photos/ARCHIVE.zip
unzip /mnt/hdd/backups/photos/ARCHIVE.zip -d /tmp/restore
```

Your files are under `/tmp/restore/data/`; `meta/manifest.json` records checksums,
selection rules, original metadata, and policy. Review restored files before
copying them back into a live source. The initial release has no restore command.

Keep room for retained archives, one uncompressed staging copy, one replacement
ZIP, and the free-space reserve. Staging is always in the destination filesystem
and is removed before publication. The SSD needs no additional complete copy. When an archive fails, it never replaces a good backup. At shutdown,
the service drains active operations before requesting cancellation; after a
crash it reconciles publication intentions and retries unfinished writes.

Sources are read live over an interval. Detected changes/unreadable files fail the
attempt, but this does not provide an atomic or application-consistent filesystem
snapshot. Filesystem snapshot integration is a later extension. Symlinks default
to rejection; `preserve` stores links without following their targets, and `skip`
records explicit omissions. Special files
and unsupported filenames are rejected. ZIPs preserve basic Unix permission bits
and modification times; the manifest carries precise metadata. ACLs, xattrs,
ownership restoration, sparse layout, and hard-link relationships are not preserved.

The service verifies complete contents before publication and rechecks archives
when it needs to retire a cohort. Corrupt archives are quarantined in the catalog
and left on disk. Unknown destination files remain untouched. See
[operations and recovery](docs/operations.md) for failure handling and removal.

## Build and contribute

Rust 1.88 or newer and a C compiler are required; SQLite is built into the binary.
Runtime tools are `rsync`, `zip`, `unzip`, and `prlimit` (from `util-linux`); APT
installs them as dependencies. For source builds, install them first.

```bash
sudo apt-get install build-essential rsync zip unzip util-linux
git clone https://github.com/dingyisun0101/syncthing-backup-tool.git
cd syncthing-backup-tool
cargo build --release --locked
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
```

Run a source build with `target/release/syncthing-backup-tool --config /absolute/config.json`.
For a non-systemd instance, select an accessible socket with
`--control-socket /absolute/control.sock`; give `reload` and `status` that same flag.
The application budget bounds job buffers/metadata; a hard process memory cap is
provided by systemd's `MemoryMax=` and is not imposed by foreground invocations.

See [architecture](docs/architecture.md), [configuration](docs/configuration.md),
[module interfaces](docs/modules.md), and [release maintenance](docs/releases.md)
for module boundaries, backend replacement, packaging,
and the signed APT publication procedure. Licensed under [MIT](LICENSE).

## Immediate backups, hooks, and audit logs

The running service can accept an immediate request without stopping:

```bash
sudo syncthing-backup-tool trigger --target photos --wait
sudo syncthing-backup-tool trigger --wait
sudo syncthing-backup-tool job JOB_ID
```

`--wait` returns success only when the job succeeds, including required hooks.
It returns nonzero for skipped/failed jobs. A busy target rejects another request.
`--timeout-seconds N` limits client waiting; the backup continues in the service.
Immediate requests use loaded configuration and do not move scheduled deadlines.

Hooks run in the `before_backup`, `after_capture`, `after_backup`, and `finally`
phases. Before-hook errors can skip, retry, fail, or explicitly continue. Mandatory
cleanup obligations survive crashes and block further work on the target until
recovered. See [hook contracts](docs/hooks.md) and the packaged Minecraft helper
at `/usr/lib/syncthing-backup-tool/minecraft-hook.py`.
For owner-only Minecraft save files, use the optional owner-side controller
described in [Minecraft setup](docs/minecraft.md).

Set `logging.audit_file` to `/var/log/syncthing-backup-tool/operations.jsonl` for
persistent timestamped operation/file logs. `max_file_bytes` and `max_files`
control rotation. Journald continues to receive summaries. Calendar schedules
support daily/weekly local times with IANA timezones; intervals remain supported.
