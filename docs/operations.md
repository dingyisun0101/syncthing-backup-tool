# Operations and recovery

Run the installed service as `syncthing-backup`, with source read/traversal access
and destination/state write access. Installation does not start the service.
Configure it, take a one-shot backup, then enable it as described in the README.

## Commands

| Command | Behavior |
| --- | --- |
| `run` (or no subcommand) | Foreground daemon, usually managed by systemd. |
| `validate` | Check JSON, ranges, exclusions, and path separation without creating state/archive files. |
| `backup --target ID` | Create one snapshot; omit target for all enabled targets. Requires the daemon to be stopped for that state directory. |
| `retain` | Execute one cleanup sweep against recorded policies, with the daemon stopped. |
| `reload` | Ask the running daemon to reread its original config path; failure retains loaded settings. |
| `status` | Read loaded service state, next deadlines, outstanding counts, last error, snapshot counts/bytes, and age of last successful capture. |
| `unit` | Generate a unit with config path, memory limit, and shutdown timeout. |

`--config PATH` and `--control-socket PATH` are global flags. The default socket
is `/run/syncthing-backup-tool/control.sock`, accessible to root and the service
account. For a foreground development instance, choose a private writable socket
path and pass it to `run`, `reload`, and `status`. `reload`/`status` do not load the
client's JSON file; they contact the daemon. No file watcher or reload signal is used.

`systemctl reload` invokes the same explicit reload command. A systemd
`daemon-reload` only refreshes service-unit definitions; it does not refresh
application JSON.

## Troubleshooting

Inspect `journalctl -u syncthing-backup-tool` and `sudo syncthing-backup-tool status`.
A live process with an old last-success timestamp is not necessarily creating
backups successfully. Persistent failures retain their last error in the catalog.

- **Permission denied:** check source file reads and traversal of every source,
  destination, config, and state parent as the service account. New Syncthing file
  permissions may mask an inherited ACL.
- **Required mount unavailable:** mount the configured device at the exact mount
  point, or correct a mistaken mount setting. The service refuses fallback writes.
- **Destination ownership mismatch:** the folder is bound to target ID, state
  directory, and device identity. Restore its original identity. The first release
  does not provide a general adoption/migration command.
- **Source changed during capture:** live reads detected a changing file or
  directory. The service retries within the configured bound; use a stable source
  view for atomic or application-consistent capture.
- **Metadata exceeds memory budget:** split the source into smaller targets or
  increase memory budget and regenerate the unit if its hard cap also changes.
- **Free-space reserve/output limit exceeded:** provide more destination capacity
  or adjust explicit limits. Keep space for a new ZIP alongside retained archives.
- **Invalid reload:** correct the reported field and rerun `reload`; the old
  configuration is still active. Worker resource changes may need an idle period.

## Failure recovery

The private state directory contains `state.sqlite3` (with WAL side files), an
instance lock, and per-filesystem write locks. Each destination has
`.snapshot-owner.json`, `.partial/`, and its completed ZIPs. Known temporary
artifacts include `<job-id>.tree/`, `<job-id>.files`, and `<job-id>.zip.part`. Do not edit these
while the service is running. SQLite transactions use full synchronization;
publication synchronizes files and both rename directories before catalog commit.

After a crash, known unfinished `.part`, staging-tree, and selection-list
artifacts are removed and the job retries from a fresh rsync capture. Unknown temporary files remain untouched. If rename succeeded
before the catalog commit, recovery verifies the intended archive and adopts
that same job once. An ambiguous or corrupt final archive prevents blind retry;
the service logs the deferred publication for operator investigation.

Deletion intentions survive a crash. Recovery reconciles files already missing
and replans pending deletions after verifying survivors; it does not blindly
complete an old deletion decision. Corrupt archives found during cleanup are
marked unhealthy and preserved; they do not satisfy the protected minimum.
Unknown ZIPs are never treated as owned just because their filenames match.

If the state database fails its integrity check, startup fails and no retention
executes. Stop the service and preserve the database, its WAL files, and archives
before repair. The initial release has no automatic catalog rebuild or unindexed
archive adoption command; ZIP manifests permit manual inspection/restoration.
Deleting the database is not a supported reset for an existing destination.

SIGTERM/SIGINT stop new admission and cleanup initiation. Active operations may
finish within the grace period, then receive cancellation at streaming boundaries.
Publication already in progress is finished durably. Systemd can forcibly stop
an operation stuck in kernel I/O after its longer stop timeout; startup then
uses the same recovery path. Source data is never deleted or rewritten. Tool process groups are terminated
on cancellation and reaped before temporary cleanup.

## Upgrades and removal

Use normal APT upgrades, then explicitly restart the service to run the new
binary. Debian conffile handling preserves local config edits. Unit overrides
under `/etc/systemd/system/` remain your responsibility across updates.

`sudo apt-get remove syncthing-backup-tool` stops the service and preserves its
config, state, archives, and service account. Purge removes the packaged config,
but state/archives are still preserved. Remove backup data manually only when
you intend to discard it. To disconnect the repository, remove its source-list
and dedicated keyring files and run `apt-get update`.
