# Configuration reference

Version 0.0.4 accepts `config_version: 1`. Unknown fields and invalid combinations
are rejected. The default file is `/etc/syncthing-backup-tool/config.json`;
`--config` chooses another path. The file is limited to 1 MiB.

The service loads once at startup and again only through `reload` or
`systemctl reload syncthing-backup-tool`. Invalid reloads leave current settings
active. Paths are absolute, cannot contain `..`, and are checked for overlap,
including existing symlink aliases. Sources cannot overlap current or historical
destinations or state storage. Destinations cannot overlap each other.

## Global fields

| Field | Default / requirement | Meaning |
| --- | --- | --- |
| `config_version` | Required, `1` | Schema version. |
| `backends` | Built-in choices below | Select compiled module implementations; unknown names are rejected. |
| `state_dir` | Required | Private directory for SQLite journal, locks, and catalog; cannot change during reload. |
| `targets` | Required array; may be empty | Independent directory backup targets. |
| `shutdown_grace_seconds` | `60` | Drain active operations before requesting cancellation; 1..86400. Unit stop timeout must be longer. |
| `queue.max_pending_jobs` | `16` | Global queued/retrying job capacity, excluding running jobs. |
| `queue.max_attempts` | `3` | Initial attempt plus retries. |
| `queue.retry_initial_seconds` | `30` | Initial exponential-backoff delay. |
| `queue.retry_max_seconds` | `900` | Backoff ceiling; at least the initial delay, at most 86400. |
| `resources.max_worker_threads` | `2` | Maximum backup/retention workers; 1..256. Coordinator infrastructure is additional. |
| `resources.max_concurrent_snapshots` | `1` | Shared operation slots; cannot exceed worker limit. Retention temporarily reserves one slot. |
| `resources.memory_budget_bytes` | `268435456` | Shared conservative job-buffer/metadata budget, divided across operation slots. |
| `resources.memory_limit_bytes` | `536870912` | Hard service cap used by `unit` to generate `MemoryMax=`; restart/unit regeneration required to change. |
| `resources.io_buffer_bytes` | `1048576` | Streaming buffer size. |
| `retention.sweep_interval_seconds` | `3600` | Independent cleanup interval; 1..31536000. |
| `logging.level` | `"info"` | `debug`, `info`, `warn`, or `error`. |
| `logging.format` | `"json"` | `json` or `text`, written to stderr/journald. |
| `logging.audit_file` | `null` | Absolute path for persistent JSON Lines operation logs; null writes operations to stderr. Active and rotated paths cannot overlap state, source, destination, or historical target data. |
| `logging.max_file_bytes` | `104857600` | Rotate the audit file at this size; at least 1 MiB. |
| `logging.max_files` | `10` | Number of rotated audit files retained, in addition to the active file; 1..100. |

The memory budget must be below the hard cap. Each operation needs 16 MiB of
fixed allowance, four buffers' worth of headroom, and a conservative metadata
reservation of 8192 bytes plus sixteen times the UTF-8 path length per entry.
This accounts for retained selection records and ZIP verification indexes/copies;
the first release rejects archives that exceed this allowance. Increase the budget or divide large trees into targets.
Actual process memory also includes the coordinator/database/catalog and runtime;
systemd enforces the total service cap, including tool subprocesses. Tools are
also launched through `prlimit` with a per-process address-space limit and a
file-size limit. The installed service checks its cgroup v2 memory limit at startup. Foreground commands need an external hard cap if
one is required.

## Target fields

| Field | Default / requirement | Meaning |
| --- | --- | --- |
| `id` | Required | Unique 1..64-character identifier: ASCII letters, digits, `_`, `-`. Keep it stable. |
| `source_dir` | Required | Readable directory; never written by the service. |
| `destination_dir` | Required | Dedicated empty directory, or one already owned by this target/state pair. |
| `enabled` | `true` | Disable future admission and cleanup. |
| `required_source_mount` | `null` | Actual mount point that must contain the source and be mounted before capture. |
| `required_destination_mount` | `null` | Actual mount point required before writing/removing archives. |
| `backup_interval_seconds` | `21600` | Backup cadence; 1..31536000. Used when `schedule` is null. |
| `schedule` | `null` | Optional daily/weekly calendar schedule; see below. |
| `run_on_startup` | `true` | Immediate first request for a new target. Existing schedules survive restarts. |
| `exclude_globs` | `[]` | Case-sensitive source-relative glob patterns using `/` separators; matched directories are pruned. |
| `symlink_policy` | `"reject"` | `reject` fails the job; `skip` records an omission; `preserve` archives the link itself. Links are never followed. |
| `consistency` | `"live"` | `live` or `application_quiesced`; the latter requires correctly configured application hooks. |
| `hooks` | Empty arrays | `before_backup`, `after_capture`, `after_backup`, and mandatory `finally` commands; see [hooks](hooks.md). |
| `archive.compression` | `"deflate"` | `deflate` or `store`. |
| `archive.compression_level` | `6` | 0..9 for deflate; ignored for store. |
| `archive.max_archive_bytes` | `107374182400` | Maximum size of a single output ZIP, including manifest and ZIP metadata. |
| `archive.max_entries` | `1000000` | Maximum manifest entries, including root, directories, and recorded skipped links; memory allowance can impose a lower limit. |
| `archive.max_depth` | `128` | Maximum directory nesting below the source root; 1..256. |
| `storage.max_staging_bytes` | `536870912000` | Maximum selected/staged payload bytes; inspected before copying and monitored during rsync. |
| `storage.min_free_bytes` | `10737418240` | Destination-filesystem reserve; `0` disables the reserve. |
| `retention.min_snapshots` | `2` | Protected minimum per policy cohort, at least one. |
| `retention.max_snapshots` | `30` | Maximum count per cohort, no smaller than the minimum. |
| `retention.max_age_seconds` | `null` | Optional age since capture start; positive integer or null. |
| `retention.max_total_bytes` | `null` | Optional sum of committed archive bytes per cohort; positive integer or null. |

Syncthing files have no built-in exclusion rules. For example,
`[".stfolder", ".stversions/**"]` omits Syncthing's marker/history, while
`["cache/**", "**/*.tmp"]` omits selected temporary data. A directory match
also matches its subtree. Glob `*` does not cross `/`; `**` can.

Files must have UTF-8 names suitable for portable ZIP paths. Backslashes,
special files, and source-relative paths beyond supported limits fail the job.
Empty directories are included. ZIP64 is enabled for large files and archives.

## Reload and retention history

Reloads affect future admission. Persisted jobs capture target settings, so a
pending job is not reinterpreted using changed source/exclusion/retention rules.
Global resource reservations are applied when it is dispatched. Resource changes
are rejected while any worker is active; unchanged resources permit reload during
a backup. `state_dir` and hard memory-cap changes always require restart.

Existing archives retain immutable source/destination and retention settings.
The cohort identifier hashes target identity, source, destination, retention
policy, and captured backend choices. Cleanup evaluates each cohort against its recorded limits. Old cohorts
without an age limit can remain indefinitely after policy changes. Cohort counts
and byte usage add together; a new limit is not a cap on all historical cohorts.

Disabling/removing a target, or changing its source/destination, leaves its old
location unmanaged. It does not relocate archives. Reusing a destination under
another target/state identity is rejected. Config refresh never changes source
contents or existing archive contents.

Only one request per target can be outstanding. Due triggers while it is busy
are skipped/logged. Restart catches up at most once per overdue target. Scheduling
checkpoints survive restarts. Explicit reload reschedules a target whose interval
or calendar schedule changed. Immediate `trigger` requests preserve these deadlines.

## Calendar schedules and hooks

For a weekly backup at 03:00 local time:

```json
"schedule": {
  "frequency": "weekly",
  "time": "03:00",
  "timezone": "America/Los_Angeles",
  "weekday": "mon"
}
```

For `daily`, omit `weekday` or set it to null. Timezones use IANA names.
Ambiguous DST times use the first occurrence; nonexistent local times are skipped.
Interval scheduling remains available by omitting `schedule`.

Each hook has a `name`, a `command` argument array beginning with an absolute
executable path, optional `environment`, `timeout_seconds` (default 60), and
`on_error` (default `fail_job`). Before/capture errors may use `skip_backup`,
`retry_backup`, `fail_job`, or `continue`. After-backup errors may fail or continue;
finally requires `fail_job`. The [hook reference](hooks.md) describes durable
cleanup and Minecraft save acknowledgement handling.

Minimum count overrides count/age/byte limits. Limits apply at sweep time and may
temporarily be exceeded. Low free space does not authorize emergency deletion of
protected backups. Separate targets sharing a filesystem serialize writes through
a filesystem lock within the state directory.

## Backend choices and temporary space

The optional `backends` object defaults to:

```json
{
  "scripts": "local_process",
  "source": "live_directory",
  "sync": "rsync",
  "archive": "infozip",
  "storage": "local",
  "state": "sqlite",
  "scheduler": "interval",
  "queue": "bounded_fifo",
  "retention": "oldest_first"
}
```

These are the compiled implementations in 0.0.2. Adding another implementation
requires its documented interface and a factory registration, not changes to
peer modules. See [module contracts](modules.md). Jobs and snapshots record their
backend choices. Changing the state implementation requires migration/restart.
Compression supports both stored and deflated ZIPs through the Info-ZIP backend.

`rsync` receives an explicit NUL-delimited selected-file list and copies into a
private directory on the HDD. `zip` compresses that stable staging tree; `unzip`
tests it before the canonical manifest/content verification. No shell evaluates
paths or arguments. The Rust service does not implement file copying or deflation.
`max_worker_threads` counts orchestration workers; rsync may spawn its own local
sender/receiver processes, and diagnostics use a bounded reader thread.

Capacity must cover retained ZIPs, the uncompressed staging tree, the new ZIP,
and the configured reserve. A full staging copy is never placed on the source
SSD. `max_staging_bytes` is checked against selected file sizes and monitored
while rsync runs; external writers and monitoring intervals mean it is not a
filesystem quota. The output ZIP's maximum size is enforced by an OS file-size
limit. Failed/cancelled staging is cleaned only for journaled job identities.
