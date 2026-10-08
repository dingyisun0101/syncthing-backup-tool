# Hooks and operation auditing (0.0.2)

Per-target `hooks` contains arrays named `before_backup`, `after_capture`,
`after_backup`, and `finally`. Each entry needs `name` and `command` (an argument
array whose first item is an absolute executable path). Defaults are a 60-second
timeout and `on_error: "fail_job"`. Optional `environment` supplies variables;
credentials should be read from protected files and never printed by scripts.
The runner does not invoke a shell implicitly and runs as the service account.

Scripts receive `BACKUP_JOB_ID`, `BACKUP_TARGET_ID`, `BACKUP_SOURCE`,
`BACKUP_DESTINATION`, and `BACKUP_PHASE`. Exit zero indicates success; scripts
must verify application acknowledgements before returning zero. Timeouts and
cancellation terminate/reap the entire child process group. Output capture is
bounded and streamed into timestamped audit events.

Before/capture hook error actions are `skip_backup`, `retry_backup`, `fail_job`,
and `continue`. Skips are terminal attempts and wait for the next schedule.
Retries use the normal bounded job policy after mandatory cleanup. `continue`
is explicit and still logs the failure. After-backup hooks may fail or continue;
they cannot skip/retry a snapshot that is already committed. Their failure yields
`completed_with_hook_failure` and preserves the archive.

Finally hooks require `on_error: "fail_job"`. Every finally hook is attempted,
even after failed preparation, cancellation, or another failed cleanup hook.
Cleanup obligations are persisted before preparation begins. Failed cleanup
blocks target admission/dispatch and is recovered on startup and periodically,
without interfering with active jobs. Hooks should be idempotent using job IDs.
After-backup hooks may replay after an interrupted publication; deduplicate their
external effects by job ID.

Use `after_capture` to restore application operation once rsync staging and source
checks succeed, before ZIP compression. `finally` repeats the same restoration
idempotently on every exit path. Set `consistency: "application_quiesced"` when
properly tested hooks provide that guarantee; default remains `live`.

The Minecraft helper reads RCON credentials from the selected server.properties,
sends save-off and save-all flush, and checks command replies. It records a private
per-server recovery lease before disabling saves. Resume enables saving only for
its own lease, then removes it durably. Servers with mod-specific writers need
an application-specific consistency assessment beyond vanilla save commands.
RCON commands use loopback; the supplied unit allows loopback networking only.

`logging.audit_file` enables rotating JSON Lines with nanosecond UTC timestamps,
sequence numbers, job/target context, operations, outcomes, and details. Every
source/staging/verification entry and process/hook phase is audited. Summary
messages also go to journald. `max_file_bytes` defaults to 100 MiB, `max_files` to
10. A null audit path sends operation records to stderr. Rotation never touches
archive/source directories. Audit paths cannot overlap target data. Validation
and unit generation remain filesystem-read-only.

`trigger --target ID --wait` enqueues through the private daemon socket using its
loaded configuration, preserving regular deadlines. `job ID` reports progress and
a durable terminal outcome; the latest 1000 outcomes are retained. Required-hook
errors produce nonzero client status. `--timeout-seconds` ends waiting without
cancelling service work. Busy targets reject duplicate immediate requests.

A target may specify `schedule` with `frequency` (`daily` or `weekly`), `time`
(`HH:MM`), `timezone` (IANA name), and `weekday` (`mon`..`sun`, for weekly).
Calendar times follow local DST. Ambiguous times use the first occurrence;
nonexistent times are skipped. `run_on_startup` still chooses an immediate first
request versus the next scheduled time. Manual reload reschedules changed cadence.

For Minecraft servers that create owner-only files during saving, run the optional
`minecraft-control.py serve` agent as the Minecraft file owner. Its protected
configuration maps fixed server IDs to server/world directories; it validates
UUID jobs and permits only prepare/resume actions over a group-restricted Unix
socket. After a confirmed save it grants the backup account read access to newly
created world files. The main backup daemon remains unprivileged and uses the
agent's request mode for all hooks. Credentials stay in server.properties.
The agent's state directory and service should be separate from the daemon's,
with a systemd dependency ensuring it is available before backup requests.
