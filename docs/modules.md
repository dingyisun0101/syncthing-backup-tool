# Module interfaces

`src/api.rs` is the public contract between replaceable modules. Interface version
is `INTERFACE_VERSION = 2`; persisted JSON/ZIP manifests separately carry their
format versions. Common records live in `domain`; settings live in `config`.
Backends do not expose SQLite connections, child-process handles, or lock internals.

`snapshot` coordinates one backup through these contracts. `daemon` owns lifetime,
threading, admission, timers, and control requests. Concrete implementations are
selected only by `backends::Modules` and `state::open`, not by peer modules.

| Contract | Initial implementation | Responsibility |
| --- | --- | --- |
| `SourceProvider` / `SourceSession` | `source::LiveDirectory` | Open a stable rooted view, inspect selected metadata, report consistency, check source identity, and reread fingerprints. |
| `Synchronizer` | `source::Rsync` | Copy the explicit file list into a private staging tree using rsync. |
| `Archiver` | `archive::InfoZip` | Create a ZIP from a staging tree and test its structure with zip/unzip. |
| `StorageProvider` / `DestinationSession` | `storage::LocalStorage` | Return a locked destination session, check mounts/capacity, supply a pinned workspace, publish, synchronize, and remove owned files. |
| `StateStore` | `state::State` (SQLite) | Persist schedules, unique outstanding jobs, publication/deletion intentions, catalog health, and operational status through transactions. |
| `SchedulingPolicy` | `scheduler::Interval` | Calculate the next deadline from explicit previous/current timestamps. |
| `QueuePolicy` | `queue::BoundedFifo` | Decide admission and retry deadlines without doing filesystem or database I/O. |
| `RetentionPolicy` | `retention::OldestFirst` | Produce an oldest-first deletion plan subject to recorded limits and the minimum. |
| `ScriptRunner` | `hooks::LocalProcess` | Run a bounded hook and report success, failure, timeout, or cancellation. |
| `EventSink` | `telemetry` JSON Lines writer | Persist timestamped operation events with bounded log rotation. |

`config::load(Path) -> Result<Config>` is the configuration-reader API.
`resources::MemoryBudget` exposes bounded `entry` reservations and a manifest
allowance. `telemetry::configure` and `telemetry::event` are the logging API.
These deterministic/support modules expose typed functions instead of requiring
an instance per call; their internals can change while those public signatures
stay stable. `process::tool` / `process::run` supervise external tools and are
shared by CLI backends. `main` is the CLI adapter, not a backend.

## Request and ownership rules

Copy, pack, and test requests contain paths, explicit limits, a cancellation
flag, and a monitoring callback. Calls are synchronous. The coordinator owns
worker slots and keeps source/destination sessions alive for the entire call.
Backends must return only after their work/subprocesses stop and must not retain
borrowed request fields. Implementations are `Send + Sync`; sessions are `Send`.

The copy request's file list is NUL-delimited and contains only selected relative
file/directory paths. It deliberately excludes `.` and skipped symlinks. A
copy implementation must not recursively discover extra files, follow source
symlinks, modify/delete source files, or touch completed archives. Directory
metadata and file contents are checked against the source/session contract.

An archiver receives the stable private tree with `data/` and
`meta/manifest.json`. It writes only the temporary output and honors its output
limit. ZIP is the interoperability contract in version 1. A different ZIP backend
can replace Info-ZIP; another archive format requires an explicit format/API
change. Canonical ZIP/manifest/SHA-256 verification in `archive` runs in addition
to a backend's `test`, so a backend cannot bypass content verification.

The initial destination-session interface is for filesystem-backed storage:
tool-readable workspace paths and seekable archive `File` handles are part of
the contract. A future object-storage implementation would need a local workspace
and durable upload publication, or a deliberate interface extension. Operation
guards release filesystem reservations on drop. No caller accesses raw directory
locks through the session interface.

State mutations must be durable before returning. Enqueue must enforce one
outstanding request per target even under concurrent callers; commitment must
atomically update the snapshot catalog and mark its job for post-processing.
Terminal completion follows required post-backup/finally hooks. Deletion intention
and deletion completion are separate operations. `catalog` returns
`(snapshot, healthy, deleting)` records; only healthy survivors protect retention.

Policy methods receive records/settings and return decisions. They must not
perform deletion or publication. Retention planning must honor the configured
minimum, and the coordinator rechecks health before executing plans. Normal
errors use `anyhow::Result`; unsupported inputs use the shared `domain::Permanent`
error to suppress futile retries. Other errors are eligible for bounded retry.

## Replacing or adding an implementation

1. Implement the corresponding trait in its module or a new backend module,
   using only the shared request/result types.
2. Register its name in `Modules::from_choices` (or the state factory). Unknown
   config names must fail validation, never silently select another backend.
3. Add contract tests for cancellation, failure, ownership, limits, and the
   relevant durable boundary. Test it through the coordinator's public interface.
4. Select it in `config.json` and apply an explicit reload. State backend changes
   need migration/restart. Pending jobs and existing snapshots retain their
   recorded backend choices and policies.

For direct library use, construct `Modules` with your own `Arc<dyn ...>`
implementations and pass it to `snapshot::create_with`. The integration test
`coordinator_accepts_independently_supplied_copy_and_archive_modules` replaces
both copy and archive modules without editing the coordinator, and verifies the
resulting committed archive. This is a Rust interface; version 0.0.2 does not
load arbitrary shared-library plugins at runtime.

Version 2 adds `ScriptRunner`, `HookRequest`/`HookResult`, `EventSink`/`OperationEvent`, source symlink inspection, durable cleanup/job status, and target-aware scheduling. Existing ZIP manifests remain readable.
