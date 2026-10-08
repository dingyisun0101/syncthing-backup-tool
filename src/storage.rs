use crate::config::{Target, resolved};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use rustix::fs::{Mode, OFlags, ResolveFlags};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

pub fn proc_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

pub fn open_beneath(root: &File, path: &Path, flags: OFlags) -> Result<File> {
    Ok(File::from(
        rustix::fs::openat2(
            root,
            path,
            flags | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS,
        )
        .with_context(|| format!("open without symlinks: {}", path.display()))?,
    ))
}

pub fn mount_present(mount: &Path) -> Result<()> {
    let mount = resolved(mount)?;
    let text = fs::read_to_string("/proc/self/mountinfo")?;
    // mountinfo encodes spaces, backslashes, tabs, and newlines as octal escapes.
    let found = text.lines().any(|line| {
        line.split_whitespace().nth(4).is_some_and(|p| {
            let decoded = p
                .replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\012", "\n")
                .replace("\\134", "\\");
            Path::new(&decoded) == mount
        })
    });
    ensure!(found, "required mount is unavailable: {}", mount.display());
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Owner {
    version: u32,
    target_id: String,
    device: u64,
    state_directory: PathBuf,
}

pub struct Destination {
    pub root: File,
    pub partial: File,
    _lock: File,
    pub target: Target,
}

impl Destination {
    pub fn open(target: &Target, initialize: bool, state_dir: &Path) -> Result<Self> {
        if let Some(mount) = &target.required_destination_mount {
            mount_present(mount)?;
        }
        if initialize {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&target.destination_dir)?;
        }
        let root = File::open(&target.destination_dir).context("open destination")?;
        ensure!(root.metadata()?.is_dir(), "destination is not a directory");
        // Lock the pinned directory inode, without creating anything in an unowned folder.
        root.try_lock_exclusive()
            .context("destination is busy or owned by another instance")?;
        let marker_path = proc_path(&root).join(".snapshot-owner.json");
        let device = root.metadata()?.dev();
        let owner = if marker_path.exists() {
            let mut marker =
                open_beneath(&root, Path::new(".snapshot-owner.json"), OFlags::RDONLY)?;
            let mut bytes = Vec::new();
            Read::by_ref(&mut marker)
                .take(4097)
                .read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 4096, "invalid ownership marker");
            serde_json::from_slice::<Owner>(&bytes)?
        } else {
            ensure!(initialize, "destination has no ownership marker");
            ensure!(
                fs::read_dir(proc_path(&root))?.next().is_none(),
                "refusing to adopt a nonempty destination without ownership marker"
            );
            let owner = Owner {
                version: 1,
                target_id: target.id.clone(),
                device,
                state_directory: resolved(state_dir)?,
            };
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&marker_path)?;
            serde_json::to_writer(&mut file, &owner)?;
            file.sync_all()?;
            root.sync_all()?;
            owner
        };
        ensure!(
            owner.version == 1
                && owner.target_id == target.id
                && owner.device == device
                && owner.state_directory == resolved(state_dir)?,
            "destination ownership or device identity mismatch"
        );
        if initialize {
            match fs::DirBuilder::new()
                .mode(0o700)
                .create(proc_path(&root).join(".partial"))
            {
                Ok(()) => root.sync_all()?,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
                Err(e) => return Err(e.into()),
            }
        }
        let partial = open_beneath(
            &root,
            Path::new(".partial"),
            OFlags::RDONLY | OFlags::DIRECTORY,
        )?;
        Ok(Self {
            _lock: root.try_clone()?,
            root,
            partial,
            target: target.clone(),
        })
    }

    pub fn check(&self) -> Result<()> {
        if let Some(mount) = &self.target.required_destination_mount {
            mount_present(mount)?;
        }
        let visible = fs::metadata(&self.target.destination_dir)?;
        let pinned = self.root.metadata()?;
        ensure!(
            visible.dev() == pinned.dev() && visible.ino() == pinned.ino(),
            "destination changed during operation"
        );
        Ok(())
    }

    pub fn free_bytes(&self) -> Result<u64> {
        let stat = rustix::fs::fstatvfs(&self.root)?;
        Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
    }

    pub fn check_space(&self, additional: u64) -> Result<()> {
        self.check()?;
        ensure!(
            self.free_bytes()?
                >= self
                    .target
                    .storage
                    .min_free_bytes
                    .saturating_add(additional),
            "destination free-space reserve would be exceeded"
        );
        Ok(())
    }

    pub fn temporary(&self, id: &str) -> Result<File> {
        ensure!(uuid::Uuid::parse_str(id).is_ok(), "invalid job identifier");
        Ok(File::from(rustix::fs::openat(
            &self.partial,
            format!("{id}.zip.part"),
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?))
    }

    pub fn publish(&self, id: &str, name: &str) -> Result<()> {
        ensure!(uuid::Uuid::parse_str(id).is_ok(), "invalid job identifier");
        self.check()?;
        safe_name(name)?;
        rustix::fs::renameat_with(
            &self.partial,
            format!("{id}.zip.part"),
            &self.root,
            name,
            rustix::fs::RenameFlags::NOREPLACE,
        )?;
        self.partial.sync_all()?;
        self.root.sync_all()?;
        Ok(())
    }

    pub fn remove_temporary(&self, id: &str) -> Result<()> {
        ensure!(uuid::Uuid::parse_str(id).is_ok(), "invalid job identifier");
        self.check()?;
        let result = rustix::fs::unlinkat(
            &self.partial,
            format!("{id}.zip.part"),
            rustix::fs::AtFlags::empty(),
        );
        if let Err(error) = result
            && error != rustix::io::Errno::NOENT
        {
            return Err(error.into());
        }
        self.partial.sync_all()?;
        Ok(())
    }
    pub fn remove_staging(&self, id: &str) -> Result<()> {
        ensure!(uuid::Uuid::parse_str(id).is_ok(), "invalid job identifier");
        use std::os::unix::fs::PermissionsExt;
        self.check()?;
        let path = proc_path(&self.partial).join(format!("{id}.tree"));
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_dir() && !metadata.is_symlink(),
                    "staging root is not an owned directory"
                );
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
                for entry in walkdir::WalkDir::new(&path).follow_links(false) {
                    let entry = entry?;
                    if entry.file_type().is_dir() {
                        fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o700))?;
                    }
                }
                fs::remove_dir_all(&path)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        match fs::remove_file(proc_path(&self.partial).join(format!("{id}.files"))) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        self.partial.sync_all()?;
        Ok(())
    }

    pub fn archive(&self, name: &str) -> Result<File> {
        safe_name(name)?;
        let file = open_beneath(&self.root, Path::new(name), OFlags::RDONLY)?;
        ensure!(file.metadata()?.is_file(), "archive is not a regular file");
        Ok(file)
    }

    pub fn remove(&self, name: &str) -> Result<()> {
        self.check()?;
        safe_name(name)?;
        rustix::fs::unlinkat(&self.root, name, rustix::fs::AtFlags::empty())?;
        self.root.sync_all()?;
        Ok(())
    }
}

fn safe_name(name: &str) -> Result<()> {
    ensure!(
        Path::new(name).components().count() == 1
            && !name.starts_with('.')
            && name.ends_with(".zip"),
        "unsafe archive filename"
    );
    Ok(())
}

/// The target directory lock serializes reservations across jobs on one filesystem.
/// A second filesystem-level lock coordinates different target directories.
pub fn filesystem_lock(destination: &Destination, state_dir: &Path) -> Result<File> {
    let device = destination.root.metadata()?.dev();
    let path = state_dir.join(format!("filesystem-{device}.lock"));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.lock_exclusive()?;
    Ok(file)
}

impl Drop for Destination {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.root);
    }
}
pub struct LocalStorage;
impl crate::api::StorageProvider for LocalStorage {
    fn open(
        &self,
        target: &Target,
        initialize: bool,
        state_dir: &Path,
    ) -> Result<Box<dyn crate::api::DestinationSession>> {
        Ok(Box::new(Destination::open(target, initialize, state_dir)?))
    }
}
impl crate::api::DestinationSession for Destination {
    fn working_directory(&self) -> PathBuf {
        PathBuf::from(format!(
            "/proc/{}/fd/{}",
            std::process::id(),
            self.partial.as_raw_fd()
        ))
    }
    fn check(&self) -> Result<()> {
        Destination::check(self)
    }
    fn check_space(&self, additional: u64) -> Result<()> {
        Destination::check_space(self, additional)
    }
    fn serialize_writes(&self, state_dir: &Path) -> Result<Box<dyn crate::api::OperationGuard>> {
        Ok(Box::new(OwnedLock(filesystem_lock(self, state_dir)?)))
    }
    fn archive(&self, name: &str) -> Result<File> {
        Destination::archive(self, name)
    }
    fn exists(&self, name: &str) -> Result<bool> {
        safe_name(name)?;
        match fs::symlink_metadata(proc_path(&self.root).join(name)) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
    fn synchronize(&self) -> Result<()> {
        self.root.sync_all()?;
        self.partial.sync_all()?;
        Ok(())
    }
    fn publish(&self, id: &str, name: &str) -> Result<()> {
        Destination::publish(self, id, name)
    }
    fn remove(&self, name: &str) -> Result<()> {
        Destination::remove(self, name)
    }
    fn remove_temporary(&self, id: &str) -> Result<()> {
        Destination::remove_temporary(self, id)
    }
    fn remove_staging(&self, id: &str) -> Result<()> {
        Destination::remove_staging(self, id)
    }
}
struct OwnedLock(File);
impl Drop for OwnedLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}
