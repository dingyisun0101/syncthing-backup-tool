# Releases and APT repository maintenance

## 0.0.1

The initial release includes the foreground daemon, explicit configuration
reload, verified ZIP64 snapshots, durable jobs/recovery, rsync/Info-ZIP backends, replaceable module contracts, bounded workers and
archive metadata, per-snapshot retention policies, status/logging, a systemd unit,
and an amd64 Debian package. Existing archive policies survive configuration edits.

GitHub release: https://github.com/dingyisun0101/syncthing-backup-tool/releases/tag/v0.0.1

APT URL: `https://raw.githubusercontent.com/dingyisun0101/syncthing-backup-tool/apt/`

Signing-key fingerprint: **`6872B71D4A81920AF5D4B1D04D12FAACD2545826`**.

## Build the Debian package

```bash
sudo apt-get install build-essential musl-tools python3 dpkg-dev gnupg rsync zip unzip util-linux
rustup target add x86_64-unknown-linux-musl
scripts/build-deb.sh
dpkg-deb --info dist/syncthing-backup-tool_0.0.1_amd64.deb
```

The script builds locked dependencies, uses a static musl binary, installs
documentation/config examples, marks the default JSON as a conffile, and supplies
maintainer scripts. For another architecture, provide a suitable Rust target
and C cross compiler, then set `BACKUP_RUST_TARGET` and `DEB_ARCH` together.

## Publish an update

Keep the signing private key outside the checkout. The initial publisher stores
its GnuPG home at `~/.local/share/syncthing-backup-tool/apt-signing`, mode 0700.
Only the exported public key belongs in Git. Back up the private key securely;
future updates need the same key or a documented key transition.

After versioning, testing, and building the new package:

```bash
git worktree add ../syncthing-backup-apt apt
scripts/build-apt.py dist/syncthing-backup-tool_VERSION_amd64.deb \
  ../syncthing-backup-apt \
  --gpg-home "$HOME/.local/share/syncthing-backup-tool/apt-signing" \
  --signing-key YOUR_FINGERPRINT
git -C ../syncthing-backup-apt add pool Packages Packages.gz Release InRelease Release.gpg \
  syncthing-backup-tool-archive-keyring.gpg syncthing-backup-tool-archive-keyring.asc
git -C ../syncthing-backup-apt commit -m "Publish APT package VERSION"
git -C ../syncthing-backup-apt push origin apt
```

For the first release, create an orphan `apt` branch containing only public
repository artifacts. The signed flat layout has `Packages`, `Packages.gz`,
`Release`, `InRelease`, `Release.gpg`, public keys, and `pool/*.deb`. The script
retains older packages and refuses to replace an existing filename with different
bytes. Signed `Release` hashes authenticate the indexes, whose hashes authenticate
package contents. Test `apt-get update` and downloading the pinned version from
the published URL before declaring a release available.

Tag the tested source and upload the package/checksums/public key to its GitHub
release. GitHub hosts package artifacts; this release is not a crates.io upload.
The CI workflow validates Rust and builds a Debian artifact, but does not receive
or use the private signing key. Publication is an explicit maintainer action.
