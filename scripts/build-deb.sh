#!/bin/sh
set -eu
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_dir"
backup_target=${BACKUP_RUST_TARGET:-x86_64-unknown-linux-musl}
deb_arch=${DEB_ARCH:-amd64}
package_version=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"][0]["version"])')
cargo build --release --locked --target "$backup_target"
package_root=$(mktemp -d)
trap 'rm -rf "$package_root"' EXIT HUP INT TERM
mkdir -p "$package_root/DEBIAN" "$package_root/usr/bin" "$package_root/lib/systemd/system" \
    "$package_root/etc/syncthing-backup-tool" "$package_root/usr/share/doc/syncthing-backup-tool/docs" \
    "$package_root/usr/share/doc/syncthing-backup-tool/packaging" "$package_root/usr/lib/syncthing-backup-tool"
install -m 0755 "target/$backup_target/release/syncthing-backup-tool" "$package_root/usr/bin/"
install -m 0644 packaging/syncthing-backup-tool.service "$package_root/lib/systemd/system/"
install -m 0640 packaging/config.json "$package_root/etc/syncthing-backup-tool/"
install -m 0644 README.md LICENSE packaging/config.example.json "$package_root/usr/share/doc/syncthing-backup-tool/"
install -m 0644 docs/*.md "$package_root/usr/share/doc/syncthing-backup-tool/docs/"
install -m 0644 packaging/config.example.json "$package_root/usr/share/doc/syncthing-backup-tool/packaging/"
install -m 0755 scripts/minecraft-hook.py scripts/minecraft-control.py "$package_root/usr/lib/syncthing-backup-tool/"
install -m 0755 packaging/postinst packaging/prerm packaging/postrm "$package_root/DEBIAN/"
printf '/etc/syncthing-backup-tool/config.json\n' > "$package_root/DEBIAN/conffiles"
installed_size=$(du -sk "$package_root/usr" "$package_root/lib" | awk '{s+=$1} END {print s}')
cat > "$package_root/DEBIAN/control" <<EOF
Package: syncthing-backup-tool
Version: $package_version
Section: admin
Priority: optional
Architecture: $deb_arch
Maintainer: Dingyi Sun <dingyisun0101@users.noreply.github.com>
Depends: adduser, systemd, acl, rsync, zip, unzip, util-linux, python3
Installed-Size: $installed_size
Homepage: https://github.com/dingyisun0101/syncthing-backup-tool
Description: Scheduled verified ZIP snapshots of directories
 Streams full directory backups into independent timestamped ZIP archives,
 verifies their contents, and retires old snapshots using recorded policies.
EOF
mkdir -p dist
dpkg-deb --root-owner-group --build "$package_root" "dist/syncthing-backup-tool_${package_version}_${deb_arch}.deb"
