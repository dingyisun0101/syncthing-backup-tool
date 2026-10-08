#!/usr/bin/env python3
"""Build a signed flat APT repository; never print or copy the private key."""
import argparse
import datetime
import email.utils
import gzip
import hashlib
import pathlib
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("package", type=pathlib.Path)
parser.add_argument("output", type=pathlib.Path)
parser.add_argument("--gpg-home", type=pathlib.Path, required=True)
parser.add_argument("--signing-key", required=True)
args = parser.parse_args()
root = args.output.resolve()
(root / "pool").mkdir(parents=True, exist_ok=True)
destination = root / "pool" / args.package.name
if destination.exists() and destination.read_bytes() != args.package.read_bytes():
    raise SystemExit("Refusing to replace an existing package version with different bytes")
if args.package.resolve() != destination:
    shutil.copyfile(args.package, destination)
packages = subprocess.run(["dpkg-scanpackages", "--multiversion", "pool", "/dev/null"],
                          cwd=root, check=True, capture_output=True).stdout
(root / "Packages").write_bytes(packages)
(root / "Packages.gz").write_bytes(gzip.compress(packages, mtime=0))
architectures = sorted({line.split(": ", 1)[1] for line in packages.decode().splitlines() if line.startswith("Architecture: ")})
release = ["Origin: syncthing-backup-tool", "Label: syncthing-backup-tool",
           "Suite: stable", "Codename: stable", "Architectures: " + " ".join(architectures),
           "Date: " + email.utils.format_datetime(datetime.datetime.now(datetime.timezone.utc), usegmt=True),
           "Description: Signed directory snapshot packages"]
for label, algorithm in [("MD5Sum", "md5"), ("SHA256", "sha256"), ("SHA512", "sha512")]:
    release.append(label + ":")
    for filename in ["Packages", "Packages.gz"]:
        content = (root / filename).read_bytes()
        release.append(f" {hashlib.new(algorithm, content).hexdigest()} {len(content)} {filename}")
(root / "Release").write_text("\n".join(release) + "\n")
gpg = ["gpg", "--homedir", str(args.gpg_home.resolve()), "--batch", "--yes", "--local-user", args.signing_key]
subprocess.run(gpg + ["--digest-algo", "SHA256", "--clearsign", "--output", str(root / "InRelease"), str(root / "Release")], check=True)
subprocess.run(gpg + ["--digest-algo", "SHA256", "--armor", "--detach-sign", "--output", str(root / "Release.gpg"), str(root / "Release")], check=True)
with (root / "syncthing-backup-tool-archive-keyring.gpg").open("wb") as output:
    subprocess.run(gpg + ["--export", args.signing_key], stdout=output, check=True)
with (root / "syncthing-backup-tool-archive-keyring.asc").open("wb") as output:
    subprocess.run(gpg + ["--armor", "--export", args.signing_key], stdout=output, check=True)
print(f"Signed APT repository: {root}")
