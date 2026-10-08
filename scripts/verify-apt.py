#!/usr/bin/env python3
"""Verify signed APT metadata and download a version without changing system APT."""
import argparse
import getpass
import hashlib
import pathlib
import subprocess
import tempfile
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo-url", default="https://raw.githubusercontent.com/dingyisun0101/syncthing-backup-tool/apt/")
parser.add_argument("--version", default="0.0.3")
parser.add_argument("--fingerprint", required=True)
parser.add_argument("--expected-package", type=pathlib.Path)
args = parser.parse_args()
with tempfile.TemporaryDirectory(prefix="sbt-apt-verify.") as scratch:
    root = pathlib.Path(scratch)
    for name in ["lists/partial", "archives/partial", "downloads", "gpg"]:
        (root / name).mkdir(parents=True)
    (root / "gpg").chmod(0o700)
    key = root / "archive-keyring.gpg"
    with urllib.request.urlopen(args.repo_url.rstrip("/") + "/syncthing-backup-tool-archive-keyring.gpg", timeout=30) as response:
        key.write_bytes(response.read())
    listing = subprocess.run(["gpg", "--homedir", str(root / "gpg"), "--batch", "--with-colons", "--show-keys", str(key)],
                             check=True, capture_output=True, text=True).stdout
    fingerprint = next(line.split(":")[9] for line in listing.splitlines() if line.startswith("fpr:"))
    if fingerprint != args.fingerprint.replace(" ", "").upper():
        raise SystemExit("APT public key does not match the expected fingerprint")
    sources = root / "sources.list"
    sources.write_text(f"deb [arch=amd64 signed-by={key}] {args.repo_url} ./\n")
    options = {
        "Dir::Etc::sourcelist": str(sources), "Dir::Etc::sourceparts": "-",
        "Dir::Etc::parts": "-", "Dir::Etc::main": "-",
        "Dir::State::lists": str(root / "lists"), "Dir::Cache::archives": str(root / "archives"),
        "Dir::Cache::pkgcache": str(root / "pkgcache.bin"), "Dir::Cache::srcpkgcache": str(root / "srcpkgcache.bin"),
        "APT::Sandbox::User": getpass.getuser(), "Acquire::Retries": "2",
    }
    command = ["apt-get"]
    for option, value in options.items():
        command += ["-o", f"{option}={value}"]
    subprocess.run(command + ["update"], check=True)
    subprocess.run(command + ["download", "syncthing-backup-tool=" + args.version], cwd=root / "downloads", check=True)
    package = next((root / "downloads").glob("*.deb"))
    if args.expected_package and hashlib.sha256(package.read_bytes()).digest() != hashlib.sha256(args.expected_package.read_bytes()).digest():
        raise SystemExit("Published package differs from the expected build")
    subprocess.run(["dpkg-deb", "--info", str(package)], check=True)
    print("PASS: live signed APT update, pinned package download, and expected package hash")
