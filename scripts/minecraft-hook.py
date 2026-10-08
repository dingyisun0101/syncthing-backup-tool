#!/usr/bin/env python3
"""Prepare/resume a Minecraft save window using the existing mcrcon client.

Credentials are read from server.properties and passed through the environment,
never command-line arguments or logs. A durable lease repairs interrupted saves.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import uuid


def event(operation, outcome, **details):
    print(json.dumps({"timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      "operation": operation, "outcome": outcome, "details": details}), flush=True)


def write_lease(path, value):
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def command(args, properties, text):
    environment = os.environ.copy()
    environment["MCRCON_PASS"] = properties["rcon.password"]
    result = subprocess.run([args.mcrcon, "-H", "127.0.0.1", "-P", properties.get("rcon.port", "25575"),
                             "-c", text], env=environment, stdin=subprocess.DEVNULL,
                            capture_output=True, text=True, timeout=args.command_timeout)
    if result.returncode:
        raise RuntimeError(f"RCON command {text!r} failed (exit {result.returncode})")
    return result.stdout.strip()


def run(args):
    job = os.environ.get("BACKUP_JOB_ID", "")
    uuid.UUID(job)
    server = args.server_dir.resolve()
    key = hashlib.sha256(str(server).encode()).hexdigest()[:24]
    args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    lease_path = args.state_dir / (key + ".json")
    with (args.state_dir / (key + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        lease = json.loads(lease_path.read_text()) if lease_path.exists() else None
        if args.phase == "resume" and lease is None:
            event("minecraft.resume", "succeeded", action="no pending lease")
            return
        if lease and lease["job_id"] != job:
            raise RuntimeError("another backup owns this server's save window")
        properties = {}
        for line in (server / "server.properties").read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                name, value = line.split("=", 1)
                properties[name.strip()] = value
        if properties.get("enable-rcon") != "true" or not properties.get("rcon.password"):
            raise RuntimeError("server.properties must enable RCON and provide its password")
        if not args.mcrcon:
            raise RuntimeError("mcrcon is not installed")
        if args.phase == "prepare":
            # Record recovery obligation before making any application-state change.
            new_lease = lease is None
            if new_lease:
                lease = {"job_id": job, "resume": True, "server": str(server)}
                write_lease(lease_path, lease)
            reply = command(args, properties, "save-off").lower()
            if "already" in reply and ("off" in reply or "disabled" in reply):
                if new_lease:
                    lease["resume"] = False
                    write_lease(lease_path, lease)
            elif "disabled" not in reply:
                raise RuntimeError("server did not acknowledge save-off")
            event("minecraft.save_off", "succeeded")
            save = "__backup_save_failure_test__" if args.simulate_save_failure else "save-all flush"
            reply = command(args, properties, save)
            if "saved the game" not in reply.lower():
                raise RuntimeError("server did not acknowledge a completed save-all flush")
            event("minecraft.save_all", "succeeded")
        else:
            if lease["resume"]:
                reply = command(args, properties, "save-on").lower()
                if "enabled" not in reply and not ("already" in reply and "on" in reply):
                    raise RuntimeError("server did not acknowledge save-on")
            lease_path.unlink()
            sync_directory(args.state_dir)
            event("minecraft.resume", "succeeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "resume"])
    parser.add_argument("--server-dir", type=pathlib.Path, required=True)
    parser.add_argument("--state-dir", type=pathlib.Path, required=True)
    parser.add_argument("--mcrcon", default=shutil.which("mcrcon"))
    parser.add_argument("--command-timeout", type=int, default=20)
    parser.add_argument("--simulate-save-failure", action="store_true", help="Exercise semantic failure and cleanup without writing a backup")
    args = parser.parse_args()
    try:
        run(args)
    except Exception as error:
        event("minecraft.hook", "failed", error=str(error))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
