# Minecraft save hooks

The packaged helpers use the existing `mcrcon` executable. Enable RCON in
`server.properties`, provide a password, and keep RCON reachable on loopback.
The helper reads the password locally and passes it through the client
environment; commands and logs do not contain the password. Set the actual
`mcrcon` path in the controller configuration.

Minecraft may replace world files with mode 0600 during `save-all flush`.
Default ACLs alone do not guarantee that the backup account can read these
files. The optional controller runs as the Minecraft owner, validates the save
acknowledgement, and grants read access to new world files while saving is paused.
The main backup daemon runs as `syncthing-backup`.

## Owner-side controller

Create `/etc/syncthing-backup-tool/minecraft-control.json`, owned by
`root:syncthing-backup`, mode 0640. Adapt these paths and the world name to your
installation; `world` must match `level-name` in server.properties:

```json
{
  "servers": {
    "main": {
      "directory": "/srv/minecraft/main",
      "world": "world",
      "mcrcon": "/usr/local/bin/mcrcon"
    }
  }
}
```

Save this unit as
`/etc/systemd/system/syncthing-backup-tool-minecraft.service` and replace
`service_mgr` with the account that owns your Minecraft files:

```ini
[Unit]
Description=Minecraft backup save controller
After=network.target

[Service]
Type=simple
User=service_mgr
Group=syncthing-backup
ExecStart=/usr/lib/syncthing-backup-tool/minecraft-control.py serve --socket /run/syncthing-backup-tool-minecraft/control.sock --config /etc/syncthing-backup-tool/minecraft-control.json --state-dir /var/lib/syncthing-backup-tool-minecraft
RuntimeDirectory=syncthing-backup-tool-minecraft
RuntimeDirectoryMode=0750
StateDirectory=syncthing-backup-tool-minecraft
StateDirectoryMode=0700
UMask=0077
Restart=on-failure
NoNewPrivileges=yes
ProtectSystem=full
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
IPAddressDeny=any
IPAddressAllow=localhost

[Install]
WantedBy=multi-user.target
```

Add `/etc/systemd/system/syncthing-backup-tool.service.d/minecraft-controller.conf`:

```ini
[Unit]
Wants=syncthing-backup-tool-minecraft.service
After=syncthing-backup-tool-minecraft.service
```

The controller accepts only configured server IDs, UUID job IDs, and
prepare/resume actions over its group-restricted Unix socket. It records private
recovery leases in its separate state directory. Initial source ACLs and parent
traversal grants from the README are still needed for non-world files.

## Target hooks

Add these fields to the Minecraft target in the main `config.json`:

```json
"consistency": "application_quiesced",
"exclude_globs": ["logs/**", "crash-reports/**", "debug/**"],
"hooks": {
  "before_backup": [{
    "name": "minecraft-save",
    "command": ["/usr/lib/syncthing-backup-tool/minecraft-control.py", "request", "--socket", "/run/syncthing-backup-tool-minecraft/control.sock", "--server", "main", "--phase", "prepare"],
    "timeout_seconds": 90,
    "on_error": "skip_backup"
  }],
  "after_capture": [{
    "name": "minecraft-resume",
    "command": ["/usr/lib/syncthing-backup-tool/minecraft-control.py", "request", "--socket", "/run/syncthing-backup-tool-minecraft/control.sock", "--server", "main", "--phase", "resume"],
    "timeout_seconds": 40,
    "on_error": "fail_job"
  }],
  "finally": [{
    "name": "minecraft-resume-finally",
    "command": ["/usr/lib/syncthing-backup-tool/minecraft-control.py", "request", "--socket", "/run/syncthing-backup-tool-minecraft/control.sock", "--server", "main", "--phase", "resume"],
    "timeout_seconds": 40,
    "on_error": "fail_job"
  }]
}
```

Preparation sends `save-off` and `save-all flush`. A successful RCON protocol
response without the completed-save acknowledgement is an error and skips the
backup. `after_capture` resumes saving before ZIP compression; mandatory
`finally` resumes it after failed preparation/capture as well. A pre-existing
external save-off state remains off. Repeated preparation preserves the job's
cleanup obligation.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now syncthing-backup-tool-minecraft
sudo syncthing-backup-tool reload
sudo syncthing-backup-tool trigger --target minecraft --wait
```

Inspect the job result, both service journals, and the operation audit log.
Failed cleanup blocks further work for that target until recovery succeeds.
Vanilla save commands do not quiesce every possible mod or external writer;
adapt and test the hook protocol for applications with additional state writers.
