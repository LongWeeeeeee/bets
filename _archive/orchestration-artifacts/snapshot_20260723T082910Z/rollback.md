# Rollback Procedure: cyberscore.service

## Snapshot reference

- Snapshot dir: `/root/main/artifacts/snapshot_20260723T082910Z/`
- Unit checksum (pre-change): `3d3ad01eb83b606f08f35fba69d59f68874baa144c4e8cbdf8832f623884d385`
- Pre-change PID: 3380020
- Pre-change ExecStart: `...cyberscore_try.py --dltv-source sourcetv --no-odds`

## One-command rollback (restore --no-odds state)

If the change was applied via drop-in `odds.conf` (renamed from `odds.conf.disabled`):

```bash
mv /etc/systemd/system/cyberscore.service.d/odds.conf /etc/systemd/system/cyberscore.service.d/odds.conf.disabled && systemctl daemon-reload && systemctl restart cyberscore.service
```

If the main unit file was edited directly (full restore from snapshot):

```bash
cp /root/main/artifacts/snapshot_20260723T082910Z/cyberscore.service /etc/systemd/system/cyberscore.service && systemctl daemon-reload && systemctl restart cyberscore.service
```

## Post-rollback verification

```bash
systemctl show cyberscore.service --property=ActiveState,SubState,MainPID,NRestarts,ExecStart
# Expect: ActiveState=active, SubState=running, ExecStart contains --no-odds

sha256sum /etc/systemd/system/cyberscore.service
# Expect: 3d3ad01eb83b606f08f35fba69d59f68874baa144c4e8cbdf8832f623884d385
```

## Notes

- The `odds.conf.disabled` drop-in was already present on the system before this plan.
  Enabling it (rename to `odds.conf`) is the preferred minimal mutation for M5.
- Log offset for the pre-change run: byte 11018361 / line 154470.
  Post-rollback errors should be attributed only to the new run segment.
- Drop-in files (oom, stats, topup) are NOT modified by this plan.
