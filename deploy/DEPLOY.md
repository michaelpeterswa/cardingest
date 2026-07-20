# Deploying cardingest on the appliance

This directory has the systemd units that run cardingest on boot with the NAS
mounted **first**. Read the safety note before wiring it up — the ordering isn't
cosmetic.

## Why ordering matters (the trap)

The container bind-mounts the host's NAS directory (`/mnt/nas/ingest`) into the
container as `/data/dest`. A Linux bind mount is a **point-in-time snapshot** of
whatever is at that path when the container starts:

- If the container starts **before** the NFS share is mounted, the bind captures
  the *empty local mountpoint*. Ingests then write to the host's local disk.
- cardingest's read-back verification reads from that **same** local dir, so it
  still passes — and cards get **erased** with the only copy on local disk.

So "start the container on boot" has a hard precondition: **the NAS share must be
mounted first**, and the container must be (re)created after that.

Two independent mechanisms enforce this:

1. **systemd ordering** — `cardingest.service` `Requires=` + `After=` the NAS
   mount unit, and uses `docker compose up -d --force-recreate` so the container
   is (re)created *after* the mount is active (defeating Docker's own restart
   policy racing the mount at daemon boot).
2. **In-app marker guard** — cardingest refuses to ingest unless a sentinel file
   (`destination.marker`, default `.cardingest-ok`) exists on the destination.
   That file lives **on the NAS share**, so it's only visible when the share is
   truly mounted. If the guard sees it missing, the job hard-fails and the card
   is left untouched. This makes the data-loss scenario impossible even if the
   ordering ever slips.

## Prerequisites

- A Linux host with Docker Engine + the `docker compose` plugin, and `systemd`.
- The `nfs-common` package (`apt install nfs-common`) for the NFS mount.
- The repo checked out at `/opt/cardingest` (adjust paths below if different),
  with `config/` and `state/` directories present.

## 1. Mount the NAS share

Edit [`mnt-nas-ingest.mount`](mnt-nas-ingest.mount) — set `What=` to your export
and `Where=` to the mountpoint. **If you change the mountpoint, rename the file**
to match (`systemd-escape -p --suffix=mount /your/path`).

```bash
sudo mkdir -p /mnt/nas/ingest
sudo cp deploy/mnt-nas-ingest.mount /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mnt-nas-ingest.mount
systemctl status mnt-nas-ingest.mount        # should be "active (mounted)"
mountpoint /mnt/nas/ingest                    # should say "is a mountpoint"
```

> **Multiple destination shares.** cardingest can route categories to different
> shares (e.g. RAW → a Lightroom share, JPEG → another) via `categories.*.dest`
> in the config. Each share is its **own** host mount, its own `.mount` unit,
> its own marker file, and its own bind in `docker-compose.appliance.yml`. Repeat
> steps 1–2 for every share, add it to `Requires=`/`After=` in
> `cardingest.service`, and add the bind mount.

## 2. Arm the safety marker (once per share)

Create the sentinel **on each share** so it only exists when that share is
mounted:

```bash
sudo touch /mnt/nas/ingest/.cardingest-ok
sudo touch /mnt/nas/lightroom/.cardingest-ok   # if you route RAW to a 2nd share
```

(You can also create it from the NAS's own file manager. If you rename it, update
`destination.marker` in `config/config.yaml`.)

## 3. Configure

- `config/config.yaml` — set `reader.usb_ids` to your reader's real IDs (see the
  "Validating the reader" section of the top-level README), the `destination`,
  categories, rules, and notifier. `destination.path` inside the container is
  `/data/dest` (already bind-mounted from `/mnt/nas/ingest`).
- Optional notifier token — create `/etc/cardingest/cardingest.env`:

  ```
  NOTIFY_PULSAR_TOKEN=your-writer-bearer-token
  ```

## 4. Install and enable the service

```bash
sudo cp deploy/cardingest.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cardingest.service
```

On every boot: network → NFS mount → docker → `cardingest` container, in that
order.

## 5. Verify

```bash
systemctl status cardingest.service          # active (exited), RemainAfterExit
docker compose -f /opt/cardingest/docker-compose.appliance.yml ps
curl -s localhost:8080/api/v1/healthz         # {"status":"ok"}
```

Open the UI at `http://<host>:8080/`, insert a card, and watch the live progress
and job history. A completed job with files landing under
`/mnt/nas/ingest/{category}/{date}/` confirms the whole chain.

## Operating notes

- **Logs**: `journalctl -u cardingest.service` for start/stop; `docker compose -f
  /opt/cardingest/docker-compose.appliance.yml logs -f` for the app's JSON logs.
- **Update**: `git -C /opt/cardingest pull && sudo systemctl restart
  cardingest.service` (the service re-pulls the image and recreates the
  container).
- **NAS outage**: the `hard` NFS mount blocks I/O until the NAS returns rather
  than risk a partial write; the marker guard also fails jobs fast if the share
  disappears. Either way, no card is erased against a bad destination — re-insert
  the card once the NAS is back.
- **Reboot test** (recommended once): `sudo reboot`, then confirm the mount is
  active and the container came up, before trusting it with real cards.
