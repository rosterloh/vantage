# systemd unit — vantage-coordinator

```bash
# 1. Build and install the binary
cargo build --release -p vantage-coordinator
sudo install -Dm755 target/release/vantage-coordinator /usr/bin/vantage-coordinator

# 2. Create a dedicated unprivileged user
sudo useradd --system --no-create-home --shell /usr/sbin/nologin vantage

# 3. Env file (VANTAGE_COORDINATOR_TOKEN, VANTAGE_TURN_*, VANTAGE_BIND — see
#    the repo's .env.example). Not committed; owned by root, unreadable by others.
sudo install -Dm600 -o root -g root /dev/null /etc/vantage/coordinator.env
sudo editor /etc/vantage/coordinator.env

# 4. Install and start the unit
sudo install -Dm644 deploy/systemd/vantage-coordinator.service \
  /etc/systemd/system/vantage-coordinator.service
sudo systemctl daemon-reload
sudo systemctl enable --now vantage-coordinator
```

`vantage-coordinator` binds `0.0.0.0:8080` by default (unprivileged port, no
extra capabilities needed). Set `VANTAGE_BIND` in the env file to change it.

## .deb package

`cargo-deb` (`cargo install cargo-deb`) builds a package that installs the
binary, this systemd unit, and a `vantage` system user/env file automatically —
see `[package.metadata.deb]` in `vantage-coordinator/Cargo.toml`:

```bash
cargo deb -p vantage-coordinator
sudo dpkg -i target/debian/vantage-coordinator_*.deb
```

The package's `postinst` creates the `vantage` system user, seeds
`/etc/vantage/coordinator.env` from the example template on first install
(mode `0640`, group `vantage`) if it doesn't already exist, and reminds you to
edit it before starting the service — the unit is installed but **not**
enabled/started automatically, since `VANTAGE_COORDINATOR_TOKEN` still needs a
real value:

```bash
sudo editor /etc/vantage/coordinator.env
sudo systemctl enable --now vantage-coordinator
```

Upgrades reuse the existing env file and `try-restart` the service if it was
already running; `apt remove` stops it but leaves `/etc/vantage/coordinator.env`
in place, `apt purge` removes it.
