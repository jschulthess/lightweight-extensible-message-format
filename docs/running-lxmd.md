# Running LxmdDaemon

`LxmdDaemon` is a Java port of the Python `lxmd` utility. It starts an LXMF message
router that can optionally act as a Propagation Node, receive inbound messages, and
forward them to an external program.

---

## Building

```
mvn package -DskipTests
```

This produces two JARs in `target/`:

| File | Purpose |
|------|---------|
| `lightweight-extensible-message-format-<version>.jar` | Library artifact (no dependencies) |
| `lightweight-extensible-message-format-<version>-lxmd.jar` | Self-contained executable JAR with all dependencies bundled |

---

## Running

### Option A — fat JAR (recommended for deployment)

```bash
java -jar target/lightweight-extensible-message-format-*-lxmd.jar [options]
```

This is the simplest way to run the daemon. No classpath management required.

### Option B — Maven exec (convenient during development)

```bash
mvn exec:java -Dexec.mainClass=examples.LxmdDaemon -Dexec.args="[options]"
```

No separate build step needed; Maven resolves the classpath automatically.

---

## Command-line options

| Flag | Description |
|------|-------------|
| `--config DIR` | Path to lxmd config directory (default: `~/.lxmd`) |
| `--rnsconfig DIR` | Path to Reticulum config directory (default: `~/.reticulum`) |
| `-p`, `--propagation-node` | Enable the LXMF Propagation Node |
| `-i PATH`, `--on-inbound PATH` | External program to run when a message is received |
| `-v`, `--verbose` | Increase log verbosity (repeatable) |
| `-q`, `--quiet` | Decrease log verbosity (repeatable) |
| `-s`, `--service` | Log to file instead of stdout |
| `--exampleconfig` | Print a fully-commented example config to stdout and exit |

---

## Configuration

On first run, a default `config.properties` is created in the config directory
(`~/.lxmd/config.properties`). Print a fully-commented example at any time with:

```bash
java -jar target/*-lxmd.jar --exampleconfig
```

The file uses flat `section.key = value` syntax:

```properties
# ── LXMF delivery destination ────────────────────────────────
lxmf.display_name = My Node
lxmf.announce_at_start = false
# lxmf.announce_interval = 360     # minutes; omit to disable

# Maximum message size accepted directly from peers (kilobytes)
lxmf.delivery_transfer_max_accepted_size = 1000

# Run an external program on every received message.
# The saved message path is passed as the first argument.
# lxmf.on_inbound = /usr/local/bin/my-handler

# ── Propagation Node ──────────────────────────────────────────
propagation.enable_node = false
propagation.announce_at_start = true
propagation.announce_interval = 360   # minutes
propagation.autopeer = true
propagation.autopeer_maxdepth = 6
# propagation.node_name = My Propagation Node
# propagation.message_storage_limit = 500    # megabytes
# propagation.max_peers = 20
```

### Sidecar files

Two optional plain-text files are read from the config directory:

| File | Purpose |
|------|---------|
| `ignored` | Destination hashes to silently drop (one 32-character hex hash per line) |
| `allowed` | Identity hashes allowed to sync when `propagation.auth_required = true` |

---

## Common examples

### Basic peer — receive messages, write to `~/.lxmd/storage/messages/`

```bash
java -jar target/*-lxmd.jar
```

### Peer with a custom handler for inbound messages

```bash
java -jar target/*-lxmd.jar --on-inbound /path/to/handler.sh
```

The handler receives the full path to the saved `.lxmf` message file as `$1`.

### Start a Propagation Node

```bash
java -jar target/*-lxmd.jar --propagation-node
```

Or set `propagation.enable_node = true` in the config file and run without `-p`.

### Use a non-default config directory

```bash
java -jar target/*-lxmd.jar --config /etc/lxmd --rnsconfig /etc/reticulum
```

### Increase verbosity for debugging

```bash
java -jar target/*-lxmd.jar -v -v
```

---

## Data layout

All runtime state is stored under the config directory:

```
~/.lxmd/
├── config.properties   configuration file
├── identity            Ed25519 key pair (created on first run)
├── ignored             optional: destination hashes to ignore
├── allowed             optional: identity hashes allowed to sync
└── storage/
    ├── lxmf/           LXMRouter internal state (ratchets, peers, stamps)
    └── messages/       received messages as .lxmf files
```

---

## Running as a systemd service

### System-wide Service

Create `/etc/systemd/system/lxmd.service`:

```ini
[Unit]
Description=LXMF Message Daemon
After=network.target

[Service]
ExecStart=/usr/bin/java -jar /opt/lxmd/lxmd.jar --config /etc/lxmd --service
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lxmd
sudo journalctl -u lxmd -f
```

Copy the fat JAR to `/opt/lxmd/lxmd.jar` and create `/etc/lxmd/config.properties`
(use `--exampleconfig` to generate a starting point).

### Userspace Service

Create `$HOME/.config/systemd/user/lxmd.service`:

```ini
[Unit]
Description=LXMF Message Daemon
After=network.target

[Service]
ExecStart=/usr/bin/java -jar /opt/lxmd/lxmd.jar --config /etc/lxmd --service
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:

```bash
systemctl --user daemon-reload
systemctl --user enale --now lxmd.service
```

If you want to automatically start lxmd without having to log in as USERNAMEHERE, do:

``` bash
sudo loginctl enable-linger USERNAMEHERE
systemctl --user enable lxmd.service
```

Copy the fat JAR to `/opt/lxmd/lxmd.jar` and create `/etc/lxmd/config.properties`
(use `--exampleconfig` to generate a starting point).
