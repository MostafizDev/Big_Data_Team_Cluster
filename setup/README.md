# TeamSpark Client Setup

Connect your laptop to the shared TeamSpark Spark cluster. The setup installs only the
thin **Spark Connect client** (`pyspark-client`) into a private Python virtual environment.
It does not install Spark or Java, and it does not run a cluster on your machine. All
computation runs on the TeamSpark server.

| | Windows | macOS |
|---|---|---|
| Script | `setup\setup.ps1` | `setup/setup.sh` |
| Installs into | `%USERPROFILE%\.teamspark` | `~/.teamspark` |
| Server URL saved as | `SPARK_REMOTE` user environment variable | `SPARK_REMOTE` in `~/.zshrc` (and `~/.bash_profile` if present) |

Both scripts read the same two files, so every machine gets an identical setup:

* [`teamspark-client.conf`](teamspark-client.conf) sets the Spark version (4.1.4), the Python version (3.12) and the port (15002).
* [`requirements.txt`](requirements.txt) is the exact package lock, copied from the server image.

## Before you start

1. **Install Tailscale** and sign in with the team account: https://tailscale.com/download.
   Your admin must allow you to reach the TeamSpark server on `tcp:15002`.
2. **Get the server address** from your admin. It is either:
   * the server's Tailscale (MagicDNS) name, for example `teamspark-mac`; or
   * its Tailscale IP, for example `100.101.102.103`.
3. **Python 3.12** is recommended but optional. If the script does not find it, it downloads a
   private copy with [uv](https://docs.astral.sh/uv/) into the TeamSpark folder. No admin rights are needed.

## Windows

Open **PowerShell** (not as administrator) in the folder that contains `setup\`, then run:

```powershell
powershell -ExecutionPolicy Bypass -File setup\setup.ps1 -ServerHost teamspark-mac
```

`-ExecutionPolicy Bypass` applies to this one run only. It is needed because downloaded scripts are blocked by default.

Options:

| Option | Meaning |
|---|---|
| `-ServerHost` | Server name or IP |
| `-Port` | Default `15002` |
| `-Remote sc://host:port` | Full Spark Connect URL instead of host and port |
| `-Python C:\path\python.exe` | Use this Python 3.12 |
| `-Force` | Rebuild the environment |
| `-NoPersist` | Don't set user environment variables |
| `-SkipVerify` | Install without testing the connection |

After setup, **open a new terminal** (or restart VS Code) so it picks up `SPARK_REMOTE`. Then run:

```powershell
& "$env:USERPROFILE\.teamspark\venv\Scripts\Activate.ps1"   # cmd.exe: %USERPROFILE%\.teamspark\venv\Scripts\activate.bat
python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate().range(5).count())"
```

## macOS

In Terminal, from the folder that contains `setup/`, run:

```bash
bash setup/setup.sh --host teamspark-mac
```

Options:

| Option | Meaning |
|---|---|
| `--host` | Server name or IP |
| `--port` | Default `15002` |
| `--remote sc://host:port` | Full Spark Connect URL instead of host and port |
| `--python /path/python3.12` | Use this Python 3.12 |
| `--force` | Rebuild the environment |
| `--no-persist` | Don't edit `~/.zshrc` |
| `--skip-verify` | Install without testing the connection |

After setup, open a new terminal or run `source ~/.zshrc`. Then run:

```bash
source ~/.teamspark/venv/bin/activate
python -c 'from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate().range(5).count())'
```

## Configuring the server through environment variables

Both scripts and `verify.py` take the server address from the first source that is set, in this order:

1. `--remote` / `-Remote`
2. `--host` / `-ServerHost` (plus the port)
3. `TEAMSPARK_REMOTE`, a full URL, for example `sc://teamspark-mac:15002`
4. `TEAMSPARK_HOST` plus `TEAMSPARK_PORT`
5. `SPARK_REMOTE`
6. The value saved by an earlier run
7. An interactive prompt

For example, `TEAMSPARK_HOST=100.101.102.103 bash setup/setup.sh`.

The result is saved as `SPARK_REMOTE`, the standard variable PySpark reads. Your code therefore needs no URL:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()      # connects to $SPARK_REMOTE
spark.read.parquet("/data/datasets/...")        # paths are on the SERVER (/data/datasets, /data/warehouse)
```

**To switch servers**, re-run the setup with the new `--host` or `-ServerHost`. Re-running is always safe.

## What the setup does

1. Works out the server URL and checks that it has the form `sc://host:port`.
2. Finds Python 3.12, or installs a private copy with uv (pinned to version 0.13.7).
3. Creates `.teamspark/venv` and installs the 14 pinned packages. It uses prebuilt wheels only;
   `pyspark-client` is the one exception, because it is published only as a pure-Python source package.
   It then runs `pip check`, and removes a full `pyspark` if one is present.
4. Saves the URL, hooks it into venv activation, and persists it for new terminals.
5. Runs `verify.py`, which checks:
   * the Python version and every package version against the lock;
   * that the environment is client-only (no Spark jars, no Java);
   * Tailscale status, DNS, TCP and the gRPC handshake;
   * the Spark session and the server's version;
   * a DataFrame `groupBy`/`agg` and a distributed sum on the cluster;
   * a Python UDF on the workers (only when the client runs Python 3.12).

You can re-run the test at any time:

```bash
~/.teamspark/venv/bin/python ~/.teamspark/verify.py                     # macOS
& "$env:USERPROFILE\.teamspark\venv\Scripts\python.exe" "$env:USERPROFILE\.teamspark\verify.py"   # Windows
```

`verify.py` exits with 0 on success, 1 when a check fails, and 2 when no server is configured or the URL is invalid.

## Troubleshooting

`verify.py` stops at the first failing check and prints hints. The common cases:

| Message | Likely cause and fix |
|---|---|
| `cannot resolve 'teamspark-mac'` | Tailscale is disconnected, or MagicDNS is off. Connect Tailscale, or use the 100.x IP. |
| `Tailscale is not connected` | Open the Tailscale app and connect (CLI: `tailscale up`). |
| `timed out connecting` | You or the server are offline in Tailscale, an ACL blocks `tcp:15002`, or the server Mac is asleep. |
| `connection refused` | The server is reachable, but TeamSpark is stopped or not exposed (admin: `docker compose ps`, `tailscale serve status`). |
| `accepts TCP but does not speak gRPC` | Wrong port, or the server is still starting. Retry in a minute. |
| `server runs Spark X` | Client and server versions differ. Update `teamspark-client.conf` and `requirements.txt`. |
| `Python 3.x != server Python 3.12` | This is a warning only. DataFrames and SQL work, but Python UDFs don't. Re-run with Python 3.12. |
| `packages do not match the lock` | Re-run the setup with `--force` / `-Force`. |

## Uninstall

* **macOS:** delete `~/.teamspark` and the `# >>> teamspark >>>` block in `~/.zshrc`.
* **Windows:** delete `%USERPROFILE%\.teamspark`, then run:
  `[Environment]::SetEnvironmentVariable("SPARK_REMOTE",$null,"User"); [Environment]::SetEnvironmentVariable("TEAMSPARK_REMOTE",$null,"User")`

## Test record (2026-11-05, against the TeamSpark server on the host Mac)

| Scenario | Result |
|---|---|
| macOS 15 (Darwin 24.6), system bash 3.2, Python 3.12.6 found | Pass: full flow, DataFrame + UDF on both workers |
| macOS: re-run with no arguments | Pass: reused the saved URL; the `~/.zshrc` block was not duplicated |
| macOS: new `zsh -i` shell / `source venv/bin/activate` | Pass: `SPARK_REMOTE` set; `SparkSession.builder.getOrCreate()` worked with no URL |
| `setup.sh` on a clean machine with no Python (Debian container) | Pass: uv installed Python 3.12.16 privately; full verification passed |
| `setup.ps1` under PowerShell 7.4 with no Python (Linux container) | Pass: uv path, venv, lock, verification, saved-URL re-run, `Activate.ps1` hook |
| `setup.ps1` static check against Windows PowerShell 5.1 (PSScriptAnalyzer compatibility rules) | Pass: 0 findings |
| Error paths: refused port, unknown host, unreachable 100.x IP, no URL, invalid URL | Pass: correct message, hints and exit code |

**Not yet tested on real Windows hardware.** That covers the `py` launcher lookup, the user
environment variables, `activate.bat`, and uv's `install.ps1`. **Not yet tested over a real
Tailscale link**, because Tailscale was not installed on the test Mac. Run both scripts once on
a team member's machine over Tailscale before rolling out.
