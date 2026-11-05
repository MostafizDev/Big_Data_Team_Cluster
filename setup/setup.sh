#!/usr/bin/env bash
# =============================================================================
# TeamSpark client setup - macOS
#
# Creates an isolated Python 3.12 virtual environment with the pinned Spark
# Connect client (pyspark-client, no local Spark, no Java), saves the server
# URL in SPARK_REMOTE, and verifies the connection with a DataFrame job.
#
#   ./setup/setup.sh --host teamspark-mac            # Tailscale MagicDNS name
#   ./setup/setup.sh --host 100.101.102.103          # Tailscale IP
#   TEAMSPARK_HOST=teamspark-mac ./setup/setup.sh    # via environment
#
# Re-running is safe; it updates the environment and the saved URL.
# Everything lives in ~/.teamspark (override with TEAMSPARK_HOME).
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEAMSPARK_HOME="${TEAMSPARK_HOME:-$HOME/.teamspark}"
VENV="$TEAMSPARK_HOME/venv"
VENV_PY="$VENV/bin/python"
CONF_FILE="$SCRIPT_DIR/teamspark-client.conf"
ENV_FILE="$TEAMSPARK_HOME/teamspark.env"

# ---- output helpers -----------------------------------------------------------
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_G=$'\033[32m'; C_Y=$'\033[33m'; C_R=$'\033[31m'; C_B=$'\033[1m'; C_0=$'\033[0m'
else
  C_G=""; C_Y=""; C_R=""; C_B=""; C_0=""
fi
step() { printf '\n%s==> %s%s\n' "$C_B" "$*" "$C_0"; }
ok()   { printf '  %s[ OK ]%s %s\n' "$C_G" "$C_0" "$*"; }
warn() { printf '  %s[WARN]%s %s\n' "$C_Y" "$C_0" "$*"; }
die()  {
  printf '  %s[FAIL]%s %s\n' "$C_R" "$C_0" "$1" >&2
  shift; for h in "$@"; do printf '         %s\n' "$h" >&2; done
  printf '\n%sERROR%s: TeamSpark setup did not complete.\n' "$C_R" "$C_0" >&2
  exit 1
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --host HOST       Server host: Tailscale MagicDNS name or 100.x.y.z IP
  --port PORT       Spark Connect port (default from teamspark-client.conf)
  --remote URL      Full Spark Connect URL (sc://host:port), overrides --host/--port
  --user NAME       Your team-member name (default: TEAMSPARK_USER or your login)
  --python PATH    Use this Python ${PYTHON_VERSION:-3.12} interpreter
  --force           Delete and recreate the virtual environment
  --no-persist      Do not add SPARK_REMOTE to ~/.zshrc / ~/.bash_profile
  --skip-verify     Install only; do not test the connection
  -h, --help        Show this help

Environment: TEAMSPARK_HOST, TEAMSPARK_PORT, TEAMSPARK_REMOTE, TEAMSPARK_USER, TEAMSPARK_HOME
EOF
}

conf_get() {
  local v
  v="$(sed -n "s/^$1=//p" "$CONF_FILE" | tail -n 1 | tr -d '\r[:space:]')"
  [ -n "$v" ] || die "$1 is missing from $CONF_FILE"
  printf '%s' "$v"
}

# ---- arguments -------------------------------------------------------------------
[ -f "$CONF_FILE" ] || die "missing $CONF_FILE" "Run this script from a complete copy of the setup/ folder."
SPARK_VERSION="$(conf_get TEAMSPARK_SPARK_VERSION)"
PYTHON_VERSION="$(conf_get TEAMSPARK_PYTHON_VERSION)"
DEFAULT_PORT="$(conf_get TEAMSPARK_DEFAULT_PORT)"
UV_VERSION="$(conf_get TEAMSPARK_UV_VERSION)"

ARG_HOST=""; ARG_PORT=""; ARG_REMOTE=""; ARG_PYTHON=""; ARG_USER=""
FORCE=0; PERSIST=1; VERIFY=1
while [ $# -gt 0 ]; do
  case "$1" in
    --host)        [ $# -ge 2 ] || die "--host needs a value"; ARG_HOST="$2"; shift 2 ;;
    --port)        [ $# -ge 2 ] || die "--port needs a value"; ARG_PORT="$2"; shift 2 ;;
    --remote)      [ $# -ge 2 ] || die "--remote needs a value"; ARG_REMOTE="$2"; shift 2 ;;
    --python)      [ $# -ge 2 ] || die "--python needs a value"; ARG_PYTHON="$2"; shift 2 ;;
    --user)        [ $# -ge 2 ] || die "--user needs a value"; ARG_USER="$2"; shift 2 ;;
    --force)       FORCE=1; shift ;;
    --no-persist)  PERSIST=0; shift ;;
    --skip-verify) VERIFY=0; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             usage >&2; die "unknown option: $1" ;;
  esac
done

printf '%sTeamSpark client setup (macOS)%s - Spark %s, Python %s\n' "$C_B" "$C_0" "$SPARK_VERSION" "$PYTHON_VERSION"
[ "$(uname -s)" = "Darwin" ] || warn "this script targets macOS; continuing on $(uname -s)"

# ---- 1. server URL -------------------------------------------------------------------
step "Resolving the TeamSpark server address"
REMOTE=""; SOURCE=""
saved_remote() { [ -f "$ENV_FILE" ] && sed -n 's/^export TEAMSPARK_REMOTE="\(.*\)"$/\1/p' "$ENV_FILE" | tail -n 1; }
if   [ -n "$ARG_REMOTE" ];                then REMOTE="$ARG_REMOTE"; SOURCE="--remote"
elif [ -n "$ARG_HOST" ];                  then REMOTE="sc://$ARG_HOST:${ARG_PORT:-${TEAMSPARK_PORT:-$DEFAULT_PORT}}"; SOURCE="--host"
elif [ -n "${TEAMSPARK_REMOTE:-}" ];      then REMOTE="$TEAMSPARK_REMOTE"; SOURCE="TEAMSPARK_REMOTE"
elif [ -n "${TEAMSPARK_HOST:-}" ];        then REMOTE="sc://$TEAMSPARK_HOST:${ARG_PORT:-${TEAMSPARK_PORT:-$DEFAULT_PORT}}"; SOURCE="TEAMSPARK_HOST"
elif [ -n "${SPARK_REMOTE:-}" ];          then REMOTE="$SPARK_REMOTE"; SOURCE="SPARK_REMOTE"
elif [ -n "$(saved_remote || true)" ];    then REMOTE="$(saved_remote)"; SOURCE="$ENV_FILE"
elif [ -t 0 ]; then
  printf '  Server host (Tailscale name or 100.x.y.z IP): '
  read -r h
  [ -n "$h" ] && REMOTE="sc://$h:${ARG_PORT:-$DEFAULT_PORT}" && SOURCE="prompt"
fi
[ -n "$REMOTE" ] || die "no server address given" \
  "Pass --host <server>, e.g. ./setup/setup.sh --host teamspark-mac" \
  "or set TEAMSPARK_HOST / TEAMSPARK_REMOTE."
case "$REMOTE" in
  sc://?*) ;;
  *) die "invalid Spark Connect URL: $REMOTE" "It must look like sc://<host>:<port>" ;;
esac
case "$REMOTE" in *[[:space:]\"\$\`]*) die "invalid characters in URL: $REMOTE" ;; esac

# Team-member identity -> user_id in the URL (shown on every job in the Spark UI).
# Same rules as app/teamspark/config.py normalize_name(): [a-z0-9_], max 32 chars.
normalize_user() {
  local n
  n="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9_]+/_/g; s/^_+//; s/_+$//' | cut -c1-32)"
  case "$n" in [0-9]*) n="$(printf 'u_%s' "$n" | cut -c1-32)" ;; esac
  printf '%s' "$n"
}
url_param() { printf '%s' "$1" | tr ';' '\n' | sed -n "s/^$2=//p" | tail -n 1; }
saved_user() { [ -f "$ENV_FILE" ] && sed -n 's/^export TEAMSPARK_USER="\(.*\)"$/\1/p' "$ENV_FILE" | tail -n 1; }
# Precedence: --user, user_id inside an explicit --remote, TEAMSPARK_USER, saved, URL, OS login.
RAW_USER="$ARG_USER"
[ -n "$RAW_USER" ] || [ "$SOURCE" != "--remote" ] || RAW_USER="$(url_param "$REMOTE" user_id)"
[ -n "$RAW_USER" ] || RAW_USER="${TEAMSPARK_USER:-$(saved_user || true)}"
[ -n "$RAW_USER" ] || RAW_USER="$(url_param "$REMOTE" user_id)"
[ -n "$RAW_USER" ] || RAW_USER="$(id -un)"
MEMBER="$(normalize_user "$RAW_USER")"
[ -n "$MEMBER" ] || die "invalid team-member name: '$RAW_USER'" "Use letters, digits and '_', e.g. --user alice"

# Rebuild the URL as sc://host:port/;user_id=<member> (other parameters are kept).
rest="${REMOTE#sc://}"; hostport="${rest%%/*}"; hostport="${hostport%%;*}"
params="$(printf '%s' "${rest#"$hostport"}" | sed -E 's#^/##' | tr ';' '\n' | grep -v -e '^user_id=' -e '^$' | tr '\n' ';' || true)"
case "$hostport" in *:*) ;; *) hostport="$hostport:${ARG_PORT:-$DEFAULT_PORT}" ;; esac
REMOTE="sc://$hostport/;${params}user_id=$MEMBER"
ok "server: $REMOTE (from $SOURCE)"
ok "team member: $MEMBER"

# ---- 2. Python ---------------------------------------------------------------------------
step "Finding Python $PYTHON_VERSION"
py_minor() { "$1" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || true; }
UV_BIN=""
find_uv() {
  for c in "$TEAMSPARK_HOME/uv/uv" "$(command -v uv 2>/dev/null || true)"; do
    if [ -n "$c" ] && [ -x "$c" ]; then UV_BIN="$c"; return 0; fi
  done
  return 1
}
find_python() {
  local c p
  for c in "python$PYTHON_VERSION" python3 python \
           "/opt/homebrew/bin/python$PYTHON_VERSION" "/usr/local/bin/python$PYTHON_VERSION" \
           "/Library/Frameworks/Python.framework/Versions/$PYTHON_VERSION/bin/python$PYTHON_VERSION"; do
    p="$(command -v "$c" 2>/dev/null || true)"
    if [ -n "$p" ] && [ "$(py_minor "$p")" = "$PYTHON_VERSION" ]; then printf '%s' "$p"; return 0; fi
  done
  if find_uv; then
    p="$(UV_PYTHON_INSTALL_DIR="$TEAMSPARK_HOME/python" "$UV_BIN" python find "$PYTHON_VERSION" 2>/dev/null || true)"
    if [ -n "$p" ] && [ "$(py_minor "$p")" = "$PYTHON_VERSION" ]; then printf '%s' "$p"; return 0; fi
  fi
  return 1
}

if [ -n "$ARG_PYTHON" ]; then
  PY="$ARG_PYTHON"
  [ "$(py_minor "$PY")" = "$PYTHON_VERSION" ] || die "$PY is not Python $PYTHON_VERSION" \
    "TeamSpark executors run Python $PYTHON_VERSION; the client must match."
elif PY="$(find_python)"; then
  :
else
  warn "Python $PYTHON_VERSION not found; installing a private copy with uv $UV_VERSION into $TEAMSPARK_HOME"
  command -v curl >/dev/null || die "curl is required to download uv"
  if ! find_uv; then
    mkdir -p "$TEAMSPARK_HOME/uv"
    curl -LsSf "https://astral.sh/uv/$UV_VERSION/install.sh" \
      | env UV_INSTALL_DIR="$TEAMSPARK_HOME/uv" UV_NO_MODIFY_PATH=1 sh >/dev/null \
      || die "failed to download uv" "Check your internet connection, or install Python $PYTHON_VERSION from python.org / 'brew install python@$PYTHON_VERSION' and re-run."
    UV_BIN="$TEAMSPARK_HOME/uv/uv"
  fi
  UV_PYTHON_INSTALL_DIR="$TEAMSPARK_HOME/python" UV_PYTHON_BIN_DIR="$TEAMSPARK_HOME/python/bin" "$UV_BIN" python install "$PYTHON_VERSION" \
    || die "uv could not install Python $PYTHON_VERSION"
  PY="$(UV_PYTHON_INSTALL_DIR="$TEAMSPARK_HOME/python" "$UV_BIN" python find "$PYTHON_VERSION")" \
    || die "Python $PYTHON_VERSION still not found after installation"
fi
ok "using $PY ($("$PY" -c 'import platform; print(platform.python_version())'))"

# ---- 3. virtual environment -----------------------------------------------------------------
step "Creating the virtual environment in $VENV"
mkdir -p "$TEAMSPARK_HOME"
if [ -d "$VENV" ]; then
  if [ "$FORCE" = 1 ]; then
    rm -rf "$VENV"; ok "removed existing environment (--force)"
  elif [ "$(py_minor "$VENV_PY")" != "$PYTHON_VERSION" ]; then
    rm -rf "$VENV"; warn "existing environment had the wrong Python; recreating"
  fi
fi
if [ ! -x "$VENV_PY" ]; then
  "$PY" -m venv "$VENV" || die "could not create the virtual environment" \
    "Your Python may lack the venv module; install Python from python.org or use --python."
fi
ok "virtual environment ready"

step "Installing pinned Spark Connect client packages"
PIP="$VENV_PY -m pip --disable-pip-version-check --no-input"
if $PIP show pyspark >/dev/null 2>&1; then
  warn "removing full 'pyspark' (local Spark) from the environment"
  $PIP uninstall -y -q pyspark
fi
$PIP install -q --only-binary=:all: --no-binary=pyspark-client -r "$SCRIPT_DIR/requirements.txt" \
  || die "package installation failed" "Check your internet connection and re-run (add --force to start clean)."
$PIP check >/dev/null || die "installed packages are inconsistent ('pip check' failed)" "Re-run with --force."
ok "installed: $(grep -c '==' "$SCRIPT_DIR/requirements.txt") pinned packages (pyspark-client $SPARK_VERSION)"

# Keep a copy of the verifier + shared config next to the venv for later re-runs.
cp "$SCRIPT_DIR/verify.py" "$SCRIPT_DIR/teamspark-client.conf" "$SCRIPT_DIR/requirements.txt" "$TEAMSPARK_HOME/"

# ---- 4. configuration -----------------------------------------------------------------------
step "Saving the server address"
cat > "$ENV_FILE" <<EOF
# Generated by TeamSpark setup on $(date -u +%Y-%m-%dT%H:%M:%SZ). Re-run setup to change.
export TEAMSPARK_REMOTE="$REMOTE"
export TEAMSPARK_USER="$MEMBER"
export SPARK_REMOTE="\$TEAMSPARK_REMOTE"
EOF
ok "wrote $ENV_FILE"

HOOK="[ -f \"$ENV_FILE\" ] && . \"$ENV_FILE\""
grep -qF "$ENV_FILE" "$VENV/bin/activate" || printf '\n# TeamSpark\n%s\n' "$HOOK" >> "$VENV/bin/activate"
ok "venv activation sets SPARK_REMOTE"

if [ "$PERSIST" = 1 ]; then
  add_block() {   # idempotently (re)write the marked block at the end of $1
    local rc="$1" tmp
    tmp="$(mktemp)"
    [ -f "$rc" ] && awk '/^# >>> teamspark >>>/{skip=1} !skip{print} /^# <<< teamspark <<</{skip=0}' "$rc" > "$tmp"
    printf '# >>> teamspark >>>\n%s\n# <<< teamspark <<<\n' "$HOOK" >> "$tmp"
    cat "$tmp" > "$rc"; rm -f "$tmp"
    ok "SPARK_REMOTE added to $rc"
  }
  add_block "$HOME/.zshrc"
  [ -f "$HOME/.bash_profile" ] && add_block "$HOME/.bash_profile"
fi

# ---- 5. verification -------------------------------------------------------------------------
if [ "$VERIFY" = 1 ]; then
  step "Verifying the connection to TeamSpark"
  if ! SPARK_REMOTE="$REMOTE" "$VENV_PY" "$TEAMSPARK_HOME/verify.py" --remote "$REMOTE"; then
    printf '\n%sERROR%s: the client is installed, but the connection test failed (see hints above).\n' "$C_R" "$C_0" >&2
    printf 'Fix the issue, then re-test with:\n  %s %s\n' "$VENV_PY" "$TEAMSPARK_HOME/verify.py" >&2
    exit 1
  fi
else
  warn "verification skipped (--skip-verify)"
fi

cat <<EOF

${C_G}TeamSpark client is ready.${C_0}
  Activate:   source "$VENV/bin/activate"     (new terminals also get SPARK_REMOTE)
  Use:        python -c 'from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate().range(5).count())'
  Re-test:    "$VENV_PY" "$TEAMSPARK_HOME/verify.py"
  IDE/Jupyter: select the interpreter $VENV_PY
EOF
