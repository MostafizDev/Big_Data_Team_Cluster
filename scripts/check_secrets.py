#!/usr/bin/env python3
"""Pre-publish check: make sure no credentials or private configuration would be committed.

Scans exactly the files git would commit (tracked + untracked, minus .gitignore).
Works before `git init` too (uses a throw-away git index).

    python scripts/check_secrets.py          # exit 0 = clean, 1 = findings
"""
from __future__ import annotations

import fnmatch
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Files that must never be committed (matched against the file name).
FORBIDDEN_FILES = [".env", ".env.local", ".env.*.local", "*.pem", "*.key", "*.p12", "*.pfx", "id_rsa*",
                   "id_ed25519*", "*.keystore", "*.jks", "tailscaled.state", "teamspark.env", "teamspark.ps1",
                   "credentials*", "*.tfstate"]
ALLOWED_FILES = {".env.example"}

# Content patterns. Example values used in the docs are allow-listed below.
PATTERNS = [
    ("Tailscale auth/API key", re.compile(r"tskey-[a-z]+-[A-Za-z0-9]{6,}-?[A-Za-z0-9]{6,}")),
    ("private key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("AWS access key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("GitHub token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b")),
    ("Slack token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}")),
    ("Spark Connect token", re.compile(r";token=[^;\s'\"]{6,}")),
    ("hard-coded secret", re.compile(
        r"(?i)\b(password|passwd|secret|api[_-]?key|access[_-]?token|auth[_-]?token)\b\s*[:=]\s*['\"]?[A-Za-z0-9/+_\-]{8,}")),
    ("Tailscale 100.x IP", re.compile(r"\b100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3}\b")),
    ("tailnet DNS name", re.compile(r"\b[a-z0-9-]+\.[a-z0-9-]+\.ts\.net\b", re.I)),
    ("e-mail address", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\b")),
]
ALLOWED_VALUES = {
    "100.101.102.103",          # documentation example IP
    "100.100.100.250",          # unroutable test IP used in docs/tests
    "100.64.0.0",               # CGNAT range in code
    "noreply@anthropic.com",
}
BINARY_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".ico", ".parquet", ".zstd", ".gz", ".zip", ".jar", ".pyc"}


def candidate_files() -> list[str]:
    git = shutil.which("git")
    if not git:
        sys.exit("git is required for this check (it applies .gitignore exactly like a commit would).")
    if (ROOT / ".git").exists():
        out = subprocess.run([git, "-C", str(ROOT), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                             check=True, capture_output=True).stdout
    else:
        with tempfile.TemporaryDirectory() as tmp:
            base = [git, f"--git-dir={tmp}/git", f"--work-tree={ROOT}"]
            subprocess.run(base + ["init", "-q"], check=True, capture_output=True)
            out = subprocess.run(base + ["ls-files", "-z", "--others", "--exclude-standard"],
                                 check=True, capture_output=True).stdout
    return sorted(p for p in out.decode().split("\0") if p)


def must_be_ignored() -> list[str]:
    """Paths that exist locally and must be ignored."""
    problems = []
    files = set(candidate_files())
    for rel in (".env", "storage/warehouse", "storage/events", "logs"):
        p = ROOT / rel
        if p.exists() and any(f == rel or f.startswith(rel + "/") for f in files):
            problems.append(f"{rel} exists and is NOT ignored by .gitignore")
    return problems


def scan() -> tuple[list[str], int]:
    findings: list[str] = []
    files = candidate_files()
    for rel in files:
        name = Path(rel).name
        if name not in ALLOWED_FILES and any(fnmatch.fnmatch(name, pat) for pat in FORBIDDEN_FILES):
            findings.append(f"{rel}: forbidden file type ({name})")
            continue
        path = ROOT / rel
        if path.suffix.lower() in BINARY_SUFFIXES or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            for label, rx in PATTERNS:
                for m in rx.finditer(line):
                    if m.group(0) in ALLOWED_VALUES or "secrets-ok" in line:
                        continue
                    findings.append(f"{rel}:{lineno}: {label}: {m.group(0)[:60]}")
    findings += must_be_ignored()
    return findings, len(files)


def main() -> int:
    findings, n = scan()
    if findings:
        print(f"FAIL: {len(findings)} potential secret(s) / private value(s) in {n} committable files:")
        for f in findings:
            print(f"  - {f}")
        print("\nRemove them (or move them to .env / ~/.teamspark), or mark a false positive with '# secrets-ok'.")
        return 1
    print(f"OK: scanned {n} committable files - no credentials or private configuration found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
