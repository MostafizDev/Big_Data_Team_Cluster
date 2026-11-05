<#
.SYNOPSIS
  TeamSpark client setup - Windows (Windows PowerShell 5.1 and PowerShell 7+).

.DESCRIPTION
  Creates an isolated Python 3.12 virtual environment with the pinned Spark
  Connect client (pyspark-client, no local Spark, no Java), saves the server
  URL in the SPARK_REMOTE user environment variable, and verifies the
  connection with a DataFrame job. Re-running is safe.
  Everything lives in %USERPROFILE%\.teamspark (override with TEAMSPARK_HOME).

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File setup\setup.ps1 -ServerHost teamspark-mac
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File setup\setup.ps1 -ServerHost 100.101.102.103 -Port 15002
.EXAMPLE
  $env:TEAMSPARK_HOST = "teamspark-mac"; .\setup\setup.ps1
#>
[CmdletBinding()]
param(
    [string]$ServerHost = "",   # Tailscale MagicDNS name or 100.x.y.z IP
    [string]$Port = "",
    [string]$Remote = "",       # full sc://host:port URL; overrides -ServerHost/-Port
    [string]$User = "",         # team-member name (default: TEAMSPARK_USER or Windows login)
    [string]$Python = "",       # explicit Python 3.12 interpreter
    [switch]$Force,             # recreate the virtual environment
    [switch]$NoPersist,         # do not set user environment variables
    [switch]$SkipVerify
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$OnWindows = ($PSVersionTable.PSEdition -eq "Desktop") -or ((Test-Path variable:IsWindows) -and $IsWindows)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$HomeDir = if ($OnWindows) { $env:USERPROFILE } else { $env:HOME }
$TeamSparkHome = if ($env:TEAMSPARK_HOME) { $env:TEAMSPARK_HOME } else { Join-Path $HomeDir ".teamspark" }
$Venv = Join-Path $TeamSparkHome "venv"
$VenvPy = if ($OnWindows) { Join-Path $Venv "Scripts\python.exe" } else { Join-Path $Venv "bin/python" }
$ConfFile = Join-Path $ScriptDir "teamspark-client.conf"
$EnvFile = Join-Path $TeamSparkHome "teamspark.ps1"

# ---- output helpers -----------------------------------------------------------
function Step([string]$m) { Write-Host ""; Write-Host "==> $m" -ForegroundColor White }
function Ok([string]$m)   { Write-Host "  [ OK ] " -ForegroundColor Green -NoNewline; Write-Host $m }
function Warn([string]$m) { Write-Host "  [WARN] " -ForegroundColor Yellow -NoNewline; Write-Host $m }
function Fail([string]$m, [string[]]$hints = @()) {
    Write-Host "  [FAIL] " -ForegroundColor Red -NoNewline; Write-Host $m
    foreach ($h in $hints) { Write-Host "         $h" }
    Write-Host ""
    Write-Host "ERROR: TeamSpark setup did not complete." -ForegroundColor Red
    exit 1
}

function Get-ConfValue([string]$key) {
    $line = Get-Content -LiteralPath $ConfFile | Where-Object { $_ -match "^$key=" } | Select-Object -Last 1
    if (-not $line) { Fail "$key is missing from $ConfFile" }
    return ($line -replace "^$key=", "").Trim()
}

# Runs a native command; returns trimmed stdout or $null. Never throws.
function Invoke-Quiet([string]$exe, [string[]]$arguments) {
    $old = $ErrorActionPreference; $ErrorActionPreference = "Continue"
    try {
        $out = & $exe @arguments 2>$null
        if ($LASTEXITCODE -eq 0 -and $out) { return (($out | Out-String).Trim()) }
    } catch { } finally { $ErrorActionPreference = $old }
    return $null
}

function Get-PyMinor([string]$exe) {
    return Invoke-Quiet $exe @("-c", "import sys; print('%d.%d' % sys.version_info[:2])")
}

# Runs a native command and fails with hints on a non-zero exit code.
function Invoke-Checked([string]$exe, [string[]]$arguments, [string]$what, [string[]]$hints = @()) {
    & $exe @arguments
    if ($LASTEXITCODE -ne 0) { Fail "$what failed (exit code $LASTEXITCODE)" $hints }
}

# ---- configuration -----------------------------------------------------------------
if (-not (Test-Path -LiteralPath $ConfFile)) {
    Fail "missing $ConfFile" @("Run this script from a complete copy of the setup\ folder.")
}
$SparkVersion  = Get-ConfValue "TEAMSPARK_SPARK_VERSION"
$PythonVersion = Get-ConfValue "TEAMSPARK_PYTHON_VERSION"
$DefaultPort   = Get-ConfValue "TEAMSPARK_DEFAULT_PORT"
$UvVersion     = Get-ConfValue "TEAMSPARK_UV_VERSION"

Write-Host "TeamSpark client setup (Windows) - Spark $SparkVersion, Python $PythonVersion" -ForegroundColor White
if (-not $OnWindows) { Warn "this script targets Windows; continuing on $([System.Environment]::OSVersion.Platform)" }

# ---- 1. server URL ---------------------------------------------------------------------
Step "Resolving the TeamSpark server address"
$PortValue = if ($Port) { $Port } elseif ($env:TEAMSPARK_PORT) { $env:TEAMSPARK_PORT } else { $DefaultPort }
$RemoteUrl = ""; $Source = ""
function Get-SavedValue([string]$name) {
    if (-not (Test-Path -LiteralPath $EnvFile)) { return "" }
    $m = Select-String -LiteralPath $EnvFile -Pattern "^\`$env:$name = '(.*)'$" | Select-Object -Last 1
    if ($m) { return $m.Matches[0].Groups[1].Value }
    return ""
}
$Saved = Get-SavedValue "TEAMSPARK_REMOTE"
if     ($Remote)                { $RemoteUrl = $Remote; $Source = "-Remote" }
elseif ($ServerHost)            { $RemoteUrl = "sc://${ServerHost}:$PortValue"; $Source = "-ServerHost" }
elseif ($env:TEAMSPARK_REMOTE)  { $RemoteUrl = $env:TEAMSPARK_REMOTE; $Source = "TEAMSPARK_REMOTE" }
elseif ($env:TEAMSPARK_HOST)    { $RemoteUrl = "sc://$($env:TEAMSPARK_HOST):$PortValue"; $Source = "TEAMSPARK_HOST" }
elseif ($env:SPARK_REMOTE)      { $RemoteUrl = $env:SPARK_REMOTE; $Source = "SPARK_REMOTE" }
elseif ($Saved)                 { $RemoteUrl = $Saved; $Source = $EnvFile }
elseif ([Environment]::UserInteractive -and -not [Console]::IsInputRedirected) {
    $h = Read-Host "  Server host (Tailscale name or 100.x.y.z IP)"
    if ($h) { $RemoteUrl = "sc://${h}:$PortValue"; $Source = "prompt" }
}
if (-not $RemoteUrl) {
    Fail "no server address given" @(
        "Pass -ServerHost <server>, e.g. .\setup\setup.ps1 -ServerHost teamspark-mac",
        "or set TEAMSPARK_HOST / TEAMSPARK_REMOTE.")
}
if ($RemoteUrl -notmatch "^sc://[^\s'`"%]+$") {
    Fail "invalid Spark Connect URL: $RemoteUrl" @("It must look like sc://<host>:<port>")
}

# Team-member identity -> user_id in the URL (shown on every job in the Spark UI).
# Same rules as app/teamspark/config.py normalize_name(): [a-z0-9_], max 32 chars.
function ConvertTo-MemberName([string]$raw) {
    $n = ($raw.ToLowerInvariant() -replace "[^a-z0-9_]+", "_").Trim("_")
    if ($n.Length -gt 32) { $n = $n.Substring(0, 32) }
    if ($n -match "^[0-9]") { $n = "u_$n"; if ($n.Length -gt 32) { $n = $n.Substring(0, 32) } }
    return $n
}
$rest = $RemoteUrl.Substring(5)
$hostPort = ($rest -split "/", 2)[0]
$hostPort = ($hostPort -split ";", 2)[0]
$tail = $rest.Substring($hostPort.Length).TrimStart("/")
$params = @($tail -split ";" | Where-Object { $_ -and ($_ -notmatch "^user_id=") })
$urlUser = ""
foreach ($p in ($tail -split ";")) { if ($p -match "^user_id=(.+)$") { $urlUser = $Matches[1] } }

# Precedence: -User, user_id inside an explicit -Remote, TEAMSPARK_USER, saved, URL, Windows login.
$RawUser = $User
if (-not $RawUser -and $Source -eq "-Remote") { $RawUser = $urlUser }
if (-not $RawUser) { $RawUser = $env:TEAMSPARK_USER }
if (-not $RawUser) { $RawUser = Get-SavedValue "TEAMSPARK_USER" }
if (-not $RawUser) { $RawUser = $urlUser }
if (-not $RawUser) { $RawUser = [Environment]::UserName }
$Member = ConvertTo-MemberName $RawUser
if (-not $Member) { Fail "invalid team-member name: '$RawUser'" @("Use letters, digits and '_', e.g. -User alice") }

if ($hostPort -notmatch ":\d+$") { $hostPort = "${hostPort}:$PortValue" }
$RemoteUrl = "sc://$hostPort/;" + (($params + @("user_id=$Member")) -join ";")
Ok "server: $RemoteUrl (from $Source)"
Ok "team member: $Member"

# ---- 2. Python -------------------------------------------------------------------------------
Step "Finding Python $PythonVersion"
$UvDir = Join-Path $TeamSparkHome "uv"
$UvPythonDir = Join-Path $TeamSparkHome "python"

function Find-Uv {
    $local = Join-Path $UvDir $(if ($OnWindows) { "uv.exe" } else { "uv" })
    if (Test-Path -LiteralPath $local) { return $local }
    $cmd = Get-Command uv -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return $null
}

function Find-Python {
    $candidates = New-Object System.Collections.Generic.List[string]
    if ($OnWindows) {
        # The py launcher knows every python.org install.
        $p = Invoke-Quiet "py" @("-$PythonVersion", "-c", "import sys; print(sys.executable)")
        if ($p) { $candidates.Add($p) }
        $nodot = $PythonVersion -replace "\.", ""
        foreach ($root in @($env:LOCALAPPDATA, $env:ProgramFiles)) {
            if ($root) {
                $candidates.Add((Join-Path $root "Programs\Python\Python$nodot\python.exe"))
                $candidates.Add((Join-Path $root "Python$nodot\python.exe"))
            }
        }
    }
    foreach ($name in @("python$PythonVersion", "python3", "python")) {
        foreach ($c in @(Get-Command $name -CommandType Application -ErrorAction SilentlyContinue)) {
            # Skip the Microsoft Store "python.exe" alias stubs.
            if ($c.Source -notmatch "WindowsApps") { $candidates.Add($c.Source) }
        }
    }
    foreach ($c in $candidates) {
        if ((Test-Path -LiteralPath $c) -and ((Get-PyMinor $c) -eq $PythonVersion)) { return $c }
    }
    $uv = Find-Uv
    if ($uv) {
        $env:UV_PYTHON_INSTALL_DIR = $UvPythonDir
        $p = Invoke-Quiet $uv @("python", "find", $PythonVersion)
        if ($p -and ((Get-PyMinor $p) -eq $PythonVersion)) { return $p }
    }
    return $null
}

if ($Python) {
    $Py = $Python
    if ((Get-PyMinor $Py) -ne $PythonVersion) {
        Fail "$Py is not Python $PythonVersion" @("TeamSpark executors run Python $PythonVersion; the client must match.")
    }
} else {
    $Py = Find-Python
}
if (-not $Py) {
    Warn "Python $PythonVersion not found; installing a private copy with uv $UvVersion into $TeamSparkHome"
    $uv = Find-Uv
    if (-not $uv) {
        New-Item -ItemType Directory -Force -Path $UvDir | Out-Null
        $env:UV_INSTALL_DIR = $UvDir
        $env:UV_NO_MODIFY_PATH = "1"
        try {
            if ($OnWindows) {
                [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
                $installer = Invoke-RestMethod "https://astral.sh/uv/$UvVersion/install.ps1"
                Invoke-Expression $installer | Out-Null
            } else {
                $installer = Join-Path $TeamSparkHome "uv-install.sh"
                Invoke-WebRequest "https://astral.sh/uv/$UvVersion/install.sh" -OutFile $installer -UseBasicParsing
                & sh $installer | Out-Null
                Remove-Item -LiteralPath $installer -Force
            }
        } catch {
            Fail "failed to download uv: $($_.Exception.Message)" @(
                "Check your internet connection, or install Python $PythonVersion from python.org",
                "(tick 'Add python.exe to PATH') or: winget install Python.Python.$PythonVersion")
        }
        $uv = Find-Uv
        if (-not $uv) { Fail "uv was not installed into $UvDir" }
    }
    $env:UV_PYTHON_INSTALL_DIR = $UvPythonDir; $env:UV_PYTHON_BIN_DIR = (Join-Path $UvPythonDir "bin")
    Invoke-Checked $uv @("python", "install", $PythonVersion) "uv python install"
    $Py = Invoke-Quiet $uv @("python", "find", $PythonVersion)
    if (-not $Py) { Fail "Python $PythonVersion still not found after installation" }
}
$PyFull = Invoke-Quiet $Py @("-c", "import platform; print(platform.python_version())")
Ok "using $Py ($PyFull)"

# ---- 3. virtual environment ----------------------------------------------------------------------
Step "Creating the virtual environment in $Venv"
New-Item -ItemType Directory -Force -Path $TeamSparkHome | Out-Null
if (Test-Path -LiteralPath $Venv) {
    if ($Force) {
        Remove-Item -LiteralPath $Venv -Recurse -Force; Ok "removed existing environment (-Force)"
    } elseif (-not (Test-Path -LiteralPath $VenvPy) -or (Get-PyMinor $VenvPy) -ne $PythonVersion) {
        Remove-Item -LiteralPath $Venv -Recurse -Force; Warn "existing environment had the wrong Python; recreating"
    }
}
if (-not (Test-Path -LiteralPath $VenvPy)) {
    Invoke-Checked $Py @("-m", "venv", $Venv) "creating the virtual environment" @(
        "Your Python may lack the venv module; install Python from python.org or use -Python.")
}
Ok "virtual environment ready"

Step "Installing pinned Spark Connect client packages"
$PipBase = @("-m", "pip", "--disable-pip-version-check", "--no-input")
if (Invoke-Quiet $VenvPy ($PipBase + @("show", "pyspark"))) {
    Warn "removing full 'pyspark' (local Spark) from the environment"
    Invoke-Checked $VenvPy ($PipBase + @("uninstall", "-y", "-q", "pyspark")) "pip uninstall pyspark"
}
$Req = Join-Path $ScriptDir "requirements.txt"
Invoke-Checked $VenvPy ($PipBase + @("install", "-q", "--only-binary=:all:", "--no-binary=pyspark-client", "-r", $Req)) "package installation" @(
    "Check your internet connection and re-run (add -Force to start clean).")
if (-not (Invoke-Quiet $VenvPy ($PipBase + @("check")))) {
    Fail "installed packages are inconsistent ('pip check' failed)" @("Re-run with -Force.")
}
$PinCount = @(Get-Content -LiteralPath $Req | Where-Object { $_ -match "==" }).Count
Ok "installed: $PinCount pinned packages (pyspark-client $SparkVersion)"

foreach ($f in @("verify.py", "teamspark-client.conf", "requirements.txt")) {
    Copy-Item -LiteralPath (Join-Path $ScriptDir $f) -Destination $TeamSparkHome -Force
}

# ---- 4. configuration -------------------------------------------------------------------------------
Step "Saving the server address"
$stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$envContent = @(
    "# Generated by TeamSpark setup on $stamp. Re-run setup to change.",
    "`$env:TEAMSPARK_REMOTE = '$RemoteUrl'",
    "`$env:TEAMSPARK_USER = '$Member'",
    "`$env:SPARK_REMOTE = `$env:TEAMSPARK_REMOTE"
)
Set-Content -LiteralPath $EnvFile -Value $envContent -Encoding ASCII
Ok "wrote $EnvFile"

$Activate = if ($OnWindows) { Join-Path $Venv "Scripts\Activate.ps1" } else { Join-Path $Venv "bin/Activate.ps1" }
if (Test-Path -LiteralPath $Activate) {
    $hook = "if (Test-Path '$EnvFile') { . '$EnvFile' }"
    if (-not (Select-String -LiteralPath $Activate -SimpleMatch $EnvFile -Quiet)) {
        Add-Content -LiteralPath $Activate -Value "`r`n# TeamSpark`r`n$hook"
    }
}
if ($OnWindows) {
    # cmd.exe activation: (re)write a marked block so a re-run with a new server/user takes effect.
    # Inside a .bat file "%" must be doubled; the URL never contains "%" (validated above).
    $bat = Join-Path $Venv "Scripts\activate.bat"
    if (Test-Path -LiteralPath $bat) {
        $lines = @(Get-Content -LiteralPath $bat)
        $keep = New-Object System.Collections.Generic.List[string]
        $skip = $false
        foreach ($l in $lines) {
            if ($l -eq "rem >>> teamspark >>>") { $skip = $true; continue }
            if ($l -eq "rem <<< teamspark <<<") { $skip = $false; continue }
            if (-not $skip) { $keep.Add($l) }
        }
        $keep.Add("rem >>> teamspark >>>")
        $keep.Add("set `"TEAMSPARK_REMOTE=$RemoteUrl`"")
        $keep.Add("set `"TEAMSPARK_USER=$Member`"")
        $keep.Add("set `"SPARK_REMOTE=$RemoteUrl`"")
        $keep.Add("rem <<< teamspark <<<")
        Set-Content -LiteralPath $bat -Value $keep -Encoding ASCII
    }
}
Ok "venv activation sets SPARK_REMOTE"

$env:TEAMSPARK_REMOTE = $RemoteUrl
$env:TEAMSPARK_USER = $Member
$env:SPARK_REMOTE = $RemoteUrl
if (-not $NoPersist) {
    if ($OnWindows) {
        [Environment]::SetEnvironmentVariable("TEAMSPARK_REMOTE", $RemoteUrl, "User")
        [Environment]::SetEnvironmentVariable("TEAMSPARK_USER", $Member, "User")
        [Environment]::SetEnvironmentVariable("SPARK_REMOTE", $RemoteUrl, "User")
        Ok "SPARK_REMOTE saved as a user environment variable (new terminals/IDEs pick it up)"
    } else {
        Warn "user environment variables are Windows-only; use setup.sh on macOS/Linux"
    }
}

# ---- 5. verification ----------------------------------------------------------------------------------
if (-not $SkipVerify) {
    Step "Verifying the connection to TeamSpark"
    & $VenvPy (Join-Path $TeamSparkHome "verify.py") --remote $RemoteUrl
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "ERROR: the client is installed, but the connection test failed (see hints above)." -ForegroundColor Red
        Write-Host "Fix the issue, then re-test with:"
        Write-Host "  & '$VenvPy' '$(Join-Path $TeamSparkHome 'verify.py')'"
        exit 1
    }
} else {
    Warn "verification skipped (-SkipVerify)"
}

$ActivateHint = if ($OnWindows) { "& '$Venv\Scripts\Activate.ps1'" } else { ". '$Activate'" }
Write-Host ""
Write-Host "TeamSpark client is ready." -ForegroundColor Green
Write-Host "  Activate:    $ActivateHint      (cmd.exe: $Venv\Scripts\activate.bat)"
Write-Host "  Use:         python -c `"from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate().range(5).count())`""
Write-Host "  Re-test:     & '$VenvPy' '$(Join-Path $TeamSparkHome 'verify.py')'"
Write-Host "  IDE/Jupyter: select the interpreter $VenvPy"
exit 0
