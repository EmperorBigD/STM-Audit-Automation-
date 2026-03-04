# =============================================================================
#  Run_Pipeline.ps1
#  One-click runner for the STM self-citation analysis pipeline.
#
#  Usage:
#    .\Run_Pipeline.ps1              — normal run
#    .\Run_Pipeline.ps1 -Verbose     — show detailed progress from Python
#
#  What it does:
#    1. Looks for exactly ONE Weekly_Status_Python*.xlsx file in the script folder
#    2. Extracts the 13-digit ISBN from the filename
#    3. Creates / reuses a Python virtual environment
#    4. Installs / updates dependencies from Requirements.txt
#    5. Runs Pipeline.py and writes a timestamped output file
# =============================================================================

param(
    [switch] $Verbose  # pass --verbose to Pipeline.py when set
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Folder configuration ─────────────────────────────────────────────────────
# By default, process files in the same folder as this script.
# To hardcode a different folder (like D:\STM), change this to: $ScriptDir = "D:\STM"
$ScriptDir = $PSScriptRoot

# ── 1. Find exactly ONE input Excel file ─────────────────────────────────────
# Exclude: temp files (~$...), previously-generated output files (STM_<isbn>_...),
# and any legacy STM_Output_... files so re-runs never pick up their own output.
[array]$InputFiles = Get-ChildItem -Path $ScriptDir -Filter "*.xlsx" |
    Where-Object {
        $_.Name -notmatch "^STM_\d{13}_" -and   # exclude our own timestamped output
        $_.Name -notmatch "^STM_Output_"   -and   # exclude legacy output naming
        $_.Name -notmatch "^~\$"                   # exclude Excel temp/lock files
    }

if ($InputFiles.Count -eq 0) {
    Write-Error "No input Excel files found in '$ScriptDir'. Place exactly one Weekly Status file there and try again."
    exit 1
}
elseif ($InputFiles.Count -gt 1) {
    Write-Warning "Found $($InputFiles.Count) Excel files in '${ScriptDir}':"
    $InputFiles | ForEach-Object { Write-Warning "  - $($_.Name)" }
    Write-Error "Please ensure ONLY ONE input Excel file is present in the STM folder, then try again."
    exit 1
}

$InputFile     = $InputFiles[0]
$InputFileName = $InputFile.Name
$InputFilePath = $InputFile.FullName

# ── 2. Extract ISBN (first 13 digits from filename) ──────────────────────────
# Standard naming convention: 9783527355044_Weekly_Status_Python_Feb 28.xlsx
$IsbnMatch = [regex]::Match($InputFileName, "^\d{13}")
if (-not $IsbnMatch.Success) {
    Write-Warning "Filename '$InputFileName' does not start with a 13-digit ISBN. Using 'UNKNOWN_ISBN' as placeholder."
    $Isbn = "UNKNOWN_ISBN"
}
else {
    $Isbn = $IsbnMatch.Value
}

# ── 3. Define output path ────────────────────────────────────────────────────
# Output lands in the same folder as the input, named: STM_<isbn>_<timestamp>.xlsx
$Timestamp      = Get-Date -Format "yyyyMMdd_HHmmss"
$OutputFileName = "STM_${Isbn}_${Timestamp}.xlsx"
$OutputFilePath = Join-Path $ScriptDir $OutputFileName

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  STM Self-Citation Pipeline Runner" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Input file    : $InputFileName"
Write-Host "  Extracted ISBN: $Isbn"
Write-Host "  Output file   : $OutputFileName"
Write-Host ""

# ── 4. Create virtual environment (skip if already exists) ──────────────────
$VenvDir = Join-Path $ScriptDir ".venv"
if (-not (Test-Path $VenvDir)) {
    Write-Host "--> Creating virtual environment..." -ForegroundColor Yellow
    python -m venv $VenvDir
}
else {
    Write-Host "--> Virtual environment already exists, skipping creation." -ForegroundColor DarkGray
}

# ── 5. Activate virtual environment ─────────────────────────────────────────
$ActivateScript = Join-Path $VenvDir "Scripts\Activate.ps1"
Write-Host "--> Activating virtual environment..." -ForegroundColor Yellow
& $ActivateScript

# ── 6. Install / update dependencies ────────────────────────────────────────
Write-Host "--> Installing / verifying dependencies from Requirements.txt..." -ForegroundColor Yellow
pip install --quiet --upgrade pip
pip install --quiet -r (Join-Path $ScriptDir "Requirements.txt")

# ── 7. Build argument list for Pipeline.py ───────────────────────────────────
$PipelineScript = Join-Path $ScriptDir "Pipeline.py"

# Build the argument array used by the & call-operator below.
# Using an array avoids quoting/splitting issues that plague Invoke-Expression.
$PyArgs = @(
    $PipelineScript,
    $InputFilePath,
    "--output", $OutputFilePath
)

if ($Verbose) {
    $PyArgs += "--verbose"
}

# ── 8. Run the pipeline ──────────────────────────────────────────────────────
Write-Host ""
Write-Host "--> Running pipeline..." -ForegroundColor Yellow
Write-Host "    python $($PyArgs -join ' ')" -ForegroundColor DarkGray
Write-Host ""

# Use the & call-operator (NOT Invoke-Expression) so that PowerShell passes
# each element of $PyArgs as a separate, correctly-quoted argument — this is
# safer and avoids injection risks when paths contain spaces.
& python @PyArgs
$ExitCode = $LASTEXITCODE

# ── 9. Deactivate virtual environment ───────────────────────────────────────
deactivate

# ── 10. Report outcome ───────────────────────────────────────────────────────
Write-Host ""
if ($ExitCode -eq 0) {
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "  Done!  Output saved to:" -ForegroundColor Green
    Write-Host "  $OutputFileName" -ForegroundColor Green
    Write-Host "============================================================" -ForegroundColor Green
}
else {
    Write-Host "============================================================" -ForegroundColor Red
    Write-Host "  Pipeline exited with error code $ExitCode." -ForegroundColor Red
    Write-Host "  Check the output above for details." -ForegroundColor Red
    Write-Host "============================================================" -ForegroundColor Red
    exit $ExitCode
}
Write-Host ""
