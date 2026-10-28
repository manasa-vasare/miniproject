# ============================================================================
# run_system.ps1 - optional convenience wrapper (Windows 11).
# All process management is done natively by controller.exe (CreateProcessW);
# this script only locates the executable and forwards the command.
#
#   .\run_system.ps1 start | stop | status
# ============================================================================
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'stop', 'status')]
    [string]$Command = 'status'
)

$root = $PSScriptRoot
$ctl = $null
foreach ($c in 'build\Release\controller.exe', 'build\RelWithDebInfo\controller.exe',
               'build\Debug\controller.exe', 'build\controller.exe') {
    $p = Join-Path $root $c
    if (Test-Path $p) { $ctl = $p; break }
}
if (-not $ctl) {
    Write-Error "controller.exe not found. Build first: cmake -S . -B build; cmake --build build --config Release"
    exit 2
}
& $ctl sys $Command
exit $LASTEXITCODE
