param(
    [string]$Output = "artifacts\durable_write_orchestrator.zip",
    [switch]$Deploy,
    [string]$ResourceGroup = "",
    [string]$FunctionApp = ""
)

$ErrorActionPreference = "Stop"

$argsList = @("scripts\package_durable_orchestrator.py", "--output", $Output)
if ($Deploy) {
    if (-not $ResourceGroup -or -not $FunctionApp) {
        Write-Error "-Deploy requires -ResourceGroup and -FunctionApp."
    }
    $argsList += @("--deploy", "--resource-group", $ResourceGroup, "--function-app", $FunctionApp)
}

python @argsList
