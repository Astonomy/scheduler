param(
    [Parameter(Mandatory=$true)][string]$Python,
    [Parameter(Mandatory=$true)][string]$TaskFile,
    [Parameter(Mandatory=$true)][string]$StateDirectory,
    [string]$Name = 'ReqflowScheduler',
    [ValidateRange(1, 1440)][int]$EveryMinutes = 1
)
$schedulerRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$pythonPath = (Resolve-Path -LiteralPath $Python).Path
$taskPath = (Resolve-Path -LiteralPath $TaskFile).Path
$statePath = [IO.Path]::GetFullPath($StateDirectory)
# Return a reviewable task definition; this script never registers or starts it.
$action = New-ScheduledTaskAction -Execute $pythonPath -Argument "-m reqflow_scheduler tick `"$taskPath`" --state-dir `"$statePath`"" -WorkingDirectory $schedulerRoot
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes $EveryMinutes)
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Days 7)
$task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
Export-ScheduledTask -InputObject $task
