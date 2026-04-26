$ErrorActionPreference = "Stop"

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$hadoopBin = Join-Path $projectRoot "hadoop\bin"
New-Item -ItemType Directory -Force -Path $hadoopBin | Out-Null

$files = @{
    "winutils.exe" = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
    "hadoop.dll" = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
}

foreach ($name in $files.Keys) {
    $target = Join-Path $hadoopBin $name
    if (Test-Path $target) {
        Write-Host "Already exists: $target"
        continue
    }

    Write-Host "Downloading $name..."
    Invoke-WebRequest -Uri $files[$name] -OutFile $target
}

Write-Host ""
Write-Host "Windows Hadoop binaries are ready in $hadoopBin"
Write-Host "The pipeline will set HADOOP_HOME and PATH automatically when it runs."
