param(
    [Parameter(Mandatory = $true)]
    [string]$InputFilePath
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($env:CODESIGNTOOL_HOME)) {
    throw "CODESIGNTOOL_HOME no esta configurado en el runner."
}

$requiredSecrets = @{
    SSL_USERNAME = $env:SSL_USERNAME
    SSL_PASSWORD = $env:SSL_PASSWORD
    SSL_TOTP_SECRET = $env:SSL_TOTP_SECRET
    SSL_CREDENTIAL_ID = $env:SSL_CREDENTIAL_ID
}

$missingSecrets = @(
    $requiredSecrets.GetEnumerator() |
        Where-Object { [string]::IsNullOrWhiteSpace($_.Value) } |
        ForEach-Object { $_.Key }
)

if ($missingSecrets.Count -gt 0) {
    throw "Faltan variables de firma: $($missingSecrets -join ', ')"
}

$toolHome = [System.IO.Path]::GetFullPath($env:CODESIGNTOOL_HOME)
$java = Join-Path $toolHome "jdk-11.0.2\bin\java.exe"
$config = Join-Path $toolHome "conf\code_sign_tool.properties"
$jar = Get-ChildItem -LiteralPath (Join-Path $toolHome "jar") `
    -Filter "code_sign_tool-*.jar" -File |
    Select-Object -First 1

if (!(Test-Path -LiteralPath $java -PathType Leaf)) {
    throw "No se encontro el Java incluido en CodeSignTool."
}
if (!(Test-Path -LiteralPath $config -PathType Leaf)) {
    throw "Falta conf\code_sign_tool.properties."
}
if ($null -eq $jar) {
    throw "No se encontro el JAR de CodeSignTool."
}

$inputFile = (Get-Item -LiteralPath $InputFilePath -ErrorAction Stop).FullName
$signArguments = @(
    "-jar"
    $jar.FullName
    "sign"
    "-username=$($env:SSL_USERNAME)"
    "-password=$($env:SSL_PASSWORD)"
    "-totp_secret=$($env:SSL_TOTP_SECRET)"
    "-credential_id=$($env:SSL_CREDENTIAL_ID)"
    "-input_file_path=$inputFile"
    "-override"
)

Write-Host "Firmando con SSL.com: $([System.IO.Path]::GetFileName($inputFile))"

Push-Location $toolHome
try {
    & $java @signArguments
    $signExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}

if ($signExitCode -ne 0) {
    throw "SSL.com CodeSignTool fallo con codigo $signExitCode."
}
