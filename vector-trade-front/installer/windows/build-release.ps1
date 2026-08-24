<#
  build-release.ps1 - Compila el exe nativo, lo empaqueta con Velopack y publica el feed.

  Reemplaza al flujo viejo de ConfigGenerator (zip sin verificar + .lnk por PowerShell).
  Genera un instalador Setup.exe firmado y un feed releases.win.json que la app consume
  desde VelopackUpdater.

  Uso:
    .\installer\windows\build-release.ps1 -Version 2.0.0 -Env production
    .\installer\windows\build-release.ps1 -Version 2.0.0 -Env production -NoSign -NoPublish

  Requisitos en esta maquina:
    - JDK 21 + Maven                         (ya)
    - GraalVM gluon + entorno MSVC x64       (vcvars64.bat cargado, o pasar -VcVars)
    - .NET SDK 8+  ->  dotnet tool install -g vpk
    - Para firmar: CODESIGNTOOL_HOME + SSL_USERNAME / SSL_PASSWORD / SSL_TOTP_SECRET /
      SSL_CREDENTIAL_ID en el entorno (SSL.com eSigner).

  La version DEBE coincidir con la property 'version' del application.<env>.properties;
  el script lo valida, porque si no coinciden la app entra en loop de actualizacion.
#>
param(
  [Parameter(Mandatory = $true)][string]$Version,
  [ValidateSet('qa', 'production')][string]$Env = 'qa',
  [string]$VcVars,
  [switch]$NoSign,
  [switch]$NoPublish
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Set-Location $repo

if ($Version -notmatch '^\d+(\.\d+){1,3}$') { throw "Version invalida: '$Version' (se espera X.Y.Z)" }

# --- identidad por entorno ---------------------------------------------------
$cfg = @{
  qa         = @{ PackId = 'VectorTradeQA'; Title = 'Vector Trade QA'; MainClass = 'cl.vc.blotter.MainAppQa' }
  production = @{ PackId = 'VectorTrade2';  Title = 'Vector Trade 2.0'; MainClass = 'cl.vc.blotter.MainApp' }
}[$Env]

$propsFile = Join-Path $repo "src\main\resources\blotter\enviroment\application.$Env.properties"
if (!(Test-Path $propsFile)) { throw "No existe $propsFile" }

# La version del properties queda horneada en el exe y es con la que el updater compara.
$declared = (Select-String -Path $propsFile -Pattern '^version=(.+)$').Matches.Groups[1].Value.Trim()
if ($declared -ne $Version) {
  throw "version=$declared en $([System.IO.Path]::GetFileName($propsFile)) pero pediste -Version $Version. " +
        "Actualiza el properties primero o la app quedara en loop de update."
}
$packIdMatch = Select-String -Path $propsFile -Pattern '^update\.packId=(.+)$'
$declaredPackId = if ($packIdMatch) {
  $packIdMatch.Matches.Groups[1].Value.Trim()
} else {
  (Select-String -Path $propsFile -Pattern '^application=(.+)$').Matches.Groups[1].Value.Trim()
}
if ($declaredPackId -ne $cfg.PackId) {
  throw "application=$declaredPackId en el properties pero el packId de $Env es $($cfg.PackId). Deben coincidir."
}

# --- preflight ---------------------------------------------------------------
if (-not (Get-Command vpk -ErrorAction SilentlyContinue)) {
  throw "Falta 'vpk'. Instalalo con:  dotnet tool install -g vpk   (requiere .NET SDK 8+)"
}
if (-not $NoSign) {
  foreach ($v in 'CODESIGNTOOL_HOME', 'SSL_USERNAME', 'SSL_PASSWORD', 'SSL_TOTP_SECRET', 'SSL_CREDENTIAL_ID') {
    if (-not (Get-Item "Env:$v" -ErrorAction SilentlyContinue)) {
      throw "Falta la variable de entorno $v (necesaria para firmar). Usa -NoSign para omitir la firma."
    }
  }
}

# --- 1. build nativo ---------------------------------------------------------
Write-Host "[1/4] gluonfx:build  ($Env, mainClass=$($cfg.MainClass))" -ForegroundColor Cyan
$mvn = "mvn -B -Pwindows -Dmain.class=$($cfg.MainClass) gluonfx:build"
if ($VcVars) {
  cmd /c "call `"$VcVars`" && $mvn"
} else {
  cmd /c $mvn
}
if ($LASTEXITCODE -ne 0) { throw "Fallo el build nativo. Si es error de linker, carga vcvars64.bat o pasa -VcVars." }

$exe = Join-Path $repo 'target\gluonfx\x86_64-windows\VectorTrade.exe'
if (!(Test-Path $exe)) { throw "No se genero $exe" }

# --- 2. staging --------------------------------------------------------------
Write-Host "[2/4] staging" -ForegroundColor Cyan
$stage = Join-Path $repo 'target\velopack\stage'
$out   = Join-Path $repo 'target\velopack\releases'
Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $stage -Force | Out-Null
New-Item -ItemType Directory -Path $out -Force | Out-Null
Copy-Item $exe (Join-Path $stage 'VectorTrade.exe')

# --- 3. vpk pack -------------------------------------------------------------
Write-Host "[3/4] vpk pack $($cfg.PackId) $Version" -ForegroundColor Cyan
$vpkArgs = @(
  'pack'
  '--packId', $cfg.PackId
  '--packTitle', $cfg.Title
  '--packVersion', $Version
  '--packDir', $stage
  '--mainExe', 'VectorTrade.exe'
  '--icon', (Join-Path $repo 'src\windows\assets\icon.ico')
  '--shortcuts', 'Desktop,StartMenuRoot'
  '--outputDir', $out
)
if (-not $NoSign) {
  $vpkArgs += '--signTemplate'
  $vpkArgs += ('"{0}" {{{{file}}}}' -f (Join-Path $PSScriptRoot 'sign-file.cmd'))
}
& vpk @vpkArgs
if ($LASTEXITCODE -ne 0) { throw "vpk pack fallo" }

Get-ChildItem $out | Format-Table Name, Length -AutoSize

# --- 4. publicar -------------------------------------------------------------
if ($NoPublish) {
  Write-Host "[4/4] -NoPublish: el feed quedo en $out" -ForegroundColor Yellow
  return
}

$server   = 'voultech@172.16.0.6'
$root     = '/home/voultech/app/VectorTrade2.0/updatefeed'
$incoming = "$root/.incoming-$Version"

Write-Host "[4/4] publicando en el feed interno de 172.16.0.6" -ForegroundColor Cyan
& ssh -o StrictHostKeyChecking=accept-new $server "rm -rf '$incoming' && mkdir -p '$incoming'"
if ($LASTEXITCODE -ne 0) { throw "no se pudo preparar el destino" }

& scp -o StrictHostKeyChecking=accept-new -r "$out\*" "${server}:$incoming/"
if ($LASTEXITCODE -ne 0) { throw "scp al servidor fallo" }

& ssh -o StrictHostKeyChecking=accept-new $server "FEED_ROOT='$root/releases' BACKUP_DIR='$root/backups' bash '$root/publish-feed.sh' --src '$incoming' && rm -rf '$incoming'"
if ($LASTEXITCODE -ne 0) { throw "publish-feed.sh fallo" }

$feed = "http://172.16.0.6:8092/updatevtautoupdate/releases.win.json"
Write-Host "Listo." -ForegroundColor Green
Write-Host "  feed     $feed"
Write-Host "  descarga http://172.16.0.6:8092/"
Write-Host "  archive  http://172.16.0.6:8092/updatevtautoupdate/archive/$Version/"
try {
  (Invoke-WebRequest -Uri $feed -TimeoutSec 10 -UseBasicParsing).Content | Write-Host
} catch {
  Write-Warning "El feed no respondio todavia: $_"
}
