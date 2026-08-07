@echo off
setlocal EnableExtensions DisableDelayedExpansion
REM ---------------------------------------------------------------------------
REM  sign-file.cmd  -  puente compatible con Velopack (--signTemplate).
REM                    PowerShell ejecuta el JAR sin reparsar secrets con cmd.
REM  Velopack invoca:  installer\windows\sign-file.cmd <ruta-al-archivo>
REM  Requiere en el entorno: CODESIGNTOOL_HOME y los secrets SSL_* (eSigner).
REM ---------------------------------------------------------------------------

if "%~1"=="" (
  echo ERROR: No se recibio el archivo que se debe firmar.
  exit /b 2
)

set "SIGN_INPUT=%~f1"
set "SIGN_SCRIPT=%~dp0sign-file.ps1"

if not exist "%SIGN_INPUT%" (
  echo ERROR: No existe el archivo que se debe firmar: "%SIGN_INPUT%"
  exit /b 2
)

if not exist "%SIGN_SCRIPT%" (
  echo ERROR: No existe "%SIGN_SCRIPT%".
  exit /b 2
)

powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass ^
  -File "%SIGN_SCRIPT%" ^
  -InputFilePath "%SIGN_INPUT%"

if not "%ERRORLEVEL%"=="0" (
  echo ERROR: El proceso seguro de firma fallo.
  exit /b 1
)

exit /b 0
