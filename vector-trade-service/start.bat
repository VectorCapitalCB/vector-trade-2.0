@echo off
cd /d "%~dp0"

echo ============================================
echo  Vector Trade Service - CORE
echo  Main: cl.vc.service.MainApp
echo  Config: src\main\resources\application.properties
echo ============================================
echo.

call mvn compile exec:java ^
    -Dexec.mainClass=cl.vc.service.MainApp ^
    -Dexec.args="src\main\resources\application.properties" ^
    -Dexec.cleanupDaemonThreads=false

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] El servicio termino con codigo: %ERRORLEVEL%
    pause
)
