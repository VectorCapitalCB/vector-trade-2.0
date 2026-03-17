@echo off
setlocal

cd /d "%~dp0"

where mvn >nul 2>nul
if errorlevel 1 (
    echo Maven no esta disponible en PATH.
    echo Instala Maven o abre una terminal donde `mvn` funcione.
    exit /b 1
)

if not "%JAVA_HOME%"=="" (
    echo JAVA_HOME=%JAVA_HOME%
)

echo Iniciando VectorTrade...
echo.

mvn -DskipTests -Dmain.class=cl.vc.blotter.MainApp javafx:run
set EXIT_CODE=%ERRORLEVEL%

echo.
if not "%EXIT_CODE%"=="0" (
    echo La aplicacion termino con error %EXIT_CODE%.
) else (
    echo La aplicacion finalizo correctamente.
)

exit /b %EXIT_CODE%
