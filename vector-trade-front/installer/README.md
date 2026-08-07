# Instalador y auto-update (Velopack)

Reemplaza al flujo viejo de `ConfigGenerator` (descargar un zip sin verificar hash,
descomprimirlo sobre la propia instalacion y recrear el `.lnk` con PowerShell).

## Pipeline

```
codigo -> mvn -Pwindows gluonfx:build -> VectorTrade.exe (native-image)
       -> vpk pack --signTemplate sign-file.cmd (SSL.com eSigner)
       -> feed firmado (releases.win.json + .nupkg + Setup.exe)
       -> scp a 172.16.0.8:/home/azureuser/apps/VECTOR-TRADE-2.0/update/<canal>
       -> nginx vt2-update lo sirve en http://172.16.0.8:8101/
       -> VelopackUpdater lee releases.win.json al arrancar
```

Todo el ciclo de release vive en 172.16.0.8 (el build es lo unico que no puede: el exe
nativo sale de native-image sobre MSVC, o sea una maquina Windows).

| Ruta | |
|---|---|
| `http://172.16.0.8:8101/` | pagina de descarga; se arma sola leyendo los feeds |
| `http://172.16.0.8:8101/qa/` | feed QA (`VectorTradeQA`) |
| `http://172.16.0.8:8101/prod/` | feed produccion (`VectorTrade2`) |
| `http://172.16.0.8:8101/archive/` | cada release publicado, navegable, para rollback |

**Promover QA a produccion es recompilar, no copiar.** El packId, la URL del feed y los
endpoints WebSocket quedan horneados en el exe nativo, asi que el binario de QA no puede
convertirse en el de produccion. Sobre el mismo commit ya validado en QA:

```powershell
# 1. application.production.properties -> version=3.1.8
.\installer\windows\build-release.ps1 -Version 3.1.8 -Env production
```

El `archive/` sirve para rollback dentro de un mismo canal, no para mover entre canales.

## Un release

```powershell
# 1. subir la version en el properties del entorno (fuente unica de verdad)
#    src\main\resources\blotter\enviroment\application.qa.properties -> version=3.1.8
# 2. build + pack + publish
.\installer\windows\build-release.ps1 -Version 3.1.8 -Env qa
```

El script aborta si `-Version` no coincide con `version=` del properties: si no coinciden,
la app se actualiza, arranca, se ve mas vieja que el feed y se vuelve a actualizar en loop.

Para probar sin firmar ni publicar: `-NoSign -NoPublish`.

## Identidades

| Entorno | packId / `application` | Feed |
|---|---|---|
| qa | `VectorTradeQA` | `http://172.16.0.8:8101/updatevt2` |
| production | `VectorTrade2` | pendiente de definir |

El `packId` debe coincidir con la property `application`, porque `VelopackUpdater` lo usa
para ubicar `%LOCALAPPDATA%\{packId}\Update.exe`. El script valida ambos.

Instalar QA no pisa una instalacion de produccion: distinto directorio, distinto acceso
directo y distinto `credentials.enc`.

## Requisitos de la maquina de build

- JDK 21 + Maven, GraalVM gluon, entorno MSVC x64 (`vcvars64.bat`, o pasar `-VcVars`)
- .NET SDK 8+ y `dotnet tool install -g vpk`
- Para firmar: `CODESIGNTOOL_HOME` y los secretos `SSL_USERNAME`, `SSL_PASSWORD`,
  `SSL_TOTP_SECRET`, `SSL_CREDENTIAL_ID` (SSL.com eSigner). Nunca en el repo.

## Migracion

`MainApp` chequea `VelopackUpdater.isInstalled()`: las instalaciones nuevas usan el updater
nuevo, las viejas siguen con `ConfigGenerator` hasta que se reinstalen con el Setup.exe.
`MainAppQa` usa Velopack directo, porque QA no tenia updater activo.
