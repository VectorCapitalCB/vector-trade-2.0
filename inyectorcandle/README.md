# inyectorcandle

Servicio Java para BCS FIX 4.4 que:

- Se conecta como `initiator` con QuickFIX/J.
- Solicita `SecurityList` completa.
- Se suscribe a market data de cada instrumento recibido.
- Crea un actor por instrumento con clave:
  - `symbol + settlement + destination + currency`
- Procesa `MarketDataSnapshotFullRefresh (W)` y `MarketDataIncrementalRefresh (X)`.
- Construye velas multi-timeframe.
- Calcula estadísticas de transado (top/bottom por monto y volumen).
- Inyecta todo a MongoDB.

## Requisitos

- Java 17
- Maven 3.9+
- Acceso de red a FIX BCS y Mongo

## Configuración

1. Editar `src/main/resources/fix/bcs-initiator.cfg`
- `SenderCompID`
- `SocketConnectHost`/`SocketConnectPort`
- `DataDictionary`

2. Editar `src/main/resources/application.properties`
- `mongo.uri`
- `mongo.database`
- `fix.logon.rawData` (si tu gateway lo requiere)
- `fix.logon.username`/`fix.logon.password` (opcional)

Tambien puedes pasar un `application.properties` externo por argumento al jar:
- `java -jar target/inyectorcandle-1.0.0-fat.jar /ruta/application.properties`
- Si no envias argumento, usa `application.properties` del classpath como fallback.

Tambien puedes sobreescribir por entorno:
- `FIX_CONFIG_FILE`
- `MONGO_URI`
- `MONGO_DATABASE`
- `FIX_LOGON_RAWDATA`
- `FIX_LOGON_USERNAME`
- `FIX_LOGON_PASSWORD`
- `FIX_PROCESS_SNAPSHOTS`
- `FIX_PROCESS_SNAPSHOT_TRADES`
- `REPLAY_ENABLED`
- `REPLAY_INPUT_PATH`
- `REPLAY_LOG_ZONE`
- `REPLAY_SLEEP_MS`
- `REPLAY_MAX_LINES`
- `REPLAY_PRESERVE_TIMING`
- `REPLAY_TIMING_SPEED`
- `REPLAY_TIMING_MAX_SLEEP_MS`
- `REPLAY_PURGE_DAY_BEFORE_INJECT`

## Replay desde logs FIX historicos

Puedes reconstruir velas desde archivos `.log` o `.log.gz`:

1. Configura:
   - `replay.enabled=true`
   - `replay.input.path=E:/VC-GITHUB/vector-trade-2.0/log-history` (archivo o carpeta)
   - `replay.log.zone=America/Santiago`
2. Ejecuta `mvn exec:java`

Notas:
- El replay usa la hora al comienzo de cada linea del log (no `tag 52`) como `eventTime`.
- Indexa `MDReqID (262)` desde `35=V` y usa ese indice al parsear respuestas `35=W/35=X`.
- Interpreta `466` de liquidacion y mapea:
  - `||| -> T2`
  - `PH||| -> CASH`
  - `PM||| -> NEXT_DAY`
  - `T+3||| -> T3`
  - `T+5||| -> T5`
- Procesa mensajes `35=W` y `35=X`, tomando entradas de trade (`269=2` o `269=B`) para construir velas.
- Si `replay.input.path` es carpeta, procesa archivos en orden alfabetico.
- Si `replay.preserve.timing=true`, duerme por delta entre timestamps de linea (acelerable con `replay.timing.speed` y cap `replay.timing.max.sleep.ms`).
- Si `replay.purge.day.before.inject=true`, limpia primero ese dia en Mongo (`md_events`, `trades`, `candles`) antes de reinyectar.

## Ejecutar

```bash
mvn clean test
mvn exec:java
java -jar target/inyectorcandle-1.0.0-fat.jar /ruta/application.properties
```

## Colecciones Mongo

- `securities`: SecurityList normalizada por clave compuesta.
- `md_events`: entradas de market data (book/estadisticos/ticks).
- `trades`: trades detectados desde FIX.
- `candles`: OHLCV por instrumento/timeframe/bucket.
- `instrument_stats`: acumulados por actor.
- `market_rankings`: ranking global (top/bottom) recalculado periodicamente.

## Notas

- Para separar correctamente cada papel por liquidacion/destino/moneda, la clave interna es siempre `symbol|settlement|destination|currency`.
- Si el feed incremental no trae `MDReqID`, el parser intenta reconstruir clave desde campos del propio mensaje.
- `fix.subscription.pause.ms` permite evitar burst extremo al suscribirse a toda la lista.
- `fix.process.snapshots=false` ignora completamente `35=W`.
- `fix.process.snapshot.trades=false` procesa `35=W` pero no inserta sus trades, evitando duplicar historicos al reiniciar.
