# vector-candle-service

Servicio WebSocket independiente para publicar velas desde MongoDB por `symbol` + `timeframe`.

## Ejecutar

```bash
mvn clean package
java -jar target/vector-candle-service-1.0.0.jar src/main/resources/application.properties
```

## Endpoint

- WebSocket: `ws://localhost:8098/ws/`

## Acciones WebSocket

- `{"action":"subscribe","symbol":"SQM","timeframe":"1m"}`
- `{"action":"unsubscribe","symbol":"SQM","timeframe":"1m"}`
