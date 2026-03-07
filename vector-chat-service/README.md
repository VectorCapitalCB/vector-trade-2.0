# vector-chat-service

Servicio WebSocket independiente para chat entre usernames con historial en MongoDB.

## Ejecutar

```bash
mvn clean package
java -jar target/vector-chat-service-1.0.0.jar src/main/resources/application.properties
```

## Endpoints

- Vista: `http://localhost:8097/`
- WebSocket: `ws://localhost:8097/ws/`
