# vector-news-service

Servicio WebSocket independiente para noticias financieras en tiempo real.

- Scraping/RSS de fuentes financieras (SEC, FED, etc).
- Filtrado por relevancia (Chile, EEUU, USD/CLP, mercados).
- Persistencia en MongoDB.
- Broadcast en vivo por WebSocket + snapshot historico al conectar.

## Ejecutar

```bash
mvn clean package
java -jar target/vector-news-service-1.0.0.jar src/main/resources/application.properties
```

## Fat Jar

```bash
mvn clean package -Pdistribution
java -jar target/vector-news-service-1.0.0.jar src/main/resources/application.properties
```

## Endpoint

- WebSocket: `ws://localhost:8099/ws/`