# vector-trade-mobile

Cliente movil (Android / iOS) de Vector Trade 2.0. Proyecto Flutter independiente:
no comparte codigo ni build con `vector-trade-front` (escritorio JavaFX), solo el
protocolo del OMS.

## Alcance actual

- Login contra el canal `service` del OMS.
- Blotter de ordenes en tiempo real, calcado de `RoutingView.fxml` +
  `Executions.fxml`: barra de filtros (Cuenta / Destino / Estado / Tipo /
  Instrumento), pestanas *Trabajando* y *Ejecutadas*, tabla con las mismas
  columnas y colores del escritorio, y barra de acciones con Modificar y
  Cancelar sobre la fila seleccionada.

La app va **bloqueada en horizontal** (`sensorLandscape` en Android,
`UISupportedInterfaceOrientations` solo landscape en iOS): la tabla del blotter
no cabe en vertical.

## Protocolo

Mismo transporte que el escritorio (`SimpleWebSocketListener` + `MessageUtilVT`):

- WebSocket binario contra `ws://<host>/websocket/`.
- Autenticacion en el upgrade: `Authorization: Basic base64(AES(user):AES(pass))`,
  AES/ECB/PKCS5 con la clave estatica de `AESEncryption` (Java). `test/crypto_test.dart`
  fija el valor de referencia producido por Java: si falla, el OMS responde 401.
- Framing: 1 byte con el id de `TopicIdentifierVT` + payload protobuf.
- Al conectar y en cada reconexion se envia `Connect{username}` (topico 21).

## Protos

Los `.pb.dart` de `lib/proto/` se generan desde `../principal-module/protos`:

```bash
protoc --dart_out=lib/proto blotter.proto marketdata.proto notification.proto routing.proto sessions.proto
```

Requiere `protobuf-compiler` y `dart pub global activate protoc_plugin`.
Regenerarlos cada vez que cambie un `.proto` del backend.

## Ambientes

`lib/config.dart` replica los endpoints de
`vector-trade-front/src/main/resources/blotter/enviroment/application.*.properties`.

## Build

```bash
flutter build apk --release
```

```bash
flutter build ipa
```
