import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:protobuf/protobuf.dart';

/// Ids de TopicIdentifierVT (principal-module .../ws/vectortrade/TopicIdentifierVT.java).
/// El framing es 1 byte de topico + payload protobuf.
class Topic {
  static const trade = 0;
  static const order = 3;
  static const orderReplaceRequest = 7;
  static const orderCancelRequest = 8;
  static const ping = 9;
  static const portfolioResponse = 13;
  static const notification = 16;
  static const disconnect = 20;
  static const connect = 21;
  static const pong = 27;
  static const userList = 28;
  static const user = 29;
  static const newOrderRequest = 30;
  static const portfolioRequest = 31;
}

/// Cliente del canal `service` del OMS. Mismo transporte que
/// SimpleWebSocketListener del escritorio: websocket binario + reconexion.
class VtSocket {
  static const _reconnectDelay = Duration(seconds: 7);
  static const _connectTimeout = Duration(seconds: 8);

  final String url;
  final String authorization;
  final void Function(int topic, Uint8List payload) onFrame;
  final void Function(bool connected) onStatus;

  WebSocket? _socket;
  Timer? _retry;
  bool _closedByUser = false;

  VtSocket({
    required this.url,
    required this.authorization,
    required this.onFrame,
    required this.onStatus,
  });

  bool get isConnected => _socket?.readyState == WebSocket.open;

  /// Primer intento; propaga el error para que el login muestre el motivo.
  Future<void> connect() async {
    _closedByUser = false;
    await _open();
  }

  Future<void> _open() async {
    final socket = await WebSocket.connect(
      url,
      headers: {'Authorization': authorization},
    ).timeout(_connectTimeout);

    _socket = socket;
    onStatus(true);

    socket.listen(
      (data) {
        if (data is List<int> && data.isNotEmpty) {
          final bytes = Uint8List.fromList(data);
          onFrame(bytes[0], Uint8List.sublistView(bytes, 1));
        }
      },
      onDone: _scheduleReconnect,
      onError: (_) => _scheduleReconnect(),
      cancelOnError: true,
    );
  }

  void send(int topic, GeneratedMessage message) {
    final socket = _socket;
    if (socket == null || socket.readyState != WebSocket.open) return;
    final payload = message.writeToBuffer();
    final frame = Uint8List(payload.length + 1);
    frame[0] = topic;
    frame.setRange(1, frame.length, payload);
    socket.add(frame);
  }

  void _scheduleReconnect() {
    _socket = null;
    onStatus(false);
    if (_closedByUser || _retry != null) return;

    _retry = Timer.periodic(_reconnectDelay, (timer) async {
      try {
        await _open();
        timer.cancel();
        _retry = null;
      } catch (_) {
        // siguiente tick
      }
    });
  }

  void close() {
    _closedByUser = true;
    _retry?.cancel();
    _retry = null;
    _socket?.close();
    _socket = null;
    onStatus(false);
  }
}
