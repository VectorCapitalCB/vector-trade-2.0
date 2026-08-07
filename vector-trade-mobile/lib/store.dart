import 'package:flutter/foundation.dart';

import 'config.dart';
import 'crypto.dart';
import 'proto/notification.pb.dart' as notif;
import 'proto/routing.pb.dart' as routing;
import 'proto/sessions.pb.dart' as sessions;
import 'ws_client.dart';

const _terminalStatus = {
  routing.OrderStatus.FILLED,
  routing.OrderStatus.CANCELED,
  routing.OrderStatus.REJECTED,
  routing.OrderStatus.DONE_FOR_DAY,
  routing.OrderStatus.STOPPED,
};

/// Estado de sesion y blotter. Replica la logica de ClientActor.onOrder
/// del escritorio: upsert por id, sin pisar ordenes ya terminales.
class BlotterStore extends ChangeNotifier {
  VtSocket? _socket;

  String username = '';
  String envKey = Env.all.first.key;
  bool connected = false;
  String? lastNotification;

  final Map<String, routing.Order> _working = {};
  final Map<String, routing.Order> _executions = {};

  List<routing.Order> get working =>
      _working.values.toList(growable: false)
        ..sort((a, b) => b.time.seconds.compareTo(a.time.seconds));

  List<routing.Order> get executions =>
      _executions.values.toList(growable: false)
        ..sort((a, b) => b.time.seconds.compareTo(a.time.seconds));

  Future<void> login(String user, String password, String env) async {
    username = user.replaceAll(' ', '').toLowerCase();
    envKey = env;

    _socket?.close();
    _working.clear();
    _executions.clear();

    final socket = VtSocket(
      url: Env.byKey(env).service,
      authorization: AesVt.basicAuth(username, password),
      onFrame: _onFrame,
      onStatus: _onStatus,
    );
    await socket.connect();
    _socket = socket;
    _sendConnect();
  }

  void logout() {
    _socket?.close();
    _socket = null;
    _working.clear();
    _executions.clear();
    username = '';
    notifyListeners();
  }

  void cancelOrder(String id) {
    _socket?.send(Topic.orderCancelRequest, routing.OrderCancelRequest(id: id));
  }

  void replaceOrder({
    required String id,
    required double quantity,
    required double price,
    double maxFloor = 0,
  }) {
    _socket?.send(
      Topic.orderReplaceRequest,
      routing.OrderReplaceRequest(
        id: id,
        quantity: quantity,
        price: price,
        maxFloor: maxFloor,
      ),
    );
  }

  void _sendConnect() {
    _socket?.send(Topic.connect, sessions.Connect(username: username));
  }

  void _onStatus(bool isConnected) {
    connected = isConnected;
    if (isConnected) _sendConnect();
    notifyListeners();
  }

  void _onFrame(int topic, Uint8List payload) {
    switch (topic) {
      case Topic.order:
        applyOrder(routing.Order.fromBuffer(payload));
        break;
      case Topic.notification:
        final n = notif.Notification.fromBuffer(payload);
        lastNotification = '${n.title}: ${n.comments}';
        notifyListeners();
        break;
      case Topic.ping:
        final ping = sessions.Ping.fromBuffer(payload);
        _socket?.send(Topic.pong, sessions.Pong(id: ping.id));
        break;
    }
  }

  /// Publico para poder testear la regla sin levantar un websocket.
  void applyOrder(routing.Order order) {
    // Las ordenes de canasta viven en su propia vista en el escritorio.
    if (order.basketID.isNotEmpty) return;

    final previous = _working[order.id];
    if (previous == null || !_terminalStatus.contains(previous.ordStatus)) {
      _working[order.id] = order;
    }

    if (order.execType == routing.ExecutionType.EXEC_TRADE) {
      _executions.putIfAbsent(order.execId, () => order);
    }

    notifyListeners();
  }

  @override
  void dispose() {
    _socket?.close();
    super.dispose();
  }
}
