import 'package:fixnum/fixnum.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:protobuf/well_known_types/google/protobuf/timestamp.pb.dart';
import 'package:vector_trade_mobile/proto/routing.pb.dart' as r;
import 'package:vector_trade_mobile/store.dart';

r.Order order({
  required String id,
  String symbol = 'SQM-B',
  r.OrderStatus status = r.OrderStatus.NEW,
  r.ExecutionType execType = r.ExecutionType.EXEC_NEW,
  String execId = '',
  String basketID = '',
  double orderQty = 100,
  int seconds = 1000,
}) => r.Order(
  id: id,
  symbol: symbol,
  ordStatus: status,
  execType: execType,
  execId: execId,
  basketID: basketID,
  orderQty: orderQty,
  time: Timestamp(seconds: Int64(seconds)),
);

void main() {
  late BlotterStore store;

  setUp(() => store = BlotterStore());
  tearDown(() => store.dispose());

  // Las reglas replicadas de ClientActor.onOrder del escritorio.
  group('applyOrder', () {
    test('hace upsert por id en vez de duplicar', () {
      store.applyOrder(order(id: '1', orderQty: 100));
      store.applyOrder(order(id: '1', orderQty: 250));

      expect(store.working, hasLength(1));
      expect(store.working.single.orderQty, 250);
    });

    test('no pisa una orden que ya esta en estado terminal', () {
      for (final terminal in [
        r.OrderStatus.FILLED,
        r.OrderStatus.CANCELED,
        r.OrderStatus.REJECTED,
        r.OrderStatus.DONE_FOR_DAY,
        r.OrderStatus.STOPPED,
      ]) {
        final s = BlotterStore();
        s.applyOrder(order(id: '1', status: terminal, orderQty: 100));
        s.applyOrder(order(id: '1', status: r.OrderStatus.NEW, orderQty: 999));

        expect(
          s.working.single.ordStatus,
          terminal,
          reason: '$terminal deberia quedar congelada',
        );
        expect(s.working.single.orderQty, 100);
        s.dispose();
      }
    });

    test('deja pasar actualizaciones sobre estados no terminales', () {
      store.applyOrder(order(id: '1', status: r.OrderStatus.NEW));
      store.applyOrder(order(id: '1', status: r.OrderStatus.FILLED));

      expect(store.working.single.ordStatus, r.OrderStatus.FILLED);
    });

    test('excluye las ordenes de canasta', () {
      store.applyOrder(order(id: '1', basketID: 'BKT-9'));

      expect(store.working, isEmpty);
      expect(store.executions, isEmpty);
    });

    test('manda a Ejecutadas solo los EXEC_TRADE, deduplicando por execId', () {
      store.applyOrder(order(id: '1', execType: r.ExecutionType.EXEC_NEW));
      expect(store.executions, isEmpty);

      store.applyOrder(
        order(id: '1', execType: r.ExecutionType.EXEC_TRADE, execId: 'E1'),
      );
      store.applyOrder(
        order(id: '1', execType: r.ExecutionType.EXEC_TRADE, execId: 'E1'),
      );
      store.applyOrder(
        order(id: '1', execType: r.ExecutionType.EXEC_TRADE, execId: 'E2'),
      );

      expect(store.executions, hasLength(2));
    });

    test('ordena por hora descendente', () {
      store.applyOrder(order(id: '1', seconds: 100));
      store.applyOrder(order(id: '2', seconds: 300));
      store.applyOrder(order(id: '3', seconds: 200));

      expect(store.working.map((o) => o.id), ['2', '3', '1']);
    });

    test('notifica a los listeners en cada orden', () {
      var notifications = 0;
      store.addListener(() => notifications++);

      store.applyOrder(order(id: '1'));
      store.applyOrder(order(id: '2'));

      expect(notifications, 2);
    });
  });
}
