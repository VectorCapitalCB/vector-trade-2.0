import 'package:flutter/material.dart';
import 'package:intl/intl.dart';

import '../proto/routing.pb.dart' as routing;
import '../store.dart';
import '../theme.dart';
import 'login_screen.dart';

final _qty = NumberFormat('#,##0.####', 'es_CL');
final _px = NumberFormat('#,##0.####', 'es_CL');
final _hhmmssSSS = DateFormat('HH:mm:ss.SSS');

/// Estado traducido y coloreado igual que createStatusColumnStyleCallback
/// de ExecutionsController.
({String text, Color? color}) _status(routing.OrderStatus s) => switch (s) {
  routing.OrderStatus.NEW => (text: 'NUEVA', color: VT.statusNew),
  routing.OrderStatus.FILLED => (text: 'CALZADA', color: VT.statusFilled),
  routing.OrderStatus.PARTIALLY_FILLED => (
    text: 'PARCIAL',
    color: VT.statusPartial,
  ),
  routing.OrderStatus.REJECTED => (text: 'RECHAZADA', color: VT.ask),
  routing.OrderStatus.CANCELED => (text: 'CANCELADA', color: VT.ask),
  routing.OrderStatus.REPLACED => (
    text: 'REMPLAZADA',
    color: VT.statusReplaced,
  ),
  _ => (text: s.name, color: null),
};

Color? _sideColor(routing.Side side) => switch (side) {
  routing.Side.BUY => VT.sideBuy,
  routing.Side.SELL || routing.Side.SELL_SHORT => VT.sideSell,
  _ => null,
};

class _Col {
  final String label;
  final double width;
  final String Function(routing.Order) value;
  final Color? Function(routing.Order)? color;
  final bool numeric;

  const _Col(
    this.label,
    this.width,
    this.value, {
    this.color,
    this.numeric = false,
  });
}

/// Columnas visibles de Executions.fxml, en el mismo orden.
final _columns = <_Col>[
  _Col('Fecha Ingreso', 100, (o) {
    final t = DateTime.fromMillisecondsSinceEpoch(
      o.time.seconds.toInt() * 1000 + o.time.nanos ~/ 1000000,
    );
    return _hhmmssSSS.format(t);
  }),
  _Col('Tipo', 52, (o) => o.side.name, color: (o) => _sideColor(o.side)),
  _Col('Instrumento', 110, (o) => o.symbol),
  _Col('Condición', 74, (o) => o.settlType.name),
  _Col('Mercado', 78, (o) => o.securityExchange.name),
  _Col('T Orden', 70, (o) => o.ordType.name),
  _Col('Cantidad', 82, (o) => _qty.format(o.orderQty), numeric: true),
  _Col('Precio', 82, (o) => _px.format(o.price), numeric: true),
  _Col('Monto', 90, (o) => _qty.format(o.amount), numeric: true),
  _Col('Ult. Precio', 82, (o) => _px.format(o.lastPx), numeric: true),
  _Col('Ult. Cantidad', 88, (o) => _qty.format(o.lastQty), numeric: true),
  _Col('Precio Promedio', 100, (o) => _px.format(o.avgPrice), numeric: true),
  _Col('Cantidad Ejecutada', 116, (o) => _qty.format(o.cumQty), numeric: true),
  _Col('Cantidad Pendiente', 116, (o) => _qty.format(o.leaves), numeric: true),
  _Col('ID', 66, (o) => o.id),
  _Col(
    'Estado',
    88,
    (o) => _status(o.ordStatus).text,
    color: (o) => _status(o.ordStatus).color,
  ),
  _Col('Motivo', 150, (o) => o.text),
  _Col('Cuenta', 84, (o) => o.account),
  _Col('Limit', 70, (o) => _px.format(o.limit), numeric: true),
];

final double _tableWidth = _columns.fold(0, (sum, c) => sum + c.width);

/// Equivalente movil de RoutingView.fxml: barra de filtros, pestanas
/// Trabajando / Ejecutadas y la tabla de Executions.fxml.
class BlotterScreen extends StatefulWidget {
  final BlotterStore store;

  const BlotterScreen({super.key, required this.store});

  @override
  State<BlotterScreen> createState() => _BlotterScreenState();
}

class _BlotterScreenState extends State<BlotterScreen>
    with SingleTickerProviderStateMixin {
  late final TabController _tabs = TabController(length: 2, vsync: this);

  final _account = TextEditingController();
  final _symbol = TextEditingController();
  final _quantity = TextEditingController();
  final _price = TextEditingController();
  final _visible = TextEditingController();

  String _exchange = 'Todos';
  String _statusFilter = 'Todos';
  String _side = 'Todos';
  String? _selectedId;

  @override
  void initState() {
    super.initState();
    widget.store.addListener(_onStoreChanged);
  }

  @override
  void dispose() {
    widget.store.removeListener(_onStoreChanged);
    _tabs.dispose();
    for (final c in [_account, _symbol, _quantity, _price, _visible]) {
      c.dispose();
    }
    super.dispose();
  }

  void _onStoreChanged() => setState(() {});

  List<routing.Order> _apply(List<routing.Order> orders) {
    return orders.where((o) {
      if (_account.text.isNotEmpty &&
          !o.account.toUpperCase().contains(_account.text.toUpperCase())) {
        return false;
      }
      if (_symbol.text.isNotEmpty &&
          !o.symbol.toUpperCase().contains(_symbol.text.toUpperCase())) {
        return false;
      }
      if (_exchange != 'Todos' && o.securityExchange.name != _exchange) {
        return false;
      }
      if (_statusFilter != 'Todos' && o.ordStatus.name != _statusFilter) {
        return false;
      }
      if (_side != 'Todos' && o.side.name != _side) return false;
      return true;
    }).toList();
  }

  routing.Order? get _selected {
    if (_selectedId == null) return null;
    for (final o in widget.store.working) {
      if (o.id == _selectedId) return o;
    }
    return null;
  }

  void _select(routing.Order order) {
    setState(() {
      _selectedId = order.id;
      _quantity.text = _qty.format(order.orderQty);
      _price.text = _px.format(order.price);
      _visible.text = order.maxFloor == 0 ? '' : _qty.format(order.maxFloor);
    });
  }

  double? _parse(TextEditingController c) =>
      double.tryParse(c.text.replaceAll('.', '').replaceAll(',', '.'));

  @override
  Widget build(BuildContext context) {
    final store = widget.store;
    final working = _apply(store.working);
    final executions = _apply(store.executions);

    return Scaffold(
      backgroundColor: VT.bgBottom,
      // El layout es fijo (barras + tabla): si el teclado lo encoge, desborda.
      // Que flote encima, como en el escritorio que no tiene teclado.
      resizeToAvoidBottomInset: false,
      body: SafeArea(
        child: Column(
          // Sin stretch cada barra se encoge al ancho de su contenido.
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            _topBar(store),
            _filterBar(),
            _tabBar(working.length, executions.length),
            Expanded(
              child: TabBarView(
                controller: _tabs,
                children: [
                  _table(working, selectable: true),
                  _table(executions, selectable: false),
                ],
              ),
            ),
            _actionBar(),
          ],
        ),
      ),
    );
  }

  Widget _topBar(BlotterStore store) {
    return Container(
      height: 34,
      color: VT.surface,
      padding: const EdgeInsets.symmetric(horizontal: 8),
      child: Row(
        children: [
          Image.asset('assets/icono.png', height: 18, width: 18),
          const SizedBox(width: 6),
          const Text(
            'Vector Trade 2.0',
            style: TextStyle(fontSize: 12, color: VT.text),
          ),
          const SizedBox(width: 8),
          Container(
            width: 8,
            height: 8,
            decoration: BoxDecoration(
              color: store.connected ? VT.bid : VT.ask,
              shape: BoxShape.circle,
            ),
          ),
          const Spacer(),
          if (store.lastNotification != null)
            Text(
              store.lastNotification!,
              style: const TextStyle(fontSize: 10, color: VT.textDim),
            ),
          const SizedBox(width: 12),
          Text(
            'User: ${store.username}',
            style: const TextStyle(
              fontSize: 11,
              color: VT.gold,
              fontWeight: FontWeight.bold,
            ),
          ),
          IconButton(
            icon: const Icon(Icons.logout, size: 16),
            padding: EdgeInsets.zero,
            constraints: const BoxConstraints(minWidth: 32, minHeight: 32),
            onPressed: () {
              store.logout();
              // El blotter entra con pushReplacement: no hay ruta debajo que
              // desapilar, hay que reemplazar de vuelta por el login.
              Navigator.of(context).pushReplacement(
                MaterialPageRoute(builder: (_) => LoginScreen(store: store)),
              );
            },
          ),
        ],
      ),
    );
  }

  /// ButtonbarFiltros de RoutingView.fxml.
  Widget _filterBar() {
    final exchanges = <String>{
      'Todos',
      for (final o in widget.store.working) o.securityExchange.name,
      for (final o in widget.store.executions) o.securityExchange.name,
    }.toList();
    final statuses = <String>{
      'Todos',
      for (final o in widget.store.working) o.ordStatus.name,
      for (final o in widget.store.executions) o.ordStatus.name,
    }.toList();

    return Container(
      height: 38,
      color: VT.bgTop,
      padding: const EdgeInsets.symmetric(horizontal: 8),
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: Row(
          children: [
            _filterField('Cuenta', _account, 110),
            _filterCombo(
              'Destino',
              _exchange,
              exchanges,
              (v) => setState(() => _exchange = v),
            ),
            _filterCombo(
              'Estado',
              _statusFilter,
              statuses,
              (v) => setState(() => _statusFilter = v),
            ),
            _filterCombo('Tipo', _side, const [
              'Todos',
              'BUY',
              'SELL',
            ], (v) => setState(() => _side = v)),
            _filterField('Instrumento', _symbol, 130),
          ],
        ),
      ),
    );
  }

  Widget _filterField(String label, TextEditingController c, double width) {
    return Padding(
      padding: const EdgeInsets.only(right: 10),
      child: Row(
        children: [
          Text('$label ', style: VT.labelStyle),
          SizedBox(
            width: width,
            height: 26,
            child: TextField(
              controller: c,
              style: const TextStyle(color: VT.textStrong, fontSize: 11),
              decoration: const InputDecoration(
                isDense: true,
                contentPadding: EdgeInsets.symmetric(horizontal: 6),
              ),
              onChanged: (_) => setState(() {}),
            ),
          ),
        ],
      ),
    );
  }

  Widget _filterCombo(
    String label,
    String value,
    List<String> items,
    ValueChanged<String> onChanged,
  ) {
    return Padding(
      padding: const EdgeInsets.only(right: 10),
      child: Row(
        children: [
          Text('$label ', style: VT.labelStyle),
          Container(
            height: 26,
            padding: const EdgeInsets.symmetric(horizontal: 6),
            decoration: BoxDecoration(
              color: VT.field,
              border: Border.all(color: VT.borderSoft),
              borderRadius: BorderRadius.circular(5),
            ),
            child: DropdownButtonHideUnderline(
              child: DropdownButton<String>(
                value: items.contains(value) ? value : items.first,
                isDense: true,
                dropdownColor: VT.field,
                style: const TextStyle(color: VT.textStrong, fontSize: 11),
                items: [
                  for (final i in items)
                    DropdownMenuItem(value: i, child: Text(i)),
                ],
                onChanged: (v) => onChanged(v!),
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _tabBar(int working, int executions) {
    return Container(
      height: 30,
      color: VT.bgTop,
      child: TabBar(
        controller: _tabs,
        isScrollable: true,
        tabAlignment: TabAlignment.start,
        labelColor: VT.textStrong,
        unselectedLabelColor: VT.textDim,
        indicator: const BoxDecoration(color: VT.rowSelected),
        indicatorSize: TabBarIndicatorSize.tab,
        dividerColor: VT.border,
        labelStyle: const TextStyle(fontSize: 12, fontWeight: FontWeight.bold),
        unselectedLabelStyle: const TextStyle(fontSize: 12),
        tabs: [
          Tab(height: 30, text: 'Trabajando ($working)'),
          Tab(height: 30, text: 'Ejecutadas ($executions)'),
        ],
      ),
    );
  }

  Widget _table(List<routing.Order> orders, {required bool selectable}) {
    return Container(
      color: VT.tableBg,
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: SizedBox(
          width: _tableWidth,
          child: Column(
            children: [
              _headerRow(),
              Expanded(
                child: ListView.builder(
                  itemCount: orders.length,
                  itemExtent: 24,
                  itemBuilder: (_, i) =>
                      _dataRow(orders[i], i, selectable: selectable),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _headerRow() {
    return Container(
      height: 26,
      color: VT.tableHeader,
      child: Row(
        children: [
          for (final c in _columns)
            Container(
              width: c.width,
              alignment: c.numeric
                  ? Alignment.centerRight
                  : Alignment.centerLeft,
              padding: const EdgeInsets.symmetric(horizontal: 5),
              decoration: const BoxDecoration(
                border: Border(right: BorderSide(color: VT.border)),
              ),
              child: Text(
                c.label,
                overflow: TextOverflow.ellipsis,
                style: const TextStyle(
                  fontSize: 10.5,
                  color: VT.label,
                  fontWeight: FontWeight.bold,
                ),
              ),
            ),
        ],
      ),
    );
  }

  Widget _dataRow(routing.Order order, int index, {required bool selectable}) {
    final selected = selectable && order.id == _selectedId;
    final background = selected
        ? VT.rowSelected
        : (index.isOdd ? VT.rowOdd : VT.rowEven);

    return GestureDetector(
      onTap: selectable ? () => _select(order) : null,
      child: Container(
        color: background,
        child: Row(
          children: [
            for (final c in _columns)
              Container(
                width: c.width,
                alignment: c.numeric
                    ? Alignment.centerRight
                    : Alignment.centerLeft,
                padding: const EdgeInsets.symmetric(horizontal: 5),
                child: Text(
                  c.value(order),
                  overflow: TextOverflow.ellipsis,
                  style: TextStyle(
                    fontSize: 11,
                    color: c.color?.call(order) ?? VT.text,
                    fontWeight: c.color != null
                        ? FontWeight.bold
                        : FontWeight.normal,
                  ),
                ),
              ),
          ],
        ),
      ),
    );
  }

  /// HBox de acciones de RoutingView.fxml, con lo que soporta el protocolo
  /// portado: modificar y cancelar sobre la orden seleccionada.
  Widget _actionBar() {
    final order = _selected;
    final enabled = order != null;

    return Container(
      height: 40,
      color: VT.bgTop,
      padding: const EdgeInsets.symmetric(horizontal: 8),
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: Row(
          children: [
            Text(
              enabled ? '${order.side.name} ${order.symbol}' : 'Sin selección',
              style: TextStyle(
                fontSize: 11,
                fontWeight: FontWeight.bold,
                color: enabled ? (_sideColor(order.side) ?? VT.text) : VT.hint,
              ),
            ),
            const SizedBox(width: 14),
            _actionField('Cantidad', _quantity),
            _actionField('Precio', _price),
            _actionField('Visible %', _visible),
            _actionButton(
              'Modificar',
              VT.btnTop,
              VT.btnBorder,
              enabled ? _replace : null,
            ),
            const SizedBox(width: 8),
            _actionButton(
              'Cancelar',
              const Color(0xFF6D2E2E),
              VT.ask,
              enabled ? () => _confirmCancel(order) : null,
            ),
          ],
        ),
      ),
    );
  }

  Widget _actionField(String label, TextEditingController c) {
    return Padding(
      padding: const EdgeInsets.only(right: 10),
      child: Row(
        children: [
          Text('$label ', style: VT.labelStyle),
          SizedBox(
            width: 84,
            height: 26,
            child: TextField(
              controller: c,
              keyboardType: TextInputType.number,
              style: const TextStyle(color: VT.textStrong, fontSize: 11),
              decoration: const InputDecoration(
                isDense: true,
                contentPadding: EdgeInsets.symmetric(horizontal: 6),
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _actionButton(
    String label,
    Color fill,
    Color border,
    VoidCallback? onTap,
  ) {
    return Opacity(
      opacity: onTap == null ? 0.45 : 1,
      child: InkWell(
        onTap: onTap,
        child: Container(
          height: 26,
          padding: const EdgeInsets.symmetric(horizontal: 14),
          alignment: Alignment.center,
          decoration: BoxDecoration(
            color: fill,
            border: Border.all(color: border),
            borderRadius: BorderRadius.circular(6),
          ),
          child: Text(
            label,
            style: const TextStyle(
              fontSize: 11,
              fontWeight: FontWeight.bold,
              color: Color(0xFFEAF1FC),
            ),
          ),
        ),
      ),
    );
  }

  void _replace() {
    final order = _selected;
    if (order == null) return;
    final quantity = _parse(_quantity);
    final price = _parse(_price);
    if (quantity == null || price == null) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Cantidad y precio deben ser numéricos.')),
      );
      return;
    }
    widget.store.replaceOrder(
      id: order.id,
      quantity: quantity,
      price: price,
      maxFloor: _parse(_visible) ?? 0,
    );
  }

  Future<void> _confirmCancel(routing.Order order) async {
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        backgroundColor: VT.surface,
        title: const Text('Cancelar orden', style: TextStyle(fontSize: 15)),
        content: Text(
          '${order.side.name} ${order.symbol}  '
          '${_qty.format(order.orderQty)} @ ${_px.format(order.price)}',
          style: const TextStyle(color: VT.label, fontSize: 12),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx, false),
            child: const Text('Volver'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(ctx, true),
            child: const Text(
              'Cancelar orden',
              style: TextStyle(color: VT.ask),
            ),
          ),
        ],
      ),
    );
    if (ok == true) widget.store.cancelOrder(order.id);
  }
}
