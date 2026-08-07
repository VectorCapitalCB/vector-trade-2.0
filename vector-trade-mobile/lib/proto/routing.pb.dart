// This is a generated file - do not edit.
//
// Generated from routing.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;
import 'package:protobuf/well_known_types/google/protobuf/timestamp.pb.dart'
    as $0;

import 'routing.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'routing.pbenum.dart';

class Order extends $pb.GeneratedMessage {
  factory Order({
    $core.String? account,
    $core.String? id,
    $core.String? symbol,
    $core.double? price,
    $core.double? avgPrice,
    $core.String? clOrdId,
    $core.double? cumQty,
    Currency? currency,
    $core.String? operator,
    $core.String? execId,
    OrderStatus? ordStatus,
    HandlInst? handlInst,
    ExecBroker? broker,
    $core.double? lastPx,
    $core.double? lastQty,
    $core.String? orderID,
    $core.double? orderQty,
    OrdType? ordType,
    $core.String? origClOrdID,
    $core.String? securityID,
    $core.String? commission,
    $core.String? commissionType,
    Side? side,
    $core.String? text,
    Tif? tif,
    $0.Timestamp? time,
    SettlType? settlType,
    $core.String? settlDate,
    $core.String? prefixID,
    $core.double? maxFloor,
    $core.bool? chkIndivisible,
    $0.Timestamp? expireTime,
    ExecutionType? execType,
    $core.double? leaves,
    SecurityType? securityType,
    $0.Timestamp? effectiveTime,
    SecurityExchangeRouting? securityExchange,
    $core.String? icebergPercentage,
    $core.String? icebergValue,
    $core.String? basketID,
    ExStrategy? exStrategy,
    $core.double? spread,
    $core.double? limit,
    $core.bool? hideOrder,
    $core.String? codeOperator,
    StrategyOrder? strategyOrder,
    $core.String? contraTrader,
    $core.String? contraBroker,
    $core.String? clOrdLinkID,
    $core.double? amount,
    $core.double? riskRate,
    $core.String? folio,
  }) {
    final result = create();
    if (account != null) result.account = account;
    if (id != null) result.id = id;
    if (symbol != null) result.symbol = symbol;
    if (price != null) result.price = price;
    if (avgPrice != null) result.avgPrice = avgPrice;
    if (clOrdId != null) result.clOrdId = clOrdId;
    if (cumQty != null) result.cumQty = cumQty;
    if (currency != null) result.currency = currency;
    if (operator != null) result.operator = operator;
    if (execId != null) result.execId = execId;
    if (ordStatus != null) result.ordStatus = ordStatus;
    if (handlInst != null) result.handlInst = handlInst;
    if (broker != null) result.broker = broker;
    if (lastPx != null) result.lastPx = lastPx;
    if (lastQty != null) result.lastQty = lastQty;
    if (orderID != null) result.orderID = orderID;
    if (orderQty != null) result.orderQty = orderQty;
    if (ordType != null) result.ordType = ordType;
    if (origClOrdID != null) result.origClOrdID = origClOrdID;
    if (securityID != null) result.securityID = securityID;
    if (commission != null) result.commission = commission;
    if (commissionType != null) result.commissionType = commissionType;
    if (side != null) result.side = side;
    if (text != null) result.text = text;
    if (tif != null) result.tif = tif;
    if (time != null) result.time = time;
    if (settlType != null) result.settlType = settlType;
    if (settlDate != null) result.settlDate = settlDate;
    if (prefixID != null) result.prefixID = prefixID;
    if (maxFloor != null) result.maxFloor = maxFloor;
    if (chkIndivisible != null) result.chkIndivisible = chkIndivisible;
    if (expireTime != null) result.expireTime = expireTime;
    if (execType != null) result.execType = execType;
    if (leaves != null) result.leaves = leaves;
    if (securityType != null) result.securityType = securityType;
    if (effectiveTime != null) result.effectiveTime = effectiveTime;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (icebergPercentage != null) result.icebergPercentage = icebergPercentage;
    if (icebergValue != null) result.icebergValue = icebergValue;
    if (basketID != null) result.basketID = basketID;
    if (exStrategy != null) result.exStrategy = exStrategy;
    if (spread != null) result.spread = spread;
    if (limit != null) result.limit = limit;
    if (hideOrder != null) result.hideOrder = hideOrder;
    if (codeOperator != null) result.codeOperator = codeOperator;
    if (strategyOrder != null) result.strategyOrder = strategyOrder;
    if (contraTrader != null) result.contraTrader = contraTrader;
    if (contraBroker != null) result.contraBroker = contraBroker;
    if (clOrdLinkID != null) result.clOrdLinkID = clOrdLinkID;
    if (amount != null) result.amount = amount;
    if (riskRate != null) result.riskRate = riskRate;
    if (folio != null) result.folio = folio;
    return result;
  }

  Order._();

  factory Order.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Order.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Order',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'account')
    ..aOS(2, _omitFieldNames ? '' : 'id')
    ..aOS(7, _omitFieldNames ? '' : 'symbol')
    ..aD(9, _omitFieldNames ? '' : 'price')
    ..aD(10, _omitFieldNames ? '' : 'avgPrice', protoName: 'avgPrice')
    ..aOS(11, _omitFieldNames ? '' : 'clOrdId', protoName: 'clOrdId')
    ..aD(14, _omitFieldNames ? '' : 'cumQty', protoName: 'cumQty')
    ..aE<Currency>(15, _omitFieldNames ? '' : 'currency',
        enumValues: Currency.values)
    ..aOS(16, _omitFieldNames ? '' : 'operator')
    ..aOS(17, _omitFieldNames ? '' : 'execId', protoName: 'execId')
    ..aE<OrderStatus>(19, _omitFieldNames ? '' : 'ordStatus',
        protoName: 'ordStatus', enumValues: OrderStatus.values)
    ..aE<HandlInst>(21, _omitFieldNames ? '' : 'handlInst',
        protoName: 'handlInst', enumValues: HandlInst.values)
    ..aE<ExecBroker>(24, _omitFieldNames ? '' : 'broker',
        enumValues: ExecBroker.values)
    ..aD(31, _omitFieldNames ? '' : 'lastPx', protoName: 'lastPx')
    ..aD(32, _omitFieldNames ? '' : 'lastQty', protoName: 'lastQty')
    ..aOS(37, _omitFieldNames ? '' : 'orderID', protoName: 'orderID')
    ..aD(38, _omitFieldNames ? '' : 'orderQty', protoName: 'orderQty')
    ..aE<OrdType>(40, _omitFieldNames ? '' : 'ordType',
        protoName: 'ordType', enumValues: OrdType.values)
    ..aOS(41, _omitFieldNames ? '' : 'origClOrdID', protoName: 'origClOrdID')
    ..aOS(48, _omitFieldNames ? '' : 'securityID', protoName: 'securityID')
    ..aOS(49, _omitFieldNames ? '' : 'commission')
    ..aOS(50, _omitFieldNames ? '' : 'commissionType')
    ..aE<Side>(54, _omitFieldNames ? '' : 'side', enumValues: Side.values)
    ..aOS(58, _omitFieldNames ? '' : 'text')
    ..aE<Tif>(59, _omitFieldNames ? '' : 'tif', enumValues: Tif.values)
    ..aOM<$0.Timestamp>(60, _omitFieldNames ? '' : 'time',
        subBuilder: $0.Timestamp.create)
    ..aE<SettlType>(63, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: SettlType.values)
    ..aOS(64, _omitFieldNames ? '' : 'settlDate', protoName: 'settlDate')
    ..aOS(97, _omitFieldNames ? '' : 'prefixID', protoName: 'prefixID')
    ..aD(111, _omitFieldNames ? '' : 'maxFloor', protoName: 'maxFloor')
    ..aOB(113, _omitFieldNames ? '' : 'chkIndivisible',
        protoName: 'chkIndivisible')
    ..aOM<$0.Timestamp>(126, _omitFieldNames ? '' : 'expireTime',
        protoName: 'expireTime', subBuilder: $0.Timestamp.create)
    ..aE<ExecutionType>(150, _omitFieldNames ? '' : 'execType',
        protoName: 'execType', enumValues: ExecutionType.values)
    ..aD(151, _omitFieldNames ? '' : 'leaves')
    ..aE<SecurityType>(167, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: SecurityType.values)
    ..aOM<$0.Timestamp>(168, _omitFieldNames ? '' : 'effectiveTime',
        protoName: 'effectiveTime', subBuilder: $0.Timestamp.create)
    ..aE<SecurityExchangeRouting>(
        207, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeRouting.values)
    ..aOS(225, _omitFieldNames ? '' : 'icebergPercentage',
        protoName: 'icebergPercentage')
    ..aOS(226, _omitFieldNames ? '' : 'icebergValue', protoName: 'icebergValue')
    ..aOS(227, _omitFieldNames ? '' : 'basketID', protoName: 'basketID')
    ..aE<ExStrategy>(228, _omitFieldNames ? '' : 'exStrategy',
        protoName: 'exStrategy', enumValues: ExStrategy.values)
    ..aD(229, _omitFieldNames ? '' : 'spread')
    ..aD(230, _omitFieldNames ? '' : 'limit')
    ..aOB(231, _omitFieldNames ? '' : 'hideOrder', protoName: 'hideOrder')
    ..aOS(232, _omitFieldNames ? '' : 'codeOperator', protoName: 'codeOperator')
    ..aE<StrategyOrder>(233, _omitFieldNames ? '' : 'strategyOrder',
        protoName: 'strategyOrder', enumValues: StrategyOrder.values)
    ..aOS(337, _omitFieldNames ? '' : 'contraTrader', protoName: 'contraTrader')
    ..aOS(375, _omitFieldNames ? '' : 'contraBroker', protoName: 'contraBroker')
    ..aOS(583, _omitFieldNames ? '' : 'clOrdLinkID', protoName: 'clOrdLinkID')
    ..aD(584, _omitFieldNames ? '' : 'amount')
    ..aD(1190, _omitFieldNames ? '' : 'riskRate', protoName: 'riskRate')
    ..aOS(5463, _omitFieldNames ? '' : 'folio')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Order clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Order copyWith(void Function(Order) updates) =>
      super.copyWith((message) => updates(message as Order)) as Order;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Order create() => Order._();
  @$core.override
  Order createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Order getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Order>(create);
  static Order? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get account => $_getSZ(0);
  @$pb.TagNumber(1)
  set account($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasAccount() => $_has(0);
  @$pb.TagNumber(1)
  void clearAccount() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get id => $_getSZ(1);
  @$pb.TagNumber(2)
  set id($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasId() => $_has(1);
  @$pb.TagNumber(2)
  void clearId() => $_clearField(2);

  @$pb.TagNumber(7)
  $core.String get symbol => $_getSZ(2);
  @$pb.TagNumber(7)
  set symbol($core.String value) => $_setString(2, value);
  @$pb.TagNumber(7)
  $core.bool hasSymbol() => $_has(2);
  @$pb.TagNumber(7)
  void clearSymbol() => $_clearField(7);

  @$pb.TagNumber(9)
  $core.double get price => $_getN(3);
  @$pb.TagNumber(9)
  set price($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(9)
  $core.bool hasPrice() => $_has(3);
  @$pb.TagNumber(9)
  void clearPrice() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.double get avgPrice => $_getN(4);
  @$pb.TagNumber(10)
  set avgPrice($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(10)
  $core.bool hasAvgPrice() => $_has(4);
  @$pb.TagNumber(10)
  void clearAvgPrice() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.String get clOrdId => $_getSZ(5);
  @$pb.TagNumber(11)
  set clOrdId($core.String value) => $_setString(5, value);
  @$pb.TagNumber(11)
  $core.bool hasClOrdId() => $_has(5);
  @$pb.TagNumber(11)
  void clearClOrdId() => $_clearField(11);

  @$pb.TagNumber(14)
  $core.double get cumQty => $_getN(6);
  @$pb.TagNumber(14)
  set cumQty($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(14)
  $core.bool hasCumQty() => $_has(6);
  @$pb.TagNumber(14)
  void clearCumQty() => $_clearField(14);

  @$pb.TagNumber(15)
  Currency get currency => $_getN(7);
  @$pb.TagNumber(15)
  set currency(Currency value) => $_setField(15, value);
  @$pb.TagNumber(15)
  $core.bool hasCurrency() => $_has(7);
  @$pb.TagNumber(15)
  void clearCurrency() => $_clearField(15);

  @$pb.TagNumber(16)
  $core.String get operator => $_getSZ(8);
  @$pb.TagNumber(16)
  set operator($core.String value) => $_setString(8, value);
  @$pb.TagNumber(16)
  $core.bool hasOperator() => $_has(8);
  @$pb.TagNumber(16)
  void clearOperator() => $_clearField(16);

  @$pb.TagNumber(17)
  $core.String get execId => $_getSZ(9);
  @$pb.TagNumber(17)
  set execId($core.String value) => $_setString(9, value);
  @$pb.TagNumber(17)
  $core.bool hasExecId() => $_has(9);
  @$pb.TagNumber(17)
  void clearExecId() => $_clearField(17);

  @$pb.TagNumber(19)
  OrderStatus get ordStatus => $_getN(10);
  @$pb.TagNumber(19)
  set ordStatus(OrderStatus value) => $_setField(19, value);
  @$pb.TagNumber(19)
  $core.bool hasOrdStatus() => $_has(10);
  @$pb.TagNumber(19)
  void clearOrdStatus() => $_clearField(19);

  @$pb.TagNumber(21)
  HandlInst get handlInst => $_getN(11);
  @$pb.TagNumber(21)
  set handlInst(HandlInst value) => $_setField(21, value);
  @$pb.TagNumber(21)
  $core.bool hasHandlInst() => $_has(11);
  @$pb.TagNumber(21)
  void clearHandlInst() => $_clearField(21);

  @$pb.TagNumber(24)
  ExecBroker get broker => $_getN(12);
  @$pb.TagNumber(24)
  set broker(ExecBroker value) => $_setField(24, value);
  @$pb.TagNumber(24)
  $core.bool hasBroker() => $_has(12);
  @$pb.TagNumber(24)
  void clearBroker() => $_clearField(24);

  @$pb.TagNumber(31)
  $core.double get lastPx => $_getN(13);
  @$pb.TagNumber(31)
  set lastPx($core.double value) => $_setDouble(13, value);
  @$pb.TagNumber(31)
  $core.bool hasLastPx() => $_has(13);
  @$pb.TagNumber(31)
  void clearLastPx() => $_clearField(31);

  @$pb.TagNumber(32)
  $core.double get lastQty => $_getN(14);
  @$pb.TagNumber(32)
  set lastQty($core.double value) => $_setDouble(14, value);
  @$pb.TagNumber(32)
  $core.bool hasLastQty() => $_has(14);
  @$pb.TagNumber(32)
  void clearLastQty() => $_clearField(32);

  @$pb.TagNumber(37)
  $core.String get orderID => $_getSZ(15);
  @$pb.TagNumber(37)
  set orderID($core.String value) => $_setString(15, value);
  @$pb.TagNumber(37)
  $core.bool hasOrderID() => $_has(15);
  @$pb.TagNumber(37)
  void clearOrderID() => $_clearField(37);

  @$pb.TagNumber(38)
  $core.double get orderQty => $_getN(16);
  @$pb.TagNumber(38)
  set orderQty($core.double value) => $_setDouble(16, value);
  @$pb.TagNumber(38)
  $core.bool hasOrderQty() => $_has(16);
  @$pb.TagNumber(38)
  void clearOrderQty() => $_clearField(38);

  @$pb.TagNumber(40)
  OrdType get ordType => $_getN(17);
  @$pb.TagNumber(40)
  set ordType(OrdType value) => $_setField(40, value);
  @$pb.TagNumber(40)
  $core.bool hasOrdType() => $_has(17);
  @$pb.TagNumber(40)
  void clearOrdType() => $_clearField(40);

  @$pb.TagNumber(41)
  $core.String get origClOrdID => $_getSZ(18);
  @$pb.TagNumber(41)
  set origClOrdID($core.String value) => $_setString(18, value);
  @$pb.TagNumber(41)
  $core.bool hasOrigClOrdID() => $_has(18);
  @$pb.TagNumber(41)
  void clearOrigClOrdID() => $_clearField(41);

  @$pb.TagNumber(48)
  $core.String get securityID => $_getSZ(19);
  @$pb.TagNumber(48)
  set securityID($core.String value) => $_setString(19, value);
  @$pb.TagNumber(48)
  $core.bool hasSecurityID() => $_has(19);
  @$pb.TagNumber(48)
  void clearSecurityID() => $_clearField(48);

  @$pb.TagNumber(49)
  $core.String get commission => $_getSZ(20);
  @$pb.TagNumber(49)
  set commission($core.String value) => $_setString(20, value);
  @$pb.TagNumber(49)
  $core.bool hasCommission() => $_has(20);
  @$pb.TagNumber(49)
  void clearCommission() => $_clearField(49);

  @$pb.TagNumber(50)
  $core.String get commissionType => $_getSZ(21);
  @$pb.TagNumber(50)
  set commissionType($core.String value) => $_setString(21, value);
  @$pb.TagNumber(50)
  $core.bool hasCommissionType() => $_has(21);
  @$pb.TagNumber(50)
  void clearCommissionType() => $_clearField(50);

  @$pb.TagNumber(54)
  Side get side => $_getN(22);
  @$pb.TagNumber(54)
  set side(Side value) => $_setField(54, value);
  @$pb.TagNumber(54)
  $core.bool hasSide() => $_has(22);
  @$pb.TagNumber(54)
  void clearSide() => $_clearField(54);

  @$pb.TagNumber(58)
  $core.String get text => $_getSZ(23);
  @$pb.TagNumber(58)
  set text($core.String value) => $_setString(23, value);
  @$pb.TagNumber(58)
  $core.bool hasText() => $_has(23);
  @$pb.TagNumber(58)
  void clearText() => $_clearField(58);

  @$pb.TagNumber(59)
  Tif get tif => $_getN(24);
  @$pb.TagNumber(59)
  set tif(Tif value) => $_setField(59, value);
  @$pb.TagNumber(59)
  $core.bool hasTif() => $_has(24);
  @$pb.TagNumber(59)
  void clearTif() => $_clearField(59);

  @$pb.TagNumber(60)
  $0.Timestamp get time => $_getN(25);
  @$pb.TagNumber(60)
  set time($0.Timestamp value) => $_setField(60, value);
  @$pb.TagNumber(60)
  $core.bool hasTime() => $_has(25);
  @$pb.TagNumber(60)
  void clearTime() => $_clearField(60);
  @$pb.TagNumber(60)
  $0.Timestamp ensureTime() => $_ensure(25);

  @$pb.TagNumber(63)
  SettlType get settlType => $_getN(26);
  @$pb.TagNumber(63)
  set settlType(SettlType value) => $_setField(63, value);
  @$pb.TagNumber(63)
  $core.bool hasSettlType() => $_has(26);
  @$pb.TagNumber(63)
  void clearSettlType() => $_clearField(63);

  @$pb.TagNumber(64)
  $core.String get settlDate => $_getSZ(27);
  @$pb.TagNumber(64)
  set settlDate($core.String value) => $_setString(27, value);
  @$pb.TagNumber(64)
  $core.bool hasSettlDate() => $_has(27);
  @$pb.TagNumber(64)
  void clearSettlDate() => $_clearField(64);

  @$pb.TagNumber(97)
  $core.String get prefixID => $_getSZ(28);
  @$pb.TagNumber(97)
  set prefixID($core.String value) => $_setString(28, value);
  @$pb.TagNumber(97)
  $core.bool hasPrefixID() => $_has(28);
  @$pb.TagNumber(97)
  void clearPrefixID() => $_clearField(97);

  @$pb.TagNumber(111)
  $core.double get maxFloor => $_getN(29);
  @$pb.TagNumber(111)
  set maxFloor($core.double value) => $_setDouble(29, value);
  @$pb.TagNumber(111)
  $core.bool hasMaxFloor() => $_has(29);
  @$pb.TagNumber(111)
  void clearMaxFloor() => $_clearField(111);

  @$pb.TagNumber(113)
  $core.bool get chkIndivisible => $_getBF(30);
  @$pb.TagNumber(113)
  set chkIndivisible($core.bool value) => $_setBool(30, value);
  @$pb.TagNumber(113)
  $core.bool hasChkIndivisible() => $_has(30);
  @$pb.TagNumber(113)
  void clearChkIndivisible() => $_clearField(113);

  @$pb.TagNumber(126)
  $0.Timestamp get expireTime => $_getN(31);
  @$pb.TagNumber(126)
  set expireTime($0.Timestamp value) => $_setField(126, value);
  @$pb.TagNumber(126)
  $core.bool hasExpireTime() => $_has(31);
  @$pb.TagNumber(126)
  void clearExpireTime() => $_clearField(126);
  @$pb.TagNumber(126)
  $0.Timestamp ensureExpireTime() => $_ensure(31);

  @$pb.TagNumber(150)
  ExecutionType get execType => $_getN(32);
  @$pb.TagNumber(150)
  set execType(ExecutionType value) => $_setField(150, value);
  @$pb.TagNumber(150)
  $core.bool hasExecType() => $_has(32);
  @$pb.TagNumber(150)
  void clearExecType() => $_clearField(150);

  @$pb.TagNumber(151)
  $core.double get leaves => $_getN(33);
  @$pb.TagNumber(151)
  set leaves($core.double value) => $_setDouble(33, value);
  @$pb.TagNumber(151)
  $core.bool hasLeaves() => $_has(33);
  @$pb.TagNumber(151)
  void clearLeaves() => $_clearField(151);

  @$pb.TagNumber(167)
  SecurityType get securityType => $_getN(34);
  @$pb.TagNumber(167)
  set securityType(SecurityType value) => $_setField(167, value);
  @$pb.TagNumber(167)
  $core.bool hasSecurityType() => $_has(34);
  @$pb.TagNumber(167)
  void clearSecurityType() => $_clearField(167);

  @$pb.TagNumber(168)
  $0.Timestamp get effectiveTime => $_getN(35);
  @$pb.TagNumber(168)
  set effectiveTime($0.Timestamp value) => $_setField(168, value);
  @$pb.TagNumber(168)
  $core.bool hasEffectiveTime() => $_has(35);
  @$pb.TagNumber(168)
  void clearEffectiveTime() => $_clearField(168);
  @$pb.TagNumber(168)
  $0.Timestamp ensureEffectiveTime() => $_ensure(35);

  @$pb.TagNumber(207)
  SecurityExchangeRouting get securityExchange => $_getN(36);
  @$pb.TagNumber(207)
  set securityExchange(SecurityExchangeRouting value) => $_setField(207, value);
  @$pb.TagNumber(207)
  $core.bool hasSecurityExchange() => $_has(36);
  @$pb.TagNumber(207)
  void clearSecurityExchange() => $_clearField(207);

  @$pb.TagNumber(225)
  $core.String get icebergPercentage => $_getSZ(37);
  @$pb.TagNumber(225)
  set icebergPercentage($core.String value) => $_setString(37, value);
  @$pb.TagNumber(225)
  $core.bool hasIcebergPercentage() => $_has(37);
  @$pb.TagNumber(225)
  void clearIcebergPercentage() => $_clearField(225);

  @$pb.TagNumber(226)
  $core.String get icebergValue => $_getSZ(38);
  @$pb.TagNumber(226)
  set icebergValue($core.String value) => $_setString(38, value);
  @$pb.TagNumber(226)
  $core.bool hasIcebergValue() => $_has(38);
  @$pb.TagNumber(226)
  void clearIcebergValue() => $_clearField(226);

  @$pb.TagNumber(227)
  $core.String get basketID => $_getSZ(39);
  @$pb.TagNumber(227)
  set basketID($core.String value) => $_setString(39, value);
  @$pb.TagNumber(227)
  $core.bool hasBasketID() => $_has(39);
  @$pb.TagNumber(227)
  void clearBasketID() => $_clearField(227);

  @$pb.TagNumber(228)
  ExStrategy get exStrategy => $_getN(40);
  @$pb.TagNumber(228)
  set exStrategy(ExStrategy value) => $_setField(228, value);
  @$pb.TagNumber(228)
  $core.bool hasExStrategy() => $_has(40);
  @$pb.TagNumber(228)
  void clearExStrategy() => $_clearField(228);

  @$pb.TagNumber(229)
  $core.double get spread => $_getN(41);
  @$pb.TagNumber(229)
  set spread($core.double value) => $_setDouble(41, value);
  @$pb.TagNumber(229)
  $core.bool hasSpread() => $_has(41);
  @$pb.TagNumber(229)
  void clearSpread() => $_clearField(229);

  @$pb.TagNumber(230)
  $core.double get limit => $_getN(42);
  @$pb.TagNumber(230)
  set limit($core.double value) => $_setDouble(42, value);
  @$pb.TagNumber(230)
  $core.bool hasLimit() => $_has(42);
  @$pb.TagNumber(230)
  void clearLimit() => $_clearField(230);

  @$pb.TagNumber(231)
  $core.bool get hideOrder => $_getBF(43);
  @$pb.TagNumber(231)
  set hideOrder($core.bool value) => $_setBool(43, value);
  @$pb.TagNumber(231)
  $core.bool hasHideOrder() => $_has(43);
  @$pb.TagNumber(231)
  void clearHideOrder() => $_clearField(231);

  @$pb.TagNumber(232)
  $core.String get codeOperator => $_getSZ(44);
  @$pb.TagNumber(232)
  set codeOperator($core.String value) => $_setString(44, value);
  @$pb.TagNumber(232)
  $core.bool hasCodeOperator() => $_has(44);
  @$pb.TagNumber(232)
  void clearCodeOperator() => $_clearField(232);

  @$pb.TagNumber(233)
  StrategyOrder get strategyOrder => $_getN(45);
  @$pb.TagNumber(233)
  set strategyOrder(StrategyOrder value) => $_setField(233, value);
  @$pb.TagNumber(233)
  $core.bool hasStrategyOrder() => $_has(45);
  @$pb.TagNumber(233)
  void clearStrategyOrder() => $_clearField(233);

  @$pb.TagNumber(337)
  $core.String get contraTrader => $_getSZ(46);
  @$pb.TagNumber(337)
  set contraTrader($core.String value) => $_setString(46, value);
  @$pb.TagNumber(337)
  $core.bool hasContraTrader() => $_has(46);
  @$pb.TagNumber(337)
  void clearContraTrader() => $_clearField(337);

  @$pb.TagNumber(375)
  $core.String get contraBroker => $_getSZ(47);
  @$pb.TagNumber(375)
  set contraBroker($core.String value) => $_setString(47, value);
  @$pb.TagNumber(375)
  $core.bool hasContraBroker() => $_has(47);
  @$pb.TagNumber(375)
  void clearContraBroker() => $_clearField(375);

  @$pb.TagNumber(583)
  $core.String get clOrdLinkID => $_getSZ(48);
  @$pb.TagNumber(583)
  set clOrdLinkID($core.String value) => $_setString(48, value);
  @$pb.TagNumber(583)
  $core.bool hasClOrdLinkID() => $_has(48);
  @$pb.TagNumber(583)
  void clearClOrdLinkID() => $_clearField(583);

  @$pb.TagNumber(584)
  $core.double get amount => $_getN(49);
  @$pb.TagNumber(584)
  set amount($core.double value) => $_setDouble(49, value);
  @$pb.TagNumber(584)
  $core.bool hasAmount() => $_has(49);
  @$pb.TagNumber(584)
  void clearAmount() => $_clearField(584);

  @$pb.TagNumber(1190)
  $core.double get riskRate => $_getN(50);
  @$pb.TagNumber(1190)
  set riskRate($core.double value) => $_setDouble(50, value);
  @$pb.TagNumber(1190)
  $core.bool hasRiskRate() => $_has(50);
  @$pb.TagNumber(1190)
  void clearRiskRate() => $_clearField(1190);

  @$pb.TagNumber(5463)
  $core.String get folio => $_getSZ(51);
  @$pb.TagNumber(5463)
  set folio($core.String value) => $_setString(51, value);
  @$pb.TagNumber(5463)
  $core.bool hasFolio() => $_has(51);
  @$pb.TagNumber(5463)
  void clearFolio() => $_clearField(5463);
}

class TradeExecution extends $pb.GeneratedMessage {
  factory TradeExecution({
    Order? order,
  }) {
    final result = create();
    if (order != null) result.order = order;
    return result;
  }

  TradeExecution._();

  factory TradeExecution.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory TradeExecution.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TradeExecution',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<Order>(1, _omitFieldNames ? '' : 'order', subBuilder: Order.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TradeExecution clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TradeExecution copyWith(void Function(TradeExecution) updates) =>
      super.copyWith((message) => updates(message as TradeExecution))
          as TradeExecution;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TradeExecution create() => TradeExecution._();
  @$core.override
  TradeExecution createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static TradeExecution getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<TradeExecution>(create);
  static TradeExecution? _defaultInstance;

  @$pb.TagNumber(1)
  Order get order => $_getN(0);
  @$pb.TagNumber(1)
  set order(Order value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOrder() => $_has(0);
  @$pb.TagNumber(1)
  void clearOrder() => $_clearField(1);
  @$pb.TagNumber(1)
  Order ensureOrder() => $_ensure(0);
}

class basketOrder extends $pb.GeneratedMessage {
  factory basketOrder({
    Order? order,
    $core.String? strategyID,
    $core.String? basketID,
    ExStrategy? exStrategy,
    $core.double? spread,
    $core.double? limit,
    $core.double? noStrategyParameters,
    $core.Iterable<StrategyParameters>? bStrgy,
  }) {
    final result = create();
    if (order != null) result.order = order;
    if (strategyID != null) result.strategyID = strategyID;
    if (basketID != null) result.basketID = basketID;
    if (exStrategy != null) result.exStrategy = exStrategy;
    if (spread != null) result.spread = spread;
    if (limit != null) result.limit = limit;
    if (noStrategyParameters != null)
      result.noStrategyParameters = noStrategyParameters;
    if (bStrgy != null) result.bStrgy.addAll(bStrgy);
    return result;
  }

  basketOrder._();

  factory basketOrder.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory basketOrder.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'basketOrder',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<Order>(1, _omitFieldNames ? '' : 'order', subBuilder: Order.create)
    ..aOS(98, _omitFieldNames ? '' : 'strategyID', protoName: 'strategyID')
    ..aOS(221, _omitFieldNames ? '' : 'basketID', protoName: 'basketID')
    ..aE<ExStrategy>(222, _omitFieldNames ? '' : 'exStrategy',
        protoName: 'exStrategy', enumValues: ExStrategy.values)
    ..aD(223, _omitFieldNames ? '' : 'spread')
    ..aD(224, _omitFieldNames ? '' : 'limit')
    ..aD(957, _omitFieldNames ? '' : 'noStrategyParameters',
        protoName: 'noStrategyParameters')
    ..pPM<StrategyParameters>(958, _omitFieldNames ? '' : 'bStrgy',
        protoName: 'bStrgy', subBuilder: StrategyParameters.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  basketOrder clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  basketOrder copyWith(void Function(basketOrder) updates) =>
      super.copyWith((message) => updates(message as basketOrder))
          as basketOrder;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static basketOrder create() => basketOrder._();
  @$core.override
  basketOrder createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static basketOrder getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<basketOrder>(create);
  static basketOrder? _defaultInstance;

  @$pb.TagNumber(1)
  Order get order => $_getN(0);
  @$pb.TagNumber(1)
  set order(Order value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOrder() => $_has(0);
  @$pb.TagNumber(1)
  void clearOrder() => $_clearField(1);
  @$pb.TagNumber(1)
  Order ensureOrder() => $_ensure(0);

  @$pb.TagNumber(98)
  $core.String get strategyID => $_getSZ(1);
  @$pb.TagNumber(98)
  set strategyID($core.String value) => $_setString(1, value);
  @$pb.TagNumber(98)
  $core.bool hasStrategyID() => $_has(1);
  @$pb.TagNumber(98)
  void clearStrategyID() => $_clearField(98);

  @$pb.TagNumber(221)
  $core.String get basketID => $_getSZ(2);
  @$pb.TagNumber(221)
  set basketID($core.String value) => $_setString(2, value);
  @$pb.TagNumber(221)
  $core.bool hasBasketID() => $_has(2);
  @$pb.TagNumber(221)
  void clearBasketID() => $_clearField(221);

  @$pb.TagNumber(222)
  ExStrategy get exStrategy => $_getN(3);
  @$pb.TagNumber(222)
  set exStrategy(ExStrategy value) => $_setField(222, value);
  @$pb.TagNumber(222)
  $core.bool hasExStrategy() => $_has(3);
  @$pb.TagNumber(222)
  void clearExStrategy() => $_clearField(222);

  @$pb.TagNumber(223)
  $core.double get spread => $_getN(4);
  @$pb.TagNumber(223)
  set spread($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(223)
  $core.bool hasSpread() => $_has(4);
  @$pb.TagNumber(223)
  void clearSpread() => $_clearField(223);

  @$pb.TagNumber(224)
  $core.double get limit => $_getN(5);
  @$pb.TagNumber(224)
  set limit($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(224)
  $core.bool hasLimit() => $_has(5);
  @$pb.TagNumber(224)
  void clearLimit() => $_clearField(224);

  @$pb.TagNumber(957)
  $core.double get noStrategyParameters => $_getN(6);
  @$pb.TagNumber(957)
  set noStrategyParameters($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(957)
  $core.bool hasNoStrategyParameters() => $_has(6);
  @$pb.TagNumber(957)
  void clearNoStrategyParameters() => $_clearField(957);

  @$pb.TagNumber(958)
  $pb.PbList<StrategyParameters> get bStrgy => $_getList(7);
}

class StrategyParameters extends $pb.GeneratedMessage {
  factory StrategyParameters({
    $core.String? strategyParameterName,
    $core.String? strategyParameterType,
    $core.double? strategyParameterValue,
  }) {
    final result = create();
    if (strategyParameterName != null)
      result.strategyParameterName = strategyParameterName;
    if (strategyParameterType != null)
      result.strategyParameterType = strategyParameterType;
    if (strategyParameterValue != null)
      result.strategyParameterValue = strategyParameterValue;
    return result;
  }

  StrategyParameters._();

  factory StrategyParameters.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory StrategyParameters.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'StrategyParameters',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(958, _omitFieldNames ? '' : 'StrategyParameterName',
        protoName: 'StrategyParameterName')
    ..aOS(959, _omitFieldNames ? '' : 'StrategyParameterType',
        protoName: 'StrategyParameterType')
    ..aD(960, _omitFieldNames ? '' : 'StrategyParameterValue',
        protoName: 'StrategyParameterValue')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StrategyParameters clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StrategyParameters copyWith(void Function(StrategyParameters) updates) =>
      super.copyWith((message) => updates(message as StrategyParameters))
          as StrategyParameters;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static StrategyParameters create() => StrategyParameters._();
  @$core.override
  StrategyParameters createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static StrategyParameters getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<StrategyParameters>(create);
  static StrategyParameters? _defaultInstance;

  @$pb.TagNumber(958)
  $core.String get strategyParameterName => $_getSZ(0);
  @$pb.TagNumber(958)
  set strategyParameterName($core.String value) => $_setString(0, value);
  @$pb.TagNumber(958)
  $core.bool hasStrategyParameterName() => $_has(0);
  @$pb.TagNumber(958)
  void clearStrategyParameterName() => $_clearField(958);

  @$pb.TagNumber(959)
  $core.String get strategyParameterType => $_getSZ(1);
  @$pb.TagNumber(959)
  set strategyParameterType($core.String value) => $_setString(1, value);
  @$pb.TagNumber(959)
  $core.bool hasStrategyParameterType() => $_has(1);
  @$pb.TagNumber(959)
  void clearStrategyParameterType() => $_clearField(959);

  @$pb.TagNumber(960)
  $core.double get strategyParameterValue => $_getN(2);
  @$pb.TagNumber(960)
  set strategyParameterValue($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(960)
  $core.bool hasStrategyParameterValue() => $_has(2);
  @$pb.TagNumber(960)
  void clearStrategyParameterValue() => $_clearField(960);
}

class OrderCancelReject extends $pb.GeneratedMessage {
  factory OrderCancelReject({
    $core.String? id,
    $core.String? text,
    $core.String? execId,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (text != null) result.text = text;
    if (execId != null) result.execId = execId;
    return result;
  }

  OrderCancelReject._();

  factory OrderCancelReject.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory OrderCancelReject.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OrderCancelReject',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'text')
    ..aOS(3, _omitFieldNames ? '' : 'execId', protoName: 'execId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderCancelReject clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderCancelReject copyWith(void Function(OrderCancelReject) updates) =>
      super.copyWith((message) => updates(message as OrderCancelReject))
          as OrderCancelReject;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static OrderCancelReject create() => OrderCancelReject._();
  @$core.override
  OrderCancelReject createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static OrderCancelReject getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<OrderCancelReject>(create);
  static OrderCancelReject? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get text => $_getSZ(1);
  @$pb.TagNumber(2)
  set text($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasText() => $_has(1);
  @$pb.TagNumber(2)
  void clearText() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get execId => $_getSZ(2);
  @$pb.TagNumber(3)
  set execId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasExecId() => $_has(2);
  @$pb.TagNumber(3)
  void clearExecId() => $_clearField(3);
}

class NewOrderRequest extends $pb.GeneratedMessage {
  factory NewOrderRequest({
    Order? order,
  }) {
    final result = create();
    if (order != null) result.order = order;
    return result;
  }

  NewOrderRequest._();

  factory NewOrderRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory NewOrderRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'NewOrderRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<Order>(1, _omitFieldNames ? '' : 'order', subBuilder: Order.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  NewOrderRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  NewOrderRequest copyWith(void Function(NewOrderRequest) updates) =>
      super.copyWith((message) => updates(message as NewOrderRequest))
          as NewOrderRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static NewOrderRequest create() => NewOrderRequest._();
  @$core.override
  NewOrderRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static NewOrderRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<NewOrderRequest>(create);
  static NewOrderRequest? _defaultInstance;

  @$pb.TagNumber(1)
  Order get order => $_getN(0);
  @$pb.TagNumber(1)
  set order(Order value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOrder() => $_has(0);
  @$pb.TagNumber(1)
  void clearOrder() => $_clearField(1);
  @$pb.TagNumber(1)
  Order ensureOrder() => $_ensure(0);
}

class OrderReplaceRequest extends $pb.GeneratedMessage {
  factory OrderReplaceRequest({
    $core.String? id,
    $core.double? quantity,
    $core.double? price,
    $core.double? limit,
    $core.double? spread,
    $core.double? maxFloor,
    $core.String? icebergPercentage,
    $core.double? amount,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (quantity != null) result.quantity = quantity;
    if (price != null) result.price = price;
    if (limit != null) result.limit = limit;
    if (spread != null) result.spread = spread;
    if (maxFloor != null) result.maxFloor = maxFloor;
    if (icebergPercentage != null) result.icebergPercentage = icebergPercentage;
    if (amount != null) result.amount = amount;
    return result;
  }

  OrderReplaceRequest._();

  factory OrderReplaceRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory OrderReplaceRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OrderReplaceRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aD(2, _omitFieldNames ? '' : 'quantity')
    ..aD(3, _omitFieldNames ? '' : 'price')
    ..aD(4, _omitFieldNames ? '' : 'limit')
    ..aD(5, _omitFieldNames ? '' : 'spread')
    ..aD(111, _omitFieldNames ? '' : 'maxFloor', protoName: 'maxFloor')
    ..aOS(225, _omitFieldNames ? '' : 'icebergPercentage',
        protoName: 'icebergPercentage')
    ..aD(584, _omitFieldNames ? '' : 'amount')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderReplaceRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderReplaceRequest copyWith(void Function(OrderReplaceRequest) updates) =>
      super.copyWith((message) => updates(message as OrderReplaceRequest))
          as OrderReplaceRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static OrderReplaceRequest create() => OrderReplaceRequest._();
  @$core.override
  OrderReplaceRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static OrderReplaceRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<OrderReplaceRequest>(create);
  static OrderReplaceRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get quantity => $_getN(1);
  @$pb.TagNumber(2)
  set quantity($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasQuantity() => $_has(1);
  @$pb.TagNumber(2)
  void clearQuantity() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get price => $_getN(2);
  @$pb.TagNumber(3)
  set price($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasPrice() => $_has(2);
  @$pb.TagNumber(3)
  void clearPrice() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get limit => $_getN(3);
  @$pb.TagNumber(4)
  set limit($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasLimit() => $_has(3);
  @$pb.TagNumber(4)
  void clearLimit() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.double get spread => $_getN(4);
  @$pb.TagNumber(5)
  set spread($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(5)
  $core.bool hasSpread() => $_has(4);
  @$pb.TagNumber(5)
  void clearSpread() => $_clearField(5);

  @$pb.TagNumber(111)
  $core.double get maxFloor => $_getN(5);
  @$pb.TagNumber(111)
  set maxFloor($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(111)
  $core.bool hasMaxFloor() => $_has(5);
  @$pb.TagNumber(111)
  void clearMaxFloor() => $_clearField(111);

  @$pb.TagNumber(225)
  $core.String get icebergPercentage => $_getSZ(6);
  @$pb.TagNumber(225)
  set icebergPercentage($core.String value) => $_setString(6, value);
  @$pb.TagNumber(225)
  $core.bool hasIcebergPercentage() => $_has(6);
  @$pb.TagNumber(225)
  void clearIcebergPercentage() => $_clearField(225);

  @$pb.TagNumber(584)
  $core.double get amount => $_getN(7);
  @$pb.TagNumber(584)
  set amount($core.double value) => $_setDouble(7, value);
  @$pb.TagNumber(584)
  $core.bool hasAmount() => $_has(7);
  @$pb.TagNumber(584)
  void clearAmount() => $_clearField(584);
}

class OrderCancelRequest extends $pb.GeneratedMessage {
  factory OrderCancelRequest({
    $core.String? id,
  }) {
    final result = create();
    if (id != null) result.id = id;
    return result;
  }

  OrderCancelRequest._();

  factory OrderCancelRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory OrderCancelRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OrderCancelRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderCancelRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderCancelRequest copyWith(void Function(OrderCancelRequest) updates) =>
      super.copyWith((message) => updates(message as OrderCancelRequest))
          as OrderCancelRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static OrderCancelRequest create() => OrderCancelRequest._();
  @$core.override
  OrderCancelRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static OrderCancelRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<OrderCancelRequest>(create);
  static OrderCancelRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);
}

class OrdersCancelRequestList extends $pb.GeneratedMessage {
  factory OrdersCancelRequestList({
    $core.Iterable<$core.String>? id,
  }) {
    final result = create();
    if (id != null) result.id.addAll(id);
    return result;
  }

  OrdersCancelRequestList._();

  factory OrdersCancelRequestList.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory OrdersCancelRequestList.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OrdersCancelRequestList',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPS(1, _omitFieldNames ? '' : 'id')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrdersCancelRequestList clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrdersCancelRequestList copyWith(
          void Function(OrdersCancelRequestList) updates) =>
      super.copyWith((message) => updates(message as OrdersCancelRequestList))
          as OrdersCancelRequestList;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static OrdersCancelRequestList create() => OrdersCancelRequestList._();
  @$core.override
  OrdersCancelRequestList createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static OrdersCancelRequestList getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<OrdersCancelRequestList>(create);
  static OrdersCancelRequestList? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<$core.String> get id => $_getList(0);
}

class OrdersCancelUnsolicited extends $pb.GeneratedMessage {
  factory OrdersCancelUnsolicited({
    $core.Iterable<Order>? orders,
  }) {
    final result = create();
    if (orders != null) result.orders.addAll(orders);
    return result;
  }

  OrdersCancelUnsolicited._();

  factory OrdersCancelUnsolicited.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory OrdersCancelUnsolicited.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OrdersCancelUnsolicited',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<Order>(1, _omitFieldNames ? '' : 'orders', subBuilder: Order.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrdersCancelUnsolicited clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrdersCancelUnsolicited copyWith(
          void Function(OrdersCancelUnsolicited) updates) =>
      super.copyWith((message) => updates(message as OrdersCancelUnsolicited))
          as OrdersCancelUnsolicited;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static OrdersCancelUnsolicited create() => OrdersCancelUnsolicited._();
  @$core.override
  OrdersCancelUnsolicited createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static OrdersCancelUnsolicited getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<OrdersCancelUnsolicited>(create);
  static OrdersCancelUnsolicited? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<Order> get orders => $_getList(0);
}

class OrderList extends $pb.GeneratedMessage {
  factory OrderList({
    $core.Iterable<Order>? orders,
  }) {
    final result = create();
    if (orders != null) result.orders.addAll(orders);
    return result;
  }

  OrderList._();

  factory OrderList.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory OrderList.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OrderList',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<Order>(1, _omitFieldNames ? '' : 'orders', subBuilder: Order.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderList clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderList copyWith(void Function(OrderList) updates) =>
      super.copyWith((message) => updates(message as OrderList)) as OrderList;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static OrderList create() => OrderList._();
  @$core.override
  OrderList createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static OrderList getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OrderList>(create);
  static OrderList? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<Order> get orders => $_getList(0);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
