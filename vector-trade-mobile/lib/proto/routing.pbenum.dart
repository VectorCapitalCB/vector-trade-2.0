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

class OrderStatus extends $pb.ProtobufEnum {
  static const OrderStatus NEW = OrderStatus._(0, _omitEnumNames ? '' : 'NEW');
  static const OrderStatus PARTIALLY_FILLED =
      OrderStatus._(1, _omitEnumNames ? '' : 'PARTIALLY_FILLED');
  static const OrderStatus FILLED =
      OrderStatus._(2, _omitEnumNames ? '' : 'FILLED');
  static const OrderStatus DONE_FOR_DAY =
      OrderStatus._(3, _omitEnumNames ? '' : 'DONE_FOR_DAY');
  static const OrderStatus CANCELED =
      OrderStatus._(4, _omitEnumNames ? '' : 'CANCELED');
  static const OrderStatus REPLACED =
      OrderStatus._(5, _omitEnumNames ? '' : 'REPLACED');
  static const OrderStatus PENDING_CANCEL =
      OrderStatus._(6, _omitEnumNames ? '' : 'PENDING_CANCEL');
  static const OrderStatus STOPPED =
      OrderStatus._(7, _omitEnumNames ? '' : 'STOPPED');
  static const OrderStatus REJECTED =
      OrderStatus._(8, _omitEnumNames ? '' : 'REJECTED');
  static const OrderStatus SUSPENDED =
      OrderStatus._(9, _omitEnumNames ? '' : 'SUSPENDED');
  static const OrderStatus PENDING_NEW =
      OrderStatus._(93, _omitEnumNames ? '' : 'PENDING_NEW');
  static const OrderStatus CALCULATED =
      OrderStatus._(92, _omitEnumNames ? '' : 'CALCULATED');
  static const OrderStatus EXPIRED =
      OrderStatus._(91, _omitEnumNames ? '' : 'EXPIRED');
  static const OrderStatus PENDING_REPLACE =
      OrderStatus._(90, _omitEnumNames ? '' : 'PENDING_REPLACE');
  static const OrderStatus ABORTED =
      OrderStatus._(94, _omitEnumNames ? '' : 'ABORTED');
  static const OrderStatus TRADE =
      OrderStatus._(95, _omitEnumNames ? '' : 'TRADE');
  static const OrderStatus LIVE_TRADE =
      OrderStatus._(96, _omitEnumNames ? '' : 'LIVE_TRADE');
  static const OrderStatus LIVE =
      OrderStatus._(97, _omitEnumNames ? '' : 'LIVE');
  static const OrderStatus PENDING_LIVE =
      OrderStatus._(98, _omitEnumNames ? '' : 'PENDING_LIVE');
  static const OrderStatus PENDING_ONLY =
      OrderStatus._(99, _omitEnumNames ? '' : 'PENDING_ONLY');
  static const OrderStatus ALL_STATUS =
      OrderStatus._(100, _omitEnumNames ? '' : 'ALL_STATUS');

  static const $core.List<OrderStatus> values = <OrderStatus>[
    NEW,
    PARTIALLY_FILLED,
    FILLED,
    DONE_FOR_DAY,
    CANCELED,
    REPLACED,
    PENDING_CANCEL,
    STOPPED,
    REJECTED,
    SUSPENDED,
    PENDING_NEW,
    CALCULATED,
    EXPIRED,
    PENDING_REPLACE,
    ABORTED,
    TRADE,
    LIVE_TRADE,
    LIVE,
    PENDING_LIVE,
    PENDING_ONLY,
    ALL_STATUS,
  ];

  static final $core.Map<$core.int, OrderStatus> _byValue =
      $pb.ProtobufEnum.initByValue(values);
  static OrderStatus? valueOf($core.int value) => _byValue[value];

  const OrderStatus._(super.value, super.name);
}

class ExecutionType extends $pb.ProtobufEnum {
  static const ExecutionType EXEC_NEW =
      ExecutionType._(0, _omitEnumNames ? '' : 'EXEC_NEW');
  static const ExecutionType EXEC_DONE_FOR_DAY =
      ExecutionType._(3, _omitEnumNames ? '' : 'EXEC_DONE_FOR_DAY');
  static const ExecutionType EXEC_CANCELED =
      ExecutionType._(4, _omitEnumNames ? '' : 'EXEC_CANCELED');
  static const ExecutionType EXEC_REPLACED =
      ExecutionType._(5, _omitEnumNames ? '' : 'EXEC_REPLACED');
  static const ExecutionType EXEC_PENDING_CANCEL =
      ExecutionType._(6, _omitEnumNames ? '' : 'EXEC_PENDING_CANCEL');
  static const ExecutionType EXEC_PENDING_REPLACE =
      ExecutionType._(7, _omitEnumNames ? '' : 'EXEC_PENDING_REPLACE');
  static const ExecutionType EXEC_REJECTED =
      ExecutionType._(8, _omitEnumNames ? '' : 'EXEC_REJECTED');
  static const ExecutionType EXEC_TRADE =
      ExecutionType._(10, _omitEnumNames ? '' : 'EXEC_TRADE');
  static const ExecutionType EXEC_RESTATED =
      ExecutionType._(11, _omitEnumNames ? '' : 'EXEC_RESTATED');
  static const ExecutionType EXEC_CORRECT =
      ExecutionType._(12, _omitEnumNames ? '' : 'EXEC_CORRECT');
  static const ExecutionType EXEC_PENDING_NE =
      ExecutionType._(13, _omitEnumNames ? '' : 'EXEC_PENDING_NE');

  static const $core.List<ExecutionType> values = <ExecutionType>[
    EXEC_NEW,
    EXEC_DONE_FOR_DAY,
    EXEC_CANCELED,
    EXEC_REPLACED,
    EXEC_PENDING_CANCEL,
    EXEC_PENDING_REPLACE,
    EXEC_REJECTED,
    EXEC_TRADE,
    EXEC_RESTATED,
    EXEC_CORRECT,
    EXEC_PENDING_NE,
  ];

  static final $core.List<ExecutionType?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 13);
  static ExecutionType? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ExecutionType._(super.value, super.name);
}

class Side extends $pb.ProtobufEnum {
  static const Side NONE_SIDE = Side._(0, _omitEnumNames ? '' : 'NONE_SIDE');
  static const Side BUY = Side._(1, _omitEnumNames ? '' : 'BUY');
  static const Side SELL = Side._(2, _omitEnumNames ? '' : 'SELL');
  static const Side SELL_SHORT = Side._(5, _omitEnumNames ? '' : 'SELL_SHORT');
  static const Side ALL_SIDE = Side._(100, _omitEnumNames ? '' : 'ALL_SIDE');

  static const $core.List<Side> values = <Side>[
    NONE_SIDE,
    BUY,
    SELL,
    SELL_SHORT,
    ALL_SIDE,
  ];

  static final $core.Map<$core.int, Side> _byValue =
      $pb.ProtobufEnum.initByValue(values);
  static Side? valueOf($core.int value) => _byValue[value];

  const Side._(super.value, super.name);
}

class Tif extends $pb.ProtobufEnum {
  static const Tif DAY = Tif._(0, _omitEnumNames ? '' : 'DAY');
  static const Tif GOOD_TIL_CANCEL =
      Tif._(1, _omitEnumNames ? '' : 'GOOD_TIL_CANCEL');
  static const Tif AT_THE_OPENING =
      Tif._(2, _omitEnumNames ? '' : 'AT_THE_OPENING');
  static const Tif IMMEDIATE_OR_CANCEL =
      Tif._(3, _omitEnumNames ? '' : 'IMMEDIATE_OR_CANCEL');
  static const Tif FILL_OR_KILL =
      Tif._(4, _omitEnumNames ? '' : 'FILL_OR_KILL');

  static const $core.List<Tif> values = <Tif>[
    DAY,
    GOOD_TIL_CANCEL,
    AT_THE_OPENING,
    IMMEDIATE_OR_CANCEL,
    FILL_OR_KILL,
  ];

  static final $core.List<Tif?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static Tif? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const Tif._(super.value, super.name);
}

class SecurityType extends $pb.ProtobufEnum {
  static const SecurityType CS = SecurityType._(0, _omitEnumNames ? '' : 'CS');
  static const SecurityType CFI =
      SecurityType._(1, _omitEnumNames ? '' : 'CFI');
  static const SecurityType MON =
      SecurityType._(2, _omitEnumNames ? '' : 'MON');
  static const SecurityType FUT =
      SecurityType._(3, _omitEnumNames ? '' : 'FUT');
  static const SecurityType OPT =
      SecurityType._(4, _omitEnumNames ? '' : 'OPT');
  static const SecurityType PAXOS =
      SecurityType._(5, _omitEnumNames ? '' : 'PAXOS');

  static const $core.List<SecurityType> values = <SecurityType>[
    CS,
    CFI,
    MON,
    FUT,
    OPT,
    PAXOS,
  ];

  static final $core.List<SecurityType?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static SecurityType? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const SecurityType._(super.value, super.name);
}

class HandlInst extends $pb.ProtobufEnum {
  static const HandlInst NONE_HANDLINST =
      HandlInst._(0, _omitEnumNames ? '' : 'NONE_HANDLINST');
  static const HandlInst PRIVATE_ORDER =
      HandlInst._(1, _omitEnumNames ? '' : 'PRIVATE_ORDER');
  static const HandlInst EXECUTION_ORDER =
      HandlInst._(2, _omitEnumNames ? '' : 'EXECUTION_ORDER');
  static const HandlInst MANUAL =
      HandlInst._(3, _omitEnumNames ? '' : 'MANUAL');
  static const HandlInst ISOLATED_MARGIN =
      HandlInst._(4, _omitEnumNames ? '' : 'ISOLATED_MARGIN');
  static const HandlInst CROSS_MARGIN =
      HandlInst._(5, _omitEnumNames ? '' : 'CROSS_MARGIN');

  static const $core.List<HandlInst> values = <HandlInst>[
    NONE_HANDLINST,
    PRIVATE_ORDER,
    EXECUTION_ORDER,
    MANUAL,
    ISOLATED_MARGIN,
    CROSS_MARGIN,
  ];

  static final $core.List<HandlInst?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static HandlInst? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const HandlInst._(super.value, super.name);
}

class SettlType extends $pb.ProtobufEnum {
  static const SettlType REGULAR =
      SettlType._(0, _omitEnumNames ? '' : 'REGULAR');
  static const SettlType CASH = SettlType._(1, _omitEnumNames ? '' : 'CASH');
  static const SettlType NEXT_DAY =
      SettlType._(2, _omitEnumNames ? '' : 'NEXT_DAY');
  static const SettlType T2 = SettlType._(3, _omitEnumNames ? '' : 'T2');
  static const SettlType T3 = SettlType._(4, _omitEnumNames ? '' : 'T3');
  static const SettlType T5 = SettlType._(9, _omitEnumNames ? '' : 'T5');

  static const $core.List<SettlType> values = <SettlType>[
    REGULAR,
    CASH,
    NEXT_DAY,
    T2,
    T3,
    T5,
  ];

  static final $core.Map<$core.int, SettlType> _byValue =
      $pb.ProtobufEnum.initByValue(values);
  static SettlType? valueOf($core.int value) => _byValue[value];

  const SettlType._(super.value, super.name);
}

class ExecBroker extends $pb.ProtobufEnum {
  static const ExecBroker NO_EXEC =
      ExecBroker._(0, _omitEnumNames ? '' : 'NO_EXEC');
  static const ExecBroker VC = ExecBroker._(1, _omitEnumNames ? '' : 'VC');
  static const ExecBroker IBKR = ExecBroker._(2, _omitEnumNames ? '' : 'IBKR');
  static const ExecBroker DTEC = ExecBroker._(3, _omitEnumNames ? '' : 'DTEC');
  static const ExecBroker XPUS = ExecBroker._(4, _omitEnumNames ? '' : 'XPUS');
  static const ExecBroker BTCL = ExecBroker._(5, _omitEnumNames ? '' : 'BTCL');
  static const ExecBroker BTGY = ExecBroker._(6, _omitEnumNames ? '' : 'BTGY');

  static const $core.List<ExecBroker> values = <ExecBroker>[
    NO_EXEC,
    VC,
    IBKR,
    DTEC,
    XPUS,
    BTCL,
    BTGY,
  ];

  static final $core.List<ExecBroker?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 6);
  static ExecBroker? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ExecBroker._(super.value, super.name);
}

class SecurityExchangeRouting extends $pb.ProtobufEnum {
  static const SecurityExchangeRouting XSGO =
      SecurityExchangeRouting._(0, _omitEnumNames ? '' : 'XSGO');
  static const SecurityExchangeRouting XSGO_OFS =
      SecurityExchangeRouting._(1, _omitEnumNames ? '' : 'XSGO_OFS');
  static const SecurityExchangeRouting XBOG =
      SecurityExchangeRouting._(2, _omitEnumNames ? '' : 'XBOG');
  static const SecurityExchangeRouting XLIM =
      SecurityExchangeRouting._(3, _omitEnumNames ? '' : 'XLIM');
  static const SecurityExchangeRouting XBCL =
      SecurityExchangeRouting._(4, _omitEnumNames ? '' : 'XBCL');
  static const SecurityExchangeRouting IB_SMART =
      SecurityExchangeRouting._(5, _omitEnumNames ? '' : 'IB_SMART');
  static const SecurityExchangeRouting BASKETS =
      SecurityExchangeRouting._(7, _omitEnumNames ? '' : 'BASKETS');
  static const SecurityExchangeRouting MEXC =
      SecurityExchangeRouting._(9, _omitEnumNames ? '' : 'MEXC');
  static const SecurityExchangeRouting ALPACA =
      SecurityExchangeRouting._(10, _omitEnumNames ? '' : 'ALPACA');
  static const SecurityExchangeRouting BBG =
      SecurityExchangeRouting._(11, _omitEnumNames ? '' : 'BBG');
  static const SecurityExchangeRouting CRYPTO_MARKET =
      SecurityExchangeRouting._(12, _omitEnumNames ? '' : 'CRYPTO_MARKET');
  static const SecurityExchangeRouting BINANCE =
      SecurityExchangeRouting._(13, _omitEnumNames ? '' : 'BINANCE');
  static const SecurityExchangeRouting ALL_SECURITY_EXCHANGE =
      SecurityExchangeRouting._(
          100, _omitEnumNames ? '' : 'ALL_SECURITY_EXCHANGE');
  static const SecurityExchangeRouting NONE_ROUTING =
      SecurityExchangeRouting._(101, _omitEnumNames ? '' : 'NONE_ROUTING');
  static const SecurityExchangeRouting RT_XP =
      SecurityExchangeRouting._(17, _omitEnumNames ? '' : 'RT_XP');
  static const SecurityExchangeRouting NUAM =
      SecurityExchangeRouting._(18, _omitEnumNames ? '' : 'NUAM');
  static const SecurityExchangeRouting XSGO_1 =
      SecurityExchangeRouting._(19, _omitEnumNames ? '' : 'XSGO_1');
  static const SecurityExchangeRouting XSGO_2 =
      SecurityExchangeRouting._(20, _omitEnumNames ? '' : 'XSGO_2');
  static const SecurityExchangeRouting XSGO_3 =
      SecurityExchangeRouting._(21, _omitEnumNames ? '' : 'XSGO_3');
  static const SecurityExchangeRouting XSGO_4 =
      SecurityExchangeRouting._(22, _omitEnumNames ? '' : 'XSGO_4');

  static const $core.List<SecurityExchangeRouting> values =
      <SecurityExchangeRouting>[
    XSGO,
    XSGO_OFS,
    XBOG,
    XLIM,
    XBCL,
    IB_SMART,
    BASKETS,
    MEXC,
    ALPACA,
    BBG,
    CRYPTO_MARKET,
    BINANCE,
    ALL_SECURITY_EXCHANGE,
    NONE_ROUTING,
    RT_XP,
    NUAM,
    XSGO_1,
    XSGO_2,
    XSGO_3,
    XSGO_4,
  ];

  static final $core.Map<$core.int, SecurityExchangeRouting> _byValue =
      $pb.ProtobufEnum.initByValue(values);
  static SecurityExchangeRouting? valueOf($core.int value) => _byValue[value];

  const SecurityExchangeRouting._(super.value, super.name);
}

class ExStrategy extends $pb.ProtobufEnum {
  static const ExStrategy NO_STRATEGY =
      ExStrategy._(0, _omitEnumNames ? '' : 'NO_STRATEGY');
  static const ExStrategy AGGRESSIVE =
      ExStrategy._(1, _omitEnumNames ? '' : 'AGGRESSIVE');
  static const ExStrategy PASSIVE =
      ExStrategy._(2, _omitEnumNames ? '' : 'PASSIVE');
  static const ExStrategy LAST = ExStrategy._(3, _omitEnumNames ? '' : 'LAST');
  static const ExStrategy PX = ExStrategy._(4, _omitEnumNames ? '' : 'PX');
  static const ExStrategy MID = ExStrategy._(5, _omitEnumNames ? '' : 'MID');
  static const ExStrategy FIXED =
      ExStrategy._(6, _omitEnumNames ? '' : 'FIXED');
  static const ExStrategy MOC = ExStrategy._(7, _omitEnumNames ? '' : 'MOC');

  static const $core.List<ExStrategy> values = <ExStrategy>[
    NO_STRATEGY,
    AGGRESSIVE,
    PASSIVE,
    LAST,
    PX,
    MID,
    FIXED,
    MOC,
  ];

  static final $core.List<ExStrategy?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 7);
  static ExStrategy? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ExStrategy._(super.value, super.name);
}

class StrategyOrder extends $pb.ProtobufEnum {
  static const StrategyOrder NONE_STRATEGY =
      StrategyOrder._(0, _omitEnumNames ? '' : 'NONE_STRATEGY');
  static const StrategyOrder OCO =
      StrategyOrder._(1, _omitEnumNames ? '' : 'OCO');
  static const StrategyOrder TRAILING =
      StrategyOrder._(2, _omitEnumNames ? '' : 'TRAILING');
  static const StrategyOrder BASKET =
      StrategyOrder._(3, _omitEnumNames ? '' : 'BASKET');
  static const StrategyOrder BEST =
      StrategyOrder._(4, _omitEnumNames ? '' : 'BEST');
  static const StrategyOrder HOLGURA =
      StrategyOrder._(5, _omitEnumNames ? '' : 'HOLGURA');
  static const StrategyOrder SCALPING =
      StrategyOrder._(6, _omitEnumNames ? '' : 'SCALPING');
  static const StrategyOrder BASKET_AGGRESSIVE =
      StrategyOrder._(7, _omitEnumNames ? '' : 'BASKET_AGGRESSIVE');
  static const StrategyOrder BASKET_PASSIVE =
      StrategyOrder._(8, _omitEnumNames ? '' : 'BASKET_PASSIVE');
  static const StrategyOrder BASKET_LAST =
      StrategyOrder._(9, _omitEnumNames ? '' : 'BASKET_LAST');
  static const StrategyOrder VWAP =
      StrategyOrder._(10, _omitEnumNames ? '' : 'VWAP');

  static const $core.List<StrategyOrder> values = <StrategyOrder>[
    NONE_STRATEGY,
    OCO,
    TRAILING,
    BASKET,
    BEST,
    HOLGURA,
    SCALPING,
    BASKET_AGGRESSIVE,
    BASKET_PASSIVE,
    BASKET_LAST,
    VWAP,
  ];

  static final $core.List<StrategyOrder?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 10);
  static StrategyOrder? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const StrategyOrder._(super.value, super.name);
}

class OrdType extends $pb.ProtobufEnum {
  static const OrdType NONE = OrdType._(0, _omitEnumNames ? '' : 'NONE');
  static const OrdType MARKET = OrdType._(1, _omitEnumNames ? '' : 'MARKET');
  static const OrdType LIMIT = OrdType._(2, _omitEnumNames ? '' : 'LIMIT');
  static const OrdType MARKET_CLOSE =
      OrdType._(3, _omitEnumNames ? '' : 'MARKET_CLOSE');
  static const OrdType STOP_LOSS =
      OrdType._(5, _omitEnumNames ? '' : 'STOP_LOSS');

  static const $core.List<OrdType> values = <OrdType>[
    NONE,
    MARKET,
    LIMIT,
    MARKET_CLOSE,
    STOP_LOSS,
  ];

  static final $core.List<OrdType?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static OrdType? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const OrdType._(super.value, super.name);
}

class Currency extends $pb.ProtobufEnum {
  static const Currency NO_CURRENCY =
      Currency._(0, _omitEnumNames ? '' : 'NO_CURRENCY');
  static const Currency CLP = Currency._(1, _omitEnumNames ? '' : 'CLP');
  static const Currency USD = Currency._(2, _omitEnumNames ? '' : 'USD');
  static const Currency CAD = Currency._(3, _omitEnumNames ? '' : 'CAD');
  static const Currency COP = Currency._(4, _omitEnumNames ? '' : 'COP');
  static const Currency PEN = Currency._(5, _omitEnumNames ? '' : 'PEN');
  static const Currency GBP = Currency._(6, _omitEnumNames ? '' : 'GBP');
  static const Currency USDC = Currency._(7, _omitEnumNames ? '' : 'USDC');
  static const Currency USDT = Currency._(8, _omitEnumNames ? '' : 'USDT');
  static const Currency BTC = Currency._(9, _omitEnumNames ? '' : 'BTC');
  static const Currency ETH = Currency._(10, _omitEnumNames ? '' : 'ETH');
  static const Currency EUR = Currency._(11, _omitEnumNames ? '' : 'EUR');

  static const $core.List<Currency> values = <Currency>[
    NO_CURRENCY,
    CLP,
    USD,
    CAD,
    COP,
    PEN,
    GBP,
    USDC,
    USDT,
    BTC,
    ETH,
    EUR,
  ];

  static final $core.List<Currency?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 11);
  static Currency? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const Currency._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
