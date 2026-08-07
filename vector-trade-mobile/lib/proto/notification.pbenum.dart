// This is a generated file - do not edit.
//
// Generated from notification.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class ConnectionSource extends $pb.ProtobufEnum {
  static const ConnectionSource BUYSIDE =
      ConnectionSource._(0, _omitEnumNames ? '' : 'BUYSIDE');
  static const ConnectionSource SELLSIDE =
      ConnectionSource._(1, _omitEnumNames ? '' : 'SELLSIDE');
  static const ConnectionSource OTHER =
      ConnectionSource._(2, _omitEnumNames ? '' : 'OTHER');

  static const $core.List<ConnectionSource> values = <ConnectionSource>[
    BUYSIDE,
    SELLSIDE,
    OTHER,
  ];

  static final $core.List<ConnectionSource?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static ConnectionSource? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ConnectionSource._(super.value, super.name);
}

class Level extends $pb.ProtobufEnum {
  static const Level UNKNOWN_STATE =
      Level._(0, _omitEnumNames ? '' : 'UNKNOWN_STATE');
  static const Level SUCCESS = Level._(1, _omitEnumNames ? '' : 'SUCCESS');
  static const Level FATAL = Level._(2, _omitEnumNames ? '' : 'FATAL');
  static const Level WARN = Level._(3, _omitEnumNames ? '' : 'WARN');
  static const Level ERROR = Level._(4, _omitEnumNames ? '' : 'ERROR');
  static const Level INFO = Level._(5, _omitEnumNames ? '' : 'INFO');

  static const $core.List<Level> values = <Level>[
    UNKNOWN_STATE,
    SUCCESS,
    FATAL,
    WARN,
    ERROR,
    INFO,
  ];

  static final $core.List<Level?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static Level? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const Level._(super.value, super.name);
}

class TypeState extends $pb.ProtobufEnum {
  static const TypeState DISCONNECTION =
      TypeState._(0, _omitEnumNames ? '' : 'DISCONNECTION');
  static const TypeState CONNECTION =
      TypeState._(1, _omitEnumNames ? '' : 'CONNECTION');
  static const TypeState TEST_REQUEST =
      TypeState._(2, _omitEnumNames ? '' : 'TEST_REQUEST');
  static const TypeState REJECT =
      TypeState._(3, _omitEnumNames ? '' : 'REJECT');

  static const $core.List<TypeState> values = <TypeState>[
    DISCONNECTION,
    CONNECTION,
    TEST_REQUEST,
    REJECT,
  ];

  static final $core.List<TypeState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static TypeState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const TypeState._(super.value, super.name);
}

class Component extends $pb.ProtobufEnum {
  static const Component VECTOR_TRADE_SERVICES =
      Component._(0, _omitEnumNames ? '' : 'VECTOR_TRADE_SERVICES');
  static const Component VECTOR_TRADE_FRONT =
      Component._(7, _omitEnumNames ? '' : 'VECTOR_TRADE_FRONT');
  static const Component ORB = Component._(1, _omitEnumNames ? '' : 'ORB');
  static const Component XRO = Component._(6, _omitEnumNames ? '' : 'XRO');
  static const Component VECTOR_SCREEN_SERVICE =
      Component._(9, _omitEnumNames ? '' : 'VECTOR_SCREEN_SERVICE');
  static const Component VECTOR_SCREEN_FRONT =
      Component._(10, _omitEnumNames ? '' : 'VECTOR_SCREEN_FRONT');
  static const Component ALGO_ADR_ARBITRAGE_NORMAL_CL =
      Component._(3, _omitEnumNames ? '' : 'ALGO_ADR_ARBITRAGE_NORMAL_CL');
  static const Component ALGO_ADR_ARBITRAGE_INVERSO_CL =
      Component._(4, _omitEnumNames ? '' : 'ALGO_ADR_ARBITRAGE_INVERSO_CL');
  static const Component ALGO_SPREAD_CL =
      Component._(5, _omitEnumNames ? '' : 'ALGO_SPREAD_CL');
  static const Component SIMULATOR =
      Component._(11, _omitEnumNames ? '' : 'SIMULATOR');
  static const Component ETF_ECH =
      Component._(12, _omitEnumNames ? '' : 'ETF_ECH');
  static const Component ETF_IPSA =
      Component._(13, _omitEnumNames ? '' : 'ETF_IPSA');
  static const Component ALGO_SPREAD_MM =
      Component._(17, _omitEnumNames ? '' : 'ALGO_SPREAD_MM');
  static const Component ALGO_MONITOR_PRICE =
      Component._(18, _omitEnumNames ? '' : 'ALGO_MONITOR_PRICE');
  static const Component ETF_IPSA2 =
      Component._(19, _omitEnumNames ? '' : 'ETF_IPSA2');
  static const Component ETF_IPSA3 =
      Component._(20, _omitEnumNames ? '' : 'ETF_IPSA3');
  static const Component ORB_NUAM =
      Component._(21, _omitEnumNames ? '' : 'ORB_NUAM');
  static const Component ORB_BCS =
      Component._(22, _omitEnumNames ? '' : 'ORB_BCS');
  static const Component ORB_ALPACA =
      Component._(23, _omitEnumNames ? '' : 'ORB_ALPACA');
  static const Component ORB_IB =
      Component._(24, _omitEnumNames ? '' : 'ORB_IB');
  static const Component XRO_NUAM =
      Component._(25, _omitEnumNames ? '' : 'XRO_NUAM');
  static const Component ALGO_AVN =
      Component._(26, _omitEnumNames ? '' : 'ALGO_AVN');

  static const $core.List<Component> values = <Component>[
    VECTOR_TRADE_SERVICES,
    VECTOR_TRADE_FRONT,
    ORB,
    XRO,
    VECTOR_SCREEN_SERVICE,
    VECTOR_SCREEN_FRONT,
    ALGO_ADR_ARBITRAGE_NORMAL_CL,
    ALGO_ADR_ARBITRAGE_INVERSO_CL,
    ALGO_SPREAD_CL,
    SIMULATOR,
    ETF_ECH,
    ETF_IPSA,
    ALGO_SPREAD_MM,
    ALGO_MONITOR_PRICE,
    ETF_IPSA2,
    ETF_IPSA3,
    ORB_NUAM,
    ORB_BCS,
    ORB_ALPACA,
    ORB_IB,
    XRO_NUAM,
    ALGO_AVN,
  ];

  static final $core.List<Component?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 26);
  static Component? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const Component._(super.value, super.name);
}

class NotificationRequestType extends $pb.ProtobufEnum {
  static const NotificationRequestType CONNECTION_REQUEST =
      NotificationRequestType._(0, _omitEnumNames ? '' : 'CONNECTION_REQUEST');
  static const NotificationRequestType MESSAGES_REQUEST =
      NotificationRequestType._(1, _omitEnumNames ? '' : 'MESSAGES_REQUEST');

  static const $core.List<NotificationRequestType> values =
      <NotificationRequestType>[
    CONNECTION_REQUEST,
    MESSAGES_REQUEST,
  ];

  static final $core.List<NotificationRequestType?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 1);
  static NotificationRequestType? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const NotificationRequestType._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
