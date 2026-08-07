// This is a generated file - do not edit.
//
// Generated from marketdata.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class TypeBook extends $pb.ProtobufEnum {
  static const TypeBook ASK = TypeBook._(0, _omitEnumNames ? '' : 'ASK');
  static const TypeBook BID = TypeBook._(1, _omitEnumNames ? '' : 'BID');

  static const $core.List<TypeBook> values = <TypeBook>[
    ASK,
    BID,
  ];

  static final $core.List<TypeBook?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 1);
  static TypeBook? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const TypeBook._(super.value, super.name);
}

class ActionIncremental extends $pb.ProtobufEnum {
  static const ActionIncremental UNKNOWN_ACT =
      ActionIncremental._(0, _omitEnumNames ? '' : 'UNKNOWN_ACT');
  static const ActionIncremental ADD_DATA_POSITIONS =
      ActionIncremental._(1, _omitEnumNames ? '' : 'ADD_DATA_POSITIONS');
  static const ActionIncremental UPDATE_DATA_POSITIONS =
      ActionIncremental._(2, _omitEnumNames ? '' : 'UPDATE_DATA_POSITIONS');
  static const ActionIncremental DELETE_DATA_POSITIONS =
      ActionIncremental._(3, _omitEnumNames ? '' : 'DELETE_DATA_POSITIONS');

  static const $core.List<ActionIncremental> values = <ActionIncremental>[
    UNKNOWN_ACT,
    ADD_DATA_POSITIONS,
    UPDATE_DATA_POSITIONS,
    DELETE_DATA_POSITIONS,
  ];

  static final $core.List<ActionIncremental?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static ActionIncremental? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ActionIncremental._(super.value, super.name);
}

class SecurityExchangeMarketData extends $pb.ProtobufEnum {
  static const SecurityExchangeMarketData BCS =
      SecurityExchangeMarketData._(0, _omitEnumNames ? '' : 'BCS');
  static const SecurityExchangeMarketData BVC =
      SecurityExchangeMarketData._(1, _omitEnumNames ? '' : 'BVC');
  static const SecurityExchangeMarketData BVL =
      SecurityExchangeMarketData._(2, _omitEnumNames ? '' : 'BVL');
  static const SecurityExchangeMarketData NYSE =
      SecurityExchangeMarketData._(3, _omitEnumNames ? '' : 'NYSE');
  static const SecurityExchangeMarketData AMEX =
      SecurityExchangeMarketData._(4, _omitEnumNames ? '' : 'AMEX');
  static const SecurityExchangeMarketData NASDAQ =
      SecurityExchangeMarketData._(5, _omitEnumNames ? '' : 'NASDAQ');
  static const SecurityExchangeMarketData DATATEC_XBCL =
      SecurityExchangeMarketData._(6, _omitEnumNames ? '' : 'DATATEC_XBCL');
  static const SecurityExchangeMarketData FH_IBKR =
      SecurityExchangeMarketData._(9, _omitEnumNames ? '' : 'FH_IBKR');
  static const SecurityExchangeMarketData BINANCE_MKD =
      SecurityExchangeMarketData._(10, _omitEnumNames ? '' : 'BINANCE_MKD');
  static const SecurityExchangeMarketData MEXC_MKD =
      SecurityExchangeMarketData._(11, _omitEnumNames ? '' : 'MEXC_MKD');
  static const SecurityExchangeMarketData ALPACA_MKD =
      SecurityExchangeMarketData._(12, _omitEnumNames ? '' : 'ALPACA_MKD');
  static const SecurityExchangeMarketData CRYPTO_MARKET_MKD =
      SecurityExchangeMarketData._(
          13, _omitEnumNames ? '' : 'CRYPTO_MARKET_MKD');
  static const SecurityExchangeMarketData BUDA_MKD =
      SecurityExchangeMarketData._(14, _omitEnumNames ? '' : 'BUDA_MKD');
  static const SecurityExchangeMarketData ORIONX_MKD =
      SecurityExchangeMarketData._(15, _omitEnumNames ? '' : 'ORIONX_MKD');
  static const SecurityExchangeMarketData NONE_MKD =
      SecurityExchangeMarketData._(16, _omitEnumNames ? '' : 'NONE_MKD');
  static const SecurityExchangeMarketData NUAM_MKD =
      SecurityExchangeMarketData._(17, _omitEnumNames ? '' : 'NUAM_MKD');

  static const $core.List<SecurityExchangeMarketData> values =
      <SecurityExchangeMarketData>[
    BCS,
    BVC,
    BVL,
    NYSE,
    AMEX,
    NASDAQ,
    DATATEC_XBCL,
    FH_IBKR,
    BINANCE_MKD,
    MEXC_MKD,
    ALPACA_MKD,
    CRYPTO_MARKET_MKD,
    BUDA_MKD,
    ORIONX_MKD,
    NONE_MKD,
    NUAM_MKD,
  ];

  static final $core.List<SecurityExchangeMarketData?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 17);
  static SecurityExchangeMarketData? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const SecurityExchangeMarketData._(super.value, super.name);
}

class Depth extends $pb.ProtobufEnum {
  static const Depth FULL_BOOK = Depth._(0, _omitEnumNames ? '' : 'FULL_BOOK');
  static const Depth TOP_OF_THE_BOOK =
      Depth._(1, _omitEnumNames ? '' : 'TOP_OF_THE_BOOK');

  static const $core.List<Depth> values = <Depth>[
    FULL_BOOK,
    TOP_OF_THE_BOOK,
  ];

  static final $core.List<Depth?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 1);
  static Depth? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const Depth._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
