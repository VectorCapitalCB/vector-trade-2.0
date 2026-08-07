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

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;
import 'package:protobuf/well_known_types/google/protobuf/timestamp.pb.dart'
    as $0;

import 'marketdata.pbenum.dart';
import 'routing.pbenum.dart' as $1;

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'marketdata.pbenum.dart';

class News extends $pb.GeneratedMessage {
  factory News({
    $core.String? texto,
    $core.String? lineoftext,
    SecurityExchangeMarketData? securityExchange,
    $0.Timestamp? t,
  }) {
    final result = create();
    if (texto != null) result.texto = texto;
    if (lineoftext != null) result.lineoftext = lineoftext;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (t != null) result.t = t;
    return result;
  }

  News._();

  factory News.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory News.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'News',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'texto')
    ..aOS(2, _omitFieldNames ? '' : 'lineoftext')
    ..aE<SecurityExchangeMarketData>(
        3, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOM<$0.Timestamp>(4, _omitFieldNames ? '' : 't',
        subBuilder: $0.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  News clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  News copyWith(void Function(News) updates) =>
      super.copyWith((message) => updates(message as News)) as News;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static News create() => News._();
  @$core.override
  News createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static News getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<News>(create);
  static News? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get texto => $_getSZ(0);
  @$pb.TagNumber(1)
  set texto($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTexto() => $_has(0);
  @$pb.TagNumber(1)
  void clearTexto() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get lineoftext => $_getSZ(1);
  @$pb.TagNumber(2)
  set lineoftext($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasLineoftext() => $_has(1);
  @$pb.TagNumber(2)
  void clearLineoftext() => $_clearField(2);

  @$pb.TagNumber(3)
  SecurityExchangeMarketData get securityExchange => $_getN(2);
  @$pb.TagNumber(3)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasSecurityExchange() => $_has(2);
  @$pb.TagNumber(3)
  void clearSecurityExchange() => $_clearField(3);

  @$pb.TagNumber(4)
  $0.Timestamp get t => $_getN(3);
  @$pb.TagNumber(4)
  set t($0.Timestamp value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasT() => $_has(3);
  @$pb.TagNumber(4)
  void clearT() => $_clearField(4);
  @$pb.TagNumber(4)
  $0.Timestamp ensureT() => $_ensure(3);
}

class SnapshotNews extends $pb.GeneratedMessage {
  factory SnapshotNews({
    $core.Iterable<News>? news,
  }) {
    final result = create();
    if (news != null) result.news.addAll(news);
    return result;
  }

  SnapshotNews._();

  factory SnapshotNews.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotNews.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotNews',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<News>(1, _omitFieldNames ? '' : 'news', subBuilder: News.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotNews clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotNews copyWith(void Function(SnapshotNews) updates) =>
      super.copyWith((message) => updates(message as SnapshotNews))
          as SnapshotNews;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotNews create() => SnapshotNews._();
  @$core.override
  SnapshotNews createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotNews getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotNews>(create);
  static SnapshotNews? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<News> get news => $_getList(0);
}

class Subscribe extends $pb.GeneratedMessage {
  factory Subscribe({
    $core.String? symbol,
    $core.String? id,
    $1.SettlType? settlType,
    $1.SecurityType? securityType,
    $core.bool? trade,
    $core.bool? statistic,
    Depth? depth,
    SecurityExchangeMarketData? securityExchange,
    $core.bool? book,
    $core.int? tradeQty,
    $core.int? bookQty,
  }) {
    final result = create();
    if (symbol != null) result.symbol = symbol;
    if (id != null) result.id = id;
    if (settlType != null) result.settlType = settlType;
    if (securityType != null) result.securityType = securityType;
    if (trade != null) result.trade = trade;
    if (statistic != null) result.statistic = statistic;
    if (depth != null) result.depth = depth;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (book != null) result.book = book;
    if (tradeQty != null) result.tradeQty = tradeQty;
    if (bookQty != null) result.bookQty = bookQty;
    return result;
  }

  Subscribe._();

  factory Subscribe.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Subscribe.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Subscribe',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'symbol')
    ..aOS(2, _omitFieldNames ? '' : 'id')
    ..aE<$1.SettlType>(3, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<$1.SecurityType>(4, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..aOB(5, _omitFieldNames ? '' : 'trade')
    ..aOB(6, _omitFieldNames ? '' : 'statistic')
    ..aE<Depth>(7, _omitFieldNames ? '' : 'depth', enumValues: Depth.values)
    ..aE<SecurityExchangeMarketData>(
        8, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOB(9, _omitFieldNames ? '' : 'book')
    ..aI(10, _omitFieldNames ? '' : 'tradeQty', protoName: 'tradeQty')
    ..aI(11, _omitFieldNames ? '' : 'bookQty', protoName: 'bookQty')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Subscribe clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Subscribe copyWith(void Function(Subscribe) updates) =>
      super.copyWith((message) => updates(message as Subscribe)) as Subscribe;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Subscribe create() => Subscribe._();
  @$core.override
  Subscribe createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Subscribe getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Subscribe>(create);
  static Subscribe? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get symbol => $_getSZ(0);
  @$pb.TagNumber(1)
  set symbol($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSymbol() => $_has(0);
  @$pb.TagNumber(1)
  void clearSymbol() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get id => $_getSZ(1);
  @$pb.TagNumber(2)
  set id($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasId() => $_has(1);
  @$pb.TagNumber(2)
  void clearId() => $_clearField(2);

  @$pb.TagNumber(3)
  $1.SettlType get settlType => $_getN(2);
  @$pb.TagNumber(3)
  set settlType($1.SettlType value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasSettlType() => $_has(2);
  @$pb.TagNumber(3)
  void clearSettlType() => $_clearField(3);

  @$pb.TagNumber(4)
  $1.SecurityType get securityType => $_getN(3);
  @$pb.TagNumber(4)
  set securityType($1.SecurityType value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasSecurityType() => $_has(3);
  @$pb.TagNumber(4)
  void clearSecurityType() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.bool get trade => $_getBF(4);
  @$pb.TagNumber(5)
  set trade($core.bool value) => $_setBool(4, value);
  @$pb.TagNumber(5)
  $core.bool hasTrade() => $_has(4);
  @$pb.TagNumber(5)
  void clearTrade() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.bool get statistic => $_getBF(5);
  @$pb.TagNumber(6)
  set statistic($core.bool value) => $_setBool(5, value);
  @$pb.TagNumber(6)
  $core.bool hasStatistic() => $_has(5);
  @$pb.TagNumber(6)
  void clearStatistic() => $_clearField(6);

  @$pb.TagNumber(7)
  Depth get depth => $_getN(6);
  @$pb.TagNumber(7)
  set depth(Depth value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasDepth() => $_has(6);
  @$pb.TagNumber(7)
  void clearDepth() => $_clearField(7);

  @$pb.TagNumber(8)
  SecurityExchangeMarketData get securityExchange => $_getN(7);
  @$pb.TagNumber(8)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasSecurityExchange() => $_has(7);
  @$pb.TagNumber(8)
  void clearSecurityExchange() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.bool get book => $_getBF(8);
  @$pb.TagNumber(9)
  set book($core.bool value) => $_setBool(8, value);
  @$pb.TagNumber(9)
  $core.bool hasBook() => $_has(8);
  @$pb.TagNumber(9)
  void clearBook() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.int get tradeQty => $_getIZ(9);
  @$pb.TagNumber(10)
  set tradeQty($core.int value) => $_setSignedInt32(9, value);
  @$pb.TagNumber(10)
  $core.bool hasTradeQty() => $_has(9);
  @$pb.TagNumber(10)
  void clearTradeQty() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.int get bookQty => $_getIZ(10);
  @$pb.TagNumber(11)
  set bookQty($core.int value) => $_setSignedInt32(10, value);
  @$pb.TagNumber(11)
  $core.bool hasBookQty() => $_has(10);
  @$pb.TagNumber(11)
  void clearBookQty() => $_clearField(11);
}

class Security extends $pb.GeneratedMessage {
  factory Security({
    $core.String? symbol,
    $core.String? securityType,
    SecurityExchangeMarketData? securityExchange,
    $core.String? currency,
    $core.String? text,
    $core.String? securityID,
  }) {
    final result = create();
    if (symbol != null) result.symbol = symbol;
    if (securityType != null) result.securityType = securityType;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (currency != null) result.currency = currency;
    if (text != null) result.text = text;
    if (securityID != null) result.securityID = securityID;
    return result;
  }

  Security._();

  factory Security.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Security.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Security',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'symbol')
    ..aOS(2, _omitFieldNames ? '' : 'securityType', protoName: 'securityType')
    ..aE<SecurityExchangeMarketData>(
        3, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOS(4, _omitFieldNames ? '' : 'currency')
    ..aOS(5, _omitFieldNames ? '' : 'text')
    ..aOS(6, _omitFieldNames ? '' : 'securityID', protoName: 'securityID')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Security clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Security copyWith(void Function(Security) updates) =>
      super.copyWith((message) => updates(message as Security)) as Security;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Security create() => Security._();
  @$core.override
  Security createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Security getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Security>(create);
  static Security? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get symbol => $_getSZ(0);
  @$pb.TagNumber(1)
  set symbol($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSymbol() => $_has(0);
  @$pb.TagNumber(1)
  void clearSymbol() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get securityType => $_getSZ(1);
  @$pb.TagNumber(2)
  set securityType($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSecurityType() => $_has(1);
  @$pb.TagNumber(2)
  void clearSecurityType() => $_clearField(2);

  @$pb.TagNumber(3)
  SecurityExchangeMarketData get securityExchange => $_getN(2);
  @$pb.TagNumber(3)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasSecurityExchange() => $_has(2);
  @$pb.TagNumber(3)
  void clearSecurityExchange() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get currency => $_getSZ(3);
  @$pb.TagNumber(4)
  set currency($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCurrency() => $_has(3);
  @$pb.TagNumber(4)
  void clearCurrency() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get text => $_getSZ(4);
  @$pb.TagNumber(5)
  set text($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasText() => $_has(4);
  @$pb.TagNumber(5)
  void clearText() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get securityID => $_getSZ(5);
  @$pb.TagNumber(6)
  set securityID($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasSecurityID() => $_has(5);
  @$pb.TagNumber(6)
  void clearSecurityID() => $_clearField(6);
}

class SecurityList extends $pb.GeneratedMessage {
  factory SecurityList({
    $core.String? id,
    $core.Iterable<Security>? listSecurities,
    SecurityExchangeMarketData? securityExchange,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (listSecurities != null) result.listSecurities.addAll(listSecurities);
    if (securityExchange != null) result.securityExchange = securityExchange;
    return result;
  }

  SecurityList._();

  factory SecurityList.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SecurityList.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SecurityList',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..pPM<Security>(2, _omitFieldNames ? '' : 'listSecurities',
        protoName: 'listSecurities', subBuilder: Security.create)
    ..aE<SecurityExchangeMarketData>(
        3, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SecurityList clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SecurityList copyWith(void Function(SecurityList) updates) =>
      super.copyWith((message) => updates(message as SecurityList))
          as SecurityList;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SecurityList create() => SecurityList._();
  @$core.override
  SecurityList createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SecurityList getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SecurityList>(create);
  static SecurityList? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<Security> get listSecurities => $_getList(1);

  @$pb.TagNumber(3)
  SecurityExchangeMarketData get securityExchange => $_getN(2);
  @$pb.TagNumber(3)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasSecurityExchange() => $_has(2);
  @$pb.TagNumber(3)
  void clearSecurityExchange() => $_clearField(3);
}

class SecurityListRequest extends $pb.GeneratedMessage {
  factory SecurityListRequest({
    $core.String? id,
  }) {
    final result = create();
    if (id != null) result.id = id;
    return result;
  }

  SecurityListRequest._();

  factory SecurityListRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SecurityListRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SecurityListRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SecurityListRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SecurityListRequest copyWith(void Function(SecurityListRequest) updates) =>
      super.copyWith((message) => updates(message as SecurityListRequest))
          as SecurityListRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SecurityListRequest create() => SecurityListRequest._();
  @$core.override
  SecurityListRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SecurityListRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SecurityListRequest>(create);
  static SecurityListRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);
}

class Unsubscribe extends $pb.GeneratedMessage {
  factory Unsubscribe({
    $core.String? id,
  }) {
    final result = create();
    if (id != null) result.id = id;
    return result;
  }

  Unsubscribe._();

  factory Unsubscribe.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Unsubscribe.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Unsubscribe',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Unsubscribe clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Unsubscribe copyWith(void Function(Unsubscribe) updates) =>
      super.copyWith((message) => updates(message as Unsubscribe))
          as Unsubscribe;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Unsubscribe create() => Unsubscribe._();
  @$core.override
  Unsubscribe createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Unsubscribe getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Unsubscribe>(create);
  static Unsubscribe? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);
}

class Snapshot extends $pb.GeneratedMessage {
  factory Snapshot({
    $core.Iterable<DataBook>? asks,
    $core.Iterable<DataBook>? bids,
    $core.Iterable<Trade>? trades,
    Statistic? statistic,
    $core.String? id,
    SecurityExchangeMarketData? securityExchange,
    $core.String? symbol,
    $1.SettlType? settlType,
    $1.SecurityType? securityType,
  }) {
    final result = create();
    if (asks != null) result.asks.addAll(asks);
    if (bids != null) result.bids.addAll(bids);
    if (trades != null) result.trades.addAll(trades);
    if (statistic != null) result.statistic = statistic;
    if (id != null) result.id = id;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (symbol != null) result.symbol = symbol;
    if (settlType != null) result.settlType = settlType;
    if (securityType != null) result.securityType = securityType;
    return result;
  }

  Snapshot._();

  factory Snapshot.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Snapshot.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Snapshot',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<DataBook>(1, _omitFieldNames ? '' : 'asks',
        subBuilder: DataBook.create)
    ..pPM<DataBook>(2, _omitFieldNames ? '' : 'bids',
        subBuilder: DataBook.create)
    ..pPM<Trade>(3, _omitFieldNames ? '' : 'trades', subBuilder: Trade.create)
    ..aOM<Statistic>(4, _omitFieldNames ? '' : 'statistic',
        subBuilder: Statistic.create)
    ..aOS(6, _omitFieldNames ? '' : 'id')
    ..aE<SecurityExchangeMarketData>(
        7, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOS(8, _omitFieldNames ? '' : 'symbol')
    ..aE<$1.SettlType>(9, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<$1.SecurityType>(11, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Snapshot clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Snapshot copyWith(void Function(Snapshot) updates) =>
      super.copyWith((message) => updates(message as Snapshot)) as Snapshot;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Snapshot create() => Snapshot._();
  @$core.override
  Snapshot createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Snapshot getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Snapshot>(create);
  static Snapshot? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<DataBook> get asks => $_getList(0);

  @$pb.TagNumber(2)
  $pb.PbList<DataBook> get bids => $_getList(1);

  @$pb.TagNumber(3)
  $pb.PbList<Trade> get trades => $_getList(2);

  @$pb.TagNumber(4)
  Statistic get statistic => $_getN(3);
  @$pb.TagNumber(4)
  set statistic(Statistic value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasStatistic() => $_has(3);
  @$pb.TagNumber(4)
  void clearStatistic() => $_clearField(4);
  @$pb.TagNumber(4)
  Statistic ensureStatistic() => $_ensure(3);

  @$pb.TagNumber(6)
  $core.String get id => $_getSZ(4);
  @$pb.TagNumber(6)
  set id($core.String value) => $_setString(4, value);
  @$pb.TagNumber(6)
  $core.bool hasId() => $_has(4);
  @$pb.TagNumber(6)
  void clearId() => $_clearField(6);

  @$pb.TagNumber(7)
  SecurityExchangeMarketData get securityExchange => $_getN(5);
  @$pb.TagNumber(7)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasSecurityExchange() => $_has(5);
  @$pb.TagNumber(7)
  void clearSecurityExchange() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.String get symbol => $_getSZ(6);
  @$pb.TagNumber(8)
  set symbol($core.String value) => $_setString(6, value);
  @$pb.TagNumber(8)
  $core.bool hasSymbol() => $_has(6);
  @$pb.TagNumber(8)
  void clearSymbol() => $_clearField(8);

  @$pb.TagNumber(9)
  $1.SettlType get settlType => $_getN(7);
  @$pb.TagNumber(9)
  set settlType($1.SettlType value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasSettlType() => $_has(7);
  @$pb.TagNumber(9)
  void clearSettlType() => $_clearField(9);

  @$pb.TagNumber(11)
  $1.SecurityType get securityType => $_getN(8);
  @$pb.TagNumber(11)
  set securityType($1.SecurityType value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasSecurityType() => $_has(8);
  @$pb.TagNumber(11)
  void clearSecurityType() => $_clearField(11);
}

class Trade extends $pb.GeneratedMessage {
  factory Trade({
    $0.Timestamp? t,
    $core.double? price,
    $core.double? qty,
    $core.double? amount,
    $core.String? buyer,
    $core.String? seller,
    $core.String? id,
    $core.String? symbol,
    $1.SettlType? settlType,
    SecurityExchangeMarketData? securityExchange,
    $1.SecurityType? securityType,
    $core.String? idGenerico,
  }) {
    final result = create();
    if (t != null) result.t = t;
    if (price != null) result.price = price;
    if (qty != null) result.qty = qty;
    if (amount != null) result.amount = amount;
    if (buyer != null) result.buyer = buyer;
    if (seller != null) result.seller = seller;
    if (id != null) result.id = id;
    if (symbol != null) result.symbol = symbol;
    if (settlType != null) result.settlType = settlType;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (securityType != null) result.securityType = securityType;
    if (idGenerico != null) result.idGenerico = idGenerico;
    return result;
  }

  Trade._();

  factory Trade.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Trade.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Trade',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<$0.Timestamp>(1, _omitFieldNames ? '' : 't',
        subBuilder: $0.Timestamp.create)
    ..aD(2, _omitFieldNames ? '' : 'price')
    ..aD(3, _omitFieldNames ? '' : 'qty')
    ..aD(4, _omitFieldNames ? '' : 'amount')
    ..aOS(5, _omitFieldNames ? '' : 'buyer')
    ..aOS(6, _omitFieldNames ? '' : 'seller')
    ..aOS(7, _omitFieldNames ? '' : 'id')
    ..aOS(8, _omitFieldNames ? '' : 'symbol')
    ..aE<$1.SettlType>(9, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<SecurityExchangeMarketData>(
        10, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aE<$1.SecurityType>(11, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..aOS(12, _omitFieldNames ? '' : 'idGenerico', protoName: 'idGenerico')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Trade clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Trade copyWith(void Function(Trade) updates) =>
      super.copyWith((message) => updates(message as Trade)) as Trade;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Trade create() => Trade._();
  @$core.override
  Trade createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Trade getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Trade>(create);
  static Trade? _defaultInstance;

  @$pb.TagNumber(1)
  $0.Timestamp get t => $_getN(0);
  @$pb.TagNumber(1)
  set t($0.Timestamp value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasT() => $_has(0);
  @$pb.TagNumber(1)
  void clearT() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.Timestamp ensureT() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.double get price => $_getN(1);
  @$pb.TagNumber(2)
  set price($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPrice() => $_has(1);
  @$pb.TagNumber(2)
  void clearPrice() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get qty => $_getN(2);
  @$pb.TagNumber(3)
  set qty($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasQty() => $_has(2);
  @$pb.TagNumber(3)
  void clearQty() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get amount => $_getN(3);
  @$pb.TagNumber(4)
  set amount($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasAmount() => $_has(3);
  @$pb.TagNumber(4)
  void clearAmount() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get buyer => $_getSZ(4);
  @$pb.TagNumber(5)
  set buyer($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasBuyer() => $_has(4);
  @$pb.TagNumber(5)
  void clearBuyer() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get seller => $_getSZ(5);
  @$pb.TagNumber(6)
  set seller($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasSeller() => $_has(5);
  @$pb.TagNumber(6)
  void clearSeller() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get id => $_getSZ(6);
  @$pb.TagNumber(7)
  set id($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasId() => $_has(6);
  @$pb.TagNumber(7)
  void clearId() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.String get symbol => $_getSZ(7);
  @$pb.TagNumber(8)
  set symbol($core.String value) => $_setString(7, value);
  @$pb.TagNumber(8)
  $core.bool hasSymbol() => $_has(7);
  @$pb.TagNumber(8)
  void clearSymbol() => $_clearField(8);

  @$pb.TagNumber(9)
  $1.SettlType get settlType => $_getN(8);
  @$pb.TagNumber(9)
  set settlType($1.SettlType value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasSettlType() => $_has(8);
  @$pb.TagNumber(9)
  void clearSettlType() => $_clearField(9);

  @$pb.TagNumber(10)
  SecurityExchangeMarketData get securityExchange => $_getN(9);
  @$pb.TagNumber(10)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasSecurityExchange() => $_has(9);
  @$pb.TagNumber(10)
  void clearSecurityExchange() => $_clearField(10);

  @$pb.TagNumber(11)
  $1.SecurityType get securityType => $_getN(10);
  @$pb.TagNumber(11)
  set securityType($1.SecurityType value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasSecurityType() => $_has(10);
  @$pb.TagNumber(11)
  void clearSecurityType() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.String get idGenerico => $_getSZ(11);
  @$pb.TagNumber(12)
  set idGenerico($core.String value) => $_setString(11, value);
  @$pb.TagNumber(12)
  $core.bool hasIdGenerico() => $_has(11);
  @$pb.TagNumber(12)
  void clearIdGenerico() => $_clearField(12);
}

class TradeGeneral extends $pb.GeneratedMessage {
  factory TradeGeneral({
    $0.Timestamp? t,
    $core.double? price,
    $core.double? qty,
    $core.double? amount,
    $core.String? buyer,
    $core.String? seller,
    $core.String? id,
    $core.String? symbol,
    $1.SettlType? settlType,
    SecurityExchangeMarketData? securityExchange,
    $1.SecurityType? securityType,
    $core.String? idGenerico,
  }) {
    final result = create();
    if (t != null) result.t = t;
    if (price != null) result.price = price;
    if (qty != null) result.qty = qty;
    if (amount != null) result.amount = amount;
    if (buyer != null) result.buyer = buyer;
    if (seller != null) result.seller = seller;
    if (id != null) result.id = id;
    if (symbol != null) result.symbol = symbol;
    if (settlType != null) result.settlType = settlType;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (securityType != null) result.securityType = securityType;
    if (idGenerico != null) result.idGenerico = idGenerico;
    return result;
  }

  TradeGeneral._();

  factory TradeGeneral.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory TradeGeneral.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TradeGeneral',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<$0.Timestamp>(1, _omitFieldNames ? '' : 't',
        subBuilder: $0.Timestamp.create)
    ..aD(2, _omitFieldNames ? '' : 'price')
    ..aD(3, _omitFieldNames ? '' : 'qty')
    ..aD(4, _omitFieldNames ? '' : 'amount')
    ..aOS(5, _omitFieldNames ? '' : 'buyer')
    ..aOS(6, _omitFieldNames ? '' : 'seller')
    ..aOS(7, _omitFieldNames ? '' : 'id')
    ..aOS(8, _omitFieldNames ? '' : 'symbol')
    ..aE<$1.SettlType>(9, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<SecurityExchangeMarketData>(
        10, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aE<$1.SecurityType>(11, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..aOS(12, _omitFieldNames ? '' : 'idGenerico', protoName: 'idGenerico')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TradeGeneral clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TradeGeneral copyWith(void Function(TradeGeneral) updates) =>
      super.copyWith((message) => updates(message as TradeGeneral))
          as TradeGeneral;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TradeGeneral create() => TradeGeneral._();
  @$core.override
  TradeGeneral createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static TradeGeneral getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<TradeGeneral>(create);
  static TradeGeneral? _defaultInstance;

  @$pb.TagNumber(1)
  $0.Timestamp get t => $_getN(0);
  @$pb.TagNumber(1)
  set t($0.Timestamp value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasT() => $_has(0);
  @$pb.TagNumber(1)
  void clearT() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.Timestamp ensureT() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.double get price => $_getN(1);
  @$pb.TagNumber(2)
  set price($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPrice() => $_has(1);
  @$pb.TagNumber(2)
  void clearPrice() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get qty => $_getN(2);
  @$pb.TagNumber(3)
  set qty($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasQty() => $_has(2);
  @$pb.TagNumber(3)
  void clearQty() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get amount => $_getN(3);
  @$pb.TagNumber(4)
  set amount($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasAmount() => $_has(3);
  @$pb.TagNumber(4)
  void clearAmount() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get buyer => $_getSZ(4);
  @$pb.TagNumber(5)
  set buyer($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasBuyer() => $_has(4);
  @$pb.TagNumber(5)
  void clearBuyer() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get seller => $_getSZ(5);
  @$pb.TagNumber(6)
  set seller($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasSeller() => $_has(5);
  @$pb.TagNumber(6)
  void clearSeller() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get id => $_getSZ(6);
  @$pb.TagNumber(7)
  set id($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasId() => $_has(6);
  @$pb.TagNumber(7)
  void clearId() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.String get symbol => $_getSZ(7);
  @$pb.TagNumber(8)
  set symbol($core.String value) => $_setString(7, value);
  @$pb.TagNumber(8)
  $core.bool hasSymbol() => $_has(7);
  @$pb.TagNumber(8)
  void clearSymbol() => $_clearField(8);

  @$pb.TagNumber(9)
  $1.SettlType get settlType => $_getN(8);
  @$pb.TagNumber(9)
  set settlType($1.SettlType value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasSettlType() => $_has(8);
  @$pb.TagNumber(9)
  void clearSettlType() => $_clearField(9);

  @$pb.TagNumber(10)
  SecurityExchangeMarketData get securityExchange => $_getN(9);
  @$pb.TagNumber(10)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasSecurityExchange() => $_has(9);
  @$pb.TagNumber(10)
  void clearSecurityExchange() => $_clearField(10);

  @$pb.TagNumber(11)
  $1.SecurityType get securityType => $_getN(10);
  @$pb.TagNumber(11)
  set securityType($1.SecurityType value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasSecurityType() => $_has(10);
  @$pb.TagNumber(11)
  void clearSecurityType() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.String get idGenerico => $_getSZ(11);
  @$pb.TagNumber(12)
  set idGenerico($core.String value) => $_setString(11, value);
  @$pb.TagNumber(12)
  $core.bool hasIdGenerico() => $_has(11);
  @$pb.TagNumber(12)
  void clearIdGenerico() => $_clearField(12);
}

class IncrementalBook extends $pb.GeneratedMessage {
  factory IncrementalBook({
    $core.Iterable<DataBook>? asks,
    $core.Iterable<DataBook>? bids,
    $core.String? id,
    SecurityExchangeMarketData? securityExchange,
    $core.String? symbol,
    $1.SettlType? settlType,
    $1.SecurityType? securityType,
  }) {
    final result = create();
    if (asks != null) result.asks.addAll(asks);
    if (bids != null) result.bids.addAll(bids);
    if (id != null) result.id = id;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (symbol != null) result.symbol = symbol;
    if (settlType != null) result.settlType = settlType;
    if (securityType != null) result.securityType = securityType;
    return result;
  }

  IncrementalBook._();

  factory IncrementalBook.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory IncrementalBook.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'IncrementalBook',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<DataBook>(1, _omitFieldNames ? '' : 'asks',
        subBuilder: DataBook.create)
    ..pPM<DataBook>(2, _omitFieldNames ? '' : 'bids',
        subBuilder: DataBook.create)
    ..aOS(4, _omitFieldNames ? '' : 'id')
    ..aE<SecurityExchangeMarketData>(
        6, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOS(7, _omitFieldNames ? '' : 'symbol')
    ..aE<$1.SettlType>(8, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<$1.SecurityType>(10, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IncrementalBook clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IncrementalBook copyWith(void Function(IncrementalBook) updates) =>
      super.copyWith((message) => updates(message as IncrementalBook))
          as IncrementalBook;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static IncrementalBook create() => IncrementalBook._();
  @$core.override
  IncrementalBook createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static IncrementalBook getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<IncrementalBook>(create);
  static IncrementalBook? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<DataBook> get asks => $_getList(0);

  @$pb.TagNumber(2)
  $pb.PbList<DataBook> get bids => $_getList(1);

  @$pb.TagNumber(4)
  $core.String get id => $_getSZ(2);
  @$pb.TagNumber(4)
  set id($core.String value) => $_setString(2, value);
  @$pb.TagNumber(4)
  $core.bool hasId() => $_has(2);
  @$pb.TagNumber(4)
  void clearId() => $_clearField(4);

  @$pb.TagNumber(6)
  SecurityExchangeMarketData get securityExchange => $_getN(3);
  @$pb.TagNumber(6)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasSecurityExchange() => $_has(3);
  @$pb.TagNumber(6)
  void clearSecurityExchange() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get symbol => $_getSZ(4);
  @$pb.TagNumber(7)
  set symbol($core.String value) => $_setString(4, value);
  @$pb.TagNumber(7)
  $core.bool hasSymbol() => $_has(4);
  @$pb.TagNumber(7)
  void clearSymbol() => $_clearField(7);

  @$pb.TagNumber(8)
  $1.SettlType get settlType => $_getN(5);
  @$pb.TagNumber(8)
  set settlType($1.SettlType value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasSettlType() => $_has(5);
  @$pb.TagNumber(8)
  void clearSettlType() => $_clearField(8);

  @$pb.TagNumber(10)
  $1.SecurityType get securityType => $_getN(6);
  @$pb.TagNumber(10)
  set securityType($1.SecurityType value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasSecurityType() => $_has(6);
  @$pb.TagNumber(10)
  void clearSecurityType() => $_clearField(10);
}

class Rejected extends $pb.GeneratedMessage {
  factory Rejected({
    $core.String? reason,
    $core.String? text,
    $core.String? id,
  }) {
    final result = create();
    if (reason != null) result.reason = reason;
    if (text != null) result.text = text;
    if (id != null) result.id = id;
    return result;
  }

  Rejected._();

  factory Rejected.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Rejected.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Rejected',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'reason')
    ..aOS(2, _omitFieldNames ? '' : 'text')
    ..aOS(3, _omitFieldNames ? '' : 'id')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Rejected clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Rejected copyWith(void Function(Rejected) updates) =>
      super.copyWith((message) => updates(message as Rejected)) as Rejected;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Rejected create() => Rejected._();
  @$core.override
  Rejected createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Rejected getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Rejected>(create);
  static Rejected? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get reason => $_getSZ(0);
  @$pb.TagNumber(1)
  set reason($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasReason() => $_has(0);
  @$pb.TagNumber(1)
  void clearReason() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get text => $_getSZ(1);
  @$pb.TagNumber(2)
  set text($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasText() => $_has(1);
  @$pb.TagNumber(2)
  void clearText() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get id => $_getSZ(2);
  @$pb.TagNumber(3)
  set id($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasId() => $_has(2);
  @$pb.TagNumber(3)
  void clearId() => $_clearField(3);
}

class DataBook extends $pb.GeneratedMessage {
  factory DataBook({
    $core.double? price,
    $core.double? size,
    TypeBook? typeBook,
    $core.String? symbol,
    SecurityExchangeMarketData? securityExchange,
    $core.String? operator,
    $core.String? account,
  }) {
    final result = create();
    if (price != null) result.price = price;
    if (size != null) result.size = size;
    if (typeBook != null) result.typeBook = typeBook;
    if (symbol != null) result.symbol = symbol;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (operator != null) result.operator = operator;
    if (account != null) result.account = account;
    return result;
  }

  DataBook._();

  factory DataBook.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory DataBook.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DataBook',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aD(1, _omitFieldNames ? '' : 'price')
    ..aD(2, _omitFieldNames ? '' : 'size')
    ..aE<TypeBook>(3, _omitFieldNames ? '' : 'typeBook',
        protoName: 'typeBook', enumValues: TypeBook.values)
    ..aOS(4, _omitFieldNames ? '' : 'symbol')
    ..aE<SecurityExchangeMarketData>(
        5, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOS(6, _omitFieldNames ? '' : 'operator')
    ..aOS(7, _omitFieldNames ? '' : 'account')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DataBook clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DataBook copyWith(void Function(DataBook) updates) =>
      super.copyWith((message) => updates(message as DataBook)) as DataBook;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static DataBook create() => DataBook._();
  @$core.override
  DataBook createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static DataBook getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<DataBook>(create);
  static DataBook? _defaultInstance;

  @$pb.TagNumber(1)
  $core.double get price => $_getN(0);
  @$pb.TagNumber(1)
  set price($core.double value) => $_setDouble(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPrice() => $_has(0);
  @$pb.TagNumber(1)
  void clearPrice() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get size => $_getN(1);
  @$pb.TagNumber(2)
  set size($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSize() => $_has(1);
  @$pb.TagNumber(2)
  void clearSize() => $_clearField(2);

  @$pb.TagNumber(3)
  TypeBook get typeBook => $_getN(2);
  @$pb.TagNumber(3)
  set typeBook(TypeBook value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasTypeBook() => $_has(2);
  @$pb.TagNumber(3)
  void clearTypeBook() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get symbol => $_getSZ(3);
  @$pb.TagNumber(4)
  set symbol($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasSymbol() => $_has(3);
  @$pb.TagNumber(4)
  void clearSymbol() => $_clearField(4);

  @$pb.TagNumber(5)
  SecurityExchangeMarketData get securityExchange => $_getN(4);
  @$pb.TagNumber(5)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasSecurityExchange() => $_has(4);
  @$pb.TagNumber(5)
  void clearSecurityExchange() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get operator => $_getSZ(5);
  @$pb.TagNumber(6)
  set operator($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasOperator() => $_has(5);
  @$pb.TagNumber(6)
  void clearOperator() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get account => $_getSZ(6);
  @$pb.TagNumber(7)
  set account($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasAccount() => $_has(6);
  @$pb.TagNumber(7)
  void clearAccount() => $_clearField(7);
}

class Ohlcv extends $pb.GeneratedMessage {
  factory Ohlcv({
    $core.double? close,
    $core.double? open,
    $core.double? low,
    $core.double? high,
    $core.double? volume,
    $core.String? id,
    $core.String? symbol,
    $1.SettlType? settlType,
    SecurityExchangeMarketData? securityExchange,
    $1.SecurityType? securityType,
    $0.Timestamp? t,
  }) {
    final result = create();
    if (close != null) result.close = close;
    if (open != null) result.open = open;
    if (low != null) result.low = low;
    if (high != null) result.high = high;
    if (volume != null) result.volume = volume;
    if (id != null) result.id = id;
    if (symbol != null) result.symbol = symbol;
    if (settlType != null) result.settlType = settlType;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (securityType != null) result.securityType = securityType;
    if (t != null) result.t = t;
    return result;
  }

  Ohlcv._();

  factory Ohlcv.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Ohlcv.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Ohlcv',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aD(1, _omitFieldNames ? '' : 'close')
    ..aD(2, _omitFieldNames ? '' : 'open')
    ..aD(3, _omitFieldNames ? '' : 'low')
    ..aD(4, _omitFieldNames ? '' : 'high')
    ..aD(5, _omitFieldNames ? '' : 'volume')
    ..aOS(6, _omitFieldNames ? '' : 'id')
    ..aOS(7, _omitFieldNames ? '' : 'symbol')
    ..aE<$1.SettlType>(8, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<SecurityExchangeMarketData>(
        9, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aE<$1.SecurityType>(10, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..aOM<$0.Timestamp>(11, _omitFieldNames ? '' : 't',
        subBuilder: $0.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Ohlcv clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Ohlcv copyWith(void Function(Ohlcv) updates) =>
      super.copyWith((message) => updates(message as Ohlcv)) as Ohlcv;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Ohlcv create() => Ohlcv._();
  @$core.override
  Ohlcv createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Ohlcv getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Ohlcv>(create);
  static Ohlcv? _defaultInstance;

  @$pb.TagNumber(1)
  $core.double get close => $_getN(0);
  @$pb.TagNumber(1)
  set close($core.double value) => $_setDouble(0, value);
  @$pb.TagNumber(1)
  $core.bool hasClose() => $_has(0);
  @$pb.TagNumber(1)
  void clearClose() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get open => $_getN(1);
  @$pb.TagNumber(2)
  set open($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOpen() => $_has(1);
  @$pb.TagNumber(2)
  void clearOpen() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get low => $_getN(2);
  @$pb.TagNumber(3)
  set low($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLow() => $_has(2);
  @$pb.TagNumber(3)
  void clearLow() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get high => $_getN(3);
  @$pb.TagNumber(4)
  set high($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasHigh() => $_has(3);
  @$pb.TagNumber(4)
  void clearHigh() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.double get volume => $_getN(4);
  @$pb.TagNumber(5)
  set volume($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(5)
  $core.bool hasVolume() => $_has(4);
  @$pb.TagNumber(5)
  void clearVolume() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get id => $_getSZ(5);
  @$pb.TagNumber(6)
  set id($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasId() => $_has(5);
  @$pb.TagNumber(6)
  void clearId() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get symbol => $_getSZ(6);
  @$pb.TagNumber(7)
  set symbol($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasSymbol() => $_has(6);
  @$pb.TagNumber(7)
  void clearSymbol() => $_clearField(7);

  @$pb.TagNumber(8)
  $1.SettlType get settlType => $_getN(7);
  @$pb.TagNumber(8)
  set settlType($1.SettlType value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasSettlType() => $_has(7);
  @$pb.TagNumber(8)
  void clearSettlType() => $_clearField(8);

  @$pb.TagNumber(9)
  SecurityExchangeMarketData get securityExchange => $_getN(8);
  @$pb.TagNumber(9)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasSecurityExchange() => $_has(8);
  @$pb.TagNumber(9)
  void clearSecurityExchange() => $_clearField(9);

  @$pb.TagNumber(10)
  $1.SecurityType get securityType => $_getN(9);
  @$pb.TagNumber(10)
  set securityType($1.SecurityType value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasSecurityType() => $_has(9);
  @$pb.TagNumber(10)
  void clearSecurityType() => $_clearField(10);

  @$pb.TagNumber(11)
  $0.Timestamp get t => $_getN(10);
  @$pb.TagNumber(11)
  set t($0.Timestamp value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasT() => $_has(10);
  @$pb.TagNumber(11)
  void clearT() => $_clearField(11);
  @$pb.TagNumber(11)
  $0.Timestamp ensureT() => $_ensure(10);
}

class Statistic extends $pb.GeneratedMessage {
  factory Statistic({
    $core.double? amount,
    $core.double? vwap,
    $core.double? imbalance,
    $core.String? ratio,
    $core.String? id,
    $core.double? tradeVolume,
    $core.double? delta,
    $core.double? previusClose,
    $core.double? referencialPrice,
    $core.double? indicativeOpening,
    $core.double? last,
    $core.double? tickDirecion,
    SecurityExchangeMarketData? securityExchange,
    $core.String? symbol,
    Ohlcv? ohlcv,
    $core.double? bidQty,
    $core.double? askPx,
    $core.double? askQty,
    $core.double? bidPx,
    $core.double? amountTheoric,
    $core.double? priceTheoric,
    $core.double? desbalTheoric,
    $core.double? ownDemand,
    $core.double? totalDeamnd,
    $core.double? totalDemand,
    $1.SettlType? settlType,
    $1.SecurityType? securityType,
    $core.double? medio,
    $core.double? thresholdLimits,
    $core.double? settlementPrice,
    $core.double? auctionClearingPrice,
    $core.double? priceLimitType,
    $core.double? lowLimitPrice,
    $core.double? highLimitPrice,
    $core.double? lastActionPrice,
    $core.double? tradeTurnover,
    $core.double? tradeQuantity,
    $core.double? turnoverQuantityExtension,
    $core.double? tradeTurnoverQuantity,
    $core.double? tradingReferencePrice,
    $core.double? secSizesGrp,
    $core.double? close,
    $core.double? open,
    $core.double? low,
    $core.double? high,
    $core.double? volume,
  }) {
    final result = create();
    if (amount != null) result.amount = amount;
    if (vwap != null) result.vwap = vwap;
    if (imbalance != null) result.imbalance = imbalance;
    if (ratio != null) result.ratio = ratio;
    if (id != null) result.id = id;
    if (tradeVolume != null) result.tradeVolume = tradeVolume;
    if (delta != null) result.delta = delta;
    if (previusClose != null) result.previusClose = previusClose;
    if (referencialPrice != null) result.referencialPrice = referencialPrice;
    if (indicativeOpening != null) result.indicativeOpening = indicativeOpening;
    if (last != null) result.last = last;
    if (tickDirecion != null) result.tickDirecion = tickDirecion;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (symbol != null) result.symbol = symbol;
    if (ohlcv != null) result.ohlcv = ohlcv;
    if (bidQty != null) result.bidQty = bidQty;
    if (askPx != null) result.askPx = askPx;
    if (askQty != null) result.askQty = askQty;
    if (bidPx != null) result.bidPx = bidPx;
    if (amountTheoric != null) result.amountTheoric = amountTheoric;
    if (priceTheoric != null) result.priceTheoric = priceTheoric;
    if (desbalTheoric != null) result.desbalTheoric = desbalTheoric;
    if (ownDemand != null) result.ownDemand = ownDemand;
    if (totalDeamnd != null) result.totalDeamnd = totalDeamnd;
    if (totalDemand != null) result.totalDemand = totalDemand;
    if (settlType != null) result.settlType = settlType;
    if (securityType != null) result.securityType = securityType;
    if (medio != null) result.medio = medio;
    if (thresholdLimits != null) result.thresholdLimits = thresholdLimits;
    if (settlementPrice != null) result.settlementPrice = settlementPrice;
    if (auctionClearingPrice != null)
      result.auctionClearingPrice = auctionClearingPrice;
    if (priceLimitType != null) result.priceLimitType = priceLimitType;
    if (lowLimitPrice != null) result.lowLimitPrice = lowLimitPrice;
    if (highLimitPrice != null) result.highLimitPrice = highLimitPrice;
    if (lastActionPrice != null) result.lastActionPrice = lastActionPrice;
    if (tradeTurnover != null) result.tradeTurnover = tradeTurnover;
    if (tradeQuantity != null) result.tradeQuantity = tradeQuantity;
    if (turnoverQuantityExtension != null)
      result.turnoverQuantityExtension = turnoverQuantityExtension;
    if (tradeTurnoverQuantity != null)
      result.tradeTurnoverQuantity = tradeTurnoverQuantity;
    if (tradingReferencePrice != null)
      result.tradingReferencePrice = tradingReferencePrice;
    if (secSizesGrp != null) result.secSizesGrp = secSizesGrp;
    if (close != null) result.close = close;
    if (open != null) result.open = open;
    if (low != null) result.low = low;
    if (high != null) result.high = high;
    if (volume != null) result.volume = volume;
    return result;
  }

  Statistic._();

  factory Statistic.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Statistic.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Statistic',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aD(1, _omitFieldNames ? '' : 'amount')
    ..aD(2, _omitFieldNames ? '' : 'vwap')
    ..aD(3, _omitFieldNames ? '' : 'imbalance')
    ..aOS(4, _omitFieldNames ? '' : 'ratio')
    ..aOS(5, _omitFieldNames ? '' : 'id')
    ..aD(6, _omitFieldNames ? '' : 'tradeVolume', protoName: 'tradeVolume')
    ..aD(7, _omitFieldNames ? '' : 'delta')
    ..aD(8, _omitFieldNames ? '' : 'previusClose', protoName: 'previusClose')
    ..aD(9, _omitFieldNames ? '' : 'referencialPrice',
        protoName: 'referencialPrice')
    ..aD(10, _omitFieldNames ? '' : 'indicativeOpening',
        protoName: 'indicativeOpening')
    ..aD(11, _omitFieldNames ? '' : 'last')
    ..aD(12, _omitFieldNames ? '' : 'tickDirecion', protoName: 'tickDirecion')
    ..aE<SecurityExchangeMarketData>(
        13, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOS(14, _omitFieldNames ? '' : 'symbol')
    ..aOM<Ohlcv>(15, _omitFieldNames ? '' : 'ohlcv', subBuilder: Ohlcv.create)
    ..aD(17, _omitFieldNames ? '' : 'bidQty', protoName: 'bidQty')
    ..aD(18, _omitFieldNames ? '' : 'askPx', protoName: 'askPx')
    ..aD(19, _omitFieldNames ? '' : 'askQty', protoName: 'askQty')
    ..aD(20, _omitFieldNames ? '' : 'bidPx', protoName: 'bidPx')
    ..aD(25, _omitFieldNames ? '' : 'amountTheoric', protoName: 'amountTheoric')
    ..aD(26, _omitFieldNames ? '' : 'priceTheoric', protoName: 'priceTheoric')
    ..aD(27, _omitFieldNames ? '' : 'desbalTheoric', protoName: 'desbalTheoric')
    ..aD(28, _omitFieldNames ? '' : 'ownDemand', protoName: 'ownDemand')
    ..aD(29, _omitFieldNames ? '' : 'totalDeamnd', protoName: 'totalDeamnd')
    ..aD(31, _omitFieldNames ? '' : 'totalDemand', protoName: 'totalDemand')
    ..aE<$1.SettlType>(32, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<$1.SecurityType>(44, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..aD(45, _omitFieldNames ? '' : 'medio')
    ..aD(46, _omitFieldNames ? '' : 'thresholdLimits',
        protoName: 'thresholdLimits')
    ..aD(48, _omitFieldNames ? '' : 'settlementPrice',
        protoName: 'settlementPrice')
    ..aD(50, _omitFieldNames ? '' : 'auctionClearingPrice',
        protoName: 'auctionClearingPrice')
    ..aD(51, _omitFieldNames ? '' : 'priceLimitType',
        protoName: 'priceLimitType')
    ..aD(52, _omitFieldNames ? '' : 'lowLimitPrice', protoName: 'lowLimitPrice')
    ..aD(53, _omitFieldNames ? '' : 'highLimitPrice',
        protoName: 'highLimitPrice')
    ..aD(54, _omitFieldNames ? '' : 'lastActionPrice',
        protoName: 'lastActionPrice')
    ..aD(55, _omitFieldNames ? '' : 'tradeTurnover', protoName: 'tradeTurnover')
    ..aD(56, _omitFieldNames ? '' : 'tradeQuantity', protoName: 'tradeQuantity')
    ..aD(57, _omitFieldNames ? '' : 'turnoverQuantityExtension',
        protoName: 'turnoverQuantityExtension')
    ..aD(58, _omitFieldNames ? '' : 'tradeTurnoverQuantity',
        protoName: 'tradeTurnoverQuantity')
    ..aD(59, _omitFieldNames ? '' : 'tradingReferencePrice',
        protoName: 'tradingReferencePrice')
    ..aD(60, _omitFieldNames ? '' : 'secSizesGrp', protoName: 'secSizesGrp')
    ..aD(61, _omitFieldNames ? '' : 'close')
    ..aD(62, _omitFieldNames ? '' : 'open')
    ..aD(63, _omitFieldNames ? '' : 'low')
    ..aD(64, _omitFieldNames ? '' : 'high')
    ..aD(65, _omitFieldNames ? '' : 'volume')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Statistic clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Statistic copyWith(void Function(Statistic) updates) =>
      super.copyWith((message) => updates(message as Statistic)) as Statistic;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Statistic create() => Statistic._();
  @$core.override
  Statistic createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Statistic getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Statistic>(create);
  static Statistic? _defaultInstance;

  @$pb.TagNumber(1)
  $core.double get amount => $_getN(0);
  @$pb.TagNumber(1)
  set amount($core.double value) => $_setDouble(0, value);
  @$pb.TagNumber(1)
  $core.bool hasAmount() => $_has(0);
  @$pb.TagNumber(1)
  void clearAmount() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get vwap => $_getN(1);
  @$pb.TagNumber(2)
  set vwap($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVwap() => $_has(1);
  @$pb.TagNumber(2)
  void clearVwap() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get imbalance => $_getN(2);
  @$pb.TagNumber(3)
  set imbalance($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasImbalance() => $_has(2);
  @$pb.TagNumber(3)
  void clearImbalance() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get ratio => $_getSZ(3);
  @$pb.TagNumber(4)
  set ratio($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasRatio() => $_has(3);
  @$pb.TagNumber(4)
  void clearRatio() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get id => $_getSZ(4);
  @$pb.TagNumber(5)
  set id($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasId() => $_has(4);
  @$pb.TagNumber(5)
  void clearId() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.double get tradeVolume => $_getN(5);
  @$pb.TagNumber(6)
  set tradeVolume($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(6)
  $core.bool hasTradeVolume() => $_has(5);
  @$pb.TagNumber(6)
  void clearTradeVolume() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.double get delta => $_getN(6);
  @$pb.TagNumber(7)
  set delta($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(7)
  $core.bool hasDelta() => $_has(6);
  @$pb.TagNumber(7)
  void clearDelta() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.double get previusClose => $_getN(7);
  @$pb.TagNumber(8)
  set previusClose($core.double value) => $_setDouble(7, value);
  @$pb.TagNumber(8)
  $core.bool hasPreviusClose() => $_has(7);
  @$pb.TagNumber(8)
  void clearPreviusClose() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.double get referencialPrice => $_getN(8);
  @$pb.TagNumber(9)
  set referencialPrice($core.double value) => $_setDouble(8, value);
  @$pb.TagNumber(9)
  $core.bool hasReferencialPrice() => $_has(8);
  @$pb.TagNumber(9)
  void clearReferencialPrice() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.double get indicativeOpening => $_getN(9);
  @$pb.TagNumber(10)
  set indicativeOpening($core.double value) => $_setDouble(9, value);
  @$pb.TagNumber(10)
  $core.bool hasIndicativeOpening() => $_has(9);
  @$pb.TagNumber(10)
  void clearIndicativeOpening() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.double get last => $_getN(10);
  @$pb.TagNumber(11)
  set last($core.double value) => $_setDouble(10, value);
  @$pb.TagNumber(11)
  $core.bool hasLast() => $_has(10);
  @$pb.TagNumber(11)
  void clearLast() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.double get tickDirecion => $_getN(11);
  @$pb.TagNumber(12)
  set tickDirecion($core.double value) => $_setDouble(11, value);
  @$pb.TagNumber(12)
  $core.bool hasTickDirecion() => $_has(11);
  @$pb.TagNumber(12)
  void clearTickDirecion() => $_clearField(12);

  @$pb.TagNumber(13)
  SecurityExchangeMarketData get securityExchange => $_getN(12);
  @$pb.TagNumber(13)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(13, value);
  @$pb.TagNumber(13)
  $core.bool hasSecurityExchange() => $_has(12);
  @$pb.TagNumber(13)
  void clearSecurityExchange() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.String get symbol => $_getSZ(13);
  @$pb.TagNumber(14)
  set symbol($core.String value) => $_setString(13, value);
  @$pb.TagNumber(14)
  $core.bool hasSymbol() => $_has(13);
  @$pb.TagNumber(14)
  void clearSymbol() => $_clearField(14);

  @$pb.TagNumber(15)
  Ohlcv get ohlcv => $_getN(14);
  @$pb.TagNumber(15)
  set ohlcv(Ohlcv value) => $_setField(15, value);
  @$pb.TagNumber(15)
  $core.bool hasOhlcv() => $_has(14);
  @$pb.TagNumber(15)
  void clearOhlcv() => $_clearField(15);
  @$pb.TagNumber(15)
  Ohlcv ensureOhlcv() => $_ensure(14);

  @$pb.TagNumber(17)
  $core.double get bidQty => $_getN(15);
  @$pb.TagNumber(17)
  set bidQty($core.double value) => $_setDouble(15, value);
  @$pb.TagNumber(17)
  $core.bool hasBidQty() => $_has(15);
  @$pb.TagNumber(17)
  void clearBidQty() => $_clearField(17);

  @$pb.TagNumber(18)
  $core.double get askPx => $_getN(16);
  @$pb.TagNumber(18)
  set askPx($core.double value) => $_setDouble(16, value);
  @$pb.TagNumber(18)
  $core.bool hasAskPx() => $_has(16);
  @$pb.TagNumber(18)
  void clearAskPx() => $_clearField(18);

  @$pb.TagNumber(19)
  $core.double get askQty => $_getN(17);
  @$pb.TagNumber(19)
  set askQty($core.double value) => $_setDouble(17, value);
  @$pb.TagNumber(19)
  $core.bool hasAskQty() => $_has(17);
  @$pb.TagNumber(19)
  void clearAskQty() => $_clearField(19);

  @$pb.TagNumber(20)
  $core.double get bidPx => $_getN(18);
  @$pb.TagNumber(20)
  set bidPx($core.double value) => $_setDouble(18, value);
  @$pb.TagNumber(20)
  $core.bool hasBidPx() => $_has(18);
  @$pb.TagNumber(20)
  void clearBidPx() => $_clearField(20);

  @$pb.TagNumber(25)
  $core.double get amountTheoric => $_getN(19);
  @$pb.TagNumber(25)
  set amountTheoric($core.double value) => $_setDouble(19, value);
  @$pb.TagNumber(25)
  $core.bool hasAmountTheoric() => $_has(19);
  @$pb.TagNumber(25)
  void clearAmountTheoric() => $_clearField(25);

  @$pb.TagNumber(26)
  $core.double get priceTheoric => $_getN(20);
  @$pb.TagNumber(26)
  set priceTheoric($core.double value) => $_setDouble(20, value);
  @$pb.TagNumber(26)
  $core.bool hasPriceTheoric() => $_has(20);
  @$pb.TagNumber(26)
  void clearPriceTheoric() => $_clearField(26);

  @$pb.TagNumber(27)
  $core.double get desbalTheoric => $_getN(21);
  @$pb.TagNumber(27)
  set desbalTheoric($core.double value) => $_setDouble(21, value);
  @$pb.TagNumber(27)
  $core.bool hasDesbalTheoric() => $_has(21);
  @$pb.TagNumber(27)
  void clearDesbalTheoric() => $_clearField(27);

  @$pb.TagNumber(28)
  $core.double get ownDemand => $_getN(22);
  @$pb.TagNumber(28)
  set ownDemand($core.double value) => $_setDouble(22, value);
  @$pb.TagNumber(28)
  $core.bool hasOwnDemand() => $_has(22);
  @$pb.TagNumber(28)
  void clearOwnDemand() => $_clearField(28);

  @$pb.TagNumber(29)
  $core.double get totalDeamnd => $_getN(23);
  @$pb.TagNumber(29)
  set totalDeamnd($core.double value) => $_setDouble(23, value);
  @$pb.TagNumber(29)
  $core.bool hasTotalDeamnd() => $_has(23);
  @$pb.TagNumber(29)
  void clearTotalDeamnd() => $_clearField(29);

  @$pb.TagNumber(31)
  $core.double get totalDemand => $_getN(24);
  @$pb.TagNumber(31)
  set totalDemand($core.double value) => $_setDouble(24, value);
  @$pb.TagNumber(31)
  $core.bool hasTotalDemand() => $_has(24);
  @$pb.TagNumber(31)
  void clearTotalDemand() => $_clearField(31);

  @$pb.TagNumber(32)
  $1.SettlType get settlType => $_getN(25);
  @$pb.TagNumber(32)
  set settlType($1.SettlType value) => $_setField(32, value);
  @$pb.TagNumber(32)
  $core.bool hasSettlType() => $_has(25);
  @$pb.TagNumber(32)
  void clearSettlType() => $_clearField(32);

  @$pb.TagNumber(44)
  $1.SecurityType get securityType => $_getN(26);
  @$pb.TagNumber(44)
  set securityType($1.SecurityType value) => $_setField(44, value);
  @$pb.TagNumber(44)
  $core.bool hasSecurityType() => $_has(26);
  @$pb.TagNumber(44)
  void clearSecurityType() => $_clearField(44);

  @$pb.TagNumber(45)
  $core.double get medio => $_getN(27);
  @$pb.TagNumber(45)
  set medio($core.double value) => $_setDouble(27, value);
  @$pb.TagNumber(45)
  $core.bool hasMedio() => $_has(27);
  @$pb.TagNumber(45)
  void clearMedio() => $_clearField(45);

  @$pb.TagNumber(46)
  $core.double get thresholdLimits => $_getN(28);
  @$pb.TagNumber(46)
  set thresholdLimits($core.double value) => $_setDouble(28, value);
  @$pb.TagNumber(46)
  $core.bool hasThresholdLimits() => $_has(28);
  @$pb.TagNumber(46)
  void clearThresholdLimits() => $_clearField(46);

  @$pb.TagNumber(48)
  $core.double get settlementPrice => $_getN(29);
  @$pb.TagNumber(48)
  set settlementPrice($core.double value) => $_setDouble(29, value);
  @$pb.TagNumber(48)
  $core.bool hasSettlementPrice() => $_has(29);
  @$pb.TagNumber(48)
  void clearSettlementPrice() => $_clearField(48);

  @$pb.TagNumber(50)
  $core.double get auctionClearingPrice => $_getN(30);
  @$pb.TagNumber(50)
  set auctionClearingPrice($core.double value) => $_setDouble(30, value);
  @$pb.TagNumber(50)
  $core.bool hasAuctionClearingPrice() => $_has(30);
  @$pb.TagNumber(50)
  void clearAuctionClearingPrice() => $_clearField(50);

  @$pb.TagNumber(51)
  $core.double get priceLimitType => $_getN(31);
  @$pb.TagNumber(51)
  set priceLimitType($core.double value) => $_setDouble(31, value);
  @$pb.TagNumber(51)
  $core.bool hasPriceLimitType() => $_has(31);
  @$pb.TagNumber(51)
  void clearPriceLimitType() => $_clearField(51);

  @$pb.TagNumber(52)
  $core.double get lowLimitPrice => $_getN(32);
  @$pb.TagNumber(52)
  set lowLimitPrice($core.double value) => $_setDouble(32, value);
  @$pb.TagNumber(52)
  $core.bool hasLowLimitPrice() => $_has(32);
  @$pb.TagNumber(52)
  void clearLowLimitPrice() => $_clearField(52);

  @$pb.TagNumber(53)
  $core.double get highLimitPrice => $_getN(33);
  @$pb.TagNumber(53)
  set highLimitPrice($core.double value) => $_setDouble(33, value);
  @$pb.TagNumber(53)
  $core.bool hasHighLimitPrice() => $_has(33);
  @$pb.TagNumber(53)
  void clearHighLimitPrice() => $_clearField(53);

  @$pb.TagNumber(54)
  $core.double get lastActionPrice => $_getN(34);
  @$pb.TagNumber(54)
  set lastActionPrice($core.double value) => $_setDouble(34, value);
  @$pb.TagNumber(54)
  $core.bool hasLastActionPrice() => $_has(34);
  @$pb.TagNumber(54)
  void clearLastActionPrice() => $_clearField(54);

  @$pb.TagNumber(55)
  $core.double get tradeTurnover => $_getN(35);
  @$pb.TagNumber(55)
  set tradeTurnover($core.double value) => $_setDouble(35, value);
  @$pb.TagNumber(55)
  $core.bool hasTradeTurnover() => $_has(35);
  @$pb.TagNumber(55)
  void clearTradeTurnover() => $_clearField(55);

  @$pb.TagNumber(56)
  $core.double get tradeQuantity => $_getN(36);
  @$pb.TagNumber(56)
  set tradeQuantity($core.double value) => $_setDouble(36, value);
  @$pb.TagNumber(56)
  $core.bool hasTradeQuantity() => $_has(36);
  @$pb.TagNumber(56)
  void clearTradeQuantity() => $_clearField(56);

  @$pb.TagNumber(57)
  $core.double get turnoverQuantityExtension => $_getN(37);
  @$pb.TagNumber(57)
  set turnoverQuantityExtension($core.double value) => $_setDouble(37, value);
  @$pb.TagNumber(57)
  $core.bool hasTurnoverQuantityExtension() => $_has(37);
  @$pb.TagNumber(57)
  void clearTurnoverQuantityExtension() => $_clearField(57);

  @$pb.TagNumber(58)
  $core.double get tradeTurnoverQuantity => $_getN(38);
  @$pb.TagNumber(58)
  set tradeTurnoverQuantity($core.double value) => $_setDouble(38, value);
  @$pb.TagNumber(58)
  $core.bool hasTradeTurnoverQuantity() => $_has(38);
  @$pb.TagNumber(58)
  void clearTradeTurnoverQuantity() => $_clearField(58);

  @$pb.TagNumber(59)
  $core.double get tradingReferencePrice => $_getN(39);
  @$pb.TagNumber(59)
  set tradingReferencePrice($core.double value) => $_setDouble(39, value);
  @$pb.TagNumber(59)
  $core.bool hasTradingReferencePrice() => $_has(39);
  @$pb.TagNumber(59)
  void clearTradingReferencePrice() => $_clearField(59);

  @$pb.TagNumber(60)
  $core.double get secSizesGrp => $_getN(40);
  @$pb.TagNumber(60)
  set secSizesGrp($core.double value) => $_setDouble(40, value);
  @$pb.TagNumber(60)
  $core.bool hasSecSizesGrp() => $_has(40);
  @$pb.TagNumber(60)
  void clearSecSizesGrp() => $_clearField(60);

  @$pb.TagNumber(61)
  $core.double get close => $_getN(41);
  @$pb.TagNumber(61)
  set close($core.double value) => $_setDouble(41, value);
  @$pb.TagNumber(61)
  $core.bool hasClose() => $_has(41);
  @$pb.TagNumber(61)
  void clearClose() => $_clearField(61);

  @$pb.TagNumber(62)
  $core.double get open => $_getN(42);
  @$pb.TagNumber(62)
  set open($core.double value) => $_setDouble(42, value);
  @$pb.TagNumber(62)
  $core.bool hasOpen() => $_has(42);
  @$pb.TagNumber(62)
  void clearOpen() => $_clearField(62);

  @$pb.TagNumber(63)
  $core.double get low => $_getN(43);
  @$pb.TagNumber(63)
  set low($core.double value) => $_setDouble(43, value);
  @$pb.TagNumber(63)
  $core.bool hasLow() => $_has(43);
  @$pb.TagNumber(63)
  void clearLow() => $_clearField(63);

  @$pb.TagNumber(64)
  $core.double get high => $_getN(44);
  @$pb.TagNumber(64)
  set high($core.double value) => $_setDouble(44, value);
  @$pb.TagNumber(64)
  $core.bool hasHigh() => $_has(44);
  @$pb.TagNumber(64)
  void clearHigh() => $_clearField(64);

  @$pb.TagNumber(65)
  $core.double get volume => $_getN(45);
  @$pb.TagNumber(65)
  set volume($core.double value) => $_setDouble(45, value);
  @$pb.TagNumber(65)
  $core.bool hasVolume() => $_has(45);
  @$pb.TagNumber(65)
  void clearVolume() => $_clearField(65);
}

class BolsaStats extends $pb.GeneratedMessage {
  factory BolsaStats({
    $core.String? id,
    $core.double? totalVolumen,
    $core.double? montoTotal,
    $core.String? horaInicio,
    $core.String? horaFin,
    $core.Iterable<RankinSymbol>? masVolatil,
    $core.Iterable<RankinSymbol>? masCayo,
    $core.Iterable<RankinSymbol>? menosCayo,
    $core.Iterable<RankinSymbol>? masTranzado,
    $core.Iterable<RankinSymbol>? bestRankin,
    $core.Iterable<RankinSymbol>? worseRankin,
    $core.double? volatilidadPromedio,
    $core.double? rangoPromedio,
    $core.double? indicePromedio,
    $core.double? indiceMaximo,
    $core.double? indiceMinimo,
    $core.double? liquidezMedia,
    $fixnum.Int64? numeroTotalTrades,
    $core.double? sentimientoPositivo,
    $core.double? sentimientoNegativo,
    $core.double? capitalizacionTotal,
    $core.double? capitalizacionPromedio,
    $core.double? precioPromedioAcumulado,
    $core.double? precioMaximoAcumulado,
    $core.String? tendenciaGeneral,
    $core.double? tendenciaPromedio,
    SecurityExchangeMarketData? securityExchange,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (totalVolumen != null) result.totalVolumen = totalVolumen;
    if (montoTotal != null) result.montoTotal = montoTotal;
    if (horaInicio != null) result.horaInicio = horaInicio;
    if (horaFin != null) result.horaFin = horaFin;
    if (masVolatil != null) result.masVolatil.addAll(masVolatil);
    if (masCayo != null) result.masCayo.addAll(masCayo);
    if (menosCayo != null) result.menosCayo.addAll(menosCayo);
    if (masTranzado != null) result.masTranzado.addAll(masTranzado);
    if (bestRankin != null) result.bestRankin.addAll(bestRankin);
    if (worseRankin != null) result.worseRankin.addAll(worseRankin);
    if (volatilidadPromedio != null)
      result.volatilidadPromedio = volatilidadPromedio;
    if (rangoPromedio != null) result.rangoPromedio = rangoPromedio;
    if (indicePromedio != null) result.indicePromedio = indicePromedio;
    if (indiceMaximo != null) result.indiceMaximo = indiceMaximo;
    if (indiceMinimo != null) result.indiceMinimo = indiceMinimo;
    if (liquidezMedia != null) result.liquidezMedia = liquidezMedia;
    if (numeroTotalTrades != null) result.numeroTotalTrades = numeroTotalTrades;
    if (sentimientoPositivo != null)
      result.sentimientoPositivo = sentimientoPositivo;
    if (sentimientoNegativo != null)
      result.sentimientoNegativo = sentimientoNegativo;
    if (capitalizacionTotal != null)
      result.capitalizacionTotal = capitalizacionTotal;
    if (capitalizacionPromedio != null)
      result.capitalizacionPromedio = capitalizacionPromedio;
    if (precioPromedioAcumulado != null)
      result.precioPromedioAcumulado = precioPromedioAcumulado;
    if (precioMaximoAcumulado != null)
      result.precioMaximoAcumulado = precioMaximoAcumulado;
    if (tendenciaGeneral != null) result.tendenciaGeneral = tendenciaGeneral;
    if (tendenciaPromedio != null) result.tendenciaPromedio = tendenciaPromedio;
    if (securityExchange != null) result.securityExchange = securityExchange;
    return result;
  }

  BolsaStats._();

  factory BolsaStats.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory BolsaStats.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'BolsaStats',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aD(2, _omitFieldNames ? '' : 'totalVolumen')
    ..aD(3, _omitFieldNames ? '' : 'montoTotal')
    ..aOS(4, _omitFieldNames ? '' : 'horaInicio')
    ..aOS(5, _omitFieldNames ? '' : 'horaFin')
    ..pPM<RankinSymbol>(6, _omitFieldNames ? '' : 'masVolatil',
        subBuilder: RankinSymbol.create)
    ..pPM<RankinSymbol>(7, _omitFieldNames ? '' : 'masCayo',
        subBuilder: RankinSymbol.create)
    ..pPM<RankinSymbol>(8, _omitFieldNames ? '' : 'menosCayo',
        subBuilder: RankinSymbol.create)
    ..pPM<RankinSymbol>(9, _omitFieldNames ? '' : 'masTranzado',
        subBuilder: RankinSymbol.create)
    ..pPM<RankinSymbol>(10, _omitFieldNames ? '' : 'bestRankin',
        subBuilder: RankinSymbol.create)
    ..pPM<RankinSymbol>(11, _omitFieldNames ? '' : 'worseRankin',
        subBuilder: RankinSymbol.create)
    ..aD(12, _omitFieldNames ? '' : 'volatilidadPromedio')
    ..aD(13, _omitFieldNames ? '' : 'rangoPromedio')
    ..aD(14, _omitFieldNames ? '' : 'indicePromedio')
    ..aD(15, _omitFieldNames ? '' : 'indiceMaximo')
    ..aD(16, _omitFieldNames ? '' : 'indiceMinimo')
    ..aD(17, _omitFieldNames ? '' : 'liquidezMedia')
    ..aInt64(18, _omitFieldNames ? '' : 'numeroTotalTrades')
    ..aD(19, _omitFieldNames ? '' : 'sentimientoPositivo')
    ..aD(20, _omitFieldNames ? '' : 'sentimientoNegativo')
    ..aD(21, _omitFieldNames ? '' : 'capitalizacionTotal')
    ..aD(22, _omitFieldNames ? '' : 'capitalizacionPromedio')
    ..aD(23, _omitFieldNames ? '' : 'precioPromedioAcumulado')
    ..aD(24, _omitFieldNames ? '' : 'precioMaximoAcumulado')
    ..aOS(25, _omitFieldNames ? '' : 'tendenciaGeneral')
    ..aD(26, _omitFieldNames ? '' : 'tendenciaPromedio')
    ..aE<SecurityExchangeMarketData>(
        27, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  BolsaStats clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  BolsaStats copyWith(void Function(BolsaStats) updates) =>
      super.copyWith((message) => updates(message as BolsaStats)) as BolsaStats;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static BolsaStats create() => BolsaStats._();
  @$core.override
  BolsaStats createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static BolsaStats getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<BolsaStats>(create);
  static BolsaStats? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get totalVolumen => $_getN(1);
  @$pb.TagNumber(2)
  set totalVolumen($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasTotalVolumen() => $_has(1);
  @$pb.TagNumber(2)
  void clearTotalVolumen() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get montoTotal => $_getN(2);
  @$pb.TagNumber(3)
  set montoTotal($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMontoTotal() => $_has(2);
  @$pb.TagNumber(3)
  void clearMontoTotal() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get horaInicio => $_getSZ(3);
  @$pb.TagNumber(4)
  set horaInicio($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasHoraInicio() => $_has(3);
  @$pb.TagNumber(4)
  void clearHoraInicio() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get horaFin => $_getSZ(4);
  @$pb.TagNumber(5)
  set horaFin($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasHoraFin() => $_has(4);
  @$pb.TagNumber(5)
  void clearHoraFin() => $_clearField(5);

  @$pb.TagNumber(6)
  $pb.PbList<RankinSymbol> get masVolatil => $_getList(5);

  @$pb.TagNumber(7)
  $pb.PbList<RankinSymbol> get masCayo => $_getList(6);

  @$pb.TagNumber(8)
  $pb.PbList<RankinSymbol> get menosCayo => $_getList(7);

  @$pb.TagNumber(9)
  $pb.PbList<RankinSymbol> get masTranzado => $_getList(8);

  @$pb.TagNumber(10)
  $pb.PbList<RankinSymbol> get bestRankin => $_getList(9);

  @$pb.TagNumber(11)
  $pb.PbList<RankinSymbol> get worseRankin => $_getList(10);

  @$pb.TagNumber(12)
  $core.double get volatilidadPromedio => $_getN(11);
  @$pb.TagNumber(12)
  set volatilidadPromedio($core.double value) => $_setDouble(11, value);
  @$pb.TagNumber(12)
  $core.bool hasVolatilidadPromedio() => $_has(11);
  @$pb.TagNumber(12)
  void clearVolatilidadPromedio() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.double get rangoPromedio => $_getN(12);
  @$pb.TagNumber(13)
  set rangoPromedio($core.double value) => $_setDouble(12, value);
  @$pb.TagNumber(13)
  $core.bool hasRangoPromedio() => $_has(12);
  @$pb.TagNumber(13)
  void clearRangoPromedio() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.double get indicePromedio => $_getN(13);
  @$pb.TagNumber(14)
  set indicePromedio($core.double value) => $_setDouble(13, value);
  @$pb.TagNumber(14)
  $core.bool hasIndicePromedio() => $_has(13);
  @$pb.TagNumber(14)
  void clearIndicePromedio() => $_clearField(14);

  @$pb.TagNumber(15)
  $core.double get indiceMaximo => $_getN(14);
  @$pb.TagNumber(15)
  set indiceMaximo($core.double value) => $_setDouble(14, value);
  @$pb.TagNumber(15)
  $core.bool hasIndiceMaximo() => $_has(14);
  @$pb.TagNumber(15)
  void clearIndiceMaximo() => $_clearField(15);

  @$pb.TagNumber(16)
  $core.double get indiceMinimo => $_getN(15);
  @$pb.TagNumber(16)
  set indiceMinimo($core.double value) => $_setDouble(15, value);
  @$pb.TagNumber(16)
  $core.bool hasIndiceMinimo() => $_has(15);
  @$pb.TagNumber(16)
  void clearIndiceMinimo() => $_clearField(16);

  @$pb.TagNumber(17)
  $core.double get liquidezMedia => $_getN(16);
  @$pb.TagNumber(17)
  set liquidezMedia($core.double value) => $_setDouble(16, value);
  @$pb.TagNumber(17)
  $core.bool hasLiquidezMedia() => $_has(16);
  @$pb.TagNumber(17)
  void clearLiquidezMedia() => $_clearField(17);

  @$pb.TagNumber(18)
  $fixnum.Int64 get numeroTotalTrades => $_getI64(17);
  @$pb.TagNumber(18)
  set numeroTotalTrades($fixnum.Int64 value) => $_setInt64(17, value);
  @$pb.TagNumber(18)
  $core.bool hasNumeroTotalTrades() => $_has(17);
  @$pb.TagNumber(18)
  void clearNumeroTotalTrades() => $_clearField(18);

  @$pb.TagNumber(19)
  $core.double get sentimientoPositivo => $_getN(18);
  @$pb.TagNumber(19)
  set sentimientoPositivo($core.double value) => $_setDouble(18, value);
  @$pb.TagNumber(19)
  $core.bool hasSentimientoPositivo() => $_has(18);
  @$pb.TagNumber(19)
  void clearSentimientoPositivo() => $_clearField(19);

  @$pb.TagNumber(20)
  $core.double get sentimientoNegativo => $_getN(19);
  @$pb.TagNumber(20)
  set sentimientoNegativo($core.double value) => $_setDouble(19, value);
  @$pb.TagNumber(20)
  $core.bool hasSentimientoNegativo() => $_has(19);
  @$pb.TagNumber(20)
  void clearSentimientoNegativo() => $_clearField(20);

  @$pb.TagNumber(21)
  $core.double get capitalizacionTotal => $_getN(20);
  @$pb.TagNumber(21)
  set capitalizacionTotal($core.double value) => $_setDouble(20, value);
  @$pb.TagNumber(21)
  $core.bool hasCapitalizacionTotal() => $_has(20);
  @$pb.TagNumber(21)
  void clearCapitalizacionTotal() => $_clearField(21);

  @$pb.TagNumber(22)
  $core.double get capitalizacionPromedio => $_getN(21);
  @$pb.TagNumber(22)
  set capitalizacionPromedio($core.double value) => $_setDouble(21, value);
  @$pb.TagNumber(22)
  $core.bool hasCapitalizacionPromedio() => $_has(21);
  @$pb.TagNumber(22)
  void clearCapitalizacionPromedio() => $_clearField(22);

  @$pb.TagNumber(23)
  $core.double get precioPromedioAcumulado => $_getN(22);
  @$pb.TagNumber(23)
  set precioPromedioAcumulado($core.double value) => $_setDouble(22, value);
  @$pb.TagNumber(23)
  $core.bool hasPrecioPromedioAcumulado() => $_has(22);
  @$pb.TagNumber(23)
  void clearPrecioPromedioAcumulado() => $_clearField(23);

  @$pb.TagNumber(24)
  $core.double get precioMaximoAcumulado => $_getN(23);
  @$pb.TagNumber(24)
  set precioMaximoAcumulado($core.double value) => $_setDouble(23, value);
  @$pb.TagNumber(24)
  $core.bool hasPrecioMaximoAcumulado() => $_has(23);
  @$pb.TagNumber(24)
  void clearPrecioMaximoAcumulado() => $_clearField(24);

  @$pb.TagNumber(25)
  $core.String get tendenciaGeneral => $_getSZ(24);
  @$pb.TagNumber(25)
  set tendenciaGeneral($core.String value) => $_setString(24, value);
  @$pb.TagNumber(25)
  $core.bool hasTendenciaGeneral() => $_has(24);
  @$pb.TagNumber(25)
  void clearTendenciaGeneral() => $_clearField(25);

  @$pb.TagNumber(26)
  $core.double get tendenciaPromedio => $_getN(25);
  @$pb.TagNumber(26)
  set tendenciaPromedio($core.double value) => $_setDouble(25, value);
  @$pb.TagNumber(26)
  $core.bool hasTendenciaPromedio() => $_has(25);
  @$pb.TagNumber(26)
  void clearTendenciaPromedio() => $_clearField(26);

  @$pb.TagNumber(27)
  SecurityExchangeMarketData get securityExchange => $_getN(26);
  @$pb.TagNumber(27)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(27, value);
  @$pb.TagNumber(27)
  $core.bool hasSecurityExchange() => $_has(26);
  @$pb.TagNumber(27)
  void clearSecurityExchange() => $_clearField(27);
}

class RankinSymbol extends $pb.GeneratedMessage {
  factory RankinSymbol({
    $core.String? id,
    SecurityExchangeMarketData? securityExchange,
    $core.String? symbol,
    $1.SettlType? settlType,
    $1.SecurityType? securityType,
    $core.double? variacionPct,
    $core.double? precioUltimo,
    $core.double? precioMaximo,
    $core.double? precioMinimo,
    $core.double? precioPromedio,
    $core.double? vwap,
    $core.double? twap,
    $core.double? volumen,
    $core.double? monto,
    $core.double? rsi,
    $core.double? ma,
    $core.double? macd,
    $core.double? liquidRatio,
    $core.double? impliedVolatility,
    $core.double? liquidRatioV2,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (symbol != null) result.symbol = symbol;
    if (settlType != null) result.settlType = settlType;
    if (securityType != null) result.securityType = securityType;
    if (variacionPct != null) result.variacionPct = variacionPct;
    if (precioUltimo != null) result.precioUltimo = precioUltimo;
    if (precioMaximo != null) result.precioMaximo = precioMaximo;
    if (precioMinimo != null) result.precioMinimo = precioMinimo;
    if (precioPromedio != null) result.precioPromedio = precioPromedio;
    if (vwap != null) result.vwap = vwap;
    if (twap != null) result.twap = twap;
    if (volumen != null) result.volumen = volumen;
    if (monto != null) result.monto = monto;
    if (rsi != null) result.rsi = rsi;
    if (ma != null) result.ma = ma;
    if (macd != null) result.macd = macd;
    if (liquidRatio != null) result.liquidRatio = liquidRatio;
    if (impliedVolatility != null) result.impliedVolatility = impliedVolatility;
    if (liquidRatioV2 != null) result.liquidRatioV2 = liquidRatioV2;
    return result;
  }

  RankinSymbol._();

  factory RankinSymbol.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory RankinSymbol.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RankinSymbol',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aE<SecurityExchangeMarketData>(
        2, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: SecurityExchangeMarketData.values)
    ..aOS(3, _omitFieldNames ? '' : 'symbol')
    ..aE<$1.SettlType>(4, _omitFieldNames ? '' : 'settlType',
        protoName: 'settlType', enumValues: $1.SettlType.values)
    ..aE<$1.SecurityType>(5, _omitFieldNames ? '' : 'securityType',
        protoName: 'securityType', enumValues: $1.SecurityType.values)
    ..aD(6, _omitFieldNames ? '' : 'variacionPct')
    ..aD(7, _omitFieldNames ? '' : 'precioUltimo')
    ..aD(8, _omitFieldNames ? '' : 'precioMaximo')
    ..aD(9, _omitFieldNames ? '' : 'precioMinimo')
    ..aD(10, _omitFieldNames ? '' : 'precioPromedio')
    ..aD(11, _omitFieldNames ? '' : 'vwap')
    ..aD(12, _omitFieldNames ? '' : 'twap')
    ..aD(13, _omitFieldNames ? '' : 'volumen')
    ..aD(14, _omitFieldNames ? '' : 'monto')
    ..aD(16, _omitFieldNames ? '' : 'rsi')
    ..aD(17, _omitFieldNames ? '' : 'ma')
    ..aD(18, _omitFieldNames ? '' : 'macd')
    ..aD(19, _omitFieldNames ? '' : 'liquidRatio', protoName: 'liquidRatio')
    ..aD(20, _omitFieldNames ? '' : 'impliedVolatility')
    ..aD(21, _omitFieldNames ? '' : 'liquidRatioV2')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RankinSymbol clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RankinSymbol copyWith(void Function(RankinSymbol) updates) =>
      super.copyWith((message) => updates(message as RankinSymbol))
          as RankinSymbol;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static RankinSymbol create() => RankinSymbol._();
  @$core.override
  RankinSymbol createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static RankinSymbol getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RankinSymbol>(create);
  static RankinSymbol? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  SecurityExchangeMarketData get securityExchange => $_getN(1);
  @$pb.TagNumber(2)
  set securityExchange(SecurityExchangeMarketData value) =>
      $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSecurityExchange() => $_has(1);
  @$pb.TagNumber(2)
  void clearSecurityExchange() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get symbol => $_getSZ(2);
  @$pb.TagNumber(3)
  set symbol($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSymbol() => $_has(2);
  @$pb.TagNumber(3)
  void clearSymbol() => $_clearField(3);

  @$pb.TagNumber(4)
  $1.SettlType get settlType => $_getN(3);
  @$pb.TagNumber(4)
  set settlType($1.SettlType value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasSettlType() => $_has(3);
  @$pb.TagNumber(4)
  void clearSettlType() => $_clearField(4);

  @$pb.TagNumber(5)
  $1.SecurityType get securityType => $_getN(4);
  @$pb.TagNumber(5)
  set securityType($1.SecurityType value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasSecurityType() => $_has(4);
  @$pb.TagNumber(5)
  void clearSecurityType() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.double get variacionPct => $_getN(5);
  @$pb.TagNumber(6)
  set variacionPct($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(6)
  $core.bool hasVariacionPct() => $_has(5);
  @$pb.TagNumber(6)
  void clearVariacionPct() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.double get precioUltimo => $_getN(6);
  @$pb.TagNumber(7)
  set precioUltimo($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(7)
  $core.bool hasPrecioUltimo() => $_has(6);
  @$pb.TagNumber(7)
  void clearPrecioUltimo() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.double get precioMaximo => $_getN(7);
  @$pb.TagNumber(8)
  set precioMaximo($core.double value) => $_setDouble(7, value);
  @$pb.TagNumber(8)
  $core.bool hasPrecioMaximo() => $_has(7);
  @$pb.TagNumber(8)
  void clearPrecioMaximo() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.double get precioMinimo => $_getN(8);
  @$pb.TagNumber(9)
  set precioMinimo($core.double value) => $_setDouble(8, value);
  @$pb.TagNumber(9)
  $core.bool hasPrecioMinimo() => $_has(8);
  @$pb.TagNumber(9)
  void clearPrecioMinimo() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.double get precioPromedio => $_getN(9);
  @$pb.TagNumber(10)
  set precioPromedio($core.double value) => $_setDouble(9, value);
  @$pb.TagNumber(10)
  $core.bool hasPrecioPromedio() => $_has(9);
  @$pb.TagNumber(10)
  void clearPrecioPromedio() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.double get vwap => $_getN(10);
  @$pb.TagNumber(11)
  set vwap($core.double value) => $_setDouble(10, value);
  @$pb.TagNumber(11)
  $core.bool hasVwap() => $_has(10);
  @$pb.TagNumber(11)
  void clearVwap() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.double get twap => $_getN(11);
  @$pb.TagNumber(12)
  set twap($core.double value) => $_setDouble(11, value);
  @$pb.TagNumber(12)
  $core.bool hasTwap() => $_has(11);
  @$pb.TagNumber(12)
  void clearTwap() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.double get volumen => $_getN(12);
  @$pb.TagNumber(13)
  set volumen($core.double value) => $_setDouble(12, value);
  @$pb.TagNumber(13)
  $core.bool hasVolumen() => $_has(12);
  @$pb.TagNumber(13)
  void clearVolumen() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.double get monto => $_getN(13);
  @$pb.TagNumber(14)
  set monto($core.double value) => $_setDouble(13, value);
  @$pb.TagNumber(14)
  $core.bool hasMonto() => $_has(13);
  @$pb.TagNumber(14)
  void clearMonto() => $_clearField(14);

  @$pb.TagNumber(16)
  $core.double get rsi => $_getN(14);
  @$pb.TagNumber(16)
  set rsi($core.double value) => $_setDouble(14, value);
  @$pb.TagNumber(16)
  $core.bool hasRsi() => $_has(14);
  @$pb.TagNumber(16)
  void clearRsi() => $_clearField(16);

  @$pb.TagNumber(17)
  $core.double get ma => $_getN(15);
  @$pb.TagNumber(17)
  set ma($core.double value) => $_setDouble(15, value);
  @$pb.TagNumber(17)
  $core.bool hasMa() => $_has(15);
  @$pb.TagNumber(17)
  void clearMa() => $_clearField(17);

  @$pb.TagNumber(18)
  $core.double get macd => $_getN(16);
  @$pb.TagNumber(18)
  set macd($core.double value) => $_setDouble(16, value);
  @$pb.TagNumber(18)
  $core.bool hasMacd() => $_has(16);
  @$pb.TagNumber(18)
  void clearMacd() => $_clearField(18);

  @$pb.TagNumber(19)
  $core.double get liquidRatio => $_getN(17);
  @$pb.TagNumber(19)
  set liquidRatio($core.double value) => $_setDouble(17, value);
  @$pb.TagNumber(19)
  $core.bool hasLiquidRatio() => $_has(17);
  @$pb.TagNumber(19)
  void clearLiquidRatio() => $_clearField(19);

  @$pb.TagNumber(20)
  $core.double get impliedVolatility => $_getN(18);
  @$pb.TagNumber(20)
  set impliedVolatility($core.double value) => $_setDouble(18, value);
  @$pb.TagNumber(20)
  $core.bool hasImpliedVolatility() => $_has(18);
  @$pb.TagNumber(20)
  void clearImpliedVolatility() => $_clearField(20);

  @$pb.TagNumber(21)
  $core.double get liquidRatioV2 => $_getN(19);
  @$pb.TagNumber(21)
  set liquidRatioV2($core.double value) => $_setDouble(19, value);
  @$pb.TagNumber(21)
  $core.bool hasLiquidRatioV2() => $_has(19);
  @$pb.TagNumber(21)
  void clearLiquidRatioV2() => $_clearField(21);
}

class SnapshotTradeGeneral extends $pb.GeneratedMessage {
  factory SnapshotTradeGeneral({
    $core.Iterable<TradeGeneral>? trades,
  }) {
    final result = create();
    if (trades != null) result.trades.addAll(trades);
    return result;
  }

  SnapshotTradeGeneral._();

  factory SnapshotTradeGeneral.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotTradeGeneral.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotTradeGeneral',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<TradeGeneral>(1, _omitFieldNames ? '' : 'trades',
        subBuilder: TradeGeneral.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotTradeGeneral clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotTradeGeneral copyWith(void Function(SnapshotTradeGeneral) updates) =>
      super.copyWith((message) => updates(message as SnapshotTradeGeneral))
          as SnapshotTradeGeneral;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotTradeGeneral create() => SnapshotTradeGeneral._();
  @$core.override
  SnapshotTradeGeneral createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotTradeGeneral getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotTradeGeneral>(create);
  static SnapshotTradeGeneral? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<TradeGeneral> get trades => $_getList(0);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
