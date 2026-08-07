// This is a generated file - do not edit.
//
// Generated from blotter.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;
import 'package:protobuf/well_known_types/google/protobuf/timestamp.pb.dart'
    as $2;

import 'blotter.pbenum.dart';
import 'marketdata.pb.dart' as $1;
import 'routing.pb.dart' as $0;

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'blotter.pbenum.dart';

class PreselectRequest extends $pb.GeneratedMessage {
  factory PreselectRequest({
    $0.Order? orders,
    $core.String? username,
    StatusPreselect? statusPreselect,
  }) {
    final result = create();
    if (orders != null) result.orders = orders;
    if (username != null) result.username = username;
    if (statusPreselect != null) result.statusPreselect = statusPreselect;
    return result;
  }

  PreselectRequest._();

  factory PreselectRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory PreselectRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PreselectRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<$0.Order>(1, _omitFieldNames ? '' : 'orders',
        subBuilder: $0.Order.create)
    ..aOS(2, _omitFieldNames ? '' : 'username')
    ..aE<StatusPreselect>(3, _omitFieldNames ? '' : 'statusPreselect',
        protoName: 'statusPreselect', enumValues: StatusPreselect.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PreselectRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PreselectRequest copyWith(void Function(PreselectRequest) updates) =>
      super.copyWith((message) => updates(message as PreselectRequest))
          as PreselectRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static PreselectRequest create() => PreselectRequest._();
  @$core.override
  PreselectRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static PreselectRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PreselectRequest>(create);
  static PreselectRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $0.Order get orders => $_getN(0);
  @$pb.TagNumber(1)
  set orders($0.Order value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOrders() => $_has(0);
  @$pb.TagNumber(1)
  void clearOrders() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.Order ensureOrders() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get username => $_getSZ(1);
  @$pb.TagNumber(2)
  set username($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUsername() => $_has(1);
  @$pb.TagNumber(2)
  void clearUsername() => $_clearField(2);

  @$pb.TagNumber(3)
  StatusPreselect get statusPreselect => $_getN(2);
  @$pb.TagNumber(3)
  set statusPreselect(StatusPreselect value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasStatusPreselect() => $_has(2);
  @$pb.TagNumber(3)
  void clearStatusPreselect() => $_clearField(3);
}

class PreselectResponse extends $pb.GeneratedMessage {
  factory PreselectResponse({
    $core.Iterable<$0.Order>? orders,
    $core.String? username,
    StatusPreselect? statusPreselect,
  }) {
    final result = create();
    if (orders != null) result.orders.addAll(orders);
    if (username != null) result.username = username;
    if (statusPreselect != null) result.statusPreselect = statusPreselect;
    return result;
  }

  PreselectResponse._();

  factory PreselectResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory PreselectResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PreselectResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<$0.Order>(1, _omitFieldNames ? '' : 'orders',
        subBuilder: $0.Order.create)
    ..aOS(2, _omitFieldNames ? '' : 'username')
    ..aE<StatusPreselect>(3, _omitFieldNames ? '' : 'statusPreselect',
        protoName: 'statusPreselect', enumValues: StatusPreselect.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PreselectResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PreselectResponse copyWith(void Function(PreselectResponse) updates) =>
      super.copyWith((message) => updates(message as PreselectResponse))
          as PreselectResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static PreselectResponse create() => PreselectResponse._();
  @$core.override
  PreselectResponse createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static PreselectResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PreselectResponse>(create);
  static PreselectResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<$0.Order> get orders => $_getList(0);

  @$pb.TagNumber(2)
  $core.String get username => $_getSZ(1);
  @$pb.TagNumber(2)
  set username($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUsername() => $_has(1);
  @$pb.TagNumber(2)
  void clearUsername() => $_clearField(2);

  @$pb.TagNumber(3)
  StatusPreselect get statusPreselect => $_getN(2);
  @$pb.TagNumber(3)
  set statusPreselect(StatusPreselect value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasStatusPreselect() => $_has(2);
  @$pb.TagNumber(3)
  void clearStatusPreselect() => $_clearField(3);
}

class PortfolioRequest extends $pb.GeneratedMessage {
  factory PortfolioRequest({
    StatusPortfolio? statusPortfolio,
    $core.String? marketdataControllerId,
    $core.String? namePortfolio,
    $core.String? username,
    Asset? asset,
  }) {
    final result = create();
    if (statusPortfolio != null) result.statusPortfolio = statusPortfolio;
    if (marketdataControllerId != null)
      result.marketdataControllerId = marketdataControllerId;
    if (namePortfolio != null) result.namePortfolio = namePortfolio;
    if (username != null) result.username = username;
    if (asset != null) result.asset = asset;
    return result;
  }

  PortfolioRequest._();

  factory PortfolioRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory PortfolioRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PortfolioRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aE<StatusPortfolio>(1, _omitFieldNames ? '' : 'statusPortfolio',
        protoName: 'statusPortfolio', enumValues: StatusPortfolio.values)
    ..aOS(2, _omitFieldNames ? '' : 'marketdataControllerId',
        protoName: 'marketdataControllerId')
    ..aOS(4, _omitFieldNames ? '' : 'namePortfolio', protoName: 'namePortfolio')
    ..aOS(5, _omitFieldNames ? '' : 'username')
    ..aOM<Asset>(6, _omitFieldNames ? '' : 'asset', subBuilder: Asset.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PortfolioRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PortfolioRequest copyWith(void Function(PortfolioRequest) updates) =>
      super.copyWith((message) => updates(message as PortfolioRequest))
          as PortfolioRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static PortfolioRequest create() => PortfolioRequest._();
  @$core.override
  PortfolioRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static PortfolioRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PortfolioRequest>(create);
  static PortfolioRequest? _defaultInstance;

  @$pb.TagNumber(1)
  StatusPortfolio get statusPortfolio => $_getN(0);
  @$pb.TagNumber(1)
  set statusPortfolio(StatusPortfolio value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStatusPortfolio() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatusPortfolio() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get marketdataControllerId => $_getSZ(1);
  @$pb.TagNumber(2)
  set marketdataControllerId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMarketdataControllerId() => $_has(1);
  @$pb.TagNumber(2)
  void clearMarketdataControllerId() => $_clearField(2);

  @$pb.TagNumber(4)
  $core.String get namePortfolio => $_getSZ(2);
  @$pb.TagNumber(4)
  set namePortfolio($core.String value) => $_setString(2, value);
  @$pb.TagNumber(4)
  $core.bool hasNamePortfolio() => $_has(2);
  @$pb.TagNumber(4)
  void clearNamePortfolio() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get username => $_getSZ(3);
  @$pb.TagNumber(5)
  set username($core.String value) => $_setString(3, value);
  @$pb.TagNumber(5)
  $core.bool hasUsername() => $_has(3);
  @$pb.TagNumber(5)
  void clearUsername() => $_clearField(5);

  @$pb.TagNumber(6)
  Asset get asset => $_getN(4);
  @$pb.TagNumber(6)
  set asset(Asset value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasAsset() => $_has(4);
  @$pb.TagNumber(6)
  void clearAsset() => $_clearField(6);
  @$pb.TagNumber(6)
  Asset ensureAsset() => $_ensure(4);
}

class PortfolioResponse extends $pb.GeneratedMessage {
  factory PortfolioResponse({
    $core.Iterable<Portfolio>? postfolio,
    StatusPortfolio? statusPortfolio,
    Asset? asset,
    $core.String? username,
    $core.String? namePortfolio,
    $core.String? marketdataControllerId,
  }) {
    final result = create();
    if (postfolio != null) result.postfolio.addAll(postfolio);
    if (statusPortfolio != null) result.statusPortfolio = statusPortfolio;
    if (asset != null) result.asset = asset;
    if (username != null) result.username = username;
    if (namePortfolio != null) result.namePortfolio = namePortfolio;
    if (marketdataControllerId != null)
      result.marketdataControllerId = marketdataControllerId;
    return result;
  }

  PortfolioResponse._();

  factory PortfolioResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory PortfolioResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PortfolioResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<Portfolio>(1, _omitFieldNames ? '' : 'postfolio',
        subBuilder: Portfolio.create)
    ..aE<StatusPortfolio>(2, _omitFieldNames ? '' : 'statusPortfolio',
        protoName: 'statusPortfolio', enumValues: StatusPortfolio.values)
    ..aOM<Asset>(3, _omitFieldNames ? '' : 'asset', subBuilder: Asset.create)
    ..aOS(4, _omitFieldNames ? '' : 'username')
    ..aOS(5, _omitFieldNames ? '' : 'namePortfolio', protoName: 'namePortfolio')
    ..aOS(6, _omitFieldNames ? '' : 'marketdataControllerId',
        protoName: 'marketdataControllerId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PortfolioResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PortfolioResponse copyWith(void Function(PortfolioResponse) updates) =>
      super.copyWith((message) => updates(message as PortfolioResponse))
          as PortfolioResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static PortfolioResponse create() => PortfolioResponse._();
  @$core.override
  PortfolioResponse createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static PortfolioResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PortfolioResponse>(create);
  static PortfolioResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<Portfolio> get postfolio => $_getList(0);

  @$pb.TagNumber(2)
  StatusPortfolio get statusPortfolio => $_getN(1);
  @$pb.TagNumber(2)
  set statusPortfolio(StatusPortfolio value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasStatusPortfolio() => $_has(1);
  @$pb.TagNumber(2)
  void clearStatusPortfolio() => $_clearField(2);

  @$pb.TagNumber(3)
  Asset get asset => $_getN(2);
  @$pb.TagNumber(3)
  set asset(Asset value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasAsset() => $_has(2);
  @$pb.TagNumber(3)
  void clearAsset() => $_clearField(3);
  @$pb.TagNumber(3)
  Asset ensureAsset() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.String get username => $_getSZ(3);
  @$pb.TagNumber(4)
  set username($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasUsername() => $_has(3);
  @$pb.TagNumber(4)
  void clearUsername() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get namePortfolio => $_getSZ(4);
  @$pb.TagNumber(5)
  set namePortfolio($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasNamePortfolio() => $_has(4);
  @$pb.TagNumber(5)
  void clearNamePortfolio() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get marketdataControllerId => $_getSZ(5);
  @$pb.TagNumber(6)
  set marketdataControllerId($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasMarketdataControllerId() => $_has(5);
  @$pb.TagNumber(6)
  void clearMarketdataControllerId() => $_clearField(6);
}

class Portfolio extends $pb.GeneratedMessage {
  factory Portfolio({
    $core.String? id,
    $core.String? idSec,
    $core.String? namePortfolio,
    $core.String? username,
    $core.Iterable<Asset>? asset,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (idSec != null) result.idSec = idSec;
    if (namePortfolio != null) result.namePortfolio = namePortfolio;
    if (username != null) result.username = username;
    if (asset != null) result.asset.addAll(asset);
    return result;
  }

  Portfolio._();

  factory Portfolio.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Portfolio.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Portfolio',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'idSec', protoName: 'idSec')
    ..aOS(3, _omitFieldNames ? '' : 'namePortfolio', protoName: 'namePortfolio')
    ..aOS(4, _omitFieldNames ? '' : 'username')
    ..pPM<Asset>(5, _omitFieldNames ? '' : 'asset', subBuilder: Asset.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Portfolio clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Portfolio copyWith(void Function(Portfolio) updates) =>
      super.copyWith((message) => updates(message as Portfolio)) as Portfolio;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Portfolio create() => Portfolio._();
  @$core.override
  Portfolio createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Portfolio getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Portfolio>(create);
  static Portfolio? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get idSec => $_getSZ(1);
  @$pb.TagNumber(2)
  set idSec($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdSec() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdSec() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get namePortfolio => $_getSZ(2);
  @$pb.TagNumber(3)
  set namePortfolio($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasNamePortfolio() => $_has(2);
  @$pb.TagNumber(3)
  void clearNamePortfolio() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get username => $_getSZ(3);
  @$pb.TagNumber(4)
  set username($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasUsername() => $_has(3);
  @$pb.TagNumber(4)
  void clearUsername() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<Asset> get asset => $_getList(4);
}

class Asset extends $pb.GeneratedMessage {
  factory Asset({
    $core.String? id,
    $core.String? symbol,
    $1.Statistic? statistic,
    $1.SecurityExchangeMarketData? securityexchange,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (symbol != null) result.symbol = symbol;
    if (statistic != null) result.statistic = statistic;
    if (securityexchange != null) result.securityexchange = securityexchange;
    return result;
  }

  Asset._();

  factory Asset.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Asset.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Asset',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'symbol')
    ..aOM<$1.Statistic>(3, _omitFieldNames ? '' : 'statistic',
        subBuilder: $1.Statistic.create)
    ..aE<$1.SecurityExchangeMarketData>(
        4, _omitFieldNames ? '' : 'securityexchange',
        enumValues: $1.SecurityExchangeMarketData.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Asset clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Asset copyWith(void Function(Asset) updates) =>
      super.copyWith((message) => updates(message as Asset)) as Asset;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Asset create() => Asset._();
  @$core.override
  Asset createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Asset getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Asset>(create);
  static Asset? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get symbol => $_getSZ(1);
  @$pb.TagNumber(2)
  set symbol($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSymbol() => $_has(1);
  @$pb.TagNumber(2)
  void clearSymbol() => $_clearField(2);

  @$pb.TagNumber(3)
  $1.Statistic get statistic => $_getN(2);
  @$pb.TagNumber(3)
  set statistic($1.Statistic value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasStatistic() => $_has(2);
  @$pb.TagNumber(3)
  void clearStatistic() => $_clearField(3);
  @$pb.TagNumber(3)
  $1.Statistic ensureStatistic() => $_ensure(2);

  @$pb.TagNumber(4)
  $1.SecurityExchangeMarketData get securityexchange => $_getN(3);
  @$pb.TagNumber(4)
  set securityexchange($1.SecurityExchangeMarketData value) =>
      $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasSecurityexchange() => $_has(3);
  @$pb.TagNumber(4)
  void clearSecurityexchange() => $_clearField(4);
}

class TypeOrd extends $pb.GeneratedMessage {
  factory TypeOrd({
    $core.double? sent,
    $core.double? working,
    $core.double? trade,
    $core.double? short,
    $core.double? px,
    $core.double? cashBought,
  }) {
    final result = create();
    if (sent != null) result.sent = sent;
    if (working != null) result.working = working;
    if (trade != null) result.trade = trade;
    if (short != null) result.short = short;
    if (px != null) result.px = px;
    if (cashBought != null) result.cashBought = cashBought;
    return result;
  }

  TypeOrd._();

  factory TypeOrd.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory TypeOrd.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TypeOrd',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aD(1, _omitFieldNames ? '' : 'sent')
    ..aD(2, _omitFieldNames ? '' : 'working')
    ..aD(3, _omitFieldNames ? '' : 'trade')
    ..aD(4, _omitFieldNames ? '' : 'short')
    ..aD(5, _omitFieldNames ? '' : 'px')
    ..aD(6, _omitFieldNames ? '' : 'cashBought', protoName: 'cashBought')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TypeOrd clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TypeOrd copyWith(void Function(TypeOrd) updates) =>
      super.copyWith((message) => updates(message as TypeOrd)) as TypeOrd;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TypeOrd create() => TypeOrd._();
  @$core.override
  TypeOrd createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static TypeOrd getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TypeOrd>(create);
  static TypeOrd? _defaultInstance;

  @$pb.TagNumber(1)
  $core.double get sent => $_getN(0);
  @$pb.TagNumber(1)
  set sent($core.double value) => $_setDouble(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSent() => $_has(0);
  @$pb.TagNumber(1)
  void clearSent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get working => $_getN(1);
  @$pb.TagNumber(2)
  set working($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasWorking() => $_has(1);
  @$pb.TagNumber(2)
  void clearWorking() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get trade => $_getN(2);
  @$pb.TagNumber(3)
  set trade($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTrade() => $_has(2);
  @$pb.TagNumber(3)
  void clearTrade() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get short => $_getN(3);
  @$pb.TagNumber(4)
  set short($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasShort() => $_has(3);
  @$pb.TagNumber(4)
  void clearShort() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.double get px => $_getN(4);
  @$pb.TagNumber(5)
  set px($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(5)
  $core.bool hasPx() => $_has(4);
  @$pb.TagNumber(5)
  void clearPx() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.double get cashBought => $_getN(5);
  @$pb.TagNumber(6)
  set cashBought($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(6)
  $core.bool hasCashBought() => $_has(5);
  @$pb.TagNumber(6)
  void clearCashBought() => $_clearField(6);
}

class SubscribeTradeMonitor extends $pb.GeneratedMessage {
  factory SubscribeTradeMonitor({
    $core.String? username,
    $0.OrderStatus? orderStatus,
    $0.SecurityExchangeRouting? securityExchange,
    $core.String? symbol,
    $0.Side? side,
    $core.String? prefix,
    $core.Iterable<$core.String>? account,
  }) {
    final result = create();
    if (username != null) result.username = username;
    if (orderStatus != null) result.orderStatus = orderStatus;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (symbol != null) result.symbol = symbol;
    if (side != null) result.side = side;
    if (prefix != null) result.prefix = prefix;
    if (account != null) result.account.addAll(account);
    return result;
  }

  SubscribeTradeMonitor._();

  factory SubscribeTradeMonitor.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SubscribeTradeMonitor.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SubscribeTradeMonitor',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'username')
    ..aE<$0.OrderStatus>(2, _omitFieldNames ? '' : 'orderStatus',
        protoName: 'orderStatus', enumValues: $0.OrderStatus.values)
    ..aE<$0.SecurityExchangeRouting>(
        3, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange',
        enumValues: $0.SecurityExchangeRouting.values)
    ..aOS(4, _omitFieldNames ? '' : 'symbol')
    ..aE<$0.Side>(5, _omitFieldNames ? '' : 'side', enumValues: $0.Side.values)
    ..aOS(6, _omitFieldNames ? '' : 'prefix')
    ..pPS(7, _omitFieldNames ? '' : 'account')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscribeTradeMonitor clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscribeTradeMonitor copyWith(
          void Function(SubscribeTradeMonitor) updates) =>
      super.copyWith((message) => updates(message as SubscribeTradeMonitor))
          as SubscribeTradeMonitor;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SubscribeTradeMonitor create() => SubscribeTradeMonitor._();
  @$core.override
  SubscribeTradeMonitor createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SubscribeTradeMonitor getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SubscribeTradeMonitor>(create);
  static SubscribeTradeMonitor? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get username => $_getSZ(0);
  @$pb.TagNumber(1)
  set username($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasUsername() => $_has(0);
  @$pb.TagNumber(1)
  void clearUsername() => $_clearField(1);

  @$pb.TagNumber(2)
  $0.OrderStatus get orderStatus => $_getN(1);
  @$pb.TagNumber(2)
  set orderStatus($0.OrderStatus value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOrderStatus() => $_has(1);
  @$pb.TagNumber(2)
  void clearOrderStatus() => $_clearField(2);

  @$pb.TagNumber(3)
  $0.SecurityExchangeRouting get securityExchange => $_getN(2);
  @$pb.TagNumber(3)
  set securityExchange($0.SecurityExchangeRouting value) =>
      $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasSecurityExchange() => $_has(2);
  @$pb.TagNumber(3)
  void clearSecurityExchange() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get symbol => $_getSZ(3);
  @$pb.TagNumber(4)
  set symbol($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasSymbol() => $_has(3);
  @$pb.TagNumber(4)
  void clearSymbol() => $_clearField(4);

  @$pb.TagNumber(5)
  $0.Side get side => $_getN(4);
  @$pb.TagNumber(5)
  set side($0.Side value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasSide() => $_has(4);
  @$pb.TagNumber(5)
  void clearSide() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get prefix => $_getSZ(5);
  @$pb.TagNumber(6)
  set prefix($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasPrefix() => $_has(5);
  @$pb.TagNumber(6)
  void clearPrefix() => $_clearField(6);

  @$pb.TagNumber(7)
  $pb.PbList<$core.String> get account => $_getList(6);
}

class Position extends $pb.GeneratedMessage {
  factory Position({
    $core.String? id,
    $core.String? account,
    $core.String? symbol,
    $0.SecurityExchangeRouting? securityexchange,
    $core.double? auxBuy,
    $core.double? sentBuy,
    $core.double? workingBuy,
    $core.double? tradeBuy,
    $core.double? pxBuy,
    $core.double? cashBoughtBuy,
    $core.double? auxBSell,
    $core.double? sentSell,
    $core.double? workingSell,
    $core.double? tradeSell,
    $core.double? tradeSellShort,
    $core.double? pxSell,
    $core.double? cashSell,
    $core.double? qtyNet,
    $core.double? pxNet,
    $core.double? amountNet,
    $core.String? settlType,
    $2.Timestamp? dateProcesor,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (account != null) result.account = account;
    if (symbol != null) result.symbol = symbol;
    if (securityexchange != null) result.securityexchange = securityexchange;
    if (auxBuy != null) result.auxBuy = auxBuy;
    if (sentBuy != null) result.sentBuy = sentBuy;
    if (workingBuy != null) result.workingBuy = workingBuy;
    if (tradeBuy != null) result.tradeBuy = tradeBuy;
    if (pxBuy != null) result.pxBuy = pxBuy;
    if (cashBoughtBuy != null) result.cashBoughtBuy = cashBoughtBuy;
    if (auxBSell != null) result.auxBSell = auxBSell;
    if (sentSell != null) result.sentSell = sentSell;
    if (workingSell != null) result.workingSell = workingSell;
    if (tradeSell != null) result.tradeSell = tradeSell;
    if (tradeSellShort != null) result.tradeSellShort = tradeSellShort;
    if (pxSell != null) result.pxSell = pxSell;
    if (cashSell != null) result.cashSell = cashSell;
    if (qtyNet != null) result.qtyNet = qtyNet;
    if (pxNet != null) result.pxNet = pxNet;
    if (amountNet != null) result.amountNet = amountNet;
    if (settlType != null) result.settlType = settlType;
    if (dateProcesor != null) result.dateProcesor = dateProcesor;
    return result;
  }

  Position._();

  factory Position.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Position.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Position',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'account')
    ..aOS(3, _omitFieldNames ? '' : 'symbol')
    ..aE<$0.SecurityExchangeRouting>(
        4, _omitFieldNames ? '' : 'securityexchange',
        enumValues: $0.SecurityExchangeRouting.values)
    ..aD(5, _omitFieldNames ? '' : 'auxBuy', protoName: 'auxBuy')
    ..aD(6, _omitFieldNames ? '' : 'sentBuy', protoName: 'sentBuy')
    ..aD(7, _omitFieldNames ? '' : 'workingBuy', protoName: 'workingBuy')
    ..aD(8, _omitFieldNames ? '' : 'tradeBuy', protoName: 'tradeBuy')
    ..aD(9, _omitFieldNames ? '' : 'pxBuy', protoName: 'pxBuy')
    ..aD(10, _omitFieldNames ? '' : 'cashBoughtBuy', protoName: 'cashBoughtBuy')
    ..aD(11, _omitFieldNames ? '' : 'auxBSell', protoName: 'auxBSell')
    ..aD(12, _omitFieldNames ? '' : 'sentSell', protoName: 'sentSell')
    ..aD(13, _omitFieldNames ? '' : 'workingSell', protoName: 'workingSell')
    ..aD(15, _omitFieldNames ? '' : 'tradeSell', protoName: 'tradeSell')
    ..aD(16, _omitFieldNames ? '' : 'tradeSellShort',
        protoName: 'tradeSellShort')
    ..aD(17, _omitFieldNames ? '' : 'pxSell', protoName: 'pxSell')
    ..aD(18, _omitFieldNames ? '' : 'cashSell', protoName: 'cashSell')
    ..aD(19, _omitFieldNames ? '' : 'qtyNet', protoName: 'qtyNet')
    ..aD(20, _omitFieldNames ? '' : 'pxNet', protoName: 'pxNet')
    ..aD(21, _omitFieldNames ? '' : 'amountNet', protoName: 'amountNet')
    ..aOS(22, _omitFieldNames ? '' : 'settlType', protoName: 'settlType')
    ..aOM<$2.Timestamp>(23, _omitFieldNames ? '' : 'dateProcesor',
        subBuilder: $2.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Position clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Position copyWith(void Function(Position) updates) =>
      super.copyWith((message) => updates(message as Position)) as Position;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Position create() => Position._();
  @$core.override
  Position createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Position getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Position>(create);
  static Position? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get account => $_getSZ(1);
  @$pb.TagNumber(2)
  set account($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAccount() => $_has(1);
  @$pb.TagNumber(2)
  void clearAccount() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get symbol => $_getSZ(2);
  @$pb.TagNumber(3)
  set symbol($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSymbol() => $_has(2);
  @$pb.TagNumber(3)
  void clearSymbol() => $_clearField(3);

  @$pb.TagNumber(4)
  $0.SecurityExchangeRouting get securityexchange => $_getN(3);
  @$pb.TagNumber(4)
  set securityexchange($0.SecurityExchangeRouting value) =>
      $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasSecurityexchange() => $_has(3);
  @$pb.TagNumber(4)
  void clearSecurityexchange() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.double get auxBuy => $_getN(4);
  @$pb.TagNumber(5)
  set auxBuy($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(5)
  $core.bool hasAuxBuy() => $_has(4);
  @$pb.TagNumber(5)
  void clearAuxBuy() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.double get sentBuy => $_getN(5);
  @$pb.TagNumber(6)
  set sentBuy($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(6)
  $core.bool hasSentBuy() => $_has(5);
  @$pb.TagNumber(6)
  void clearSentBuy() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.double get workingBuy => $_getN(6);
  @$pb.TagNumber(7)
  set workingBuy($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(7)
  $core.bool hasWorkingBuy() => $_has(6);
  @$pb.TagNumber(7)
  void clearWorkingBuy() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.double get tradeBuy => $_getN(7);
  @$pb.TagNumber(8)
  set tradeBuy($core.double value) => $_setDouble(7, value);
  @$pb.TagNumber(8)
  $core.bool hasTradeBuy() => $_has(7);
  @$pb.TagNumber(8)
  void clearTradeBuy() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.double get pxBuy => $_getN(8);
  @$pb.TagNumber(9)
  set pxBuy($core.double value) => $_setDouble(8, value);
  @$pb.TagNumber(9)
  $core.bool hasPxBuy() => $_has(8);
  @$pb.TagNumber(9)
  void clearPxBuy() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.double get cashBoughtBuy => $_getN(9);
  @$pb.TagNumber(10)
  set cashBoughtBuy($core.double value) => $_setDouble(9, value);
  @$pb.TagNumber(10)
  $core.bool hasCashBoughtBuy() => $_has(9);
  @$pb.TagNumber(10)
  void clearCashBoughtBuy() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.double get auxBSell => $_getN(10);
  @$pb.TagNumber(11)
  set auxBSell($core.double value) => $_setDouble(10, value);
  @$pb.TagNumber(11)
  $core.bool hasAuxBSell() => $_has(10);
  @$pb.TagNumber(11)
  void clearAuxBSell() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.double get sentSell => $_getN(11);
  @$pb.TagNumber(12)
  set sentSell($core.double value) => $_setDouble(11, value);
  @$pb.TagNumber(12)
  $core.bool hasSentSell() => $_has(11);
  @$pb.TagNumber(12)
  void clearSentSell() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.double get workingSell => $_getN(12);
  @$pb.TagNumber(13)
  set workingSell($core.double value) => $_setDouble(12, value);
  @$pb.TagNumber(13)
  $core.bool hasWorkingSell() => $_has(12);
  @$pb.TagNumber(13)
  void clearWorkingSell() => $_clearField(13);

  @$pb.TagNumber(15)
  $core.double get tradeSell => $_getN(13);
  @$pb.TagNumber(15)
  set tradeSell($core.double value) => $_setDouble(13, value);
  @$pb.TagNumber(15)
  $core.bool hasTradeSell() => $_has(13);
  @$pb.TagNumber(15)
  void clearTradeSell() => $_clearField(15);

  @$pb.TagNumber(16)
  $core.double get tradeSellShort => $_getN(14);
  @$pb.TagNumber(16)
  set tradeSellShort($core.double value) => $_setDouble(14, value);
  @$pb.TagNumber(16)
  $core.bool hasTradeSellShort() => $_has(14);
  @$pb.TagNumber(16)
  void clearTradeSellShort() => $_clearField(16);

  @$pb.TagNumber(17)
  $core.double get pxSell => $_getN(15);
  @$pb.TagNumber(17)
  set pxSell($core.double value) => $_setDouble(15, value);
  @$pb.TagNumber(17)
  $core.bool hasPxSell() => $_has(15);
  @$pb.TagNumber(17)
  void clearPxSell() => $_clearField(17);

  @$pb.TagNumber(18)
  $core.double get cashSell => $_getN(16);
  @$pb.TagNumber(18)
  set cashSell($core.double value) => $_setDouble(16, value);
  @$pb.TagNumber(18)
  $core.bool hasCashSell() => $_has(16);
  @$pb.TagNumber(18)
  void clearCashSell() => $_clearField(18);

  @$pb.TagNumber(19)
  $core.double get qtyNet => $_getN(17);
  @$pb.TagNumber(19)
  set qtyNet($core.double value) => $_setDouble(17, value);
  @$pb.TagNumber(19)
  $core.bool hasQtyNet() => $_has(17);
  @$pb.TagNumber(19)
  void clearQtyNet() => $_clearField(19);

  @$pb.TagNumber(20)
  $core.double get pxNet => $_getN(18);
  @$pb.TagNumber(20)
  set pxNet($core.double value) => $_setDouble(18, value);
  @$pb.TagNumber(20)
  $core.bool hasPxNet() => $_has(18);
  @$pb.TagNumber(20)
  void clearPxNet() => $_clearField(20);

  @$pb.TagNumber(21)
  $core.double get amountNet => $_getN(19);
  @$pb.TagNumber(21)
  set amountNet($core.double value) => $_setDouble(19, value);
  @$pb.TagNumber(21)
  $core.bool hasAmountNet() => $_has(19);
  @$pb.TagNumber(21)
  void clearAmountNet() => $_clearField(21);

  @$pb.TagNumber(22)
  $core.String get settlType => $_getSZ(20);
  @$pb.TagNumber(22)
  set settlType($core.String value) => $_setString(20, value);
  @$pb.TagNumber(22)
  $core.bool hasSettlType() => $_has(20);
  @$pb.TagNumber(22)
  void clearSettlType() => $_clearField(22);

  @$pb.TagNumber(23)
  $2.Timestamp get dateProcesor => $_getN(21);
  @$pb.TagNumber(23)
  set dateProcesor($2.Timestamp value) => $_setField(23, value);
  @$pb.TagNumber(23)
  $core.bool hasDateProcesor() => $_has(21);
  @$pb.TagNumber(23)
  void clearDateProcesor() => $_clearField(23);
  @$pb.TagNumber(23)
  $2.Timestamp ensureDateProcesor() => $_ensure(21);
}

class PositionHistory extends $pb.GeneratedMessage {
  factory PositionHistory({
    $core.String? instrument,
    $core.double? availableQuantity,
    $core.double? ph,
    $core.double? pm,
    $core.double? averagePurchasePrice,
    $core.double? purchaseAmount,
    $core.double? marketPrice,
    $core.double? marketValue,
    $core.double? priceVariation,
    $core.bool? simultaneous,
    $core.double? guarantee,
    $core.String? account,
    $core.double? garantia,
    $core.double? compraPlazo,
    $2.Timestamp? dateProcesor,
  }) {
    final result = create();
    if (instrument != null) result.instrument = instrument;
    if (availableQuantity != null) result.availableQuantity = availableQuantity;
    if (ph != null) result.ph = ph;
    if (pm != null) result.pm = pm;
    if (averagePurchasePrice != null)
      result.averagePurchasePrice = averagePurchasePrice;
    if (purchaseAmount != null) result.purchaseAmount = purchaseAmount;
    if (marketPrice != null) result.marketPrice = marketPrice;
    if (marketValue != null) result.marketValue = marketValue;
    if (priceVariation != null) result.priceVariation = priceVariation;
    if (simultaneous != null) result.simultaneous = simultaneous;
    if (guarantee != null) result.guarantee = guarantee;
    if (account != null) result.account = account;
    if (garantia != null) result.garantia = garantia;
    if (compraPlazo != null) result.compraPlazo = compraPlazo;
    if (dateProcesor != null) result.dateProcesor = dateProcesor;
    return result;
  }

  PositionHistory._();

  factory PositionHistory.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory PositionHistory.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PositionHistory',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'instrument')
    ..aD(2, _omitFieldNames ? '' : 'availableQuantity',
        protoName: 'availableQuantity')
    ..aD(3, _omitFieldNames ? '' : 'ph')
    ..aD(4, _omitFieldNames ? '' : 'pm')
    ..aD(5, _omitFieldNames ? '' : 'averagePurchasePrice',
        protoName: 'averagePurchasePrice')
    ..aD(6, _omitFieldNames ? '' : 'purchaseAmount',
        protoName: 'purchaseAmount')
    ..aD(7, _omitFieldNames ? '' : 'marketPrice', protoName: 'marketPrice')
    ..aD(8, _omitFieldNames ? '' : 'marketValue', protoName: 'marketValue')
    ..aD(9, _omitFieldNames ? '' : 'priceVariation',
        protoName: 'priceVariation')
    ..aOB(10, _omitFieldNames ? '' : 'simultaneous')
    ..aD(11, _omitFieldNames ? '' : 'guarantee')
    ..aOS(12, _omitFieldNames ? '' : 'account')
    ..aD(13, _omitFieldNames ? '' : 'garantia')
    ..aD(14, _omitFieldNames ? '' : 'compraPlazo', protoName: 'compraPlazo')
    ..aOM<$2.Timestamp>(15, _omitFieldNames ? '' : 'dateProcesor',
        subBuilder: $2.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PositionHistory clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PositionHistory copyWith(void Function(PositionHistory) updates) =>
      super.copyWith((message) => updates(message as PositionHistory))
          as PositionHistory;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static PositionHistory create() => PositionHistory._();
  @$core.override
  PositionHistory createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static PositionHistory getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PositionHistory>(create);
  static PositionHistory? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get instrument => $_getSZ(0);
  @$pb.TagNumber(1)
  set instrument($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasInstrument() => $_has(0);
  @$pb.TagNumber(1)
  void clearInstrument() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get availableQuantity => $_getN(1);
  @$pb.TagNumber(2)
  set availableQuantity($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAvailableQuantity() => $_has(1);
  @$pb.TagNumber(2)
  void clearAvailableQuantity() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get ph => $_getN(2);
  @$pb.TagNumber(3)
  set ph($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasPh() => $_has(2);
  @$pb.TagNumber(3)
  void clearPh() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get pm => $_getN(3);
  @$pb.TagNumber(4)
  set pm($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasPm() => $_has(3);
  @$pb.TagNumber(4)
  void clearPm() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.double get averagePurchasePrice => $_getN(4);
  @$pb.TagNumber(5)
  set averagePurchasePrice($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(5)
  $core.bool hasAveragePurchasePrice() => $_has(4);
  @$pb.TagNumber(5)
  void clearAveragePurchasePrice() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.double get purchaseAmount => $_getN(5);
  @$pb.TagNumber(6)
  set purchaseAmount($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(6)
  $core.bool hasPurchaseAmount() => $_has(5);
  @$pb.TagNumber(6)
  void clearPurchaseAmount() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.double get marketPrice => $_getN(6);
  @$pb.TagNumber(7)
  set marketPrice($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(7)
  $core.bool hasMarketPrice() => $_has(6);
  @$pb.TagNumber(7)
  void clearMarketPrice() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.double get marketValue => $_getN(7);
  @$pb.TagNumber(8)
  set marketValue($core.double value) => $_setDouble(7, value);
  @$pb.TagNumber(8)
  $core.bool hasMarketValue() => $_has(7);
  @$pb.TagNumber(8)
  void clearMarketValue() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.double get priceVariation => $_getN(8);
  @$pb.TagNumber(9)
  set priceVariation($core.double value) => $_setDouble(8, value);
  @$pb.TagNumber(9)
  $core.bool hasPriceVariation() => $_has(8);
  @$pb.TagNumber(9)
  void clearPriceVariation() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.bool get simultaneous => $_getBF(9);
  @$pb.TagNumber(10)
  set simultaneous($core.bool value) => $_setBool(9, value);
  @$pb.TagNumber(10)
  $core.bool hasSimultaneous() => $_has(9);
  @$pb.TagNumber(10)
  void clearSimultaneous() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.double get guarantee => $_getN(10);
  @$pb.TagNumber(11)
  set guarantee($core.double value) => $_setDouble(10, value);
  @$pb.TagNumber(11)
  $core.bool hasGuarantee() => $_has(10);
  @$pb.TagNumber(11)
  void clearGuarantee() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.String get account => $_getSZ(11);
  @$pb.TagNumber(12)
  set account($core.String value) => $_setString(11, value);
  @$pb.TagNumber(12)
  $core.bool hasAccount() => $_has(11);
  @$pb.TagNumber(12)
  void clearAccount() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.double get garantia => $_getN(12);
  @$pb.TagNumber(13)
  set garantia($core.double value) => $_setDouble(12, value);
  @$pb.TagNumber(13)
  $core.bool hasGarantia() => $_has(12);
  @$pb.TagNumber(13)
  void clearGarantia() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.double get compraPlazo => $_getN(13);
  @$pb.TagNumber(14)
  set compraPlazo($core.double value) => $_setDouble(13, value);
  @$pb.TagNumber(14)
  $core.bool hasCompraPlazo() => $_has(13);
  @$pb.TagNumber(14)
  void clearCompraPlazo() => $_clearField(14);

  @$pb.TagNumber(15)
  $2.Timestamp get dateProcesor => $_getN(14);
  @$pb.TagNumber(15)
  set dateProcesor($2.Timestamp value) => $_setField(15, value);
  @$pb.TagNumber(15)
  $core.bool hasDateProcesor() => $_has(14);
  @$pb.TagNumber(15)
  void clearDateProcesor() => $_clearField(15);
  @$pb.TagNumber(15)
  $2.Timestamp ensureDateProcesor() => $_ensure(14);
}

class Prestamos extends $pb.GeneratedMessage {
  factory Prestamos({
    $core.String? nemotecnico,
    $core.double? cantidadVigente,
    $core.double? precioAyer,
    $core.double? monto,
    $core.String? fechaIngreso,
    $core.String? fechaVto,
    $core.String? plazoPimo,
  }) {
    final result = create();
    if (nemotecnico != null) result.nemotecnico = nemotecnico;
    if (cantidadVigente != null) result.cantidadVigente = cantidadVigente;
    if (precioAyer != null) result.precioAyer = precioAyer;
    if (monto != null) result.monto = monto;
    if (fechaIngreso != null) result.fechaIngreso = fechaIngreso;
    if (fechaVto != null) result.fechaVto = fechaVto;
    if (plazoPimo != null) result.plazoPimo = plazoPimo;
    return result;
  }

  Prestamos._();

  factory Prestamos.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Prestamos.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Prestamos',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'nemotecnico')
    ..aD(2, _omitFieldNames ? '' : 'cantidadVigente')
    ..aD(3, _omitFieldNames ? '' : 'precioAyer')
    ..aD(4, _omitFieldNames ? '' : 'monto')
    ..aOS(5, _omitFieldNames ? '' : 'fechaIngreso')
    ..aOS(6, _omitFieldNames ? '' : 'fechaVto')
    ..aOS(7, _omitFieldNames ? '' : 'plazoPimo')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Prestamos clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Prestamos copyWith(void Function(Prestamos) updates) =>
      super.copyWith((message) => updates(message as Prestamos)) as Prestamos;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Prestamos create() => Prestamos._();
  @$core.override
  Prestamos createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Prestamos getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Prestamos>(create);
  static Prestamos? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get nemotecnico => $_getSZ(0);
  @$pb.TagNumber(1)
  set nemotecnico($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasNemotecnico() => $_has(0);
  @$pb.TagNumber(1)
  void clearNemotecnico() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get cantidadVigente => $_getN(1);
  @$pb.TagNumber(2)
  set cantidadVigente($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCantidadVigente() => $_has(1);
  @$pb.TagNumber(2)
  void clearCantidadVigente() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get precioAyer => $_getN(2);
  @$pb.TagNumber(3)
  set precioAyer($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasPrecioAyer() => $_has(2);
  @$pb.TagNumber(3)
  void clearPrecioAyer() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get monto => $_getN(3);
  @$pb.TagNumber(4)
  set monto($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMonto() => $_has(3);
  @$pb.TagNumber(4)
  void clearMonto() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get fechaIngreso => $_getSZ(4);
  @$pb.TagNumber(5)
  set fechaIngreso($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasFechaIngreso() => $_has(4);
  @$pb.TagNumber(5)
  void clearFechaIngreso() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get fechaVto => $_getSZ(5);
  @$pb.TagNumber(6)
  set fechaVto($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasFechaVto() => $_has(5);
  @$pb.TagNumber(6)
  void clearFechaVto() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get plazoPimo => $_getSZ(6);
  @$pb.TagNumber(7)
  set plazoPimo($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasPlazoPimo() => $_has(6);
  @$pb.TagNumber(7)
  void clearPlazoPimo() => $_clearField(7);
}

class Balance extends $pb.GeneratedMessage {
  factory Balance({
    $core.String? cuenta,
    $core.double? cartera,
    $core.double? cupo,
    $core.double? saldoDisponible,
    $core.double? garantiasConstituidas,
    $core.double? garantiasExigidas,
    $core.double? garantiasReservadas,
    $core.double? limiteFinanciero,
    $core.double? garantiasDisponible,
    $core.double? ordenesActivasCompras,
    $core.double? ordenesActivasVentas,
    $core.double? ordenesCalzadasCompras,
    $core.double? ordenesCalzadasVentas,
    $core.double? ordenesCestaCompras,
    $core.double? ordenesCestaVentas,
    $core.double? rendimiento,
    $core.double? total,
    $2.Timestamp? dateProcesor,
  }) {
    final result = create();
    if (cuenta != null) result.cuenta = cuenta;
    if (cartera != null) result.cartera = cartera;
    if (cupo != null) result.cupo = cupo;
    if (saldoDisponible != null) result.saldoDisponible = saldoDisponible;
    if (garantiasConstituidas != null)
      result.garantiasConstituidas = garantiasConstituidas;
    if (garantiasExigidas != null) result.garantiasExigidas = garantiasExigidas;
    if (garantiasReservadas != null)
      result.garantiasReservadas = garantiasReservadas;
    if (limiteFinanciero != null) result.limiteFinanciero = limiteFinanciero;
    if (garantiasDisponible != null)
      result.garantiasDisponible = garantiasDisponible;
    if (ordenesActivasCompras != null)
      result.ordenesActivasCompras = ordenesActivasCompras;
    if (ordenesActivasVentas != null)
      result.ordenesActivasVentas = ordenesActivasVentas;
    if (ordenesCalzadasCompras != null)
      result.ordenesCalzadasCompras = ordenesCalzadasCompras;
    if (ordenesCalzadasVentas != null)
      result.ordenesCalzadasVentas = ordenesCalzadasVentas;
    if (ordenesCestaCompras != null)
      result.ordenesCestaCompras = ordenesCestaCompras;
    if (ordenesCestaVentas != null)
      result.ordenesCestaVentas = ordenesCestaVentas;
    if (rendimiento != null) result.rendimiento = rendimiento;
    if (total != null) result.total = total;
    if (dateProcesor != null) result.dateProcesor = dateProcesor;
    return result;
  }

  Balance._();

  factory Balance.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Balance.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Balance',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'cuenta')
    ..aD(2, _omitFieldNames ? '' : 'cartera')
    ..aD(3, _omitFieldNames ? '' : 'cupo')
    ..aD(4, _omitFieldNames ? '' : 'saldoDisponible',
        protoName: 'saldoDisponible')
    ..aD(5, _omitFieldNames ? '' : 'garantiasConstituidas',
        protoName: 'garantiasConstituidas')
    ..aD(6, _omitFieldNames ? '' : 'garantiasExigidas',
        protoName: 'garantiasExigidas')
    ..aD(7, _omitFieldNames ? '' : 'garantiasReservadas',
        protoName: 'garantiasReservadas')
    ..aD(8, _omitFieldNames ? '' : 'limiteFinanciero',
        protoName: 'limiteFinanciero')
    ..aD(9, _omitFieldNames ? '' : 'garantiasDisponible',
        protoName: 'garantiasDisponible')
    ..aD(10, _omitFieldNames ? '' : 'ordenesActivasCompras',
        protoName: 'ordenesActivasCompras')
    ..aD(11, _omitFieldNames ? '' : 'ordenesActivasVentas',
        protoName: 'ordenesActivasVentas')
    ..aD(12, _omitFieldNames ? '' : 'ordenesCalzadasCompras',
        protoName: 'ordenesCalzadasCompras')
    ..aD(13, _omitFieldNames ? '' : 'ordenesCalzadasVentas',
        protoName: 'ordenesCalzadasVentas')
    ..aD(14, _omitFieldNames ? '' : 'ordenesCestaCompras',
        protoName: 'ordenesCestaCompras')
    ..aD(15, _omitFieldNames ? '' : 'ordenesCestaVentas',
        protoName: 'ordenesCestaVentas')
    ..aD(16, _omitFieldNames ? '' : 'rendimiento')
    ..aD(17, _omitFieldNames ? '' : 'total')
    ..aOM<$2.Timestamp>(18, _omitFieldNames ? '' : 'dateProcesor',
        subBuilder: $2.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Balance clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Balance copyWith(void Function(Balance) updates) =>
      super.copyWith((message) => updates(message as Balance)) as Balance;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Balance create() => Balance._();
  @$core.override
  Balance createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Balance getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Balance>(create);
  static Balance? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get cuenta => $_getSZ(0);
  @$pb.TagNumber(1)
  set cuenta($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCuenta() => $_has(0);
  @$pb.TagNumber(1)
  void clearCuenta() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get cartera => $_getN(1);
  @$pb.TagNumber(2)
  set cartera($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCartera() => $_has(1);
  @$pb.TagNumber(2)
  void clearCartera() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get cupo => $_getN(2);
  @$pb.TagNumber(3)
  set cupo($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCupo() => $_has(2);
  @$pb.TagNumber(3)
  void clearCupo() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get saldoDisponible => $_getN(3);
  @$pb.TagNumber(4)
  set saldoDisponible($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasSaldoDisponible() => $_has(3);
  @$pb.TagNumber(4)
  void clearSaldoDisponible() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.double get garantiasConstituidas => $_getN(4);
  @$pb.TagNumber(5)
  set garantiasConstituidas($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(5)
  $core.bool hasGarantiasConstituidas() => $_has(4);
  @$pb.TagNumber(5)
  void clearGarantiasConstituidas() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.double get garantiasExigidas => $_getN(5);
  @$pb.TagNumber(6)
  set garantiasExigidas($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(6)
  $core.bool hasGarantiasExigidas() => $_has(5);
  @$pb.TagNumber(6)
  void clearGarantiasExigidas() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.double get garantiasReservadas => $_getN(6);
  @$pb.TagNumber(7)
  set garantiasReservadas($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(7)
  $core.bool hasGarantiasReservadas() => $_has(6);
  @$pb.TagNumber(7)
  void clearGarantiasReservadas() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.double get limiteFinanciero => $_getN(7);
  @$pb.TagNumber(8)
  set limiteFinanciero($core.double value) => $_setDouble(7, value);
  @$pb.TagNumber(8)
  $core.bool hasLimiteFinanciero() => $_has(7);
  @$pb.TagNumber(8)
  void clearLimiteFinanciero() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.double get garantiasDisponible => $_getN(8);
  @$pb.TagNumber(9)
  set garantiasDisponible($core.double value) => $_setDouble(8, value);
  @$pb.TagNumber(9)
  $core.bool hasGarantiasDisponible() => $_has(8);
  @$pb.TagNumber(9)
  void clearGarantiasDisponible() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.double get ordenesActivasCompras => $_getN(9);
  @$pb.TagNumber(10)
  set ordenesActivasCompras($core.double value) => $_setDouble(9, value);
  @$pb.TagNumber(10)
  $core.bool hasOrdenesActivasCompras() => $_has(9);
  @$pb.TagNumber(10)
  void clearOrdenesActivasCompras() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.double get ordenesActivasVentas => $_getN(10);
  @$pb.TagNumber(11)
  set ordenesActivasVentas($core.double value) => $_setDouble(10, value);
  @$pb.TagNumber(11)
  $core.bool hasOrdenesActivasVentas() => $_has(10);
  @$pb.TagNumber(11)
  void clearOrdenesActivasVentas() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.double get ordenesCalzadasCompras => $_getN(11);
  @$pb.TagNumber(12)
  set ordenesCalzadasCompras($core.double value) => $_setDouble(11, value);
  @$pb.TagNumber(12)
  $core.bool hasOrdenesCalzadasCompras() => $_has(11);
  @$pb.TagNumber(12)
  void clearOrdenesCalzadasCompras() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.double get ordenesCalzadasVentas => $_getN(12);
  @$pb.TagNumber(13)
  set ordenesCalzadasVentas($core.double value) => $_setDouble(12, value);
  @$pb.TagNumber(13)
  $core.bool hasOrdenesCalzadasVentas() => $_has(12);
  @$pb.TagNumber(13)
  void clearOrdenesCalzadasVentas() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.double get ordenesCestaCompras => $_getN(13);
  @$pb.TagNumber(14)
  set ordenesCestaCompras($core.double value) => $_setDouble(13, value);
  @$pb.TagNumber(14)
  $core.bool hasOrdenesCestaCompras() => $_has(13);
  @$pb.TagNumber(14)
  void clearOrdenesCestaCompras() => $_clearField(14);

  @$pb.TagNumber(15)
  $core.double get ordenesCestaVentas => $_getN(14);
  @$pb.TagNumber(15)
  set ordenesCestaVentas($core.double value) => $_setDouble(14, value);
  @$pb.TagNumber(15)
  $core.bool hasOrdenesCestaVentas() => $_has(14);
  @$pb.TagNumber(15)
  void clearOrdenesCestaVentas() => $_clearField(15);

  @$pb.TagNumber(16)
  $core.double get rendimiento => $_getN(15);
  @$pb.TagNumber(16)
  set rendimiento($core.double value) => $_setDouble(15, value);
  @$pb.TagNumber(16)
  $core.bool hasRendimiento() => $_has(15);
  @$pb.TagNumber(16)
  void clearRendimiento() => $_clearField(16);

  @$pb.TagNumber(17)
  $core.double get total => $_getN(16);
  @$pb.TagNumber(17)
  set total($core.double value) => $_setDouble(16, value);
  @$pb.TagNumber(17)
  $core.bool hasTotal() => $_has(16);
  @$pb.TagNumber(17)
  void clearTotal() => $_clearField(17);

  @$pb.TagNumber(18)
  $2.Timestamp get dateProcesor => $_getN(17);
  @$pb.TagNumber(18)
  set dateProcesor($2.Timestamp value) => $_setField(18, value);
  @$pb.TagNumber(18)
  $core.bool hasDateProcesor() => $_has(17);
  @$pb.TagNumber(18)
  void clearDateProcesor() => $_clearField(18);
  @$pb.TagNumber(18)
  $2.Timestamp ensureDateProcesor() => $_ensure(17);
}

class SnapshotPositionHistory extends $pb.GeneratedMessage {
  factory SnapshotPositionHistory({
    $core.String? id,
    $core.Iterable<PositionHistory>? positionsHistory,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (positionsHistory != null)
      result.positionsHistory.addAll(positionsHistory);
    return result;
  }

  SnapshotPositionHistory._();

  factory SnapshotPositionHistory.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotPositionHistory.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotPositionHistory',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..pPM<PositionHistory>(2, _omitFieldNames ? '' : 'positionsHistory',
        protoName: 'positionsHistory', subBuilder: PositionHistory.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotPositionHistory clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotPositionHistory copyWith(
          void Function(SnapshotPositionHistory) updates) =>
      super.copyWith((message) => updates(message as SnapshotPositionHistory))
          as SnapshotPositionHistory;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotPositionHistory create() => SnapshotPositionHistory._();
  @$core.override
  SnapshotPositionHistory createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotPositionHistory getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotPositionHistory>(create);
  static SnapshotPositionHistory? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<PositionHistory> get positionsHistory => $_getList(1);
}

class PositionsTotals extends $pb.GeneratedMessage {
  factory PositionsTotals({
    $core.String? total,
    $core.double? buy,
    $core.double? sell,
    $core.double? net,
  }) {
    final result = create();
    if (total != null) result.total = total;
    if (buy != null) result.buy = buy;
    if (sell != null) result.sell = sell;
    if (net != null) result.net = net;
    return result;
  }

  PositionsTotals._();

  factory PositionsTotals.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory PositionsTotals.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PositionsTotals',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'total')
    ..aD(2, _omitFieldNames ? '' : 'buy')
    ..aD(3, _omitFieldNames ? '' : 'sell')
    ..aD(4, _omitFieldNames ? '' : 'net')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PositionsTotals clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PositionsTotals copyWith(void Function(PositionsTotals) updates) =>
      super.copyWith((message) => updates(message as PositionsTotals))
          as PositionsTotals;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static PositionsTotals create() => PositionsTotals._();
  @$core.override
  PositionsTotals createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static PositionsTotals getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PositionsTotals>(create);
  static PositionsTotals? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get total => $_getSZ(0);
  @$pb.TagNumber(1)
  set total($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTotal() => $_has(0);
  @$pb.TagNumber(1)
  void clearTotal() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get buy => $_getN(1);
  @$pb.TagNumber(2)
  set buy($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBuy() => $_has(1);
  @$pb.TagNumber(2)
  void clearBuy() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get sell => $_getN(2);
  @$pb.TagNumber(3)
  set sell($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSell() => $_has(2);
  @$pb.TagNumber(3)
  void clearSell() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get net => $_getN(3);
  @$pb.TagNumber(4)
  set net($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasNet() => $_has(3);
  @$pb.TagNumber(4)
  void clearNet() => $_clearField(4);
}

class TradesTotals extends $pb.GeneratedMessage {
  factory TradesTotals({
    $core.double? tlTotal,
    $core.double? tlTotalBuy,
    $core.double? tcTotalBuy,
    $core.double? tlTotalSell,
    $core.double? tcTotalSell,
    $core.double? tlTotalAmount,
    $core.double? tcTotalAmount,
  }) {
    final result = create();
    if (tlTotal != null) result.tlTotal = tlTotal;
    if (tlTotalBuy != null) result.tlTotalBuy = tlTotalBuy;
    if (tcTotalBuy != null) result.tcTotalBuy = tcTotalBuy;
    if (tlTotalSell != null) result.tlTotalSell = tlTotalSell;
    if (tcTotalSell != null) result.tcTotalSell = tcTotalSell;
    if (tlTotalAmount != null) result.tlTotalAmount = tlTotalAmount;
    if (tcTotalAmount != null) result.tcTotalAmount = tcTotalAmount;
    return result;
  }

  TradesTotals._();

  factory TradesTotals.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory TradesTotals.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TradesTotals',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aD(1, _omitFieldNames ? '' : 'tlTotal', protoName: 'tlTotal')
    ..aD(2, _omitFieldNames ? '' : 'tlTotalBuy', protoName: 'tlTotalBuy')
    ..aD(3, _omitFieldNames ? '' : 'tcTotalBuy', protoName: 'tcTotalBuy')
    ..aD(4, _omitFieldNames ? '' : 'tlTotalSell', protoName: 'tlTotalSell')
    ..aD(5, _omitFieldNames ? '' : 'tcTotalSell', protoName: 'tcTotalSell')
    ..aD(6, _omitFieldNames ? '' : 'tlTotalAmount', protoName: 'tlTotalAmount')
    ..aD(7, _omitFieldNames ? '' : 'tcTotalAmount', protoName: 'tcTotalAmount')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TradesTotals clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TradesTotals copyWith(void Function(TradesTotals) updates) =>
      super.copyWith((message) => updates(message as TradesTotals))
          as TradesTotals;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TradesTotals create() => TradesTotals._();
  @$core.override
  TradesTotals createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static TradesTotals getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<TradesTotals>(create);
  static TradesTotals? _defaultInstance;

  @$pb.TagNumber(1)
  $core.double get tlTotal => $_getN(0);
  @$pb.TagNumber(1)
  set tlTotal($core.double value) => $_setDouble(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTlTotal() => $_has(0);
  @$pb.TagNumber(1)
  void clearTlTotal() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get tlTotalBuy => $_getN(1);
  @$pb.TagNumber(2)
  set tlTotalBuy($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasTlTotalBuy() => $_has(1);
  @$pb.TagNumber(2)
  void clearTlTotalBuy() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.double get tcTotalBuy => $_getN(2);
  @$pb.TagNumber(3)
  set tcTotalBuy($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTcTotalBuy() => $_has(2);
  @$pb.TagNumber(3)
  void clearTcTotalBuy() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get tlTotalSell => $_getN(3);
  @$pb.TagNumber(4)
  set tlTotalSell($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasTlTotalSell() => $_has(3);
  @$pb.TagNumber(4)
  void clearTlTotalSell() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.double get tcTotalSell => $_getN(4);
  @$pb.TagNumber(5)
  set tcTotalSell($core.double value) => $_setDouble(4, value);
  @$pb.TagNumber(5)
  $core.bool hasTcTotalSell() => $_has(4);
  @$pb.TagNumber(5)
  void clearTcTotalSell() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.double get tlTotalAmount => $_getN(5);
  @$pb.TagNumber(6)
  set tlTotalAmount($core.double value) => $_setDouble(5, value);
  @$pb.TagNumber(6)
  $core.bool hasTlTotalAmount() => $_has(5);
  @$pb.TagNumber(6)
  void clearTlTotalAmount() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.double get tcTotalAmount => $_getN(6);
  @$pb.TagNumber(7)
  set tcTotalAmount($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(7)
  $core.bool hasTcTotalAmount() => $_has(6);
  @$pb.TagNumber(7)
  void clearTcTotalAmount() => $_clearField(7);
}

class SnapshotOrders extends $pb.GeneratedMessage {
  factory SnapshotOrders({
    $core.String? id,
    $core.Iterable<$0.Order>? orders,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (orders != null) result.orders.addAll(orders);
    return result;
  }

  SnapshotOrders._();

  factory SnapshotOrders.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotOrders.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotOrders',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..pPM<$0.Order>(2, _omitFieldNames ? '' : 'orders',
        subBuilder: $0.Order.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotOrders clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotOrders copyWith(void Function(SnapshotOrders) updates) =>
      super.copyWith((message) => updates(message as SnapshotOrders))
          as SnapshotOrders;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotOrders create() => SnapshotOrders._();
  @$core.override
  SnapshotOrders createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotOrders getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotOrders>(create);
  static SnapshotOrders? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<$0.Order> get orders => $_getList(1);
}

class SnapshotTrades extends $pb.GeneratedMessage {
  factory SnapshotTrades({
    $core.Iterable<$0.Order>? trades,
    $core.String? id,
  }) {
    final result = create();
    if (trades != null) result.trades.addAll(trades);
    if (id != null) result.id = id;
    return result;
  }

  SnapshotTrades._();

  factory SnapshotTrades.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotTrades.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotTrades',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<$0.Order>(1, _omitFieldNames ? '' : 'trades',
        subBuilder: $0.Order.create)
    ..aOS(2, _omitFieldNames ? '' : 'id')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotTrades clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotTrades copyWith(void Function(SnapshotTrades) updates) =>
      super.copyWith((message) => updates(message as SnapshotTrades))
          as SnapshotTrades;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotTrades create() => SnapshotTrades._();
  @$core.override
  SnapshotTrades createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotTrades getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotTrades>(create);
  static SnapshotTrades? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<$0.Order> get trades => $_getList(0);

  @$pb.TagNumber(2)
  $core.String get id => $_getSZ(1);
  @$pb.TagNumber(2)
  set id($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasId() => $_has(1);
  @$pb.TagNumber(2)
  void clearId() => $_clearField(2);
}

class SnapshotPositions extends $pb.GeneratedMessage {
  factory SnapshotPositions({
    $core.String? id,
    $core.Iterable<Position>? positions,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (positions != null) result.positions.addAll(positions);
    return result;
  }

  SnapshotPositions._();

  factory SnapshotPositions.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotPositions.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotPositions',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(2, _omitFieldNames ? '' : 'id')
    ..pPM<Position>(3, _omitFieldNames ? '' : 'positions',
        subBuilder: Position.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotPositions clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotPositions copyWith(void Function(SnapshotPositions) updates) =>
      super.copyWith((message) => updates(message as SnapshotPositions))
          as SnapshotPositions;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotPositions create() => SnapshotPositions._();
  @$core.override
  SnapshotPositions createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotPositions getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotPositions>(create);
  static SnapshotPositions? _defaultInstance;

  @$pb.TagNumber(2)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(2)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(2)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(2)
  void clearId() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<Position> get positions => $_getList(1);
}

class SnapshotRequest extends $pb.GeneratedMessage {
  factory SnapshotRequest({
    $core.String? producer,
    $core.String? operator,
  }) {
    final result = create();
    if (producer != null) result.producer = producer;
    if (operator != null) result.operator = operator;
    return result;
  }

  SnapshotRequest._();

  factory SnapshotRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'producer')
    ..aOS(2, _omitFieldNames ? '' : 'operator')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotRequest copyWith(void Function(SnapshotRequest) updates) =>
      super.copyWith((message) => updates(message as SnapshotRequest))
          as SnapshotRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotRequest create() => SnapshotRequest._();
  @$core.override
  SnapshotRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotRequest>(create);
  static SnapshotRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get producer => $_getSZ(0);
  @$pb.TagNumber(1)
  set producer($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasProducer() => $_has(0);
  @$pb.TagNumber(1)
  void clearProducer() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get operator => $_getSZ(1);
  @$pb.TagNumber(2)
  set operator($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOperator() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperator() => $_clearField(2);
}

class OrderTickRequest extends $pb.GeneratedMessage {
  factory OrderTickRequest({
    $0.Order? order,
    TickAction? action,
    $core.int? ticks,
  }) {
    final result = create();
    if (order != null) result.order = order;
    if (action != null) result.action = action;
    if (ticks != null) result.ticks = ticks;
    return result;
  }

  OrderTickRequest._();

  factory OrderTickRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory OrderTickRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OrderTickRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<$0.Order>(1, _omitFieldNames ? '' : 'order',
        subBuilder: $0.Order.create)
    ..aE<TickAction>(2, _omitFieldNames ? '' : 'action',
        enumValues: TickAction.values)
    ..aI(3, _omitFieldNames ? '' : 'ticks', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderTickRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OrderTickRequest copyWith(void Function(OrderTickRequest) updates) =>
      super.copyWith((message) => updates(message as OrderTickRequest))
          as OrderTickRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static OrderTickRequest create() => OrderTickRequest._();
  @$core.override
  OrderTickRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static OrderTickRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<OrderTickRequest>(create);
  static OrderTickRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $0.Order get order => $_getN(0);
  @$pb.TagNumber(1)
  set order($0.Order value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOrder() => $_has(0);
  @$pb.TagNumber(1)
  void clearOrder() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.Order ensureOrder() => $_ensure(0);

  @$pb.TagNumber(2)
  TickAction get action => $_getN(1);
  @$pb.TagNumber(2)
  set action(TickAction value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAction() => $_has(1);
  @$pb.TagNumber(2)
  void clearAction() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.int get ticks => $_getIZ(2);
  @$pb.TagNumber(3)
  set ticks($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTicks() => $_has(2);
  @$pb.TagNumber(3)
  void clearTicks() => $_clearField(3);
}

class StrategyColor extends $pb.GeneratedMessage {
  factory StrategyColor({
    $core.String? id,
    $core.String? name,
    $core.String? prefix,
    $core.String? buyColor,
    $core.String? sellColor,
    $core.String? updateBy,
    $core.String? lastUpdate,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (name != null) result.name = name;
    if (prefix != null) result.prefix = prefix;
    if (buyColor != null) result.buyColor = buyColor;
    if (sellColor != null) result.sellColor = sellColor;
    if (updateBy != null) result.updateBy = updateBy;
    if (lastUpdate != null) result.lastUpdate = lastUpdate;
    return result;
  }

  StrategyColor._();

  factory StrategyColor.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory StrategyColor.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'StrategyColor',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'name')
    ..aOS(3, _omitFieldNames ? '' : 'prefix')
    ..aOS(4, _omitFieldNames ? '' : 'buyColor', protoName: 'buyColor')
    ..aOS(5, _omitFieldNames ? '' : 'sellColor', protoName: 'sellColor')
    ..aOS(6, _omitFieldNames ? '' : 'updateBy', protoName: 'updateBy')
    ..aOS(7, _omitFieldNames ? '' : 'lastUpdate', protoName: 'lastUpdate')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StrategyColor clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StrategyColor copyWith(void Function(StrategyColor) updates) =>
      super.copyWith((message) => updates(message as StrategyColor))
          as StrategyColor;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static StrategyColor create() => StrategyColor._();
  @$core.override
  StrategyColor createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static StrategyColor getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<StrategyColor>(create);
  static StrategyColor? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get prefix => $_getSZ(2);
  @$pb.TagNumber(3)
  set prefix($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasPrefix() => $_has(2);
  @$pb.TagNumber(3)
  void clearPrefix() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get buyColor => $_getSZ(3);
  @$pb.TagNumber(4)
  set buyColor($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasBuyColor() => $_has(3);
  @$pb.TagNumber(4)
  void clearBuyColor() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get sellColor => $_getSZ(4);
  @$pb.TagNumber(5)
  set sellColor($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasSellColor() => $_has(4);
  @$pb.TagNumber(5)
  void clearSellColor() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get updateBy => $_getSZ(5);
  @$pb.TagNumber(6)
  set updateBy($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasUpdateBy() => $_has(5);
  @$pb.TagNumber(6)
  void clearUpdateBy() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get lastUpdate => $_getSZ(6);
  @$pb.TagNumber(7)
  set lastUpdate($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasLastUpdate() => $_has(6);
  @$pb.TagNumber(7)
  void clearLastUpdate() => $_clearField(7);
}

class Strategy extends $pb.GeneratedMessage {
  factory Strategy({
    $core.String? id,
    $core.String? name,
    $core.String? prefix,
    $core.String? buyColor,
    $core.String? sellColor,
    $core.String? updateBy,
    $core.String? lastUpdate,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (name != null) result.name = name;
    if (prefix != null) result.prefix = prefix;
    if (buyColor != null) result.buyColor = buyColor;
    if (sellColor != null) result.sellColor = sellColor;
    if (updateBy != null) result.updateBy = updateBy;
    if (lastUpdate != null) result.lastUpdate = lastUpdate;
    return result;
  }

  Strategy._();

  factory Strategy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Strategy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Strategy',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'name')
    ..aOS(3, _omitFieldNames ? '' : 'prefix')
    ..aOS(4, _omitFieldNames ? '' : 'buyColor', protoName: 'buyColor')
    ..aOS(5, _omitFieldNames ? '' : 'sellColor', protoName: 'sellColor')
    ..aOS(6, _omitFieldNames ? '' : 'updateBy', protoName: 'updateBy')
    ..aOS(7, _omitFieldNames ? '' : 'lastUpdate', protoName: 'lastUpdate')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Strategy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Strategy copyWith(void Function(Strategy) updates) =>
      super.copyWith((message) => updates(message as Strategy)) as Strategy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Strategy create() => Strategy._();
  @$core.override
  Strategy createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Strategy getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Strategy>(create);
  static Strategy? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get prefix => $_getSZ(2);
  @$pb.TagNumber(3)
  set prefix($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasPrefix() => $_has(2);
  @$pb.TagNumber(3)
  void clearPrefix() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get buyColor => $_getSZ(3);
  @$pb.TagNumber(4)
  set buyColor($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasBuyColor() => $_has(3);
  @$pb.TagNumber(4)
  void clearBuyColor() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get sellColor => $_getSZ(4);
  @$pb.TagNumber(5)
  set sellColor($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasSellColor() => $_has(4);
  @$pb.TagNumber(5)
  void clearSellColor() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get updateBy => $_getSZ(5);
  @$pb.TagNumber(6)
  set updateBy($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasUpdateBy() => $_has(5);
  @$pb.TagNumber(6)
  void clearUpdateBy() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get lastUpdate => $_getSZ(6);
  @$pb.TagNumber(7)
  set lastUpdate($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasLastUpdate() => $_has(6);
  @$pb.TagNumber(7)
  void clearLastUpdate() => $_clearField(7);
}

class UpdateStrategy extends $pb.GeneratedMessage {
  factory UpdateStrategy({
    Strategy? strategy,
  }) {
    final result = create();
    if (strategy != null) result.strategy = strategy;
    return result;
  }

  UpdateStrategy._();

  factory UpdateStrategy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory UpdateStrategy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UpdateStrategy',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<Strategy>(1, _omitFieldNames ? '' : 'strategy',
        subBuilder: Strategy.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UpdateStrategy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UpdateStrategy copyWith(void Function(UpdateStrategy) updates) =>
      super.copyWith((message) => updates(message as UpdateStrategy))
          as UpdateStrategy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static UpdateStrategy create() => UpdateStrategy._();
  @$core.override
  UpdateStrategy createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static UpdateStrategy getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<UpdateStrategy>(create);
  static UpdateStrategy? _defaultInstance;

  @$pb.TagNumber(1)
  Strategy get strategy => $_getN(0);
  @$pb.TagNumber(1)
  set strategy(Strategy value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStrategy() => $_has(0);
  @$pb.TagNumber(1)
  void clearStrategy() => $_clearField(1);
  @$pb.TagNumber(1)
  Strategy ensureStrategy() => $_ensure(0);
}

class DeleteStrategy extends $pb.GeneratedMessage {
  factory DeleteStrategy({
    Strategy? strategy,
  }) {
    final result = create();
    if (strategy != null) result.strategy = strategy;
    return result;
  }

  DeleteStrategy._();

  factory DeleteStrategy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory DeleteStrategy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DeleteStrategy',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<Strategy>(1, _omitFieldNames ? '' : 'strategy',
        subBuilder: Strategy.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteStrategy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteStrategy copyWith(void Function(DeleteStrategy) updates) =>
      super.copyWith((message) => updates(message as DeleteStrategy))
          as DeleteStrategy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static DeleteStrategy create() => DeleteStrategy._();
  @$core.override
  DeleteStrategy createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static DeleteStrategy getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DeleteStrategy>(create);
  static DeleteStrategy? _defaultInstance;

  @$pb.TagNumber(1)
  Strategy get strategy => $_getN(0);
  @$pb.TagNumber(1)
  set strategy(Strategy value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStrategy() => $_has(0);
  @$pb.TagNumber(1)
  void clearStrategy() => $_clearField(1);
  @$pb.TagNumber(1)
  Strategy ensureStrategy() => $_ensure(0);
}

class AddStrategyLVOMS extends $pb.GeneratedMessage {
  factory AddStrategyLVOMS({
    Strategy? strategy,
  }) {
    final result = create();
    if (strategy != null) result.strategy = strategy;
    return result;
  }

  AddStrategyLVOMS._();

  factory AddStrategyLVOMS.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory AddStrategyLVOMS.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AddStrategyLVOMS',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<Strategy>(1, _omitFieldNames ? '' : 'strategy',
        subBuilder: Strategy.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AddStrategyLVOMS clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AddStrategyLVOMS copyWith(void Function(AddStrategyLVOMS) updates) =>
      super.copyWith((message) => updates(message as AddStrategyLVOMS))
          as AddStrategyLVOMS;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static AddStrategyLVOMS create() => AddStrategyLVOMS._();
  @$core.override
  AddStrategyLVOMS createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static AddStrategyLVOMS getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<AddStrategyLVOMS>(create);
  static AddStrategyLVOMS? _defaultInstance;

  @$pb.TagNumber(1)
  Strategy get strategy => $_getN(0);
  @$pb.TagNumber(1)
  set strategy(Strategy value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStrategy() => $_has(0);
  @$pb.TagNumber(1)
  void clearStrategy() => $_clearField(1);
  @$pb.TagNumber(1)
  Strategy ensureStrategy() => $_ensure(0);
}

class CierreCarteraResumida extends $pb.GeneratedMessage {
  factory CierreCarteraResumida({
    $core.String? idAsesor,
    $2.Timestamp? fechaCierre,
    $core.double? precioTasaMercado,
    $core.double? precioTasaCompra,
    $core.String? numCuenta,
    $core.String? nombreCliente,
    $core.double? libre,
    $core.double? garantia,
    $core.double? comprasPlazo,
    $core.double? ventasPlazo,
    $core.double? prestamosAcc,
    $core.String? identificador,
    $core.String? nemotecNico,
    $core.String? nombreAsesor,
    $core.double? libreClp,
    $core.double? garantiaClp,
    $core.double? simCompraClp,
    $core.double? simVentaClp,
    $core.double? valorMercadoClp,
    $core.String? codSubClaseInstrumento,
    $core.String? codigoMoneda,
    $2.Timestamp? dateProcesor,
  }) {
    final result = create();
    if (idAsesor != null) result.idAsesor = idAsesor;
    if (fechaCierre != null) result.fechaCierre = fechaCierre;
    if (precioTasaMercado != null) result.precioTasaMercado = precioTasaMercado;
    if (precioTasaCompra != null) result.precioTasaCompra = precioTasaCompra;
    if (numCuenta != null) result.numCuenta = numCuenta;
    if (nombreCliente != null) result.nombreCliente = nombreCliente;
    if (libre != null) result.libre = libre;
    if (garantia != null) result.garantia = garantia;
    if (comprasPlazo != null) result.comprasPlazo = comprasPlazo;
    if (ventasPlazo != null) result.ventasPlazo = ventasPlazo;
    if (prestamosAcc != null) result.prestamosAcc = prestamosAcc;
    if (identificador != null) result.identificador = identificador;
    if (nemotecNico != null) result.nemotecNico = nemotecNico;
    if (nombreAsesor != null) result.nombreAsesor = nombreAsesor;
    if (libreClp != null) result.libreClp = libreClp;
    if (garantiaClp != null) result.garantiaClp = garantiaClp;
    if (simCompraClp != null) result.simCompraClp = simCompraClp;
    if (simVentaClp != null) result.simVentaClp = simVentaClp;
    if (valorMercadoClp != null) result.valorMercadoClp = valorMercadoClp;
    if (codSubClaseInstrumento != null)
      result.codSubClaseInstrumento = codSubClaseInstrumento;
    if (codigoMoneda != null) result.codigoMoneda = codigoMoneda;
    if (dateProcesor != null) result.dateProcesor = dateProcesor;
    return result;
  }

  CierreCarteraResumida._();

  factory CierreCarteraResumida.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory CierreCarteraResumida.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CierreCarteraResumida',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'idAsesor')
    ..aOM<$2.Timestamp>(2, _omitFieldNames ? '' : 'fechaCierre',
        subBuilder: $2.Timestamp.create)
    ..aD(3, _omitFieldNames ? '' : 'precioTasaMercado')
    ..aD(4, _omitFieldNames ? '' : 'precioTasaCompra')
    ..aOS(5, _omitFieldNames ? '' : 'numCuenta')
    ..aOS(6, _omitFieldNames ? '' : 'nombreCliente')
    ..aD(7, _omitFieldNames ? '' : 'libre')
    ..aD(8, _omitFieldNames ? '' : 'garantia')
    ..aD(9, _omitFieldNames ? '' : 'comprasPlazo')
    ..aD(10, _omitFieldNames ? '' : 'ventasPlazo')
    ..aD(11, _omitFieldNames ? '' : 'prestamosAcc')
    ..aOS(12, _omitFieldNames ? '' : 'identificador')
    ..aOS(13, _omitFieldNames ? '' : 'nemotecNico')
    ..aOS(14, _omitFieldNames ? '' : 'nombreAsesor')
    ..aD(15, _omitFieldNames ? '' : 'libreClp')
    ..aD(16, _omitFieldNames ? '' : 'garantiaClp')
    ..aD(17, _omitFieldNames ? '' : 'simCompraClp')
    ..aD(18, _omitFieldNames ? '' : 'simVentaClp')
    ..aD(19, _omitFieldNames ? '' : 'valorMercadoClp')
    ..aOS(20, _omitFieldNames ? '' : 'codSubClaseInstrumento')
    ..aOS(21, _omitFieldNames ? '' : 'codigoMoneda', protoName: 'codigoMoneda')
    ..aOM<$2.Timestamp>(22, _omitFieldNames ? '' : 'dateProcesor',
        subBuilder: $2.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CierreCarteraResumida clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CierreCarteraResumida copyWith(
          void Function(CierreCarteraResumida) updates) =>
      super.copyWith((message) => updates(message as CierreCarteraResumida))
          as CierreCarteraResumida;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static CierreCarteraResumida create() => CierreCarteraResumida._();
  @$core.override
  CierreCarteraResumida createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static CierreCarteraResumida getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CierreCarteraResumida>(create);
  static CierreCarteraResumida? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get idAsesor => $_getSZ(0);
  @$pb.TagNumber(1)
  set idAsesor($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdAsesor() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdAsesor() => $_clearField(1);

  @$pb.TagNumber(2)
  $2.Timestamp get fechaCierre => $_getN(1);
  @$pb.TagNumber(2)
  set fechaCierre($2.Timestamp value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasFechaCierre() => $_has(1);
  @$pb.TagNumber(2)
  void clearFechaCierre() => $_clearField(2);
  @$pb.TagNumber(2)
  $2.Timestamp ensureFechaCierre() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.double get precioTasaMercado => $_getN(2);
  @$pb.TagNumber(3)
  set precioTasaMercado($core.double value) => $_setDouble(2, value);
  @$pb.TagNumber(3)
  $core.bool hasPrecioTasaMercado() => $_has(2);
  @$pb.TagNumber(3)
  void clearPrecioTasaMercado() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.double get precioTasaCompra => $_getN(3);
  @$pb.TagNumber(4)
  set precioTasaCompra($core.double value) => $_setDouble(3, value);
  @$pb.TagNumber(4)
  $core.bool hasPrecioTasaCompra() => $_has(3);
  @$pb.TagNumber(4)
  void clearPrecioTasaCompra() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get numCuenta => $_getSZ(4);
  @$pb.TagNumber(5)
  set numCuenta($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasNumCuenta() => $_has(4);
  @$pb.TagNumber(5)
  void clearNumCuenta() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get nombreCliente => $_getSZ(5);
  @$pb.TagNumber(6)
  set nombreCliente($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasNombreCliente() => $_has(5);
  @$pb.TagNumber(6)
  void clearNombreCliente() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.double get libre => $_getN(6);
  @$pb.TagNumber(7)
  set libre($core.double value) => $_setDouble(6, value);
  @$pb.TagNumber(7)
  $core.bool hasLibre() => $_has(6);
  @$pb.TagNumber(7)
  void clearLibre() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.double get garantia => $_getN(7);
  @$pb.TagNumber(8)
  set garantia($core.double value) => $_setDouble(7, value);
  @$pb.TagNumber(8)
  $core.bool hasGarantia() => $_has(7);
  @$pb.TagNumber(8)
  void clearGarantia() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.double get comprasPlazo => $_getN(8);
  @$pb.TagNumber(9)
  set comprasPlazo($core.double value) => $_setDouble(8, value);
  @$pb.TagNumber(9)
  $core.bool hasComprasPlazo() => $_has(8);
  @$pb.TagNumber(9)
  void clearComprasPlazo() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.double get ventasPlazo => $_getN(9);
  @$pb.TagNumber(10)
  set ventasPlazo($core.double value) => $_setDouble(9, value);
  @$pb.TagNumber(10)
  $core.bool hasVentasPlazo() => $_has(9);
  @$pb.TagNumber(10)
  void clearVentasPlazo() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.double get prestamosAcc => $_getN(10);
  @$pb.TagNumber(11)
  set prestamosAcc($core.double value) => $_setDouble(10, value);
  @$pb.TagNumber(11)
  $core.bool hasPrestamosAcc() => $_has(10);
  @$pb.TagNumber(11)
  void clearPrestamosAcc() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.String get identificador => $_getSZ(11);
  @$pb.TagNumber(12)
  set identificador($core.String value) => $_setString(11, value);
  @$pb.TagNumber(12)
  $core.bool hasIdentificador() => $_has(11);
  @$pb.TagNumber(12)
  void clearIdentificador() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.String get nemotecNico => $_getSZ(12);
  @$pb.TagNumber(13)
  set nemotecNico($core.String value) => $_setString(12, value);
  @$pb.TagNumber(13)
  $core.bool hasNemotecNico() => $_has(12);
  @$pb.TagNumber(13)
  void clearNemotecNico() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.String get nombreAsesor => $_getSZ(13);
  @$pb.TagNumber(14)
  set nombreAsesor($core.String value) => $_setString(13, value);
  @$pb.TagNumber(14)
  $core.bool hasNombreAsesor() => $_has(13);
  @$pb.TagNumber(14)
  void clearNombreAsesor() => $_clearField(14);

  @$pb.TagNumber(15)
  $core.double get libreClp => $_getN(14);
  @$pb.TagNumber(15)
  set libreClp($core.double value) => $_setDouble(14, value);
  @$pb.TagNumber(15)
  $core.bool hasLibreClp() => $_has(14);
  @$pb.TagNumber(15)
  void clearLibreClp() => $_clearField(15);

  @$pb.TagNumber(16)
  $core.double get garantiaClp => $_getN(15);
  @$pb.TagNumber(16)
  set garantiaClp($core.double value) => $_setDouble(15, value);
  @$pb.TagNumber(16)
  $core.bool hasGarantiaClp() => $_has(15);
  @$pb.TagNumber(16)
  void clearGarantiaClp() => $_clearField(16);

  @$pb.TagNumber(17)
  $core.double get simCompraClp => $_getN(16);
  @$pb.TagNumber(17)
  set simCompraClp($core.double value) => $_setDouble(16, value);
  @$pb.TagNumber(17)
  $core.bool hasSimCompraClp() => $_has(16);
  @$pb.TagNumber(17)
  void clearSimCompraClp() => $_clearField(17);

  @$pb.TagNumber(18)
  $core.double get simVentaClp => $_getN(17);
  @$pb.TagNumber(18)
  set simVentaClp($core.double value) => $_setDouble(17, value);
  @$pb.TagNumber(18)
  $core.bool hasSimVentaClp() => $_has(17);
  @$pb.TagNumber(18)
  void clearSimVentaClp() => $_clearField(18);

  @$pb.TagNumber(19)
  $core.double get valorMercadoClp => $_getN(18);
  @$pb.TagNumber(19)
  set valorMercadoClp($core.double value) => $_setDouble(18, value);
  @$pb.TagNumber(19)
  $core.bool hasValorMercadoClp() => $_has(18);
  @$pb.TagNumber(19)
  void clearValorMercadoClp() => $_clearField(19);

  @$pb.TagNumber(20)
  $core.String get codSubClaseInstrumento => $_getSZ(19);
  @$pb.TagNumber(20)
  set codSubClaseInstrumento($core.String value) => $_setString(19, value);
  @$pb.TagNumber(20)
  $core.bool hasCodSubClaseInstrumento() => $_has(19);
  @$pb.TagNumber(20)
  void clearCodSubClaseInstrumento() => $_clearField(20);

  @$pb.TagNumber(21)
  $core.String get codigoMoneda => $_getSZ(20);
  @$pb.TagNumber(21)
  set codigoMoneda($core.String value) => $_setString(20, value);
  @$pb.TagNumber(21)
  $core.bool hasCodigoMoneda() => $_has(20);
  @$pb.TagNumber(21)
  void clearCodigoMoneda() => $_clearField(21);

  @$pb.TagNumber(22)
  $2.Timestamp get dateProcesor => $_getN(21);
  @$pb.TagNumber(22)
  set dateProcesor($2.Timestamp value) => $_setField(22, value);
  @$pb.TagNumber(22)
  $core.bool hasDateProcesor() => $_has(21);
  @$pb.TagNumber(22)
  void clearDateProcesor() => $_clearField(22);
  @$pb.TagNumber(22)
  $2.Timestamp ensureDateProcesor() => $_ensure(21);
}

class SaldoCaja extends $pb.GeneratedMessage {
  factory SaldoCaja({
    $core.String? identificador,
    $core.String? idCuenta,
    $core.String? numCuenta,
    $core.String? descripcionCuenta,
    $2.Timestamp? fechaCierre,
    $core.String? estadoSaldoCaja,
    $core.String? tipoCaja,
    $core.String? montoMonedaCaja,
    $core.String? montoEnPesos,
    $core.String? montoPorCobrarMonedaCuenta,
    $core.String? montoPorPagarMonedaCuenta,
    $core.String? montoTransito,
    $core.String? garantiaEfectivo,
    $core.String? prestamo,
    $2.Timestamp? dateProcesor,
  }) {
    final result = create();
    if (identificador != null) result.identificador = identificador;
    if (idCuenta != null) result.idCuenta = idCuenta;
    if (numCuenta != null) result.numCuenta = numCuenta;
    if (descripcionCuenta != null) result.descripcionCuenta = descripcionCuenta;
    if (fechaCierre != null) result.fechaCierre = fechaCierre;
    if (estadoSaldoCaja != null) result.estadoSaldoCaja = estadoSaldoCaja;
    if (tipoCaja != null) result.tipoCaja = tipoCaja;
    if (montoMonedaCaja != null) result.montoMonedaCaja = montoMonedaCaja;
    if (montoEnPesos != null) result.montoEnPesos = montoEnPesos;
    if (montoPorCobrarMonedaCuenta != null)
      result.montoPorCobrarMonedaCuenta = montoPorCobrarMonedaCuenta;
    if (montoPorPagarMonedaCuenta != null)
      result.montoPorPagarMonedaCuenta = montoPorPagarMonedaCuenta;
    if (montoTransito != null) result.montoTransito = montoTransito;
    if (garantiaEfectivo != null) result.garantiaEfectivo = garantiaEfectivo;
    if (prestamo != null) result.prestamo = prestamo;
    if (dateProcesor != null) result.dateProcesor = dateProcesor;
    return result;
  }

  SaldoCaja._();

  factory SaldoCaja.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SaldoCaja.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SaldoCaja',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'identificador')
    ..aOS(2, _omitFieldNames ? '' : 'idCuenta')
    ..aOS(3, _omitFieldNames ? '' : 'numCuenta')
    ..aOS(4, _omitFieldNames ? '' : 'descripcionCuenta')
    ..aOM<$2.Timestamp>(5, _omitFieldNames ? '' : 'fechaCierre',
        subBuilder: $2.Timestamp.create)
    ..aOS(6, _omitFieldNames ? '' : 'estadoSaldoCaja')
    ..aOS(7, _omitFieldNames ? '' : 'tipoCaja')
    ..aOS(8, _omitFieldNames ? '' : 'montoMonedaCaja')
    ..aOS(9, _omitFieldNames ? '' : 'montoEnPesos')
    ..aOS(10, _omitFieldNames ? '' : 'montoPorCobrarMonedaCuenta')
    ..aOS(11, _omitFieldNames ? '' : 'montoPorPagarMonedaCuenta')
    ..aOS(12, _omitFieldNames ? '' : 'montoTransito')
    ..aOS(13, _omitFieldNames ? '' : 'garantiaEfectivo')
    ..aOS(14, _omitFieldNames ? '' : 'prestamo')
    ..aOM<$2.Timestamp>(15, _omitFieldNames ? '' : 'dateProcesor',
        subBuilder: $2.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SaldoCaja clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SaldoCaja copyWith(void Function(SaldoCaja) updates) =>
      super.copyWith((message) => updates(message as SaldoCaja)) as SaldoCaja;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SaldoCaja create() => SaldoCaja._();
  @$core.override
  SaldoCaja createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SaldoCaja getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SaldoCaja>(create);
  static SaldoCaja? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get identificador => $_getSZ(0);
  @$pb.TagNumber(1)
  set identificador($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentificador() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentificador() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get idCuenta => $_getSZ(1);
  @$pb.TagNumber(2)
  set idCuenta($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdCuenta() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdCuenta() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get numCuenta => $_getSZ(2);
  @$pb.TagNumber(3)
  set numCuenta($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasNumCuenta() => $_has(2);
  @$pb.TagNumber(3)
  void clearNumCuenta() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get descripcionCuenta => $_getSZ(3);
  @$pb.TagNumber(4)
  set descripcionCuenta($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasDescripcionCuenta() => $_has(3);
  @$pb.TagNumber(4)
  void clearDescripcionCuenta() => $_clearField(4);

  @$pb.TagNumber(5)
  $2.Timestamp get fechaCierre => $_getN(4);
  @$pb.TagNumber(5)
  set fechaCierre($2.Timestamp value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasFechaCierre() => $_has(4);
  @$pb.TagNumber(5)
  void clearFechaCierre() => $_clearField(5);
  @$pb.TagNumber(5)
  $2.Timestamp ensureFechaCierre() => $_ensure(4);

  @$pb.TagNumber(6)
  $core.String get estadoSaldoCaja => $_getSZ(5);
  @$pb.TagNumber(6)
  set estadoSaldoCaja($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasEstadoSaldoCaja() => $_has(5);
  @$pb.TagNumber(6)
  void clearEstadoSaldoCaja() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get tipoCaja => $_getSZ(6);
  @$pb.TagNumber(7)
  set tipoCaja($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasTipoCaja() => $_has(6);
  @$pb.TagNumber(7)
  void clearTipoCaja() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.String get montoMonedaCaja => $_getSZ(7);
  @$pb.TagNumber(8)
  set montoMonedaCaja($core.String value) => $_setString(7, value);
  @$pb.TagNumber(8)
  $core.bool hasMontoMonedaCaja() => $_has(7);
  @$pb.TagNumber(8)
  void clearMontoMonedaCaja() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.String get montoEnPesos => $_getSZ(8);
  @$pb.TagNumber(9)
  set montoEnPesos($core.String value) => $_setString(8, value);
  @$pb.TagNumber(9)
  $core.bool hasMontoEnPesos() => $_has(8);
  @$pb.TagNumber(9)
  void clearMontoEnPesos() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.String get montoPorCobrarMonedaCuenta => $_getSZ(9);
  @$pb.TagNumber(10)
  set montoPorCobrarMonedaCuenta($core.String value) => $_setString(9, value);
  @$pb.TagNumber(10)
  $core.bool hasMontoPorCobrarMonedaCuenta() => $_has(9);
  @$pb.TagNumber(10)
  void clearMontoPorCobrarMonedaCuenta() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.String get montoPorPagarMonedaCuenta => $_getSZ(10);
  @$pb.TagNumber(11)
  set montoPorPagarMonedaCuenta($core.String value) => $_setString(10, value);
  @$pb.TagNumber(11)
  $core.bool hasMontoPorPagarMonedaCuenta() => $_has(10);
  @$pb.TagNumber(11)
  void clearMontoPorPagarMonedaCuenta() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.String get montoTransito => $_getSZ(11);
  @$pb.TagNumber(12)
  set montoTransito($core.String value) => $_setString(11, value);
  @$pb.TagNumber(12)
  $core.bool hasMontoTransito() => $_has(11);
  @$pb.TagNumber(12)
  void clearMontoTransito() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.String get garantiaEfectivo => $_getSZ(12);
  @$pb.TagNumber(13)
  set garantiaEfectivo($core.String value) => $_setString(12, value);
  @$pb.TagNumber(13)
  $core.bool hasGarantiaEfectivo() => $_has(12);
  @$pb.TagNumber(13)
  void clearGarantiaEfectivo() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.String get prestamo => $_getSZ(13);
  @$pb.TagNumber(14)
  set prestamo($core.String value) => $_setString(13, value);
  @$pb.TagNumber(14)
  $core.bool hasPrestamo() => $_has(13);
  @$pb.TagNumber(14)
  void clearPrestamo() => $_clearField(14);

  @$pb.TagNumber(15)
  $2.Timestamp get dateProcesor => $_getN(14);
  @$pb.TagNumber(15)
  set dateProcesor($2.Timestamp value) => $_setField(15, value);
  @$pb.TagNumber(15)
  $core.bool hasDateProcesor() => $_has(14);
  @$pb.TagNumber(15)
  void clearDateProcesor() => $_clearField(15);
  @$pb.TagNumber(15)
  $2.Timestamp ensureDateProcesor() => $_ensure(14);
}

class ValuesPatrimonio extends $pb.GeneratedMessage {
  factory ValuesPatrimonio({
    $core.double? values,
    $core.double? porcentage,
    $core.String? description,
  }) {
    final result = create();
    if (values != null) result.values = values;
    if (porcentage != null) result.porcentage = porcentage;
    if (description != null) result.description = description;
    return result;
  }

  ValuesPatrimonio._();

  factory ValuesPatrimonio.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ValuesPatrimonio.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ValuesPatrimonio',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aD(1, _omitFieldNames ? '' : 'values')
    ..aD(2, _omitFieldNames ? '' : 'porcentage')
    ..aOS(3, _omitFieldNames ? '' : 'description')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ValuesPatrimonio clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ValuesPatrimonio copyWith(void Function(ValuesPatrimonio) updates) =>
      super.copyWith((message) => updates(message as ValuesPatrimonio))
          as ValuesPatrimonio;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ValuesPatrimonio create() => ValuesPatrimonio._();
  @$core.override
  ValuesPatrimonio createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static ValuesPatrimonio getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ValuesPatrimonio>(create);
  static ValuesPatrimonio? _defaultInstance;

  @$pb.TagNumber(1)
  $core.double get values => $_getN(0);
  @$pb.TagNumber(1)
  set values($core.double value) => $_setDouble(0, value);
  @$pb.TagNumber(1)
  $core.bool hasValues() => $_has(0);
  @$pb.TagNumber(1)
  void clearValues() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.double get porcentage => $_getN(1);
  @$pb.TagNumber(2)
  set porcentage($core.double value) => $_setDouble(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPorcentage() => $_has(1);
  @$pb.TagNumber(2)
  void clearPorcentage() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get description => $_getSZ(2);
  @$pb.TagNumber(3)
  set description($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasDescription() => $_has(2);
  @$pb.TagNumber(3)
  void clearDescription() => $_clearField(3);
}

class Simultaneas extends $pb.GeneratedMessage {
  factory Simultaneas({
    $core.String? tipoSimul,
    $core.String? detalleSimultanea,
    $core.String? identCliente,
    $core.String? numCuenta,
    $core.String? fechaOperacion,
    $core.String? fechaVcto,
    $core.String? nombreCliente,
    $core.String? plazo,
    $core.String? plazoRem,
    $core.String? nemotecnico,
    $core.String? cantidad,
    $core.String? tasa,
    $core.String? precioPH,
    $core.String? precioPlazo,
    $core.String? precioMercado,
    $core.String? montoContado,
    $core.String? costoDiario2,
    $core.String? montoPresente,
    $core.String? montoPlazo,
    $core.String? codInst,
    $core.String? cantidadOrig,
    $core.String? corredorVenta,
    $core.String? corredorCompra,
    $core.String? folioFactPH,
    $core.String? folioFactTP,
    $core.String? id,
    $2.Timestamp? dateProcesor,
  }) {
    final result = create();
    if (tipoSimul != null) result.tipoSimul = tipoSimul;
    if (detalleSimultanea != null) result.detalleSimultanea = detalleSimultanea;
    if (identCliente != null) result.identCliente = identCliente;
    if (numCuenta != null) result.numCuenta = numCuenta;
    if (fechaOperacion != null) result.fechaOperacion = fechaOperacion;
    if (fechaVcto != null) result.fechaVcto = fechaVcto;
    if (nombreCliente != null) result.nombreCliente = nombreCliente;
    if (plazo != null) result.plazo = plazo;
    if (plazoRem != null) result.plazoRem = plazoRem;
    if (nemotecnico != null) result.nemotecnico = nemotecnico;
    if (cantidad != null) result.cantidad = cantidad;
    if (tasa != null) result.tasa = tasa;
    if (precioPH != null) result.precioPH = precioPH;
    if (precioPlazo != null) result.precioPlazo = precioPlazo;
    if (precioMercado != null) result.precioMercado = precioMercado;
    if (montoContado != null) result.montoContado = montoContado;
    if (costoDiario2 != null) result.costoDiario2 = costoDiario2;
    if (montoPresente != null) result.montoPresente = montoPresente;
    if (montoPlazo != null) result.montoPlazo = montoPlazo;
    if (codInst != null) result.codInst = codInst;
    if (cantidadOrig != null) result.cantidadOrig = cantidadOrig;
    if (corredorVenta != null) result.corredorVenta = corredorVenta;
    if (corredorCompra != null) result.corredorCompra = corredorCompra;
    if (folioFactPH != null) result.folioFactPH = folioFactPH;
    if (folioFactTP != null) result.folioFactTP = folioFactTP;
    if (id != null) result.id = id;
    if (dateProcesor != null) result.dateProcesor = dateProcesor;
    return result;
  }

  Simultaneas._();

  factory Simultaneas.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Simultaneas.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Simultaneas',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'tipoSimul', protoName: 'tipo_Simul')
    ..aOS(2, _omitFieldNames ? '' : 'DetalleSimultanea',
        protoName: 'Detalle_Simultanea')
    ..aOS(3, _omitFieldNames ? '' : 'identCliente', protoName: 'ident_Cliente')
    ..aOS(4, _omitFieldNames ? '' : 'numCuenta', protoName: 'num_Cuenta')
    ..aOS(5, _omitFieldNames ? '' : 'fechaOperacion',
        protoName: 'fecha_Operacion')
    ..aOS(6, _omitFieldNames ? '' : 'fechaVcto', protoName: 'fecha_Vcto')
    ..aOS(7, _omitFieldNames ? '' : 'nombreCliente',
        protoName: 'nombre_Cliente')
    ..aOS(8, _omitFieldNames ? '' : 'plazo')
    ..aOS(9, _omitFieldNames ? '' : 'plazoRem', protoName: 'plazo_Rem')
    ..aOS(10, _omitFieldNames ? '' : 'nemotecnico')
    ..aOS(11, _omitFieldNames ? '' : 'cantidad')
    ..aOS(12, _omitFieldNames ? '' : 'tasa')
    ..aOS(13, _omitFieldNames ? '' : 'precioPH', protoName: 'precio_PH')
    ..aOS(14, _omitFieldNames ? '' : 'precioPlazo', protoName: 'precio_Plazo')
    ..aOS(15, _omitFieldNames ? '' : 'precioMercado',
        protoName: 'precio_Mercado')
    ..aOS(16, _omitFieldNames ? '' : 'montoContado', protoName: 'monto_Contado')
    ..aOS(17, _omitFieldNames ? '' : 'costoDiario2', protoName: 'costo_Diario2')
    ..aOS(18, _omitFieldNames ? '' : 'montoPresente',
        protoName: 'monto_Presente')
    ..aOS(19, _omitFieldNames ? '' : 'montoPlazo', protoName: 'monto_Plazo')
    ..aOS(20, _omitFieldNames ? '' : 'codInst', protoName: 'cod_Inst')
    ..aOS(21, _omitFieldNames ? '' : 'cantidadOrig', protoName: 'cantidad_Orig')
    ..aOS(22, _omitFieldNames ? '' : 'corredorVenta',
        protoName: 'corredor_Venta')
    ..aOS(23, _omitFieldNames ? '' : 'corredorCompra',
        protoName: 'corredor_Compra')
    ..aOS(24, _omitFieldNames ? '' : 'folioFactPH', protoName: 'folio_Fact_PH')
    ..aOS(25, _omitFieldNames ? '' : 'folioFactTP', protoName: 'folio_Fact_TP')
    ..aOS(26, _omitFieldNames ? '' : 'id')
    ..aOM<$2.Timestamp>(27, _omitFieldNames ? '' : 'dateProcesor',
        subBuilder: $2.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Simultaneas clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Simultaneas copyWith(void Function(Simultaneas) updates) =>
      super.copyWith((message) => updates(message as Simultaneas))
          as Simultaneas;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Simultaneas create() => Simultaneas._();
  @$core.override
  Simultaneas createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Simultaneas getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Simultaneas>(create);
  static Simultaneas? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get tipoSimul => $_getSZ(0);
  @$pb.TagNumber(1)
  set tipoSimul($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTipoSimul() => $_has(0);
  @$pb.TagNumber(1)
  void clearTipoSimul() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get detalleSimultanea => $_getSZ(1);
  @$pb.TagNumber(2)
  set detalleSimultanea($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDetalleSimultanea() => $_has(1);
  @$pb.TagNumber(2)
  void clearDetalleSimultanea() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get identCliente => $_getSZ(2);
  @$pb.TagNumber(3)
  set identCliente($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIdentCliente() => $_has(2);
  @$pb.TagNumber(3)
  void clearIdentCliente() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get numCuenta => $_getSZ(3);
  @$pb.TagNumber(4)
  set numCuenta($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasNumCuenta() => $_has(3);
  @$pb.TagNumber(4)
  void clearNumCuenta() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get fechaOperacion => $_getSZ(4);
  @$pb.TagNumber(5)
  set fechaOperacion($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasFechaOperacion() => $_has(4);
  @$pb.TagNumber(5)
  void clearFechaOperacion() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get fechaVcto => $_getSZ(5);
  @$pb.TagNumber(6)
  set fechaVcto($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasFechaVcto() => $_has(5);
  @$pb.TagNumber(6)
  void clearFechaVcto() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get nombreCliente => $_getSZ(6);
  @$pb.TagNumber(7)
  set nombreCliente($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasNombreCliente() => $_has(6);
  @$pb.TagNumber(7)
  void clearNombreCliente() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.String get plazo => $_getSZ(7);
  @$pb.TagNumber(8)
  set plazo($core.String value) => $_setString(7, value);
  @$pb.TagNumber(8)
  $core.bool hasPlazo() => $_has(7);
  @$pb.TagNumber(8)
  void clearPlazo() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.String get plazoRem => $_getSZ(8);
  @$pb.TagNumber(9)
  set plazoRem($core.String value) => $_setString(8, value);
  @$pb.TagNumber(9)
  $core.bool hasPlazoRem() => $_has(8);
  @$pb.TagNumber(9)
  void clearPlazoRem() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.String get nemotecnico => $_getSZ(9);
  @$pb.TagNumber(10)
  set nemotecnico($core.String value) => $_setString(9, value);
  @$pb.TagNumber(10)
  $core.bool hasNemotecnico() => $_has(9);
  @$pb.TagNumber(10)
  void clearNemotecnico() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.String get cantidad => $_getSZ(10);
  @$pb.TagNumber(11)
  set cantidad($core.String value) => $_setString(10, value);
  @$pb.TagNumber(11)
  $core.bool hasCantidad() => $_has(10);
  @$pb.TagNumber(11)
  void clearCantidad() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.String get tasa => $_getSZ(11);
  @$pb.TagNumber(12)
  set tasa($core.String value) => $_setString(11, value);
  @$pb.TagNumber(12)
  $core.bool hasTasa() => $_has(11);
  @$pb.TagNumber(12)
  void clearTasa() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.String get precioPH => $_getSZ(12);
  @$pb.TagNumber(13)
  set precioPH($core.String value) => $_setString(12, value);
  @$pb.TagNumber(13)
  $core.bool hasPrecioPH() => $_has(12);
  @$pb.TagNumber(13)
  void clearPrecioPH() => $_clearField(13);

  @$pb.TagNumber(14)
  $core.String get precioPlazo => $_getSZ(13);
  @$pb.TagNumber(14)
  set precioPlazo($core.String value) => $_setString(13, value);
  @$pb.TagNumber(14)
  $core.bool hasPrecioPlazo() => $_has(13);
  @$pb.TagNumber(14)
  void clearPrecioPlazo() => $_clearField(14);

  @$pb.TagNumber(15)
  $core.String get precioMercado => $_getSZ(14);
  @$pb.TagNumber(15)
  set precioMercado($core.String value) => $_setString(14, value);
  @$pb.TagNumber(15)
  $core.bool hasPrecioMercado() => $_has(14);
  @$pb.TagNumber(15)
  void clearPrecioMercado() => $_clearField(15);

  @$pb.TagNumber(16)
  $core.String get montoContado => $_getSZ(15);
  @$pb.TagNumber(16)
  set montoContado($core.String value) => $_setString(15, value);
  @$pb.TagNumber(16)
  $core.bool hasMontoContado() => $_has(15);
  @$pb.TagNumber(16)
  void clearMontoContado() => $_clearField(16);

  @$pb.TagNumber(17)
  $core.String get costoDiario2 => $_getSZ(16);
  @$pb.TagNumber(17)
  set costoDiario2($core.String value) => $_setString(16, value);
  @$pb.TagNumber(17)
  $core.bool hasCostoDiario2() => $_has(16);
  @$pb.TagNumber(17)
  void clearCostoDiario2() => $_clearField(17);

  @$pb.TagNumber(18)
  $core.String get montoPresente => $_getSZ(17);
  @$pb.TagNumber(18)
  set montoPresente($core.String value) => $_setString(17, value);
  @$pb.TagNumber(18)
  $core.bool hasMontoPresente() => $_has(17);
  @$pb.TagNumber(18)
  void clearMontoPresente() => $_clearField(18);

  @$pb.TagNumber(19)
  $core.String get montoPlazo => $_getSZ(18);
  @$pb.TagNumber(19)
  set montoPlazo($core.String value) => $_setString(18, value);
  @$pb.TagNumber(19)
  $core.bool hasMontoPlazo() => $_has(18);
  @$pb.TagNumber(19)
  void clearMontoPlazo() => $_clearField(19);

  @$pb.TagNumber(20)
  $core.String get codInst => $_getSZ(19);
  @$pb.TagNumber(20)
  set codInst($core.String value) => $_setString(19, value);
  @$pb.TagNumber(20)
  $core.bool hasCodInst() => $_has(19);
  @$pb.TagNumber(20)
  void clearCodInst() => $_clearField(20);

  @$pb.TagNumber(21)
  $core.String get cantidadOrig => $_getSZ(20);
  @$pb.TagNumber(21)
  set cantidadOrig($core.String value) => $_setString(20, value);
  @$pb.TagNumber(21)
  $core.bool hasCantidadOrig() => $_has(20);
  @$pb.TagNumber(21)
  void clearCantidadOrig() => $_clearField(21);

  @$pb.TagNumber(22)
  $core.String get corredorVenta => $_getSZ(21);
  @$pb.TagNumber(22)
  set corredorVenta($core.String value) => $_setString(21, value);
  @$pb.TagNumber(22)
  $core.bool hasCorredorVenta() => $_has(21);
  @$pb.TagNumber(22)
  void clearCorredorVenta() => $_clearField(22);

  @$pb.TagNumber(23)
  $core.String get corredorCompra => $_getSZ(22);
  @$pb.TagNumber(23)
  set corredorCompra($core.String value) => $_setString(22, value);
  @$pb.TagNumber(23)
  $core.bool hasCorredorCompra() => $_has(22);
  @$pb.TagNumber(23)
  void clearCorredorCompra() => $_clearField(23);

  @$pb.TagNumber(24)
  $core.String get folioFactPH => $_getSZ(23);
  @$pb.TagNumber(24)
  set folioFactPH($core.String value) => $_setString(23, value);
  @$pb.TagNumber(24)
  $core.bool hasFolioFactPH() => $_has(23);
  @$pb.TagNumber(24)
  void clearFolioFactPH() => $_clearField(24);

  @$pb.TagNumber(25)
  $core.String get folioFactTP => $_getSZ(24);
  @$pb.TagNumber(25)
  set folioFactTP($core.String value) => $_setString(24, value);
  @$pb.TagNumber(25)
  $core.bool hasFolioFactTP() => $_has(24);
  @$pb.TagNumber(25)
  void clearFolioFactTP() => $_clearField(25);

  @$pb.TagNumber(26)
  $core.String get id => $_getSZ(25);
  @$pb.TagNumber(26)
  set id($core.String value) => $_setString(25, value);
  @$pb.TagNumber(26)
  $core.bool hasId() => $_has(25);
  @$pb.TagNumber(26)
  void clearId() => $_clearField(26);

  @$pb.TagNumber(27)
  $2.Timestamp get dateProcesor => $_getN(26);
  @$pb.TagNumber(27)
  set dateProcesor($2.Timestamp value) => $_setField(27, value);
  @$pb.TagNumber(27)
  $core.bool hasDateProcesor() => $_has(26);
  @$pb.TagNumber(27)
  void clearDateProcesor() => $_clearField(27);
  @$pb.TagNumber(27)
  $2.Timestamp ensureDateProcesor() => $_ensure(26);
}

class SnapshotSimultaneas extends $pb.GeneratedMessage {
  factory SnapshotSimultaneas({
    $core.String? id,
    $core.String? account,
    $core.Iterable<Simultaneas>? simultaneas,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (account != null) result.account = account;
    if (simultaneas != null) result.simultaneas.addAll(simultaneas);
    return result;
  }

  SnapshotSimultaneas._();

  factory SnapshotSimultaneas.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotSimultaneas.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotSimultaneas',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'account')
    ..pPM<Simultaneas>(3, _omitFieldNames ? '' : 'simultaneas',
        subBuilder: Simultaneas.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotSimultaneas clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotSimultaneas copyWith(void Function(SnapshotSimultaneas) updates) =>
      super.copyWith((message) => updates(message as SnapshotSimultaneas))
          as SnapshotSimultaneas;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotSimultaneas create() => SnapshotSimultaneas._();
  @$core.override
  SnapshotSimultaneas createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotSimultaneas getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotSimultaneas>(create);
  static SnapshotSimultaneas? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get account => $_getSZ(1);
  @$pb.TagNumber(2)
  set account($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAccount() => $_has(1);
  @$pb.TagNumber(2)
  void clearAccount() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<Simultaneas> get simultaneas => $_getList(2);
}

class SnapshotPrestamos extends $pb.GeneratedMessage {
  factory SnapshotPrestamos({
    $core.String? id,
    $core.String? account,
    $core.Iterable<Prestamos>? prestamos,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (account != null) result.account = account;
    if (prestamos != null) result.prestamos.addAll(prestamos);
    return result;
  }

  SnapshotPrestamos._();

  factory SnapshotPrestamos.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SnapshotPrestamos.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotPrestamos',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'account')
    ..pPM<Prestamos>(3, _omitFieldNames ? '' : 'prestamos',
        subBuilder: Prestamos.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotPrestamos clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotPrestamos copyWith(void Function(SnapshotPrestamos) updates) =>
      super.copyWith((message) => updates(message as SnapshotPrestamos))
          as SnapshotPrestamos;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SnapshotPrestamos create() => SnapshotPrestamos._();
  @$core.override
  SnapshotPrestamos createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SnapshotPrestamos getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SnapshotPrestamos>(create);
  static SnapshotPrestamos? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get account => $_getSZ(1);
  @$pb.TagNumber(2)
  set account($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAccount() => $_has(1);
  @$pb.TagNumber(2)
  void clearAccount() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<Prestamos> get prestamos => $_getList(2);
}

class Patrimonio extends $pb.GeneratedMessage {
  factory Patrimonio({
    ValuesPatrimonio? activos,
    ValuesPatrimonio? liquidez,
    ValuesPatrimonio? caja,
    ValuesPatrimonio? cuentaTransitoriasPorCobrarPagar,
    ValuesPatrimonio? garantiaEfectivo,
    ValuesPatrimonio? accionesNacionales,
    ValuesPatrimonio? simultaneas,
    ValuesPatrimonio? prestamos,
    ValuesPatrimonio? accionesextranjeras,
    ValuesPatrimonio? fondosMutos,
    ValuesPatrimonio? rvNacional,
    ValuesPatrimonio? rvExtranjeros,
    ValuesPatrimonio? fondoInversionRentaVariable,
    ValuesPatrimonio? activosInmobiliarios,
    ValuesPatrimonio? eftsRentaVariable,
    ValuesPatrimonio? inversionesAlternativas,
    ValuesPatrimonio? derivados,
    ValuesPatrimonio? rentaVariable,
    ValuesPatrimonio? rentaFija,
    $core.String? cuenta,
    $0.Currency? currency,
    $core.double? auxAccionesNacionales,
    $core.double? auxSimultanea,
    $core.double? auxPrestamo,
    $core.double? auxValorDeMercado,
    $2.Timestamp? dateProcesor,
  }) {
    final result = create();
    if (activos != null) result.activos = activos;
    if (liquidez != null) result.liquidez = liquidez;
    if (caja != null) result.caja = caja;
    if (cuentaTransitoriasPorCobrarPagar != null)
      result.cuentaTransitoriasPorCobrarPagar =
          cuentaTransitoriasPorCobrarPagar;
    if (garantiaEfectivo != null) result.garantiaEfectivo = garantiaEfectivo;
    if (accionesNacionales != null)
      result.accionesNacionales = accionesNacionales;
    if (simultaneas != null) result.simultaneas = simultaneas;
    if (prestamos != null) result.prestamos = prestamos;
    if (accionesextranjeras != null)
      result.accionesextranjeras = accionesextranjeras;
    if (fondosMutos != null) result.fondosMutos = fondosMutos;
    if (rvNacional != null) result.rvNacional = rvNacional;
    if (rvExtranjeros != null) result.rvExtranjeros = rvExtranjeros;
    if (fondoInversionRentaVariable != null)
      result.fondoInversionRentaVariable = fondoInversionRentaVariable;
    if (activosInmobiliarios != null)
      result.activosInmobiliarios = activosInmobiliarios;
    if (eftsRentaVariable != null) result.eftsRentaVariable = eftsRentaVariable;
    if (inversionesAlternativas != null)
      result.inversionesAlternativas = inversionesAlternativas;
    if (derivados != null) result.derivados = derivados;
    if (rentaVariable != null) result.rentaVariable = rentaVariable;
    if (rentaFija != null) result.rentaFija = rentaFija;
    if (cuenta != null) result.cuenta = cuenta;
    if (currency != null) result.currency = currency;
    if (auxAccionesNacionales != null)
      result.auxAccionesNacionales = auxAccionesNacionales;
    if (auxSimultanea != null) result.auxSimultanea = auxSimultanea;
    if (auxPrestamo != null) result.auxPrestamo = auxPrestamo;
    if (auxValorDeMercado != null) result.auxValorDeMercado = auxValorDeMercado;
    if (dateProcesor != null) result.dateProcesor = dateProcesor;
    return result;
  }

  Patrimonio._();

  factory Patrimonio.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Patrimonio.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Patrimonio',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOM<ValuesPatrimonio>(1, _omitFieldNames ? '' : 'activos',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(2, _omitFieldNames ? '' : 'liquidez',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(3, _omitFieldNames ? '' : 'caja',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(
        4, _omitFieldNames ? '' : 'cuentaTransitoriasPorCobrarPagar',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(5, _omitFieldNames ? '' : 'garantiaEfectivo',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(6, _omitFieldNames ? '' : 'accionesNacionales',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(7, _omitFieldNames ? '' : 'simultaneas',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(8, _omitFieldNames ? '' : 'prestamos',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(9, _omitFieldNames ? '' : 'accionesextranjeras',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(10, _omitFieldNames ? '' : 'fondosMutos',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(11, _omitFieldNames ? '' : 'rvNacional',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(12, _omitFieldNames ? '' : 'rvExtranjeros',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(
        13, _omitFieldNames ? '' : 'fondoInversionRentaVariable',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(14, _omitFieldNames ? '' : 'activosInmobiliarios',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(15, _omitFieldNames ? '' : 'eftsRentaVariable',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(
        16, _omitFieldNames ? '' : 'inversionesAlternativas',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(17, _omitFieldNames ? '' : 'derivados',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(18, _omitFieldNames ? '' : 'rentaVariable',
        subBuilder: ValuesPatrimonio.create)
    ..aOM<ValuesPatrimonio>(19, _omitFieldNames ? '' : 'rentaFija',
        subBuilder: ValuesPatrimonio.create)
    ..aOS(20, _omitFieldNames ? '' : 'cuenta')
    ..aE<$0.Currency>(21, _omitFieldNames ? '' : 'currency',
        enumValues: $0.Currency.values)
    ..aD(22, _omitFieldNames ? '' : 'auxAccionesNacionales',
        protoName: 'aux_accionesNacionales')
    ..aD(23, _omitFieldNames ? '' : 'auxSimultanea')
    ..aD(24, _omitFieldNames ? '' : 'auxPrestamo')
    ..aD(25, _omitFieldNames ? '' : 'auxValorDeMercado')
    ..aOM<$2.Timestamp>(29, _omitFieldNames ? '' : 'dateProcesor',
        subBuilder: $2.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Patrimonio clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Patrimonio copyWith(void Function(Patrimonio) updates) =>
      super.copyWith((message) => updates(message as Patrimonio)) as Patrimonio;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Patrimonio create() => Patrimonio._();
  @$core.override
  Patrimonio createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Patrimonio getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Patrimonio>(create);
  static Patrimonio? _defaultInstance;

  @$pb.TagNumber(1)
  ValuesPatrimonio get activos => $_getN(0);
  @$pb.TagNumber(1)
  set activos(ValuesPatrimonio value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActivos() => $_has(0);
  @$pb.TagNumber(1)
  void clearActivos() => $_clearField(1);
  @$pb.TagNumber(1)
  ValuesPatrimonio ensureActivos() => $_ensure(0);

  @$pb.TagNumber(2)
  ValuesPatrimonio get liquidez => $_getN(1);
  @$pb.TagNumber(2)
  set liquidez(ValuesPatrimonio value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasLiquidez() => $_has(1);
  @$pb.TagNumber(2)
  void clearLiquidez() => $_clearField(2);
  @$pb.TagNumber(2)
  ValuesPatrimonio ensureLiquidez() => $_ensure(1);

  @$pb.TagNumber(3)
  ValuesPatrimonio get caja => $_getN(2);
  @$pb.TagNumber(3)
  set caja(ValuesPatrimonio value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasCaja() => $_has(2);
  @$pb.TagNumber(3)
  void clearCaja() => $_clearField(3);
  @$pb.TagNumber(3)
  ValuesPatrimonio ensureCaja() => $_ensure(2);

  @$pb.TagNumber(4)
  ValuesPatrimonio get cuentaTransitoriasPorCobrarPagar => $_getN(3);
  @$pb.TagNumber(4)
  set cuentaTransitoriasPorCobrarPagar(ValuesPatrimonio value) =>
      $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasCuentaTransitoriasPorCobrarPagar() => $_has(3);
  @$pb.TagNumber(4)
  void clearCuentaTransitoriasPorCobrarPagar() => $_clearField(4);
  @$pb.TagNumber(4)
  ValuesPatrimonio ensureCuentaTransitoriasPorCobrarPagar() => $_ensure(3);

  @$pb.TagNumber(5)
  ValuesPatrimonio get garantiaEfectivo => $_getN(4);
  @$pb.TagNumber(5)
  set garantiaEfectivo(ValuesPatrimonio value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasGarantiaEfectivo() => $_has(4);
  @$pb.TagNumber(5)
  void clearGarantiaEfectivo() => $_clearField(5);
  @$pb.TagNumber(5)
  ValuesPatrimonio ensureGarantiaEfectivo() => $_ensure(4);

  @$pb.TagNumber(6)
  ValuesPatrimonio get accionesNacionales => $_getN(5);
  @$pb.TagNumber(6)
  set accionesNacionales(ValuesPatrimonio value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasAccionesNacionales() => $_has(5);
  @$pb.TagNumber(6)
  void clearAccionesNacionales() => $_clearField(6);
  @$pb.TagNumber(6)
  ValuesPatrimonio ensureAccionesNacionales() => $_ensure(5);

  @$pb.TagNumber(7)
  ValuesPatrimonio get simultaneas => $_getN(6);
  @$pb.TagNumber(7)
  set simultaneas(ValuesPatrimonio value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasSimultaneas() => $_has(6);
  @$pb.TagNumber(7)
  void clearSimultaneas() => $_clearField(7);
  @$pb.TagNumber(7)
  ValuesPatrimonio ensureSimultaneas() => $_ensure(6);

  @$pb.TagNumber(8)
  ValuesPatrimonio get prestamos => $_getN(7);
  @$pb.TagNumber(8)
  set prestamos(ValuesPatrimonio value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasPrestamos() => $_has(7);
  @$pb.TagNumber(8)
  void clearPrestamos() => $_clearField(8);
  @$pb.TagNumber(8)
  ValuesPatrimonio ensurePrestamos() => $_ensure(7);

  @$pb.TagNumber(9)
  ValuesPatrimonio get accionesextranjeras => $_getN(8);
  @$pb.TagNumber(9)
  set accionesextranjeras(ValuesPatrimonio value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasAccionesextranjeras() => $_has(8);
  @$pb.TagNumber(9)
  void clearAccionesextranjeras() => $_clearField(9);
  @$pb.TagNumber(9)
  ValuesPatrimonio ensureAccionesextranjeras() => $_ensure(8);

  @$pb.TagNumber(10)
  ValuesPatrimonio get fondosMutos => $_getN(9);
  @$pb.TagNumber(10)
  set fondosMutos(ValuesPatrimonio value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasFondosMutos() => $_has(9);
  @$pb.TagNumber(10)
  void clearFondosMutos() => $_clearField(10);
  @$pb.TagNumber(10)
  ValuesPatrimonio ensureFondosMutos() => $_ensure(9);

  @$pb.TagNumber(11)
  ValuesPatrimonio get rvNacional => $_getN(10);
  @$pb.TagNumber(11)
  set rvNacional(ValuesPatrimonio value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasRvNacional() => $_has(10);
  @$pb.TagNumber(11)
  void clearRvNacional() => $_clearField(11);
  @$pb.TagNumber(11)
  ValuesPatrimonio ensureRvNacional() => $_ensure(10);

  @$pb.TagNumber(12)
  ValuesPatrimonio get rvExtranjeros => $_getN(11);
  @$pb.TagNumber(12)
  set rvExtranjeros(ValuesPatrimonio value) => $_setField(12, value);
  @$pb.TagNumber(12)
  $core.bool hasRvExtranjeros() => $_has(11);
  @$pb.TagNumber(12)
  void clearRvExtranjeros() => $_clearField(12);
  @$pb.TagNumber(12)
  ValuesPatrimonio ensureRvExtranjeros() => $_ensure(11);

  @$pb.TagNumber(13)
  ValuesPatrimonio get fondoInversionRentaVariable => $_getN(12);
  @$pb.TagNumber(13)
  set fondoInversionRentaVariable(ValuesPatrimonio value) =>
      $_setField(13, value);
  @$pb.TagNumber(13)
  $core.bool hasFondoInversionRentaVariable() => $_has(12);
  @$pb.TagNumber(13)
  void clearFondoInversionRentaVariable() => $_clearField(13);
  @$pb.TagNumber(13)
  ValuesPatrimonio ensureFondoInversionRentaVariable() => $_ensure(12);

  @$pb.TagNumber(14)
  ValuesPatrimonio get activosInmobiliarios => $_getN(13);
  @$pb.TagNumber(14)
  set activosInmobiliarios(ValuesPatrimonio value) => $_setField(14, value);
  @$pb.TagNumber(14)
  $core.bool hasActivosInmobiliarios() => $_has(13);
  @$pb.TagNumber(14)
  void clearActivosInmobiliarios() => $_clearField(14);
  @$pb.TagNumber(14)
  ValuesPatrimonio ensureActivosInmobiliarios() => $_ensure(13);

  @$pb.TagNumber(15)
  ValuesPatrimonio get eftsRentaVariable => $_getN(14);
  @$pb.TagNumber(15)
  set eftsRentaVariable(ValuesPatrimonio value) => $_setField(15, value);
  @$pb.TagNumber(15)
  $core.bool hasEftsRentaVariable() => $_has(14);
  @$pb.TagNumber(15)
  void clearEftsRentaVariable() => $_clearField(15);
  @$pb.TagNumber(15)
  ValuesPatrimonio ensureEftsRentaVariable() => $_ensure(14);

  @$pb.TagNumber(16)
  ValuesPatrimonio get inversionesAlternativas => $_getN(15);
  @$pb.TagNumber(16)
  set inversionesAlternativas(ValuesPatrimonio value) => $_setField(16, value);
  @$pb.TagNumber(16)
  $core.bool hasInversionesAlternativas() => $_has(15);
  @$pb.TagNumber(16)
  void clearInversionesAlternativas() => $_clearField(16);
  @$pb.TagNumber(16)
  ValuesPatrimonio ensureInversionesAlternativas() => $_ensure(15);

  @$pb.TagNumber(17)
  ValuesPatrimonio get derivados => $_getN(16);
  @$pb.TagNumber(17)
  set derivados(ValuesPatrimonio value) => $_setField(17, value);
  @$pb.TagNumber(17)
  $core.bool hasDerivados() => $_has(16);
  @$pb.TagNumber(17)
  void clearDerivados() => $_clearField(17);
  @$pb.TagNumber(17)
  ValuesPatrimonio ensureDerivados() => $_ensure(16);

  @$pb.TagNumber(18)
  ValuesPatrimonio get rentaVariable => $_getN(17);
  @$pb.TagNumber(18)
  set rentaVariable(ValuesPatrimonio value) => $_setField(18, value);
  @$pb.TagNumber(18)
  $core.bool hasRentaVariable() => $_has(17);
  @$pb.TagNumber(18)
  void clearRentaVariable() => $_clearField(18);
  @$pb.TagNumber(18)
  ValuesPatrimonio ensureRentaVariable() => $_ensure(17);

  @$pb.TagNumber(19)
  ValuesPatrimonio get rentaFija => $_getN(18);
  @$pb.TagNumber(19)
  set rentaFija(ValuesPatrimonio value) => $_setField(19, value);
  @$pb.TagNumber(19)
  $core.bool hasRentaFija() => $_has(18);
  @$pb.TagNumber(19)
  void clearRentaFija() => $_clearField(19);
  @$pb.TagNumber(19)
  ValuesPatrimonio ensureRentaFija() => $_ensure(18);

  @$pb.TagNumber(20)
  $core.String get cuenta => $_getSZ(19);
  @$pb.TagNumber(20)
  set cuenta($core.String value) => $_setString(19, value);
  @$pb.TagNumber(20)
  $core.bool hasCuenta() => $_has(19);
  @$pb.TagNumber(20)
  void clearCuenta() => $_clearField(20);

  @$pb.TagNumber(21)
  $0.Currency get currency => $_getN(20);
  @$pb.TagNumber(21)
  set currency($0.Currency value) => $_setField(21, value);
  @$pb.TagNumber(21)
  $core.bool hasCurrency() => $_has(20);
  @$pb.TagNumber(21)
  void clearCurrency() => $_clearField(21);

  @$pb.TagNumber(22)
  $core.double get auxAccionesNacionales => $_getN(21);
  @$pb.TagNumber(22)
  set auxAccionesNacionales($core.double value) => $_setDouble(21, value);
  @$pb.TagNumber(22)
  $core.bool hasAuxAccionesNacionales() => $_has(21);
  @$pb.TagNumber(22)
  void clearAuxAccionesNacionales() => $_clearField(22);

  @$pb.TagNumber(23)
  $core.double get auxSimultanea => $_getN(22);
  @$pb.TagNumber(23)
  set auxSimultanea($core.double value) => $_setDouble(22, value);
  @$pb.TagNumber(23)
  $core.bool hasAuxSimultanea() => $_has(22);
  @$pb.TagNumber(23)
  void clearAuxSimultanea() => $_clearField(23);

  @$pb.TagNumber(24)
  $core.double get auxPrestamo => $_getN(23);
  @$pb.TagNumber(24)
  set auxPrestamo($core.double value) => $_setDouble(23, value);
  @$pb.TagNumber(24)
  $core.bool hasAuxPrestamo() => $_has(23);
  @$pb.TagNumber(24)
  void clearAuxPrestamo() => $_clearField(24);

  @$pb.TagNumber(25)
  $core.double get auxValorDeMercado => $_getN(24);
  @$pb.TagNumber(25)
  set auxValorDeMercado($core.double value) => $_setDouble(24, value);
  @$pb.TagNumber(25)
  $core.bool hasAuxValorDeMercado() => $_has(24);
  @$pb.TagNumber(25)
  void clearAuxValorDeMercado() => $_clearField(25);

  @$pb.TagNumber(29)
  $2.Timestamp get dateProcesor => $_getN(25);
  @$pb.TagNumber(29)
  set dateProcesor($2.Timestamp value) => $_setField(29, value);
  @$pb.TagNumber(29)
  $core.bool hasDateProcesor() => $_has(25);
  @$pb.TagNumber(29)
  void clearDateProcesor() => $_clearField(29);
  @$pb.TagNumber(29)
  $2.Timestamp ensureDateProcesor() => $_ensure(25);
}

class SubMultibook extends $pb.GeneratedMessage {
  factory SubMultibook({
    $core.int? positions,
    $1.Subscribe? subscribeBook,
  }) {
    final result = create();
    if (positions != null) result.positions = positions;
    if (subscribeBook != null) result.subscribeBook = subscribeBook;
    return result;
  }

  SubMultibook._();

  factory SubMultibook.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory SubMultibook.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SubMultibook',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aI(2, _omitFieldNames ? '' : 'positions')
    ..aOM<$1.Subscribe>(3, _omitFieldNames ? '' : 'subscribeBook',
        protoName: 'subscribeBook', subBuilder: $1.Subscribe.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubMultibook clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubMultibook copyWith(void Function(SubMultibook) updates) =>
      super.copyWith((message) => updates(message as SubMultibook))
          as SubMultibook;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static SubMultibook create() => SubMultibook._();
  @$core.override
  SubMultibook createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static SubMultibook getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SubMultibook>(create);
  static SubMultibook? _defaultInstance;

  @$pb.TagNumber(2)
  $core.int get positions => $_getIZ(0);
  @$pb.TagNumber(2)
  set positions($core.int value) => $_setSignedInt32(0, value);
  @$pb.TagNumber(2)
  $core.bool hasPositions() => $_has(0);
  @$pb.TagNumber(2)
  void clearPositions() => $_clearField(2);

  @$pb.TagNumber(3)
  $1.Subscribe get subscribeBook => $_getN(1);
  @$pb.TagNumber(3)
  set subscribeBook($1.Subscribe value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasSubscribeBook() => $_has(1);
  @$pb.TagNumber(3)
  void clearSubscribeBook() => $_clearField(3);
  @$pb.TagNumber(3)
  $1.Subscribe ensureSubscribeBook() => $_ensure(1);
}

class Multibook extends $pb.GeneratedMessage {
  factory Multibook({
    $core.String? username,
    $core.Iterable<SubMultibook>? submultibook,
  }) {
    final result = create();
    if (username != null) result.username = username;
    if (submultibook != null) result.submultibook.addAll(submultibook);
    return result;
  }

  Multibook._();

  factory Multibook.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Multibook.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Multibook',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'username')
    ..pPM<SubMultibook>(2, _omitFieldNames ? '' : 'submultibook',
        subBuilder: SubMultibook.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Multibook clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Multibook copyWith(void Function(Multibook) updates) =>
      super.copyWith((message) => updates(message as Multibook)) as Multibook;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Multibook create() => Multibook._();
  @$core.override
  Multibook createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Multibook getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Multibook>(create);
  static Multibook? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get username => $_getSZ(0);
  @$pb.TagNumber(1)
  set username($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasUsername() => $_has(0);
  @$pb.TagNumber(1)
  void clearUsername() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<SubMultibook> get submultibook => $_getList(1);
}

class User extends $pb.GeneratedMessage {
  factory User({
    $core.String? id,
    $core.String? username,
    $core.String? email,
    $core.String? fname,
    $core.String? lname,
    $core.Iterable<$core.String>? marginaccount,
    $core.String? phone,
    $core.Iterable<$core.String>? account,
    $core.bool? active,
    StatusUser? statusUser,
    $core.String? token,
    $core.String? password,
    UserRolesMaps? roles,
    $core.bool? isAdmin,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (username != null) result.username = username;
    if (email != null) result.email = email;
    if (fname != null) result.fname = fname;
    if (lname != null) result.lname = lname;
    if (marginaccount != null) result.marginaccount.addAll(marginaccount);
    if (phone != null) result.phone = phone;
    if (account != null) result.account.addAll(account);
    if (active != null) result.active = active;
    if (statusUser != null) result.statusUser = statusUser;
    if (token != null) result.token = token;
    if (password != null) result.password = password;
    if (roles != null) result.roles = roles;
    if (isAdmin != null) result.isAdmin = isAdmin;
    return result;
  }

  User._();

  factory User.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory User.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'User',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aOS(2, _omitFieldNames ? '' : 'username')
    ..aOS(3, _omitFieldNames ? '' : 'email')
    ..aOS(4, _omitFieldNames ? '' : 'fname')
    ..aOS(5, _omitFieldNames ? '' : 'lname')
    ..pPS(6, _omitFieldNames ? '' : 'marginaccount')
    ..aOS(7, _omitFieldNames ? '' : 'phone')
    ..pPS(8, _omitFieldNames ? '' : 'account')
    ..aOB(10, _omitFieldNames ? '' : 'active')
    ..aE<StatusUser>(11, _omitFieldNames ? '' : 'statusUser',
        protoName: 'statusUser', enumValues: StatusUser.values)
    ..aOS(12, _omitFieldNames ? '' : 'token')
    ..aOS(13, _omitFieldNames ? '' : 'password')
    ..aOM<UserRolesMaps>(14, _omitFieldNames ? '' : 'roles',
        subBuilder: UserRolesMaps.create)
    ..aOB(15, _omitFieldNames ? '' : 'isAdmin', protoName: 'isAdmin')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  User clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  User copyWith(void Function(User) updates) =>
      super.copyWith((message) => updates(message as User)) as User;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static User create() => User._();
  @$core.override
  User createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static User getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<User>(create);
  static User? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get username => $_getSZ(1);
  @$pb.TagNumber(2)
  set username($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUsername() => $_has(1);
  @$pb.TagNumber(2)
  void clearUsername() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get email => $_getSZ(2);
  @$pb.TagNumber(3)
  set email($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasEmail() => $_has(2);
  @$pb.TagNumber(3)
  void clearEmail() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get fname => $_getSZ(3);
  @$pb.TagNumber(4)
  set fname($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasFname() => $_has(3);
  @$pb.TagNumber(4)
  void clearFname() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get lname => $_getSZ(4);
  @$pb.TagNumber(5)
  set lname($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasLname() => $_has(4);
  @$pb.TagNumber(5)
  void clearLname() => $_clearField(5);

  @$pb.TagNumber(6)
  $pb.PbList<$core.String> get marginaccount => $_getList(5);

  @$pb.TagNumber(7)
  $core.String get phone => $_getSZ(6);
  @$pb.TagNumber(7)
  set phone($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasPhone() => $_has(6);
  @$pb.TagNumber(7)
  void clearPhone() => $_clearField(7);

  @$pb.TagNumber(8)
  $pb.PbList<$core.String> get account => $_getList(7);

  @$pb.TagNumber(10)
  $core.bool get active => $_getBF(8);
  @$pb.TagNumber(10)
  set active($core.bool value) => $_setBool(8, value);
  @$pb.TagNumber(10)
  $core.bool hasActive() => $_has(8);
  @$pb.TagNumber(10)
  void clearActive() => $_clearField(10);

  @$pb.TagNumber(11)
  StatusUser get statusUser => $_getN(9);
  @$pb.TagNumber(11)
  set statusUser(StatusUser value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasStatusUser() => $_has(9);
  @$pb.TagNumber(11)
  void clearStatusUser() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.String get token => $_getSZ(10);
  @$pb.TagNumber(12)
  set token($core.String value) => $_setString(10, value);
  @$pb.TagNumber(12)
  $core.bool hasToken() => $_has(10);
  @$pb.TagNumber(12)
  void clearToken() => $_clearField(12);

  @$pb.TagNumber(13)
  $core.String get password => $_getSZ(11);
  @$pb.TagNumber(13)
  set password($core.String value) => $_setString(11, value);
  @$pb.TagNumber(13)
  $core.bool hasPassword() => $_has(11);
  @$pb.TagNumber(13)
  void clearPassword() => $_clearField(13);

  @$pb.TagNumber(14)
  UserRolesMaps get roles => $_getN(12);
  @$pb.TagNumber(14)
  set roles(UserRolesMaps value) => $_setField(14, value);
  @$pb.TagNumber(14)
  $core.bool hasRoles() => $_has(12);
  @$pb.TagNumber(14)
  void clearRoles() => $_clearField(14);
  @$pb.TagNumber(14)
  UserRolesMaps ensureRoles() => $_ensure(12);

  @$pb.TagNumber(15)
  $core.bool get isAdmin => $_getBF(13);
  @$pb.TagNumber(15)
  set isAdmin($core.bool value) => $_setBool(13, value);
  @$pb.TagNumber(15)
  $core.bool hasIsAdmin() => $_has(13);
  @$pb.TagNumber(15)
  void clearIsAdmin() => $_clearField(15);
}

class UserRolesMaps extends $pb.GeneratedMessage {
  factory UserRolesMaps({
    $core.Iterable<$core.String>? codeOperator,
    $core.Iterable<$0.SecurityExchangeRouting>? destinoRouting,
    $core.Iterable<$1.SecurityExchangeMarketData>? destinoMKD,
    $core.Iterable<$0.ExecBroker>? broker,
    $core.Iterable<$0.StrategyOrder>? strategy,
    $core.Iterable<$core.String>? defaultRouting,
    $core.String? perfil,
    $core.Iterable<$core.String>? access,
    $core.String? palanca,
  }) {
    final result = create();
    if (codeOperator != null) result.codeOperator.addAll(codeOperator);
    if (destinoRouting != null) result.destinoRouting.addAll(destinoRouting);
    if (destinoMKD != null) result.destinoMKD.addAll(destinoMKD);
    if (broker != null) result.broker.addAll(broker);
    if (strategy != null) result.strategy.addAll(strategy);
    if (defaultRouting != null) result.defaultRouting.addAll(defaultRouting);
    if (perfil != null) result.perfil = perfil;
    if (access != null) result.access.addAll(access);
    if (palanca != null) result.palanca = palanca;
    return result;
  }

  UserRolesMaps._();

  factory UserRolesMaps.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory UserRolesMaps.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UserRolesMaps',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPS(1, _omitFieldNames ? '' : 'codeOperator', protoName: 'codeOperator')
    ..pc<$0.SecurityExchangeRouting>(
        2, _omitFieldNames ? '' : 'destinoRouting', $pb.PbFieldType.KE,
        protoName: 'destinoRouting',
        valueOf: $0.SecurityExchangeRouting.valueOf,
        enumValues: $0.SecurityExchangeRouting.values,
        defaultEnumValue: $0.SecurityExchangeRouting.XSGO)
    ..pc<$1.SecurityExchangeMarketData>(
        3, _omitFieldNames ? '' : 'destinoMKD', $pb.PbFieldType.KE,
        protoName: 'destinoMKD',
        valueOf: $1.SecurityExchangeMarketData.valueOf,
        enumValues: $1.SecurityExchangeMarketData.values,
        defaultEnumValue: $1.SecurityExchangeMarketData.BCS)
    ..pc<$0.ExecBroker>(4, _omitFieldNames ? '' : 'broker', $pb.PbFieldType.KE,
        valueOf: $0.ExecBroker.valueOf,
        enumValues: $0.ExecBroker.values,
        defaultEnumValue: $0.ExecBroker.NO_EXEC)
    ..pc<$0.StrategyOrder>(
        5, _omitFieldNames ? '' : 'strategy', $pb.PbFieldType.KE,
        valueOf: $0.StrategyOrder.valueOf,
        enumValues: $0.StrategyOrder.values,
        defaultEnumValue: $0.StrategyOrder.NONE_STRATEGY)
    ..pPS(6, _omitFieldNames ? '' : 'defaultRouting',
        protoName: 'defaultRouting')
    ..aOS(7, _omitFieldNames ? '' : 'perfil')
    ..pPS(8, _omitFieldNames ? '' : 'access')
    ..aOS(9, _omitFieldNames ? '' : 'palanca')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UserRolesMaps clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UserRolesMaps copyWith(void Function(UserRolesMaps) updates) =>
      super.copyWith((message) => updates(message as UserRolesMaps))
          as UserRolesMaps;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static UserRolesMaps create() => UserRolesMaps._();
  @$core.override
  UserRolesMaps createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static UserRolesMaps getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<UserRolesMaps>(create);
  static UserRolesMaps? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<$core.String> get codeOperator => $_getList(0);

  @$pb.TagNumber(2)
  $pb.PbList<$0.SecurityExchangeRouting> get destinoRouting => $_getList(1);

  @$pb.TagNumber(3)
  $pb.PbList<$1.SecurityExchangeMarketData> get destinoMKD => $_getList(2);

  @$pb.TagNumber(4)
  $pb.PbList<$0.ExecBroker> get broker => $_getList(3);

  @$pb.TagNumber(5)
  $pb.PbList<$0.StrategyOrder> get strategy => $_getList(4);

  @$pb.TagNumber(6)
  $pb.PbList<$core.String> get defaultRouting => $_getList(5);

  @$pb.TagNumber(7)
  $core.String get perfil => $_getSZ(6);
  @$pb.TagNumber(7)
  set perfil($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasPerfil() => $_has(6);
  @$pb.TagNumber(7)
  void clearPerfil() => $_clearField(7);

  @$pb.TagNumber(8)
  $pb.PbList<$core.String> get access => $_getList(7);

  @$pb.TagNumber(9)
  $core.String get palanca => $_getSZ(8);
  @$pb.TagNumber(9)
  set palanca($core.String value) => $_setString(8, value);
  @$pb.TagNumber(9)
  $core.bool hasPalanca() => $_has(8);
  @$pb.TagNumber(9)
  void clearPalanca() => $_clearField(9);
}

class UserList extends $pb.GeneratedMessage {
  factory UserList({
    $core.Iterable<User>? users,
    StatusUser? statusUser,
  }) {
    final result = create();
    if (users != null) result.users.addAll(users);
    if (statusUser != null) result.statusUser = statusUser;
    return result;
  }

  UserList._();

  factory UserList.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory UserList.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UserList',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..pPM<User>(1, _omitFieldNames ? '' : 'users', subBuilder: User.create)
    ..aE<StatusUser>(2, _omitFieldNames ? '' : 'statusUser',
        protoName: 'statusUser', enumValues: StatusUser.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UserList clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UserList copyWith(void Function(UserList) updates) =>
      super.copyWith((message) => updates(message as UserList)) as UserList;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static UserList create() => UserList._();
  @$core.override
  UserList createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static UserList getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<UserList>(create);
  static UserList? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<User> get users => $_getList(0);

  @$pb.TagNumber(2)
  StatusUser get statusUser => $_getN(1);
  @$pb.TagNumber(2)
  set statusUser(StatusUser value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasStatusUser() => $_has(1);
  @$pb.TagNumber(2)
  void clearStatusUser() => $_clearField(2);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
