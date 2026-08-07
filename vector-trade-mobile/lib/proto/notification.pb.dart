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
import 'package:protobuf/well_known_types/google/protobuf/timestamp.pb.dart'
    as $0;

import 'notification.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'notification.pbenum.dart';

class NotificationRequest extends $pb.GeneratedMessage {
  factory NotificationRequest({
    $core.String? id,
    NotificationRequestType? notificationRequestType,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (notificationRequestType != null)
      result.notificationRequestType = notificationRequestType;
    return result;
  }

  NotificationRequest._();

  factory NotificationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory NotificationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'NotificationRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aE<NotificationRequestType>(
        2, _omitFieldNames ? '' : 'notificationRequestType',
        protoName: 'notificationRequestType',
        enumValues: NotificationRequestType.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  NotificationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  NotificationRequest copyWith(void Function(NotificationRequest) updates) =>
      super.copyWith((message) => updates(message as NotificationRequest))
          as NotificationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static NotificationRequest create() => NotificationRequest._();
  @$core.override
  NotificationRequest createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static NotificationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<NotificationRequest>(create);
  static NotificationRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  NotificationRequestType get notificationRequestType => $_getN(1);
  @$pb.TagNumber(2)
  set notificationRequestType(NotificationRequestType value) =>
      $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasNotificationRequestType() => $_has(1);
  @$pb.TagNumber(2)
  void clearNotificationRequestType() => $_clearField(2);
}

class NotificationResponse extends $pb.GeneratedMessage {
  factory NotificationResponse({
    $core.String? id,
    $core.Iterable<Notification>? notificationlist,
  }) {
    final result = create();
    if (id != null) result.id = id;
    if (notificationlist != null)
      result.notificationlist.addAll(notificationlist);
    return result;
  }

  NotificationResponse._();

  factory NotificationResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory NotificationResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'NotificationResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..pPM<Notification>(2, _omitFieldNames ? '' : 'notificationlist',
        subBuilder: Notification.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  NotificationResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  NotificationResponse copyWith(void Function(NotificationResponse) updates) =>
      super.copyWith((message) => updates(message as NotificationResponse))
          as NotificationResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static NotificationResponse create() => NotificationResponse._();
  @$core.override
  NotificationResponse createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static NotificationResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<NotificationResponse>(create);
  static NotificationResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<Notification> get notificationlist => $_getList(1);
}

class Notification extends $pb.GeneratedMessage {
  factory Notification({
    $core.String? title,
    $core.String? message,
    $core.String? comments,
    $core.String? securityExchange,
    TypeState? typeState,
    Component? component,
    Level? level,
    $0.Timestamp? time,
  }) {
    final result = create();
    if (title != null) result.title = title;
    if (message != null) result.message = message;
    if (comments != null) result.comments = comments;
    if (securityExchange != null) result.securityExchange = securityExchange;
    if (typeState != null) result.typeState = typeState;
    if (component != null) result.component = component;
    if (level != null) result.level = level;
    if (time != null) result.time = time;
    return result;
  }

  Notification._();

  factory Notification.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory Notification.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Notification',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'messages'),
      createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'title')
    ..aOS(2, _omitFieldNames ? '' : 'message')
    ..aOS(3, _omitFieldNames ? '' : 'comments')
    ..aOS(4, _omitFieldNames ? '' : 'securityExchange',
        protoName: 'securityExchange')
    ..aE<TypeState>(5, _omitFieldNames ? '' : 'typeState',
        protoName: 'typeState', enumValues: TypeState.values)
    ..aE<Component>(6, _omitFieldNames ? '' : 'component',
        enumValues: Component.values)
    ..aE<Level>(7, _omitFieldNames ? '' : 'level', enumValues: Level.values)
    ..aOM<$0.Timestamp>(60, _omitFieldNames ? '' : 'time',
        subBuilder: $0.Timestamp.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Notification clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Notification copyWith(void Function(Notification) updates) =>
      super.copyWith((message) => updates(message as Notification))
          as Notification;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Notification create() => Notification._();
  @$core.override
  Notification createEmptyInstance() => create();
  @$core.pragma('dart2js:noInline')
  static Notification getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Notification>(create);
  static Notification? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get title => $_getSZ(0);
  @$pb.TagNumber(1)
  set title($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTitle() => $_has(0);
  @$pb.TagNumber(1)
  void clearTitle() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get message => $_getSZ(1);
  @$pb.TagNumber(2)
  set message($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMessage() => $_has(1);
  @$pb.TagNumber(2)
  void clearMessage() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get comments => $_getSZ(2);
  @$pb.TagNumber(3)
  set comments($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasComments() => $_has(2);
  @$pb.TagNumber(3)
  void clearComments() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get securityExchange => $_getSZ(3);
  @$pb.TagNumber(4)
  set securityExchange($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasSecurityExchange() => $_has(3);
  @$pb.TagNumber(4)
  void clearSecurityExchange() => $_clearField(4);

  @$pb.TagNumber(5)
  TypeState get typeState => $_getN(4);
  @$pb.TagNumber(5)
  set typeState(TypeState value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasTypeState() => $_has(4);
  @$pb.TagNumber(5)
  void clearTypeState() => $_clearField(5);

  @$pb.TagNumber(6)
  Component get component => $_getN(5);
  @$pb.TagNumber(6)
  set component(Component value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasComponent() => $_has(5);
  @$pb.TagNumber(6)
  void clearComponent() => $_clearField(6);

  @$pb.TagNumber(7)
  Level get level => $_getN(6);
  @$pb.TagNumber(7)
  set level(Level value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasLevel() => $_has(6);
  @$pb.TagNumber(7)
  void clearLevel() => $_clearField(7);

  @$pb.TagNumber(60)
  $0.Timestamp get time => $_getN(7);
  @$pb.TagNumber(60)
  set time($0.Timestamp value) => $_setField(60, value);
  @$pb.TagNumber(60)
  $core.bool hasTime() => $_has(7);
  @$pb.TagNumber(60)
  void clearTime() => $_clearField(60);
  @$pb.TagNumber(60)
  $0.Timestamp ensureTime() => $_ensure(7);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
