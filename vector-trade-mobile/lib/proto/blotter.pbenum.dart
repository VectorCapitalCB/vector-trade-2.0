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

class TickAction extends $pb.ProtobufEnum {
  static const TickAction UNKNOWN_TICK_ACTION =
      TickAction._(0, _omitEnumNames ? '' : 'UNKNOWN_TICK_ACTION');
  static const TickAction UP = TickAction._(1, _omitEnumNames ? '' : 'UP');
  static const TickAction DOWN = TickAction._(2, _omitEnumNames ? '' : 'DOWN');

  static const $core.List<TickAction> values = <TickAction>[
    UNKNOWN_TICK_ACTION,
    UP,
    DOWN,
  ];

  static final $core.List<TickAction?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static TickAction? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const TickAction._(super.value, super.name);
}

class StatusPortfolio extends $pb.ProtobufEnum {
  static const StatusPortfolio NEW_PORTFOLIO =
      StatusPortfolio._(0, _omitEnumNames ? '' : 'NEW_PORTFOLIO');
  static const StatusPortfolio UPDATE_PORTFOLIO =
      StatusPortfolio._(1, _omitEnumNames ? '' : 'UPDATE_PORTFOLIO');
  static const StatusPortfolio DELETE_PORTFOLIO =
      StatusPortfolio._(2, _omitEnumNames ? '' : 'DELETE_PORTFOLIO');
  static const StatusPortfolio SNAPSHOT_PORTFOLIO =
      StatusPortfolio._(3, _omitEnumNames ? '' : 'SNAPSHOT_PORTFOLIO');
  static const StatusPortfolio ADD_ASSET =
      StatusPortfolio._(4, _omitEnumNames ? '' : 'ADD_ASSET');
  static const StatusPortfolio REMOVE_ASSET =
      StatusPortfolio._(5, _omitEnumNames ? '' : 'REMOVE_ASSET');
  static const StatusPortfolio UPDATE_ASSET =
      StatusPortfolio._(6, _omitEnumNames ? '' : 'UPDATE_ASSET');
  static const StatusPortfolio ALL_PORTFOLIO =
      StatusPortfolio._(7, _omitEnumNames ? '' : 'ALL_PORTFOLIO');

  static const $core.List<StatusPortfolio> values = <StatusPortfolio>[
    NEW_PORTFOLIO,
    UPDATE_PORTFOLIO,
    DELETE_PORTFOLIO,
    SNAPSHOT_PORTFOLIO,
    ADD_ASSET,
    REMOVE_ASSET,
    UPDATE_ASSET,
    ALL_PORTFOLIO,
  ];

  static final $core.List<StatusPortfolio?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 7);
  static StatusPortfolio? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const StatusPortfolio._(super.value, super.name);
}

class StatusPreselect extends $pb.ProtobufEnum {
  static const StatusPreselect SNAPSHOT_PRESELECT =
      StatusPreselect._(0, _omitEnumNames ? '' : 'SNAPSHOT_PRESELECT');
  static const StatusPreselect ADD_PRESELECT =
      StatusPreselect._(1, _omitEnumNames ? '' : 'ADD_PRESELECT');
  static const StatusPreselect REMOVE_PRESELECT =
      StatusPreselect._(2, _omitEnumNames ? '' : 'REMOVE_PRESELECT');

  static const $core.List<StatusPreselect> values = <StatusPreselect>[
    SNAPSHOT_PRESELECT,
    ADD_PRESELECT,
    REMOVE_PRESELECT,
  ];

  static final $core.List<StatusPreselect?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static StatusPreselect? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const StatusPreselect._(super.value, super.name);
}

class StatusUser extends $pb.ProtobufEnum {
  static const StatusUser UPDATE_USER =
      StatusUser._(0, _omitEnumNames ? '' : 'UPDATE_USER');
  static const StatusUser ADD_USER =
      StatusUser._(1, _omitEnumNames ? '' : 'ADD_USER');
  static const StatusUser REMOVE_USER =
      StatusUser._(2, _omitEnumNames ? '' : 'REMOVE_USER');
  static const StatusUser SNAPSHOT_USER =
      StatusUser._(3, _omitEnumNames ? '' : 'SNAPSHOT_USER');

  static const $core.List<StatusUser> values = <StatusUser>[
    UPDATE_USER,
    ADD_USER,
    REMOVE_USER,
    SNAPSHOT_USER,
  ];

  static final $core.List<StatusUser?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static StatusUser? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const StatusUser._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
