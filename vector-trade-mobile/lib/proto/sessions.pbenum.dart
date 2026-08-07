// This is a generated file - do not edit.
//
// Generated from sessions.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class Enviroment extends $pb.ProtobufEnum {
  static const Enviroment PRODUCTION =
      Enviroment._(0, _omitEnumNames ? '' : 'PRODUCTION');
  static const Enviroment TEST = Enviroment._(1, _omitEnumNames ? '' : 'TEST');
  static const Enviroment LOCALHOST =
      Enviroment._(2, _omitEnumNames ? '' : 'LOCALHOST');
  static const Enviroment PRODUCTION_VPN =
      Enviroment._(3, _omitEnumNames ? '' : 'PRODUCTION_VPN');
  static const Enviroment QA = Enviroment._(4, _omitEnumNames ? '' : 'QA');
  static const Enviroment ARB = Enviroment._(5, _omitEnumNames ? '' : 'ARB');

  static const $core.List<Enviroment> values = <Enviroment>[
    PRODUCTION,
    TEST,
    LOCALHOST,
    PRODUCTION_VPN,
    QA,
    ARB,
  ];

  static final $core.List<Enviroment?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static Enviroment? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const Enviroment._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
