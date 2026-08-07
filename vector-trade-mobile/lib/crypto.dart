import 'dart:convert';

import 'package:encrypt/encrypt.dart';

/// Equivalente Dart de cl.vc.module.protocolbuff.crypt.AESEncryption
/// (AES/ECB/PKCS5Padding, clave estatica de 16 bytes). Debe producir el mismo
/// texto que el escritorio o el OMS rechaza el upgrade con 401.
class AesVt {
  static final _encrypter = Encrypter(
    AES(Key.fromUtf8('YourSecretKey123'), mode: AESMode.ecb, padding: 'PKCS7'),
  );
  static final _iv = IV.fromLength(16); // ignorado en ECB

  static String encrypt(String value) =>
      _encrypter.encrypt(value, iv: _iv).base64;

  /// Header `Authorization: Basic ...` que espera el websocket del OMS.
  static String basicAuth(String username, String password) {
    final credentials = '${encrypt(username)}:${encrypt(password)}';
    return 'Basic ${base64.encode(utf8.encode(credentials))}';
  }
}
