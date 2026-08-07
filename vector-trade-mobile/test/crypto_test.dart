import 'package:flutter_test/flutter_test.dart';
import 'package:vector_trade_mobile/crypto.dart';

void main() {
  // Valor de referencia producido por cl.vc.module.protocolbuff.crypt.AESEncryption
  // (Java) para el mismo par usuario/contrasena. Si esto cambia, el OMS responde 401.
  test('basicAuth coincide con AESEncryption de Java', () {
    expect(
      AesVt.basicAuth('vnazar', 's3cr3t-demo'),
      'Basic WnpBWmEyZUhHaXk1dWxqcnpMNmxLZz09OmRjdUpOV1RjODJQTFEyYnJWc29CaVE9PQ==',
    );
  });
}
