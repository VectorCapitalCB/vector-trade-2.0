/// Endpoints por ambiente, espejo de
/// vector-trade-front/src/main/resources/blotter/enviroment/application.*.properties
class Env {
  final String key;
  final String service;

  const Env(this.key, this.service);

  static const all = <Env>[
    Env('production', 'ws://68.211.112.146:8086/websocket/'),
    Env('qa', 'ws://172.16.0.8:8096/websocket/'),
    Env('arb', 'ws://68.211.112.146:8095/websocket/'),
  ];

  static Env byKey(String key) =>
      all.firstWhere((e) => e.key == key, orElse: () => all.first);
}
