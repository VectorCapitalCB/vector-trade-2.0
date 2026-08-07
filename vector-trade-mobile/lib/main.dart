import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import 'screens/login_screen.dart';
import 'store.dart';
import 'theme.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // El blotter es una tabla ancha: en vertical no cabe ninguna columna util.
  SystemChrome.setPreferredOrientations([
    DeviceOrientation.landscapeLeft,
    DeviceOrientation.landscapeRight,
  ]);
  runApp(const VectorTradeMobile());
}

class VectorTradeMobile extends StatefulWidget {
  const VectorTradeMobile({super.key});

  @override
  State<VectorTradeMobile> createState() => _VectorTradeMobileState();
}

class _VectorTradeMobileState extends State<VectorTradeMobile> {
  final _store = BlotterStore();

  @override
  void dispose() {
    _store.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Vector Trade',
      debugShowCheckedModeBanner: false,
      theme: VT.theme(),
      home: LoginScreen(store: _store),
    );
  }
}
