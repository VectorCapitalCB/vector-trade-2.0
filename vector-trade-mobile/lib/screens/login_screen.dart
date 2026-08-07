import 'package:flutter/material.dart';
import 'package:flutter_secure_storage/flutter_secure_storage.dart';

import '../config.dart';
import '../store.dart';
import '../theme.dart';
import 'blotter_screen.dart';

/// Equivalente movil de view/Login.fxml.
class LoginScreen extends StatefulWidget {
  final BlotterStore store;

  const LoginScreen({super.key, required this.store});

  @override
  State<LoginScreen> createState() => _LoginScreenState();
}

class _LoginScreenState extends State<LoginScreen> {
  static const _storage = FlutterSecureStorage();

  final _user = TextEditingController();
  final _password = TextEditingController();
  // En dos columnas el recorrido espacial salta de Usuario a Ambiente, asi que
  // el "siguiente" del teclado se encadena a mano.
  final _passwordFocus = FocusNode();
  String _env = Env.all.first.key;
  bool _remember = false;
  bool _busy = false;
  String _status = '';

  @override
  void initState() {
    super.initState();
    _loadCredentials();
  }

  @override
  void dispose() {
    _user.dispose();
    _password.dispose();
    _passwordFocus.dispose();
    super.dispose();
  }

  Future<void> _loadCredentials() async {
    final saved = await _storage.read(key: 'credentials');
    if (saved == null || !mounted) return;
    final parts = saved.split(':');
    if (parts.length != 3) return;
    setState(() {
      _user.text = parts[0];
      _password.text = parts[1];
      _env = Env.byKey(parts[2]).key;
      _remember = true;
    });
  }

  Future<void> _login() async {
    // AES/PKCS7 revienta con RangeError si el texto viene vacio.
    if (_user.text.trim().isEmpty || _password.text.isEmpty) {
      setState(() => _status = 'Ingresa usuario y contrasena.');
      return;
    }

    setState(() {
      _busy = true;
      _status = 'Conectando...';
    });

    try {
      await widget.store.login(_user.text, _password.text, _env);

      if (_remember) {
        await _storage.write(
          key: 'credentials',
          value: '${_user.text}:${_password.text}:$_env',
        );
      } else {
        await _storage.delete(key: 'credentials');
      }

      if (!mounted) return;
      Navigator.of(context).pushReplacement(
        MaterialPageRoute(builder: (_) => BlotterScreen(store: widget.store)),
      );
    } catch (e) {
      if (!mounted) return;
      setState(() {
        _busy = false;
        _status = _describe(e);
      });
    }
  }

  String _describe(Object e) {
    final text = e.toString();
    if (text.contains('401')) return 'El usuario o contrasena no son validos.';
    if (text.contains('TimeoutException')) return 'El servicio no responde.';
    return text;
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Container(
        decoration: const BoxDecoration(
          gradient: LinearGradient(
            begin: Alignment.topCenter,
            end: Alignment.bottomCenter,
            colors: [VT.bgTop, VT.bgBottom],
          ),
        ),
        child: SafeArea(
          child: Center(
            child: SingleChildScrollView(
              padding: const EdgeInsets.all(24),
              child: ConstrainedBox(
                constraints: const BoxConstraints(maxWidth: 560),
                child: _card(),
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _card() {
    return Container(
      padding: const EdgeInsets.fromLTRB(30, 28, 30, 24),
      decoration: BoxDecoration(
        color: VT.surface.withValues(alpha: 0.97),
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: VT.border),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Image.asset('assets/icono.png', height: 34, width: 34),
              const SizedBox(width: 10),
              const Text(
                'Vector Trade 2.0',
                style: TextStyle(
                  fontSize: 24,
                  fontWeight: FontWeight.bold,
                  color: VT.text,
                ),
              ),
            ],
          ),
          const SizedBox(height: 4),
          const Text(
            'Acceso a Plataforma',
            style: TextStyle(fontSize: 12, color: VT.textDim),
          ),
          const SizedBox(height: 14),
          // Dos columnas: en horizontal la tarjeta vertical no cabe en pantalla.
          // El orden explicito evita que Tab salte de Usuario a Ambiente, que
          // es lo que hace el recorrido espacial por defecto.
          FocusTraversalGroup(
            policy: OrderedTraversalPolicy(),
            child: Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      FocusTraversalOrder(
                        order: const NumericFocusOrder(1),
                        child: _field(
                          'Usuario',
                          _user,
                          action: TextInputAction.next,
                          onSubmitted: (_) => _passwordFocus.requestFocus(),
                        ),
                      ),
                      const SizedBox(height: 12),
                      FocusTraversalOrder(
                        order: const NumericFocusOrder(2),
                        child: _field(
                          'Contrasena',
                          _password,
                          obscure: true,
                          focusNode: _passwordFocus,
                          action: TextInputAction.done,
                          onSubmitted: (_) => _login(),
                        ),
                      ),
                    ],
                  ),
                ),
                const SizedBox(width: 20),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('Ambiente', style: VT.labelStyle),
                      const SizedBox(height: 6),
                      FocusTraversalOrder(
                        order: const NumericFocusOrder(3),
                        child: DropdownButtonFormField<String>(
                          initialValue: _env,
                          dropdownColor: VT.field,
                          items: [
                            for (final env in Env.all)
                              DropdownMenuItem(
                                value: env.key,
                                child: Text(env.key),
                              ),
                          ],
                          onChanged: _busy
                              ? null
                              : (v) => setState(() => _env = v!),
                        ),
                      ),
                      Row(
                        children: [
                          FocusTraversalOrder(
                            order: const NumericFocusOrder(4),
                            child: Checkbox(
                              value: _remember,
                              onChanged: (v) =>
                                  setState(() => _remember = v ?? false),
                              side: const BorderSide(color: VT.borderSoft),
                            ),
                          ),
                          const Expanded(
                            child: Text(
                              'Guardar usuario y contrasena',
                              style: TextStyle(
                                fontSize: 12,
                                fontWeight: FontWeight.bold,
                                color: VT.label,
                              ),
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
          const SizedBox(height: 10),
          SizedBox(
            height: 36,
            width: double.infinity,
            child: DecoratedBox(
              decoration: BoxDecoration(
                gradient: const LinearGradient(
                  begin: Alignment.topCenter,
                  end: Alignment.bottomCenter,
                  colors: [VT.btnTop, VT.btnBottom],
                ),
                borderRadius: BorderRadius.circular(6),
                border: Border.all(color: VT.btnBorder),
              ),
              child: TextButton(
                onPressed: _busy ? null : _login,
                child: _busy
                    ? const SizedBox(
                        height: 16,
                        width: 16,
                        child: CircularProgressIndicator(
                          strokeWidth: 2,
                          color: VT.text,
                        ),
                      )
                    : const Text(
                        'Ingresar',
                        style: TextStyle(color: Color(0xFFEAF1FC)),
                      ),
              ),
            ),
          ),
          const SizedBox(height: 10),
          Text(
            _status,
            style: const TextStyle(fontSize: 11, color: Color(0xFF9AA5B5)),
          ),
        ],
      ),
    );
  }

  Widget _field(
    String label,
    TextEditingController controller, {
    bool obscure = false,
    FocusNode? focusNode,
    TextInputAction? action,
    ValueChanged<String>? onSubmitted,
  }) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(label, style: VT.labelStyle),
        const SizedBox(height: 6),
        TextField(
          controller: controller,
          focusNode: focusNode,
          obscureText: obscure,
          enabled: !_busy,
          autocorrect: false,
          enableSuggestions: false,
          textInputAction: action,
          onSubmitted: onSubmitted,
          style: const TextStyle(color: VT.textStrong),
          decoration: InputDecoration(hintText: label),
        ),
      ],
    );
  }
}
