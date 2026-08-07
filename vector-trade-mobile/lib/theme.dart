import 'package:flutter/material.dart';

/// Paleta tomada de vector-trade-front (blotter/css/style.css y Login.fxml)
/// para que el movil se vea como el escritorio.
class VT {
  static const bgTop = Color(0xFF111317);
  static const bgBottom = Color(0xFF191D23);
  static const surface = Color(0xFF1A1E24);
  static const field = Color(0xFF21262F);
  static const border = Color(0xFF2E3642);
  static const borderSoft = Color(0xFF343D4B);

  static const text = Color(0xFFE5EAF2);
  static const textStrong = Color(0xFFF4F7FB);
  static const textDim = Color(0xFF9DA7B8);
  static const label = Color(0xFFB9C2D1);
  static const hint = Color(0xFF788395);

  static const accent = Color(0xFF9FC5EF);
  static const link = Color(0xFF7ED1FF);
  static const brand = Color(0xFF2B3178);

  static const bid = Color(0xFF18FC00);
  static const ask = Color(0xFFE50606);
  static const gold = Color(0xFFFFD700);

  // Tabla: mismos valores que .table-view / .table-row-cell de style.css.
  static const tableBg = Color(0xFF1A1E24);
  static const tableHeader = Color(0xFF1F242C);
  static const rowOdd = Color(0xFF21262F);
  static const rowEven = Color(0xFF1D222A);
  static const rowSelected = Color(0xFF2E4A6D);

  // Colores de celda de ExecutionsController.
  static const sideBuy = Color(0xFF008000); // green
  static const sideSell = Color(0xFFFF0000); // red
  static const statusNew = Color(0xFF00BFFF);
  static const statusFilled = Color(0xFF008000);
  static const statusPartial = Color(0xFFE7CB0D);
  static const statusReplaced = Color(0xFFEA6F08);

  static const btnTop = Color(0xFF2E4A6D);
  static const btnBottom = Color(0xFF22344D);
  static const btnBorder = Color(0xFF4B6280);

  static ThemeData theme() {
    final base = ThemeData.dark(useMaterial3: true);
    return base.copyWith(
      scaffoldBackgroundColor: bgBottom,
      colorScheme: base.colorScheme.copyWith(
        primary: accent,
        surface: surface,
        onSurface: text,
      ),
      appBarTheme: const AppBarTheme(
        backgroundColor: surface,
        foregroundColor: text,
        elevation: 0,
        surfaceTintColor: Colors.transparent,
      ),
      inputDecorationTheme: InputDecorationTheme(
        filled: true,
        fillColor: field,
        hintStyle: const TextStyle(color: hint, fontSize: 13),
        contentPadding: const EdgeInsets.symmetric(
          horizontal: 12,
          vertical: 12,
        ),
        border: OutlineInputBorder(
          borderRadius: BorderRadius.circular(5),
          borderSide: const BorderSide(color: borderSoft),
        ),
        enabledBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(5),
          borderSide: const BorderSide(color: borderSoft),
        ),
        focusedBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(5),
          borderSide: const BorderSide(color: accent),
        ),
      ),
      textTheme: base.textTheme.apply(bodyColor: text, displayColor: text),
      dividerColor: border,
    );
  }

  static const labelStyle = TextStyle(
    fontSize: 12,
    fontWeight: FontWeight.bold,
    color: label,
  );
}
