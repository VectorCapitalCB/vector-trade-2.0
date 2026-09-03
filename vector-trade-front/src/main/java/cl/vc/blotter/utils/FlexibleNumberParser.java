package cl.vc.blotter.utils;

import java.math.BigDecimal;

public final class FlexibleNumberParser {

    private FlexibleNumberParser() {
    }

    public static double parse(String value) {
        if (value == null) {
            return 0d;
        }

        String cleaned = value.trim();
        if (cleaned.isEmpty()) {
            return 0d;
        }

        cleaned = cleaned.replace("\u00A0", "").replace(" ", "");
        cleaned = cleaned.replaceAll("[^0-9,.-]", "");
        if (cleaned.isEmpty()) {
            return 0d;
        }

        int commaCount = cleaned.length() - cleaned.replace(",", "").length();
        int dotCount = cleaned.length() - cleaned.replace(".", "").length();

        if (commaCount > 0 && dotCount > 0) {
            if (cleaned.lastIndexOf('.') > cleaned.lastIndexOf(',')) {
                cleaned = cleaned.replace(",", "");
            } else {
                cleaned = cleaned.replace(".", "").replace(",", ".");
            }
        } else if (commaCount > 0) {
            cleaned = normalizeSingleSeparator(cleaned, ',', commaCount);
        } else if (dotCount > 0) {
            cleaned = normalizeSingleSeparator(cleaned, '.', dotCount);
        }

        return Double.parseDouble(cleaned);
    }

    public static BigDecimal parseBigDecimal(String value) {
        return BigDecimal.valueOf(parse(value));
    }

    /**
     * Parseo estricto para los campos numericos del blotter (precio, limite, spread, cantidad).
     *
     * <p>Estos campos los escribe el propio front en Locale.US ({@code formatearNumero},
     * {@code BigDecimal.toPlainString}), asi que el PUNTO ES SIEMPRE DECIMAL y la coma es
     * separador de miles. Replica la semantica de produccion
     * ({@code Double.parseDouble(texto.replace(",", ""))}).
     *
     * <p>No confundir con {@link #parse(String)}, que aplica heuristica de formato chileno
     * (punto como miles) y existe para importar planillas pegadas del portapapeles. Usar
     * {@code parse} en un campo de precio corrompe los instrumentos de 3 decimales por un
     * factor 1000: {@code parse("0.001") == 1.0}.
     */
    public static double parseBlotterField(String value) {
        String cleaned = cleanBlotterField(value);
        return cleaned.isEmpty() ? 0d : Double.parseDouble(cleaned);
    }

    /**
     * Variante {@link BigDecimal} de {@link #parseBlotterField(String)}, construida desde el
     * texto para conservar la escala del campo (produccion usa {@code new BigDecimal(...)}).
     */
    public static BigDecimal parseBlotterFieldBigDecimal(String value) {
        String cleaned = cleanBlotterField(value);
        return cleaned.isEmpty() ? BigDecimal.ZERO : new BigDecimal(cleaned);
    }

    private static String cleanBlotterField(String value) {
        if (value == null) {
            return "";
        }
        String cleaned = value.trim()
                .replace("\u00A0", "")
                .replace(" ", "")
                .replace(",", "");
        cleaned = cleaned.replaceAll("[^0-9.-]", "");
        return cleaned.equals("-") || cleaned.equals(".") ? "" : cleaned;
    }

    private static String normalizeSingleSeparator(String value, char separator, int count) {
        String token = String.valueOf(separator);
        if (count > 1) {
            return value.replace(token, "");
        }

        int index = value.indexOf(separator);
        String before = value.substring(0, index);
        String after = value.substring(index + 1);
        boolean thousands = before.replace("-", "").matches("\\d+")
                && after.matches("\\d{3}")
                && before.replace("-", "").length() <= 3;

        if (thousands) {
            return before + after;
        }
        return separator == ',' ? before + "." + after : value;
    }
}
