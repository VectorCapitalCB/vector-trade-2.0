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
