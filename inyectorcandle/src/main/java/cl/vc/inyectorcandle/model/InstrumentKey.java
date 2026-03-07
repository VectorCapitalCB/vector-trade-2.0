package cl.vc.inyectorcandle.model;

public record InstrumentKey(String symbol, String settlement, String destination, String currency, String securityType) {

    public InstrumentKey {
        symbol = normalize(symbol, "UNKNOWN_SYMBOL");
        settlement = normalize(settlement, "UNKNOWN_SETTL");
        destination = normalize(destination, "UNKNOWN_DEST");
        currency = normalize(currency, "UNKNOWN_CCY");
        securityType = normalize(securityType, "UNKNOWN_SECTYPE");
    }

    public String id() {
        return symbol + "|" + settlement + "|" + destination + "|" + currency + "|" + securityType;
    }

    public static InstrumentKey fromValues(String symbol, String settlement, String destination, String currency) {
        return new InstrumentKey(symbol, settlement, destination, currency, null);
    }

    public static InstrumentKey fromValues(String symbol, String settlement, String destination, String currency, String securityType) {
        return new InstrumentKey(symbol, settlement, destination, currency, securityType);
    }

    private static String normalize(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value.trim().toUpperCase();
    }
}
