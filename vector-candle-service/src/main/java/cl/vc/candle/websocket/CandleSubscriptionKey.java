package cl.vc.candle.websocket;

import java.util.Objects;

public class CandleSubscriptionKey {
    private final String symbol;
    private final String timeframe;

    public CandleSubscriptionKey(String symbol, String timeframe) {
        this.symbol = symbol == null ? "" : symbol.trim();
        this.timeframe = timeframe == null ? "" : timeframe.trim();
    }

    public String getSymbol() {
        return symbol;
    }

    public String getTimeframe() {
        return timeframe;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CandleSubscriptionKey that)) return false;
        return Objects.equals(symbol, that.symbol) && Objects.equals(timeframe, that.timeframe);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, timeframe);
    }
}
