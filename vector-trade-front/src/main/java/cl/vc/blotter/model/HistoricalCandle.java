package cl.vc.blotter.model;

import java.time.LocalDate;

public record HistoricalCandle(
        String symbol,
        LocalDate date,
        double open,
        double high,
        double low,
        double close,
        double volume
) {
}
