package cl.vc.candle.websocket;

import org.bson.Document;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CandleProtoMarketPublisherTest {
    private static final ZoneId SANTIAGO = ZoneId.of("America/Santiago");

    @Test
    void buildsHourlyOhlcCandlesAnchoredAtMarketOpen() {
        JSONArray rows = CandleProtoMarketPublisher.buildTradeCandleRows(List.of(
                trade("2026-08-11T09:05:00-04:00", 100d, 10d),
                trade("2026-08-11T09:35:00-04:00", 105d, 5d),
                trade("2026-08-11T10:04:00-04:00", 98d, 7d),
                trade("2026-08-11T10:05:00-04:00", 110d, 3d)
        ), LocalDate.of(2026, 8, 11), SANTIAGO, 60);

        assertEquals(2, rows.length());
        assertEquals(100d, rows.getJSONObject(0).getDouble("open"));
        assertEquals(105d, rows.getJSONObject(0).getDouble("high"));
        assertEquals(98d, rows.getJSONObject(0).getDouble("low"));
        assertEquals(98d, rows.getJSONObject(0).getDouble("close"));
        assertEquals(98d, rows.getJSONObject(0).getDouble("last"));
        assertEquals(22d, rows.getJSONObject(0).getDouble("volume"));
        assertEquals(110d, rows.getJSONObject(1).getDouble("close"));
    }

    @Test
    void buildsFourHourCandlesAndIgnoresTradesOutsideSession() {
        JSONArray rows = CandleProtoMarketPublisher.buildTradeCandleRows(List.of(
                trade("2026-08-11T09:00:00-04:00", 90d, 1d),
                trade("2026-08-11T09:05:00-04:00", 100d, 10d),
                trade("2026-08-11T13:04:00-04:00", 120d, 5d),
                trade("2026-08-11T13:05:00-04:00", 115d, 2d)
        ), LocalDate.of(2026, 8, 11), SANTIAGO, 240);

        assertEquals(2, rows.length());
        assertEquals(100d, rows.getJSONObject(0).getDouble("open"));
        assertEquals(120d, rows.getJSONObject(0).getDouble("close"));
        assertEquals(115d, rows.getJSONObject(1).getDouble("open"));
    }

    @Test
    void buildsThirtyMinuteCandlesFromIntradayLastTrades() {
        JSONArray rows = CandleProtoMarketPublisher.buildTradeCandleRows(List.of(
                trade("2026-08-11T09:05:00-04:00", 100d, 10d),
                trade("2026-08-11T09:34:00-04:00", 102d, 5d),
                trade("2026-08-11T09:35:00-04:00", 101d, 7d)
        ), LocalDate.of(2026, 8, 11), SANTIAGO, 30);

        assertEquals(2, rows.length());
        assertEquals(102d, rows.getJSONObject(0).getDouble("last"));
        assertEquals(101d, rows.getJSONObject(1).getDouble("last"));
    }

    @Test
    void buildsOneFiveAndFifteenMinuteCandlesFromNineFive() {
        List<Document> trades = List.of(
                trade("2026-08-11T09:05:00-04:00", 100d, 10d),
                trade("2026-08-11T09:09:59-04:00", 102d, 5d),
                trade("2026-08-11T09:10:00-04:00", 101d, 7d),
                trade("2026-08-11T09:15:00-04:00", 103d, 4d)
        );

        JSONArray fiveMinutes = CandleProtoMarketPublisher.buildTradeCandleRows(
                trades, LocalDate.of(2026, 8, 11), SANTIAGO, 5);
        JSONArray oneMinute = CandleProtoMarketPublisher.buildTradeCandleRows(
                trades, LocalDate.of(2026, 8, 11), SANTIAGO, 1);
        JSONArray fifteenMinutes = CandleProtoMarketPublisher.buildTradeCandleRows(
                trades, LocalDate.of(2026, 8, 11), SANTIAGO, 15);

        assertEquals(3, fiveMinutes.length());
        assertEquals(4, oneMinute.length());
        assertEquals(1, fifteenMinutes.length());
        assertEquals(102d, fiveMinutes.getJSONObject(0).getDouble("last"));
        assertEquals(103d, fifteenMinutes.getJSONObject(0).getDouble("last"));
    }

    @Test
    void rebuildsPreviousTradingDaysFromStoredMinuteCandles() {
        JSONArray rows = CandleProtoMarketPublisher.buildStoredCandleRows(List.of(
                stored("2026-08-10T13:05:00Z", 100d, 105d, 99d, 103d, 10d),
                stored("2026-08-10T17:04:00Z", 103d, 110d, 102d, 108d, 20d),
                stored("2026-08-10T17:05:00Z", 108d, 109d, 104d, 105d, 30d),
                stored("2026-08-11T13:05:00Z", 200d, 200d, 200d, 200d, 1d)
        ), LocalDate.of(2026, 8, 11), SANTIAGO, 240);

        assertEquals(2, rows.length());
        assertEquals(100d, rows.getJSONObject(0).getDouble("open"));
        assertEquals(110d, rows.getJSONObject(0).getDouble("high"));
        assertEquals(108d, rows.getJSONObject(0).getDouble("last"));
        assertEquals(105d, rows.getJSONObject(1).getDouble("last"));
    }

    private Document trade(String eventTime, double price, double quantity) {
        return new Document("symbol", "SQM-B")
                .append("eventTime", eventTime)
                .append("price", price)
                .append("quantity", quantity);
    }

    private Document stored(String bucketStart, double open, double high, double low,
                            double close, double volume) {
        return new Document("bucketStart", bucketStart)
                .append("open", open)
                .append("high", high)
                .append("low", low)
                .append("close", close)
                .append("volume", volume);
    }
}
