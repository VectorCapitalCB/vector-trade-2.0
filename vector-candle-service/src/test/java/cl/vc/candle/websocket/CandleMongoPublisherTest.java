package cl.vc.candle.websocket;

import org.bson.Document;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CandleMongoPublisherTest {

    @Test
    void parsesDailyOhlcvFromClosePriceDocument() {
        Document document = new Document("protobufData", new JSONObject()
                .put("close", 66_950d)
                .put("ohlcv", new JSONObject()
                        .put("open", 66_301d)
                        .put("high", 67_000d)
                        .put("low", 66_000d)
                        .put("close", 66_950d)
                        .put("volume", 228_176d))
                .toString());

        JSONObject row = CandleMongoPublisher.closePriceRow(document, "SQM-B", "2026-08-10");

        assertNotNull(row);
        assertEquals("SQM-B", row.getString("symbol"));
        assertEquals("2026-08-10", row.getString("date"));
        assertEquals(66_301d, row.getDouble("open"));
        assertEquals(67_000d, row.getDouble("high"));
        assertEquals(66_000d, row.getDouble("low"));
        assertEquals(66_950d, row.getDouble("close"));
        assertEquals(228_176d, row.getDouble("volume"));
    }

    @Test
    void fallsBackToCloseForIncompleteCurrentDay() {
        Document document = new Document("protobufData", new JSONObject()
                .put("close", 66_950d)
                .put("last", 66_950d)
                .put("ohlcv", new JSONObject().put("close", 66_950d))
                .toString());

        JSONObject row = CandleMongoPublisher.closePriceRow(document, "SQM-B", "2026-08-11");

        assertNotNull(row);
        assertEquals(66_950d, row.getDouble("open"));
        assertEquals(66_950d, row.getDouble("high"));
        assertEquals(66_950d, row.getDouble("low"));
        assertEquals(66_950d, row.getDouble("close"));
    }
}
