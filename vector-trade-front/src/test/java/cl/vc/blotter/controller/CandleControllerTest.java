package cl.vc.blotter.controller;

import cl.vc.blotter.model.HistoricalCandle;
import cl.vc.blotter.model.TradeCandle;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.jfree.data.Range;

import java.awt.Color;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CandleControllerTest {
    private static final ZoneId SANTIAGO = ZoneId.of("America/Santiago");

    @Test
    void noInventaVelasParaMinutosSinOperaciones() {
        CandleController controller = new CandleController();
        MarketDataMessage.TradeGeneral trade = tradeAt(10, 17, 1026.0, 50.0);

        CandleController.DatasetBuildResult result = controller.buildDatasetFromTrades(List.of(trade), 1);

        assertEquals(1, result.items.size());
        assertEquals(1026.0, result.items.get(0).getOpen().doubleValue());
        assertEquals(1026.0, result.items.get(0).getClose().doubleValue());
        assertEquals(result.firstBucket, result.lastBucket);
    }

    @Test
    void agrupaSoloLasOperacionesRealesDeCadaMinuto() {
        CandleController controller = new CandleController();

        CandleController.DatasetBuildResult result = controller.buildDatasetFromTrades(List.of(
                tradeAt(10, 17, 1026.0, 50.0),
                tradeAt(10, 17, 1027.0, 25.0),
                tradeAt(10, 20, 1025.0, 10.0)
        ), 1);

        assertEquals(2, result.items.size());
        assertEquals(1026.0, result.items.get(0).getOpen().doubleValue());
        assertEquals(1027.0, result.items.get(0).getClose().doubleValue());
        assertEquals(75.0, result.items.get(0).getVolume().doubleValue());
        assertEquals(1025.0, result.items.get(1).getClose().doubleValue());
    }

    @Test
    void agrupaCincoHorasDesdeLaAperturaBcsDeLasNueveCinco() {
        CandleController controller = new CandleController();

        CandleController.DatasetBuildResult result = controller.buildDatasetFromTrades(List.of(
                tradeAt(9, 5, 25.00, 10.0),
                tradeAt(14, 4, 24.50, 20.0),
                tradeAt(14, 5, 24.40, 30.0)
        ), 300);

        assertEquals(2, result.items.size());
        assertEquals(LocalDateTime.of(2026, 8, 11, 9, 5).atZone(SANTIAGO).toInstant(), result.firstBucket);
        assertEquals(LocalDateTime.of(2026, 8, 11, 14, 5).atZone(SANTIAGO).toInstant(), result.lastBucket);
        assertEquals(24.50, result.items.get(0).getClose().doubleValue());
        assertEquals(24.40, result.items.get(1).getClose().doubleValue());
    }

    @Test
    void construyeVelasDiariasDesdeClosePrices() {
        CandleController controller = new CandleController("SQM-B");

        CandleController.DatasetBuildResult result = controller.buildDatasetFromHistory(List.of(
                new HistoricalCandle("SQM-B", LocalDate.of(2026, 8, 7),
                        66_671d, 67_299d, 65_035d, 65_789d, 331_180d),
                new HistoricalCandle("SQM-B", LocalDate.of(2026, 8, 10),
                        66_301d, 67_000d, 66_000d, 66_950d, 228_176d)
        ));

        assertEquals(2, result.items.size());
        assertEquals(66_671d, result.items.get(0).getOpen().doubleValue());
        assertEquals(65_789d, result.items.get(0).getClose().doubleValue());
        assertEquals(66_950d, result.items.get(1).getClose().doubleValue());
        assertEquals(List.of(65_789d, 66_950d), result.closes);
    }

    @Test
    void construyeDatasetDesdeVelasIntradiaDelServidor() {
        CandleController controller = new CandleController("SQM-B");
        Instant first = LocalDateTime.of(2026, 8, 11, 9, 30).atZone(SANTIAGO).toInstant();

        CandleController.DatasetBuildResult result = controller.buildDatasetFromTradeCandles(List.of(
                new TradeCandle("SQM-B", 60, first, 100d, 106d, 98d, 104d, 25d),
                new TradeCandle("SQM-B", 60, first.plusSeconds(3600), 104d, 105d, 95d, 97d, 40d)
        ));

        assertEquals(2, result.items.size());
        assertEquals(100d, result.items.get(0).getOpen().doubleValue());
        assertEquals(104d, result.items.get(0).getClose().doubleValue());
        assertEquals(97d, result.items.get(1).getClose().doubleValue());
    }

    @Test
    void pintaBordeYMechaSegunDireccionDeCadaVela() {
        CandleController controller = new CandleController();
        Instant first = LocalDateTime.of(2026, 8, 11, 9, 30).atZone(SANTIAGO).toInstant();
        CandleController.DatasetBuildResult result = controller.buildDatasetFromTradeCandles(List.of(
                new TradeCandle("SQM-B", 60, first, 100d, 106d, 98d, 104d, 25d),
                new TradeCandle("SQM-B", 60, first.plusSeconds(3600), 104d, 105d, 95d, 97d, 40d)
        ));
        Color green = new Color(0x22, 0xc5, 0x5e);
        Color red = new Color(0xef, 0x44, 0x44);
        CandleController.DirectionalCandlestickRenderer renderer =
                new CandleController.DirectionalCandlestickRenderer(result.dataset, green, red);

        assertEquals(green, renderer.getItemOutlinePaint(0, 0));
        assertEquals(red, renderer.getItemOutlinePaint(0, 1));
    }

    @Test
    void laVistaIntradiaInicialUsaLaSesionDelUltimoDato() {
        CandleController controller = new CandleController();
        Instant latest = LocalDateTime.of(2026, 8, 11, 15, 5).atZone(SANTIAGO).toInstant();

        assertEquals(
                LocalDateTime.of(2026, 8, 11, 8, 50).atZone(SANTIAGO).toInstant(),
                controller.intradayRangeStart(latest));
        assertEquals(
                LocalDateTime.of(2026, 8, 11, 17, 15).atZone(SANTIAGO).toInstant(),
                controller.intradayRangeEnd(latest));
    }

    @Test
    void ajustaElPrecioSoloConLasVelasVisibles() {
        CandleController controller = new CandleController("SQM-B");
        Instant oldDay = LocalDateTime.of(2026, 8, 10, 10, 0).atZone(SANTIAGO).toInstant();
        Instant latestDay = LocalDateTime.of(2026, 8, 11, 10, 0).atZone(SANTIAGO).toInstant();
        CandleController.DatasetBuildResult result = controller.buildDatasetFromTradeCandles(List.of(
                new TradeCandle("SQM-B", 60, oldDay, 60d, 70d, 55d, 65d, 10d),
                new TradeCandle("SQM-B", 60, latestDay, 100d, 106d, 98d, 104d, 20d)
        ));

        Range visibleTime = new Range(
                latestDay.minusSeconds(60).toEpochMilli(),
                latestDay.plusSeconds(60).toEpochMilli());
        Range priceRange = CandleController.visiblePriceRange(result.items, visibleTime);

        assertEquals(97.36d, priceRange.getLowerBound(), 0.0001d);
        assertEquals(106.64d, priceRange.getUpperBound(), 0.0001d);
    }

    private MarketDataMessage.TradeGeneral tradeAt(int hour, int minute, double price, double quantity) {
        var instant = LocalDateTime.of(2026, 8, 11, hour, minute).atZone(SANTIAGO).toInstant();
        return MarketDataMessage.TradeGeneral.newBuilder()
                .setSymbol("ZOFRI")
                .setPrice(price)
                .setQty(quantity)
                .setT(Timestamp.newBuilder()
                        .setSeconds(instant.getEpochSecond())
                        .setNanos(instant.getNano())
                        .build())
                .build();
    }
}
