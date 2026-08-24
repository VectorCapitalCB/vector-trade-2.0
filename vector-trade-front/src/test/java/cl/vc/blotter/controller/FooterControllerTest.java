package cl.vc.blotter.controller;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FooterControllerTest {

    @Test
    void usesOhlcvCloseForTheThirdDollarValue() {
        MarketDataMessage.Statistic statistic = MarketDataMessage.Statistic.newBuilder()
                .setClose(915.40d)
                .setLast(915.50d)
                .setOhlcv(MarketDataMessage.Ohlcv.newBuilder().setClose(916.55d))
                .build();

        assertEquals(916.55d, FooterController.resolveDollarReferencePrice(statistic));
    }

    @Test
    void fallsBackToCloseAndThenLast() {
        MarketDataMessage.Statistic closeStatistic = MarketDataMessage.Statistic.newBuilder()
                .setClose(916.55d)
                .setLast(915.50d)
                .build();
        MarketDataMessage.Statistic lastStatistic = MarketDataMessage.Statistic.newBuilder()
                .setLast(916.55d)
                .build();

        assertEquals(916.55d, FooterController.resolveDollarReferencePrice(closeStatistic));
        assertEquals(916.55d, FooterController.resolveDollarReferencePrice(lastStatistic));
    }
}
