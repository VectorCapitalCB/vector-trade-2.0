package cl.vc.blotter.model;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookVOTest {

    @Test
    void alwaysDetectsTopPriceAndAvoidsMarkingShiftedDepthAsABlock() {
        List<Double> previous = List.of(25.10, 25.09, 25.08);
        LinkedHashSet<Double> prices = new LinkedHashSet<>(previous);

        assertFalse(BookVO.shouldFlashPriceChange(previous, prices, 0, 25.10));
        assertTrue(BookVO.shouldFlashPriceChange(previous, prices, 0, 25.09));
        assertFalse(BookVO.shouldFlashPriceChange(previous, prices, 1, 25.08));
        assertTrue(BookVO.shouldFlashPriceChange(previous, prices, 2, 25.07));
        assertFalse(BookVO.shouldFlashPriceChange(List.of(), new LinkedHashSet<>(), 0, 25.10));
    }

    @Test
    void priceChangeCanBeObservedByEveryBookView() {
        OrderBookEntry entry = new OrderBookEntry("id", 0, 0, null,
                "LTM", "", "", MarketDataMessage.SecurityExchangeMarketData.BCS);

        entry.markPriceChanged();

        assertTrue(entry.hasPriceChanged());
        assertTrue(entry.hasPriceChanged());
        assertTrue(entry.getPriceChangeSequence() > 0);
    }

    @Test
    void eachRealPriceChangeGetsItsOwnEvent() {
        OrderBookEntry first = new OrderBookEntry("id", 25.10, 100, null,
                "LTM", "", "", MarketDataMessage.SecurityExchangeMarketData.BCS);
        OrderBookEntry second = new OrderBookEntry("id", 25.11, 100, null,
                "LTM", "", "", MarketDataMessage.SecurityExchangeMarketData.BCS);

        first.markPriceChanged();
        second.markPriceChanged();

        assertTrue(second.getPriceChangeSequence() > first.getPriceChangeSequence());
    }
}
