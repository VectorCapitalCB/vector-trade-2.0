package cl.vc.inyectorcandle.fix;

import org.junit.jupiter.api.Test;
import quickfix.fix44.MarketDataRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class FixRequestFactoryTest {

    @Test
    void subscribeTradesShouldPropagateDestinationAndCurrencyForXsgo() throws Exception {
        MarketDataRequest request = FixRequestFactory.subscribeTrades("SQM-B", "T2", "XSGO", "CLP");

        MarketDataRequest.NoRelatedSym group = new MarketDataRequest.NoRelatedSym();
        request.getGroup(1, group);

        assertEquals("SQM-B", group.getString(55));
        assertEquals("|||", group.getString(876));
        assertEquals("XSGO", group.getString(207));
        assertFalse(group.isSetField(15));
    }

    @Test
    void subscribeTradesShouldPropagateDestinationAndCurrencyForOfs() throws Exception {
        MarketDataRequest request = FixRequestFactory.subscribeTrades("SPY", "T2", "OFS", "USD");

        MarketDataRequest.NoRelatedSym group = new MarketDataRequest.NoRelatedSym();
        request.getGroup(1, group);

        assertEquals("SPY", group.getString(55));
        assertEquals("|||", group.getString(876));
        assertEquals("OFS", group.getString(207));
        assertFalse(group.isSetField(15));
    }
}
