package cl.vc.service.multibook;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Multibook2RepositoryTest {

    @Test
    void keepsFiftyLegacyPositionsOnOnePage() {
        JSONArray pages = Multibook2Repository.toPages(List.of(row(0), row(49)), null);

        assertEquals(1, pages.length());
        assertEquals(50, pages.getJSONObject(0).getInt("bookCount"));
        assertEquals(0, pages.getJSONObject(0).getJSONArray("books").getJSONObject(0).getInt("slot"));
        assertEquals(49, pages.getJSONObject(0).getJSONArray("books").getJSONObject(1).getInt("slot"));
    }

    @Test
    void startsANewPageAfterTheFiftiethPosition() {
        JSONArray pages = Multibook2Repository.toPages(List.of(row(50)), null);

        assertEquals(2, pages.length());
        assertEquals(0, pages.getJSONObject(1).getJSONArray("books").getJSONObject(0).getInt("slot"));
    }

    private static BlotterMessage.SubMultibook row(int position) {
        return BlotterMessage.SubMultibook.newBuilder()
                .setPositions(position)
                .setSubscribeBook(MarketDataMessage.Subscribe.newBuilder()
                        .setSymbol("TEST" + position)
                        .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                        .setBook(true)
                        .build())
                .build();
    }
}
