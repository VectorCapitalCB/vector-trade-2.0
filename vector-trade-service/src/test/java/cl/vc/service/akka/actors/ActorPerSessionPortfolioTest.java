package cl.vc.service.akka.actors;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ActorPerSessionPortfolioTest {

    @Test
    void duplicatePortfolioNamePreservesExistingPortfolioAndAssets() {
        BlotterMessage.Portfolio existing = BlotterMessage.Portfolio.newBuilder()
                .setNamePortfolio("Principal")
                .setUsername("fricci")
                .addAsset(BlotterMessage.Asset.newBuilder().setSymbol("LTM").build())
                .build();
        HashMap<String, BlotterMessage.Portfolio> portfolios = new HashMap<>();
        portfolios.put(existing.getNamePortfolio(), existing);

        BlotterMessage.Portfolio found =
                ActorPerSession.findPortfolioIgnoreCase(portfolios, " principal ");

        assertSame(existing, found);
        assertEquals(1, found.getAssetCount());
        assertEquals("LTM", found.getAsset(0).getSymbol());
    }

    @Test
    void unknownOrBlankNameDoesNotMatchExistingPortfolio() {
        HashMap<String, BlotterMessage.Portfolio> portfolios = new HashMap<>();
        portfolios.put("Principal", BlotterMessage.Portfolio.newBuilder()
                .setNamePortfolio("Principal")
                .build());

        assertNull(ActorPerSession.findPortfolioIgnoreCase(portfolios, "Nuevo"));
        assertNull(ActorPerSession.findPortfolioIgnoreCase(portfolios, "  "));
        assertNull(ActorPerSession.findPortfolioIgnoreCase(null, "Principal"));
    }
}
