package cl.vc.blotter.controller;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrincipalControllerTest {

    @Test
    void snapshotKeepsPrincipalPortfolioFromRedis() {
        BlotterMessage.Portfolio principal = portfolio("Principal");
        BlotterMessage.Portfolio ipsa = portfolio("IPSA");
        BlotterMessage.Portfolio watchlist = portfolio("Watchlist");

        List<BlotterMessage.Portfolio> ordered =
                PrincipalController.orderedPortfoliosForSnapshot(List.of(watchlist, principal, ipsa));

        assertEquals(List.of("IPSA", "Principal", "Watchlist"),
                ordered.stream().map(BlotterMessage.Portfolio::getNamePortfolio).toList());
    }

    private static BlotterMessage.Portfolio portfolio(String name) {
        return BlotterMessage.Portfolio.newBuilder()
                .setId(name)
                .setNamePortfolio(name)
                .setUsername("daedo")
                .build();
    }
}
