package cl.vc.blotter.adaptor;

import akka.actor.AbstractActor;
import akka.actor.Props;
import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.application.Platform;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public class CandleChannelActor extends AbstractActor {
    private static final int MAX_INCREMENTAL_TRADES = 200_000;

    private final Set<String> seenTradeIds = new HashSet<>();

    public static Props props() {
        return Props.create(CandleChannelActor.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MarketDataMessage.BolsaStats.class, this::onBolsaStats)
                .match(MarketDataMessage.TradeGeneral.class, this::onTradeGeneral)
                .match(MarketDataMessage.SnapshotTradeGeneral.class, this::onSnapshotTradeGeneral)
                .build();
    }

    private void onBolsaStats(MarketDataMessage.BolsaStats stats) {
        if (stats.getId() != null && stats.getId().startsWith("hist:")) {
            Repository.addBolsaStatsHistory(stats);
            if (Repository.getStatsController() != null) {
                Repository.getStatsController().addHistorical(stats);
            }
            return;
        }

        Repository.setStats(stats);
        Repository.addBolsaStatsHistory(stats);
        if (Repository.getStatsController() != null) {
            Repository.getStatsController().add(stats);
        }
    }

    private void onSnapshotTradeGeneral(MarketDataMessage.SnapshotTradeGeneral snapshot) {
        Platform.runLater(() -> {
            Repository.getCandleTradeGenerales().clear();
            seenTradeIds.clear();
            snapshot.getTradesList().forEach(this::addSnapshotTradeIfNew);
        });
    }

    private void onTradeGeneral(MarketDataMessage.TradeGeneral trade) {
        Platform.runLater(() -> addTradeIfNew(trade));
    }

    private void addTradeIfNew(MarketDataMessage.TradeGeneral trade) {
        String id = trade.getIdGenerico().isEmpty() ? trade.getId() : trade.getIdGenerico();
        if (id == null || id.isBlank()) {
            Repository.getCandleTradeGenerales().add(trade);
            trimIncrementalWindow();
            return;
        }
        if (seenTradeIds.add(id)) {
            Repository.getCandleTradeGenerales().add(trade);
            trimIncrementalWindow();
        }
    }

    private void addSnapshotTradeIfNew(MarketDataMessage.TradeGeneral trade) {
        String id = trade.getIdGenerico().isEmpty() ? trade.getId() : trade.getIdGenerico();
        if (id == null || id.isBlank()) {
            Repository.getCandleTradeGenerales().add(trade);
            return;
        }
        if (seenTradeIds.add(id)) {
            Repository.getCandleTradeGenerales().add(trade);
        }
    }

    private void trimIncrementalWindow() {
        if (Repository.getCandleTradeGenerales().size() > MAX_INCREMENTAL_TRADES) {
            Repository.getCandleTradeGenerales().remove(0, Repository.getCandleTradeGenerales().size() - MAX_INCREMENTAL_TRADES);
        }
    }
}
