package cl.vc.inyectorcandle.actor;

import cl.vc.inyectorcandle.model.*;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import cl.vc.inyectorcandle.service.RankingService;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MarketActorSystem implements AutoCloseable {

    private final MongoMarketRepository repository;
    private final List<Duration> candleTimeframes;
    private final Map<InstrumentKey, InstrumentActor> actors = new ConcurrentHashMap<>();
    private final RankingService rankingService = new RankingService();
    private final ScheduledExecutorService rankingScheduler = Executors.newSingleThreadScheduledExecutor();

    public MarketActorSystem(MongoMarketRepository repository, List<Duration> candleTimeframes) {
        this.repository = repository;
        this.candleTimeframes = candleTimeframes;
    }

    public void startRankings(Duration interval) {
        rankingScheduler.scheduleAtFixedRate(this::publishRankings, 10, interval.toSeconds(), TimeUnit.SECONDS);
    }

    public void onSecurity(SecurityDefinition security) {
        repository.upsertSecurity(security);
        actors.computeIfAbsent(security.key(), this::createActor);
    }

    public void onMarketData(MarketDataEvent event) {
        actorFor(event.key()).tell(new InstrumentCommand.OnMarketData(event));
    }

    public void onTrade(TradeEvent event) {
        actorFor(event.key()).tell(new InstrumentCommand.OnTrade(event));
    }

    public void purgeDay(LocalDate day, ZoneId zoneId) {
        repository.purgeDay(day, zoneId);
    }

    public void logInjectionAnalysis(Set<LocalDate> days, ZoneId zoneId, int topN) {
        repository.logInjectionAnalysis(days, zoneId, topN);
    }

    public List<InstrumentStats> currentStats() {
        return actors.values().stream().map(InstrumentActor::snapshot).toList();
    }

    private InstrumentActor actorFor(InstrumentKey key) {
        return actors.computeIfAbsent(key, this::createActor);
    }

    private InstrumentActor createActor(InstrumentKey key) {
        return new InstrumentActor(key, candleTimeframes, repository);
    }

    private void publishRankings() {
        MarketRankingSnapshot ranking = rankingService.build(currentStats(), 20);
        repository.upsertRanking(ranking);
    }

    @Override
    public void close() {
        rankingScheduler.shutdown();
        try {
            if (!rankingScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                rankingScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            rankingScheduler.shutdownNow();
        }
        for (InstrumentActor actor : actors.values()) {
            actor.stop();
        }
    }
}
