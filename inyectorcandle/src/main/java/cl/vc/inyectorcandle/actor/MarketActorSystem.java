package cl.vc.inyectorcandle.actor;

import cl.vc.inyectorcandle.model.*;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import cl.vc.inyectorcandle.service.RankingService;
import cl.vc.inyectorcandle.stats.DailyStatsFinalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(MarketActorSystem.class);

    private final MongoMarketRepository repository;
    private final List<Duration> candleTimeframes;
    private final long statsThrottleMs;
    private final long openCandleFlushMs;
    private final Map<InstrumentKey, InstrumentActor> actors = new ConcurrentHashMap<>();
    private final RankingService rankingService = new RankingService();
    private final ScheduledExecutorService rankingScheduler = Executors.newSingleThreadScheduledExecutor();

    public MarketActorSystem(MongoMarketRepository repository, List<Duration> candleTimeframes,
                             long statsThrottleMs, long openCandleFlushMs) {
        this.repository = repository;
        this.candleTimeframes = candleTimeframes;
        this.statsThrottleMs = statsThrottleMs;
        this.openCandleFlushMs = openCandleFlushMs;
    }

    public void startRankings(Duration interval) {
        rankingScheduler.scheduleAtFixedRate(this::publishRankings, 10, interval.toSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Publica el dia en curso en {@code instrument_daily} y reconstruye su agregado, con la misma
     * forma que dejan los replays de logs. Sin esto el dia de hoy no existe para el front, que
     * consulta la historia por fecha aunque la fecha sea la de hoy.
     */
    public void startDailyStats(Duration interval, String market, String currency, DailyStatsFinalizer finalizer) {
        rankingScheduler.scheduleAtFixedRate(() -> publishDailyStats(market, currency, finalizer),
                interval.toSeconds(), interval.toSeconds(), TimeUnit.SECONDS);
    }

    private void publishDailyStats(String market, String currency, DailyStatsFinalizer finalizer) {
        LocalDate day = null;
        int published = 0;
        for (InstrumentActor actor : actors.values()) {
            InstrumentDailyStats stats = actor.dailySnapshot(market, currency);
            if (stats == null) {
                continue;
            }
            repository.upsertInstrumentDaily(stats, "LIVE");
            day = stats.tradingDay();
            published++;
        }
        if (day == null) {
            return;
        }
        finalizer.runForDay(day);
        LOG.info("Estadistica del dia {} publicada instrumentos={}", day, published);
    }

    public void onSecurity(SecurityDefinition security) {
        repository.upsertSecurity(security);
        actors.computeIfAbsent(security.key(), this::createActor);
    }

    /**
     * Solo persiste la definicion, sin levantar el actor. El feed ITCH publica ~16.000 instrumentos
     * y crear un hilo por cada uno agotaria el proceso: el actor se crea con el primer trade.
     */
    public void onSecurityDefinition(SecurityDefinition security) {
        repository.upsertSecurity(security);
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
        return new InstrumentActor(key, candleTimeframes, repository, statsThrottleMs, openCandleFlushMs);
    }

    private void publishRankings() {
        // Sin este barrido el throttle del actor deja sin persistir el ultimo tramo de cada rafaga:
        // un instrumento que opera y se queda quieto conserva en Mongo el estado del primer trade.
        for (InstrumentActor actor : actors.values()) {
            actor.tell(new InstrumentCommand.Flush());
        }
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
