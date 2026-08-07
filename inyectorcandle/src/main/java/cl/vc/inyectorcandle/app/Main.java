package cl.vc.inyectorcandle.app;

import cl.vc.inyectorcandle.actor.MarketActorSystem;
import cl.vc.inyectorcandle.alpaca.AlpacaCandleSource;
import cl.vc.inyectorcandle.config.AppConfig;
import cl.vc.inyectorcandle.config.ConfigLoader;
import cl.vc.inyectorcandle.fix.BcsFixApplication;
import cl.vc.inyectorcandle.fix.FixInitiatorRunner;
import cl.vc.inyectorcandle.mkd.ItchMarketDataSource;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import cl.vc.inyectorcandle.replay.FixLogReplayService;
import cl.vc.inyectorcandle.itch.ItchLogReplay;
import cl.vc.inyectorcandle.model.MarketSession;
import cl.vc.inyectorcandle.stats.DailyStatsFinalizer;
import cl.vc.inyectorcandle.stats.FixStatsLogReplay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String configPath = args != null && args.length > 0 ? args[0] : null;
        AppConfig config = ConfigLoader.load(configPath);

        MongoMarketRepository repository = new MongoMarketRepository(config.mongoUri(), config.mongoDatabase(),
                config.mongoWriteQueueSize(), config.mongoWriteBatchSize(), config.mongoWriteFlushMs(),
                config.statsMarket());

        // Solo leen logs y escriben estadistica: no levantan actores ni feed. FIX primero, porque
        // su OHLCV es el oficial y el paso ITCH no lo pisa, solo lo complementa.
        if (config.statsReplayEnabled() || config.itchReplayEnabled()) {
            MarketSession session = MarketSession.of(config.statsZone(), config.statsSessionOpen(),
                    config.statsSessionClose());
            try {
                if (config.statsReplayEnabled()) {
                    new FixStatsLogReplay(repository, config.statsMarket(), config.statsCurrency(), session)
                            .replay(config.statsReplayInputPath());
                }
                if (config.itchReplayEnabled()) {
                    new ItchLogReplay(repository, config.statsMarket(), config.statsCurrency(), session)
                            .replay(config.itchReplayInputPath());
                }
                new DailyStatsFinalizer(repository, config.statsMarket(), session).run();
            } finally {
                repository.close();
            }
            return;
        }

        MarketActorSystem actorSystem = new MarketActorSystem(repository, config.candleTimeframes(),
                config.statsThrottleMs(), config.openCandleFlushMs());
        actorSystem.startRankings(config.rankingInterval());
        actorSystem.startDailyStats(config.statsLiveInterval(), config.statsMarket(), config.statsCurrency(),
                new DailyStatsFinalizer(repository, config.statsMarket(), MarketSession.of(config.statsZone(),
                        config.statsSessionOpen(), config.statsSessionClose())));

        AlpacaCandleSource alpaca = startAlpacaIfEnabled(config, repository);

        if (config.replayEnabled()) {
            FixLogReplayService replay = null;
            boolean actorClosed = false;
            try {
                replay = new FixLogReplayService(
                        actorSystem,
                        config.replayLogZoneId(),
                        config.replaySleepMs(),
                        config.replayMaxLines(),
                        config.replayPreserveTiming(),
                        config.replayTimingSpeed(),
                        config.replayTimingMaxSleepMs(),
                        config.replayPurgeDayBeforeInject()
                );
                replay.replay(config.replayInputPath());
                actorSystem.close();
                actorClosed = true;
                Set<java.time.LocalDate> days = replay.injectedDays();
                repository.logInjectionAnalysis(days, ZoneId.of(config.replayLogZoneId()), 10);
            } finally {
                LOG.info("Shutting down inyectorcandle replay");
                closeQuietly(alpaca);
                if (!actorClosed) {
                    actorSystem.close();
                }
                repository.close();
            }
            return;
        }

        AutoCloseable feed = startFeed(config, actorSystem);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down inyectorcandle");
            closeQuietly(feed);
            closeQuietly(alpaca);
            actorSystem.close();
            repository.close();
        }));

        LOG.info("inyectorcandle started source={}", config.mkdSource());

        new CountDownLatch(1).await();
    }

    private static AutoCloseable startFeed(AppConfig config, MarketActorSystem actorSystem) throws Exception {
        if (config.itchSource()) {
            ItchMarketDataSource source = new ItchMarketDataSource(actorSystem, config.itchHost(), config.itchPort(),
                    config.itchExchange(), config.itchCurrency(), config.processSnapshotTrades(),
                    config.itchSubscribe(), config.itchLogPath());
            source.start();
            return source;
        }

        BcsFixApplication app = new BcsFixApplication(config, actorSystem);
        FixInitiatorRunner initiator = new FixInitiatorRunner(app, config.fixConfigFile());
        initiator.start();
        return initiator::close;
    }

    private static AlpacaCandleSource startAlpacaIfEnabled(AppConfig config, MongoMarketRepository repository) {
        if (!config.alpacaEnabled()) {
            return null;
        }
        if (config.alpacaKeyId() == null || config.alpacaSecretKey() == null) {
            LOG.error("alpaca.enabled=true pero faltan alpaca.key.id / alpaca.secret.key, no se inicia Alpaca");
            return null;
        }

        AlpacaCandleSource source = new AlpacaCandleSource(repository,
                new AlpacaCandleSource.AlpacaConfig(config.alpacaKeyId(), config.alpacaSecretKey(), config.alpacaFeed(),
                        config.alpacaSymbols(), config.alpacaDataUrl(), config.alpacaStreamUrl(),
                        config.alpacaBackfillDays(), config.alpacaPollSeconds()),
                config.candleTimeframes());
        source.start();
        return source;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("Error cerrando {}: {}", closeable.getClass().getSimpleName(), e.getMessage());
        }
    }
}
