package cl.vc.inyectorcandle.app;

import cl.vc.inyectorcandle.actor.MarketActorSystem;
import cl.vc.inyectorcandle.config.AppConfig;
import cl.vc.inyectorcandle.config.ConfigLoader;
import cl.vc.inyectorcandle.fix.BcsFixApplication;
import cl.vc.inyectorcandle.fix.FixInitiatorRunner;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import cl.vc.inyectorcandle.replay.FixLogReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        AppConfig config = ConfigLoader.load();

        MongoMarketRepository repository = new MongoMarketRepository(config.mongoUri(), config.mongoDatabase());
        MarketActorSystem actorSystem = new MarketActorSystem(repository, config.candleTimeframes());
        actorSystem.startRankings(config.rankingInterval());

        if (config.replayEnabled()) {
            try {
                FixLogReplayService replay = new FixLogReplayService(
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
            } finally {
                LOG.info("Shutting down inyectorcandle replay");
                actorSystem.close();
                repository.close();
            }
            return;
        }

        BcsFixApplication app = new BcsFixApplication(config, actorSystem);
        FixInitiatorRunner initiator = new FixInitiatorRunner(app, config.fixConfigFile());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down inyectorcandle");
            initiator.close();
            actorSystem.close();
            repository.close();
        }));

        initiator.start();
        LOG.info("inyectorcandle started");

        new CountDownLatch(1).await();
    }
}
