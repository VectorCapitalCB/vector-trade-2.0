package cl.vc.inyectorcandle.fix;

import cl.vc.inyectorcandle.actor.MarketActorSystem;
import cl.vc.inyectorcandle.config.AppConfig;
import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.SecurityDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.MessageCracker;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.*;
import quickfix.fix44.MarketDataIncrementalRefresh;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.MarketDataRequestReject;
import quickfix.fix44.MarketDataSnapshotFullRefresh;
import quickfix.fix44.News;
import quickfix.fix44.SecurityList;
import quickfix.fix44.SecurityListRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BcsFixApplication extends MessageCracker implements Application {
    private static final Logger LOG = LoggerFactory.getLogger(BcsFixApplication.class);
    private static final List<String> SUBSCRIPTION_SETTLEMENTS = List.of("T2", "CASH", "NEXT_DAY");

    private final AppConfig config;
    private final MarketActorSystem actorSystem;
    private final Map<String, InstrumentKey> mdReqToKey = new ConcurrentHashMap<>();

    private SessionID currentSession;

    public BcsFixApplication(AppConfig config, MarketActorSystem actorSystem) {
        this.config = config;
        this.actorSystem = actorSystem;
    }

    public Map<String, InstrumentKey> mdReqToKey() {
        return mdReqToKey;
    }

    @Override
    public void onCreate(SessionID sessionId) {
        LOG.info("FIX session created: {}", sessionId);
    }

    @Override
    public void onLogon(SessionID sessionId) {
        currentSession = sessionId;
        LOG.info("FIX logon successful: {}", sessionId);

        SecurityListRequest req = FixRequestFactory.securityListRequest(config.securityListRequestType(), config.securityListScope());
        send(req);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        LOG.warn("FIX logout: {}", sessionId);
        currentSession = null;
    }

    @Override
    public void toAdmin(quickfix.Message message, SessionID sessionId) {
        try {
            if (message.getHeader().isSetField(MsgType.FIELD)
                    && MsgType.LOGON.equals(message.getHeader().getString(MsgType.FIELD))) {
                if (config.rawData() != null) {
                    message.getHeader().setField(new RawDataLength(config.rawData().length()));
                    message.getHeader().setField(new RawData(config.rawData()));
                }
                if (config.username() != null) {
                    message.setField(new Username(config.username()));
                }
                if (config.password() != null) {
                    message.setField(new Password(config.password()));
                }
            }

        } catch (Exception e) {
            LOG.error("Error adding admin fields", e);
        }
    }

    @Override
    public void fromAdmin(quickfix.Message message, SessionID sessionId) {
        LOG.debug("fromAdmin: {}", message);
    }

    @Override
    public void toApp(quickfix.Message message, SessionID sessionId) {
        LOG.debug("toApp: {}", message);
    }

    @Override
    public void fromApp(quickfix.Message message, SessionID sessionId) {
        try {
            crack(message, sessionId);
        } catch (Exception e) {
            LOG.error("Cannot crack message {}", message, e);
        }
    }

    public void onMessage(SecurityList message, SessionID sessionId) {
        try {
            List<SecurityDefinition> securities = SecurityListParser.parse(message);
            LOG.info("SecurityList received with {} items", securities.size());

            for (SecurityDefinition sec : securities) {
                actorSystem.onSecurity(sec);
            }

            Map<SubscriptionSeed, SecurityDefinition> seeds = new LinkedHashMap<>();
            for (SecurityDefinition sec : securities) {
                SubscriptionSeed seed = new SubscriptionSeed(
                        sec.key().symbol(),
                        sec.key().destination(),
                        sec.key().currency(),
                        sec.key().securityType()
                );
                seeds.putIfAbsent(seed, sec);
            }

            for (SecurityDefinition sec : seeds.values()) {
                for (String settlement : SUBSCRIPTION_SETTLEMENTS) {
                    MarketDataRequest request = FixRequestFactory.subscribeTrades(
                            sec.key().symbol(),
                            settlement,
                            sec.key().destination(),
                            sec.key().currency()
                    );

                    InstrumentKey subscriptionKey = InstrumentKey.fromValues(
                            sec.key().symbol(),
                            settlement,
                            sec.key().destination(),
                            sec.key().currency(),
                            sec.key().securityType()
                    );
                    String mdReqId = request.getString(MDReqID.FIELD);
                    mdReqToKey.put(mdReqId, subscriptionKey);
                    send(request);

                    if (config.securitySubscriptionPauseMs() > 0) {
                        Thread.sleep(config.securitySubscriptionPauseMs());
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("Error processing SecurityList", e);
        }
    }

    public void onMessage(MarketDataSnapshotFullRefresh message, SessionID sessionId) {
        try {
            if (!config.processSnapshots()) {
                LOG.debug("Ignoring MarketDataSnapshotFullRefresh because fix.process.snapshots=false");
                return;
            }

            String mdReqId = message.isSetField(MDReqID.FIELD) ? message.getString(MDReqID.FIELD) : null;
            InstrumentKey key = mdReqId == null ? null : mdReqToKey.get(mdReqId);
            if (key == null) {
                key = InstrumentKey.fromValues(
                        message.isSetField(Symbol.FIELD) ? message.getString(Symbol.FIELD) : null,
                        message.isSetField(SettlType.FIELD) ? message.getString(SettlType.FIELD) : null,
                        message.isSetField(SecurityExchange.FIELD) ? message.getString(SecurityExchange.FIELD) : null,
                        message.isSetField(Currency.FIELD) ? message.getString(Currency.FIELD) : null,
                        message.isSetField(SecurityType.FIELD) ? message.getString(SecurityType.FIELD) : null
                );
            }

            var parsed = MarketDataParser.parseSnapshot(message, key);
            parsed.events().forEach(actorSystem::onMarketData);
            if (config.processSnapshotTrades()) {
                parsed.trades().forEach(actorSystem::onTrade);
            }

        } catch (Exception e) {
            LOG.error("Error processing MarketDataSnapshotFullRefresh", e);
        }
    }

    public void onMessage(MarketDataIncrementalRefresh message, SessionID sessionId) {
        try {
            var parsed = MarketDataParser.parseIncremental(message, mdReqToKey);
            parsed.events().forEach(actorSystem::onMarketData);
            parsed.trades().forEach(actorSystem::onTrade);
        } catch (Exception e) {
            LOG.error("Error processing MarketDataIncrementalRefresh", e);
        }
    }

    public void onMessage(MarketDataRequestReject message, SessionID sessionId) {
        try {
            String mdReqId = message.isSetField(MDReqID.FIELD) ? message.getString(MDReqID.FIELD) : "unknown";
            LOG.warn("MarketDataRequestReject for MDReqID {}: {}", mdReqId,
                    message.isSetField(Text.FIELD) ? message.getString(Text.FIELD) : "without text");
        } catch (Exception e) {
            LOG.error("Error processing MarketDataRequestReject", e);
        }
    }

    public void onMessage(News message, SessionID sessionId) {
        try {
            String text = message.isSetField(Text.FIELD) ? message.getString(Text.FIELD) : "";
            LOG.info("News: {}", text);
        } catch (Exception e) {
            LOG.error("Error processing news", e);
        }
    }

    private void send(quickfix.Message message) {
        if (currentSession == null) {
            LOG.warn("Cannot send {} because session is not logged in", message.getClass().getSimpleName());
            return;
        }

        try {
            Session.sendToTarget(message, currentSession);
            if (config.marketDataThrottleMs() > 0 && message instanceof MarketDataRequest) {
                Thread.sleep(config.marketDataThrottleMs());
            }
        } catch (Exception e) {
            LOG.error("Cannot send message {}", message, e);
        }
    }

    private record SubscriptionSeed(String symbol, String destination, String currency, String securityType) {
    }
}
