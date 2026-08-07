package cl.vc.inyectorcandle.mkd;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import cl.vc.inyectorcandle.actor.MarketActorSystem;
import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.SecurityDefinition;
import cl.vc.inyectorcandle.model.TradeEvent;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consume el feed protobuf del consumidor ITCH (NUAM) por TCP y alimenta el motor de velas.
 * Es la alternativa a {@code BcsFixApplication}: mismo transporte que usa el OMS, sin sesion FIX.
 */
public final class ItchMarketDataSource implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ItchMarketDataSource.class);
    private static final List<RoutingMessage.SettlType> SETTL_TYPES =
            List.of(RoutingMessage.SettlType.T2, RoutingMessage.SettlType.NEXT_DAY, RoutingMessage.SettlType.CASH);

    private final MarketActorSystem market;
    private final MarketDataMessage.SecurityExchangeMarketData exchange;
    private final String currency;
    private final boolean processSnapshotTrades;
    private final boolean subscribe;
    private final ActorSystem akka;
    private final NettyProtobufClient client;
    private final Set<String> subscribed = ConcurrentHashMap.newKeySet();

    public ItchMarketDataSource(MarketActorSystem market, String host, int port, String exchangeName,
                                String currency, boolean processSnapshotTrades, boolean subscribe,
                                String logPath) throws Exception {
        this.market = market;
        this.exchange = MarketDataMessage.SecurityExchangeMarketData.valueOf(exchangeName);
        this.currency = currency;
        this.processSnapshotTrades = processSnapshotTrades;
        this.subscribe = subscribe;
        this.akka = ActorSystem.create("inyectorcandle-mkd");
        ActorRef receiver = akka.actorOf(Props.create(Receiver.class, () -> new Receiver(this)), "itch-receiver");
        // ORB es el componente con el que el OMS abre su conexion de market data; el handshake
        // del consumidor ITCH espera ese valor, con otro no responde el SecurityList.
        this.client = new NettyProtobufClient(host + ":" + port, receiver, logPath, exchangeName,
                NotificationMessage.Component.ORB, false);
        LOG.info("ItchMarketDataSource configurado host={} port={} exchange={} currency={}", host, port, exchangeName, currency);
    }

    public void start() {
        client.startService();
    }

    private void onConnect(SessionsMessage.Connect connect) {
        LOG.info("Conectado al feed destination={} component={}", connect.getDestination(), connect.getComponent());
        subscribed.clear();
        client.sendMessage(MarketDataMessage.SecurityListRequest.newBuilder().setId(IDGenerator.getID()).build());
    }

    private void onSecurityList(MarketDataMessage.SecurityList securityList) {
        int sent = 0;
        for (MarketDataMessage.Security security : securityList.getListSecuritiesList()) {
            RoutingMessage.SecurityType securityType = parseSecurityType(security.getSecurityType());
            String symbol = security.getSymbol();

            // Solo se persiste la definicion: el actor del instrumento se crea con su primer trade.
            market.onSecurityDefinition(new SecurityDefinition(
                    InstrumentKey.fromValues(symbol, RoutingMessage.SettlType.T2.name(), exchange.name(),
                            resolveCurrency(security.getCurrency()), securityType.name()),
                    security.getSecurityID(), security.getText(), securityType.name(), null));

            if (!subscribe) {
                continue;
            }
            for (RoutingMessage.SettlType settlType : SETTL_TYPES) {
                if (!subscribed.add(symbol + "|" + settlType.name())) {
                    continue;
                }
                client.sendMessage(MarketDataMessage.Subscribe.newBuilder()
                        .setId(IDGenerator.getID())
                        .setSymbol(symbol)
                        .setSettlType(settlType)
                        .setSecurityType(securityType)
                        .setSecurityExchange(exchange)
                        .setTrade(true)
                        .setStatistic(false)
                        .setBook(false)
                        .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                        .build());
                sent++;
            }
        }
        LOG.info("SecurityList recibida securities={} suscripciones enviadas={}", securityList.getListSecuritiesCount(), sent);
    }

    private void onTrade(MarketDataMessage.Trade trade) {
        market.onTrade(toTradeEvent(trade, "ITCH"));
    }

    private void onSnapshot(MarketDataMessage.Snapshot snapshot) {
        if (!processSnapshotTrades) {
            return;
        }
        for (MarketDataMessage.Trade trade : snapshot.getTradesList()) {
            market.onTrade(toTradeEvent(trade, "ITCH_SNAPSHOT"));
        }
    }

    private TradeEvent toTradeEvent(MarketDataMessage.Trade trade, String sourceMsgType) {
        InstrumentKey key = InstrumentKey.fromValues(
                trade.getSymbol(),
                trade.getSettlType().name(),
                trade.getSecurityExchange().name(),
                currency,
                trade.getSecurityType().name());

        BigDecimal price = trade.getPrice() > 0 ? BigDecimal.valueOf(trade.getPrice()) : null;
        BigDecimal qty = trade.getQty() > 0 ? BigDecimal.valueOf(trade.getQty()) : null;
        BigDecimal amount = trade.getAmount() > 0 ? BigDecimal.valueOf(trade.getAmount()) : null;
        String tradeId = trade.getIdGenerico().isEmpty() ? trade.getId() : trade.getIdGenerico();

        return new TradeEvent(key, toInstant(trade.getT()), price, qty, amount, null, tradeId, '0', trade.getId(), sourceMsgType);
    }

    private String resolveCurrency(String securityCurrency) {
        return securityCurrency == null || securityCurrency.isBlank() ? currency : securityCurrency;
    }

    private static RoutingMessage.SecurityType parseSecurityType(String value) {
        if (value == null || value.isBlank()) {
            return RoutingMessage.SecurityType.CS;
        }
        try {
            return RoutingMessage.SecurityType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return RoutingMessage.SecurityType.CS;
        }
    }

    private static Instant toInstant(Timestamp t) {
        if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) {
            return Instant.now();
        }
        return Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
    }

    @Override
    public void close() {
        try {
            client.stopService();
        } catch (Exception e) {
            LOG.warn("Error deteniendo cliente ITCH: {}", e.getMessage());
        }
        akka.terminate();
    }

    public static class Receiver extends AbstractActor {
        private final ItchMarketDataSource source;

        public Receiver(ItchMarketDataSource source) {
            this.source = source;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(TransportingObjects.class, this::onTransport)
                    .build();
        }

        private void onTransport(TransportingObjects conn) {
            try {
                Object message = conn.getMessage();
                if (message instanceof MarketDataMessage.Trade trade) {
                    source.onTrade(trade);
                } else if (message instanceof MarketDataMessage.Snapshot snapshot) {
                    source.onSnapshot(snapshot);
                } else if (message instanceof MarketDataMessage.SecurityList securityList) {
                    source.onSecurityList(securityList);
                } else if (message instanceof SessionsMessage.Connect connect) {
                    source.onConnect(connect);
                } else if (message instanceof SessionsMessage.Disconnect disconnect) {
                    LOG.warn("Feed desconectado destination={} text={}", disconnect.getDestination(), disconnect.getText());
                } else if (message instanceof MarketDataMessage.Rejected rejected) {
                    LOG.warn("Suscripcion rechazada id={} reason={}", rejected.getId(), rejected.getReason());
                }
            } catch (Exception e) {
                LOG.error("Error procesando mensaje del feed", e);
            }
        }
    }
}
