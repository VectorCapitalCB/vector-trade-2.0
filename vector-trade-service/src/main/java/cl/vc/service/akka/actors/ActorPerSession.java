package cl.vc.service.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.ws.vectortrade.MessageUtilVT;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.mkd.ActorPerSubscriptionMkd;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import cl.vc.service.util.IgpaPortfolioService;
import cl.vc.service.util.IpsaPortfolioService;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.keycloak.representations.idm.UserRepresentation;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;


@Slf4j
public class ActorPerSession extends AbstractActor {

    private final Session session;

    private final HashMap<String, ActorRef> actorPerSubscriptionMkdHash = new HashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private SessionsMessage.Connect connect = SessionsMessage.Connect.newBuilder().build();
    private String idSession;

    private ActorPerSession(Session session) {
        this.session = session;
        if (session != null) {
            idSession = session.getRemote().toString();
        }
    }

    public static Props props(Session session) {
        return Props.create(ActorPerSession.class, session);
    }

    @Override
    public Receive createReceive() {

        return receiveBuilder()
                .match(ByteBuffer.class, this::onByteBuffer)
                .match(BlotterMessage.PreselectRequest.class, this::onPreselect)
                .match(BlotterMessage.SnapshotPositionHistory.class, this::onSnapshotPositionHistory)
                .match(BlotterMessage.PortfolioRequest.class, this::onPortfolioRequest)
                .match(BlotterMessage.Patrimonio.class, this::onPatrimonio)
                .match(BlotterMessage.PositionHistory.class, this::onPositionHistory)
                .match(BlotterMessage.Balance.class, this::onBalance)
                .match(MarketDataMessage.Subscribe.class, this::onSubscribe)
                .match(MarketDataMessage.Rejected.class, this::onRejectedM)
                .match(RoutingMessage.Order.class, this::onOrders)
                .match(RoutingMessage.OrderCancelReject.class, this::onRejected)
                .match(MarketDataMessage.Unsubscribe.class, this::onUnSubscribe)
                .match(MarketDataMessage.Snapshot.class, this::onSnapshot)
                .match(RoutingMessage.NewOrderRequest.class, this::onNewOrderRequest)
                .match(RoutingMessage.OrderReplaceRequest.class, this::onReplaceRequest)
                .match(RoutingMessage.OrderCancelRequest.class, this::onCancelRequest)
                .match(NotificationMessage.Notification.class, this::onNotification)
                .match(MarketDataMessage.News.class, this::onNews)
                .match(MarketDataMessage.Trade.class, this::onTrade)
                .match(MarketDataMessage.TradeGeneral.class, this::onTradeGeneral)
                .match(MarketDataMessage.Statistic.class, this::onStatistic)
                .match(MarketDataMessage.IncrementalBook.class, this::onIncrementalBook)
                .match(SnapshotPositionsAccount.class, this::onPositionsSnapshotPositions)
                .match(SessionsMessage.Connect.class, this::onConnect)
                .match(SessionsMessage.Ping.class, this::onPing)
                .match(BlotterMessage.User.class, this::onUser)
                .match(BlotterMessage.UserList.class, this::onUserList)
                .match(BlotterMessage.Multibook.class, this::onMultibook)
                .match(MarketDataMessage.BolsaStats.class, this::onBolsaStats)
                .match(BlotterMessage.SnapshotSimultaneas.class, this::onSnapshotSimultaneas)
                .match(BlotterMessage.SnapshotPrestamos.class, this::onSnapshotPrestamos)
                .build();
    }

    @Override
    public void preStart() {

        MainApp.getMessageEventBus().subscribe(getSelf(), "news");
        MainApp.getMessageEventBus().subscribe(getSelf(), "BolsaStats");

        if (Boolean.parseBoolean(MainApp.getProperties().getProperty("isgenetaltrade"))) {
            MainApp.getMessageEventBus().subscribe(getSelf(), "TradeGeneral");
        }

        if (session != null) {
            Runnable task = () -> {
                SessionsMessage.Ping ping = SessionsMessage.Ping.newBuilder().setId(IDGenerator.getID()).build();
                sendMessages(ping);
            };
            scheduler.scheduleAtFixedRate(task, 1, 30, TimeUnit.SECONDS);
        }

        if (MainApp.getProperties().getProperty("sendsecuritylist").equals("true")) {
            if (MainApp.getSendsecuritylist()) {
                MainApp.getSecurityExchangeMaps().forEach((key, value) -> sendMessages(value));
            }
        }

    }

    @Override
    public void postStop() {

        try {

            MainApp.getMessageEventBus().unsubscribe(getSelf(), "news");
            MainApp.getMessageEventBus().unsubscribe(getSelf(), "TradeGeneral");
            MainApp.getMessageEventBus().unsubscribe(getSelf(), "BolsaStats");

            if (connect != null) {
                connect.getUsername();
                UserRepresentation userRepresentation = MainApp.getKeycloakService().getUserByUsername(connect.getUsername());

                if (userRepresentation.getAttributes() != null) {
                    userRepresentation.getAttributes().forEach((key, value) -> {
                        if (key.startsWith("account")) {
                            value.forEach(r -> {
                                ActorRef actorRef = MainApp.getAccountGroupUser().get(r);
                                actorRef.tell(new ActorGroupPerAccount.RemoveUser(idSession), ActorRef.noSender());
                            });
                        }
                    });
                }
            }

            MainApp.getSessionAdminList().remove(getSelf());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void sendMessages(Message message) {
        try {

            if (session != null && session.isOpen() && message != null) {
                ByteBuffer messsage = MessageUtilVT.serializeMessageByteBuffer(message);
                session.getRemote().sendBytes(messsage);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onBolsaStats(MarketDataMessage.BolsaStats stats) {
        sendMessages(stats);
    }

    public void onSnapshotPrestamos(BlotterMessage.SnapshotPrestamos snapshot) {
        sendMessages(snapshot);
    }

    public void onSnapshotSimultaneas(BlotterMessage.SnapshotSimultaneas snapshotSimultaneas) {
        sendMessages(snapshotSimultaneas);
    }

    public void onSnapshotPositionHistory(BlotterMessage.SnapshotPositionHistory snapshotPositions) {
        sendMessages(snapshotPositions);
    }

    public void onPositionHistory(BlotterMessage.PositionHistory positionHistory) {
        try {
            String account = positionHistory.getAccount();
            ActorRef accountActor = addAccount(account);
            accountActor.tell(positionHistory, getSelf());
        } catch (Exception e) {
            log.error("Error al procesar PositionHistory: ", e);
        }
    }

    public void onByteBuffer(ByteBuffer byteBuffer) {
        try {

            Message message = MessageUtilVT.onDeserializeMessage(byteBuffer);
            getSelf().tell(message, ActorRef.noSender());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void onPositionsSnapshotPositions(SnapshotPositionsAccount snapshotPositions) {
        sendMessages(snapshotPositions.getSnapshotPositions());

    }

    public void onPing(SessionsMessage.Ping ping) {
        SessionsMessage.Pong pong = SessionsMessage.Pong.newBuilder().setId(IDGenerator.getID()).build();
        sendMessages(pong);
    }

    public void onMultibook(BlotterMessage.Multibook multiBook) {

        try {

            if (connect.getUsername().isEmpty()) {
                return;
            }

            String username = connect.getUsername();

            LinkedHashMap<Integer, BlotterMessage.SubMultibook> mergedByPosition = new LinkedHashMap<>();

            if (MainApp.getMultiBookMaps().containsKey(username)) {
                List<BlotterMessage.SubMultibook> existing = MainApp.getMultiBookMaps().get(username);
                if (existing != null) {
                    existing.forEach(sub -> mergedByPosition.put(sub.getPositions(), sub));
                }
            }

            multiBook.getSubmultibookList().forEach(sub -> mergedByPosition.put(sub.getPositions(), sub));

            List<BlotterMessage.SubMultibook> list = new ArrayList<>(mergedByPosition.values());
            MainApp.getMultiBookMaps().put(username, list);
            BlotterMessage.Multibook.Builder multibook = BlotterMessage.Multibook.newBuilder()
                    .addAllSubmultibook(list)
                    .setUsername(username);
            sendMessages(multibook.build());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void onBalance(BlotterMessage.Balance balance) {
        sendMessages(balance);
    }

    public void onPatrimonio(BlotterMessage.Patrimonio patrimonio) {
        sendMessages(patrimonio);
    }

    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {
        sendMessages(incrementalBook);
    }

    public void onStatistic(MarketDataMessage.Statistic statistic) {
        sendMessages(statistic);
    }

    public void onTrade(MarketDataMessage.Trade trade) {
        sendMessages(trade);
    }

    public void onTradeGeneral(MarketDataMessage.TradeGeneral trade) {
        sendMessages(trade);
    }

    private void onRejected(RoutingMessage.OrderCancelReject rejected) {
        sendMessages(rejected);
    }

    public void onNews(MarketDataMessage.News msg) {
        sendMessages(msg);
    }

    private void onOrders(RoutingMessage.Order order) {
        sendMessages(order);
    }

    public void onNotification(NotificationMessage.Notification msg) {
        sendMessages(msg);
    }

    private void onRejectedM(MarketDataMessage.Rejected conn) {
        sendMessages(conn);
    }

    public void onSnapshot(MarketDataMessage.Snapshot msg) {
        sendMessages(msg);
    }

    public void onUnSubscribe(MarketDataMessage.Unsubscribe msg) {
        try {

            ActorRef actorRef = actorPerSubscriptionMkdHash.get(msg.getId());

            if (actorRef != null) {
                actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
                actorPerSubscriptionMkdHash.remove(msg.getId());
            }


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onPreselect(BlotterMessage.PreselectRequest request) {

        try {

            BlotterMessage.PreselectResponse.Builder response = BlotterMessage.PreselectResponse.newBuilder();
            response.setUsername(request.getUsername());
            response.setStatusPreselect(BlotterMessage.StatusPreselect.SNAPSHOT_PRESELECT);

            List<RoutingMessage.Order> orderList;

            if (request.getStatusPreselect().equals(BlotterMessage.StatusPreselect.SNAPSHOT_PRESELECT)) {
                orderList = MainApp.getPreSelectordersMap().computeIfAbsent(request.getUsername(), k -> new ArrayList<>());
                response.addAllOrders(orderList);

            } else if (request.getStatusPreselect().equals(BlotterMessage.StatusPreselect.ADD_PRESELECT)) {
                orderList = MainApp.getPreSelectordersMap().get(request.getUsername());

                if (orderList == null) {
                    orderList = new ArrayList<>();
                }
                orderList.add(request.getOrders());
                MainApp.getPreSelectordersMap().put(request.getUsername(), orderList);
                response.addAllOrders(orderList);

            } else if (request.getStatusPreselect().equals(BlotterMessage.StatusPreselect.REMOVE_PRESELECT)) {

                orderList = MainApp.getPreSelectordersMap().get(request.getUsername());

                Predicate<RoutingMessage.Order> predicate = objeto -> objeto.getId().equals(request.getOrders().getId());
                orderList.removeIf(predicate);

                MainApp.getPreSelectordersMap().put(request.getUsername(), orderList);
                response.addAllOrders(orderList);
            }

            sendMessages(response.build());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onSubscribe(MarketDataMessage.Subscribe msg) {

        try {

            String id = msg.getId();

            if(msg.getSymbol().isEmpty()){
                return;
            }

            if (!actorPerSubscriptionMkdHash.containsKey(id)) {
                ActorRef client = getContext().actorOf(new RoundRobinPool(1).props(ActorPerSubscriptionMkd.props(getSelf(), msg).withDispatcher("ActorperMkd")));
                actorPerSubscriptionMkdHash.put(id, client);

            } else {
                actorPerSubscriptionMkdHash.get(id).tell(msg, ActorRef.noSender());
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onConnect(SessionsMessage.Connect onConnect) {

        try {

            this.connect = onConnect;
            String username = onConnect.getUsername();
            log.info("onConnect - procesando usuario: {}", username);

            BlotterMessage.User userMsg;
            try {
                userMsg = MainApp.getKeycloakService().getRolesUserKeycloak(username);
            } catch (Exception ex) {
                log.error("Error obteniendo roles de usuario '{}' desde Keycloak: {}", username, ex.getMessage(), ex);
                return;
            }

            sendMessages(userMsg);

            if (!MainApp.getListNews().isEmpty()) {
                MarketDataMessage.SnapshotNews.Builder news = MarketDataMessage.SnapshotNews.newBuilder();
                news.addAllNews(MainApp.getListNews());
                sendMessages(news.build());
            }

            if (Boolean.parseBoolean(MainApp.getProperties().getProperty("isgenetaltrade"))) {
                MarketDataMessage.SnapshotTradeGeneral snapshotTradeGeneral = MarketDataMessage.SnapshotTradeGeneral.newBuilder()
                        .addAllTrades(MainApp.getTradeGeneral()).build();
                sendMessages(snapshotTradeGeneral);
            }

            if (userMsg != null) {
                userMsg.getAccountList().forEach(this::addAccount);
            }

            if (username.isEmpty()) {
                return;
            }

            if (MainApp.getMultiBookMaps().containsKey(username)) {
                List<BlotterMessage.SubMultibook> list = MainApp.getMultiBookMaps().get(username);
                BlotterMessage.Multibook.Builder multibook = BlotterMessage.Multibook.newBuilder()
                        .addAllSubmultibook(list)
                        .setUsername(username);
                sendMessages(multibook.build());
            }

        } catch (Exception e) {
            log.error("Error general en onConnect: {}", e.getMessage(), e);
        }
    }

    private void onUserList(BlotterMessage.UserList userLists) {

        try {

            if (userLists.getStatusUser().equals(BlotterMessage.StatusUser.SNAPSHOT_USER)) {

                BlotterMessage.UserList.Builder userListBuilder = BlotterMessage.UserList.newBuilder();
                List<UserRepresentation> usersKey = MainApp.getKeycloakService().getAllUseres();
                for (UserRepresentation userRep : usersKey) {
                    try {
                        BlotterMessage.User userMsg = MainApp.getKeycloakService().getRolesUserKeycloak(userRep.getUsername());
                        userListBuilder.addUsers(userMsg);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
                BlotterMessage.UserList userList = userListBuilder.build();
                sendMessages(userList);
                BlotterMessage.User userMsg = MainApp.getKeycloakService().getRolesUserKeycloak(connect.getUsername());
                sendMessages(userMsg);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    private void onUser(BlotterMessage.User user) {
        try {
            if (user.getStatusUser().equals(BlotterMessage.StatusUser.UPDATE_USER)) {
                if (!user.getPassword().isEmpty()) {
                    MainApp.getKeycloakService().changePassword(user.getUsername(), user.getPassword());
                }
                MainApp.getKeycloakService().updateUser(user);
            }
            sendMessages(user);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private ActorRef addAccount(String account) {
        ActorRef actorRef;
        if (!MainApp.getAccountGroupUser().containsKey(account)) {
            actorRef = MainApp.getSystem().actorOf(ActorGroupPerAccount.props(account, 0d, 3.0).withDispatcher("ActorperAccount"));

            MainApp.getAccountGroupUser().put(account, actorRef);
        } else {
            actorRef = MainApp.getAccountGroupUser().get(account);
        }

        actorRef.tell(new ActorGroupPerAccount.NewActorSession(getSelf(), idSession), ActorRef.noSender());

        return actorRef;
    }

    private ActorRef ensureActorForVoultechAndBindSession(String account) {
        ActorRef ref = MainApp.getAccountGroupUser().computeIfAbsent(account, acc ->
                MainApp.getSystem().actorOf(
                        ActorGroupPerAccount.props(acc, null, null)
                                .withDispatcher("ActorperAccount")
                )
        );
        ref.tell(new ActorGroupPerAccount.NewActorSession(getSelf(), idSession), ActorRef.noSender());
        return ref;
    }

    public void onNewOrderRequest(RoutingMessage.NewOrderRequest msg) {
        try {
            final RoutingMessage.Order order = msg.getOrder();
            final String op = java.util.Optional.ofNullable(order.getOperator()).orElse("").toLowerCase(java.util.Locale.ROOT);


            if (op.contains("voultech")) {
                log.info("🚀 [PassThrough] voultech NewOrder id={} sym={} acc={}",
                        order.getId(), order.getSymbol(), order.getAccount());

                addAccountSitioPrivado(msg);

                MainApp.getIdOrders().put(order.getId(), order);

                ActorRef ref = ensureActorForVoultechAndBindSession(order.getAccount());

                ref.tell(msg, ActorRef.noSender());
                return;
            }

            MainApp.getIdOrders().put(order.getId(), order);
            ActorRef actorRef = addAccount(order.getAccount());
            actorRef.tell(msg, ActorRef.noSender());

        } catch (Exception ex) {
            log.error("onNewOrderRequest error", ex);
        }
    }

    private void addAccountSitioPrivado(RoutingMessage.NewOrderRequest msg) {

        try {

            UserRepresentation userRepresentation = MainApp.getKeycloakService().getUserByUsername(msg.getOrder().getOperator());
            String userId = userRepresentation.getId();
            MainApp.getKeycloakService().updateUserAccount(userId, msg.getOrder().getAccount());
            //MainApp.getKeycloakService().updateUserAccountMargin(userId, msg.getOrder().getAccount() + "|-1");

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public void onReplaceRequest(RoutingMessage.OrderReplaceRequest msg) {
        try {
            RoutingMessage.Order base = MainApp.getIdOrders().get(msg.getId());
            if (base == null) {
                log.warn("Replace sin orden base indexada: {}", msg.getId());
                return;
            }
            String op = java.util.Optional.ofNullable(base.getOperator()).orElse("").toLowerCase();

            if (op.contains("voultech")) {
                ensureActorForVoultechAndBindSession(base.getAccount());
            }
            MainApp.getAccountGroupUser().get(base.getAccount()).tell(msg, ActorRef.noSender());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onCancelRequest(RoutingMessage.OrderCancelRequest msg) {
        try {
            RoutingMessage.Order base = MainApp.getIdOrders().get(msg.getId());
            if (base == null) {
                log.warn("Cancel sin orden base indexada: {}", msg.getId());
                return;
            }
            String op = java.util.Optional.ofNullable(base.getOperator()).orElse("").toLowerCase();

            if (op.contains("voultech")) {
                ensureActorForVoultechAndBindSession(base.getAccount());
            }
            MainApp.getAccountGroupUser().get(base.getAccount()).tell(msg, ActorRef.noSender());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onPortfolioRequest(BlotterMessage.PortfolioRequest portfolioRequest) {

        try {
            ensureSystemPortfolios(portfolioRequest.getUsername());

            if (portfolioRequest.getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO)) {

                ensureSystemPortfolios(portfolioRequest.getUsername());
                HashMap<String, BlotterMessage.Portfolio> porfolios =
                        MainApp.getPortfolioMaps().get(portfolioRequest.getUsername());

                BlotterMessage.PortfolioResponse.Builder portfolioResponse = BlotterMessage.PortfolioResponse.newBuilder()
                        .addAllPostfolio(porfolios.values())
                        .setMarketdataControllerId(portfolioRequest.getMarketdataControllerId())
                        .setStatusPortfolio(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO);

                sendMessages(portfolioResponse.build());

            } else if (portfolioRequest.getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.NEW_PORTFOLIO)) {

                HashMap<String, BlotterMessage.Portfolio> porfolios =
                        MainApp.getPortfolioMaps().get(portfolioRequest.getUsername());

                if (isProtectedPortfolio(portfolioRequest.getNamePortfolio())) {
                    sendPortfolioBlockedNotification("No se puede crear un portafolio con nombre " + portfolioRequest.getNamePortfolio());
                    return;
                }

                // Crea portafolio nuevo
                BlotterMessage.Portfolio newPortfolio = BlotterMessage.Portfolio.newBuilder()
                        .setId(IDGenerator.getID())
                        .setNamePortfolio(portfolioRequest.getNamePortfolio())
                        .setUsername(portfolioRequest.getUsername())
                        .build();

                porfolios.put(newPortfolio.getNamePortfolio(), newPortfolio);
                MainApp.getPortfolioMaps().put(portfolioRequest.getUsername(), porfolios);

                BlotterMessage.PortfolioResponse portfolioResponse = BlotterMessage.PortfolioResponse.newBuilder()
                        .setStatusPortfolio(BlotterMessage.StatusPortfolio.NEW_PORTFOLIO)
                        .setNamePortfolio(portfolioRequest.getNamePortfolio())
                        .setUsername(portfolioRequest.getUsername())
                        .setMarketdataControllerId(portfolioRequest.getMarketdataControllerId())
                        .addPostfolio(newPortfolio)
                        .build();

                sendMessages(portfolioResponse);

            } else if (portfolioRequest.getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.ADD_ASSET)) {

                BlotterMessage.Asset asset = portfolioRequest.getAsset();
                String namePortfolio = portfolioRequest.getNamePortfolio();
                String username = portfolioRequest.getUsername();

                if (isSystemManagedPortfolio(namePortfolio)
                        || (isProtectedPortfolio(namePortfolio) && !isPrincipalPortfolio(namePortfolio))) {
                    sendPortfolioBlockedNotification("El portafolio " + namePortfolio + " es generado por el sistema");
                    return;
                }

                HashMap<String, BlotterMessage.Portfolio> portfolioHashMap =
                        MainApp.getPortfolioMaps().get(username);

                BlotterMessage.Portfolio portfolio = portfolioHashMap.get(namePortfolio);
                BlotterMessage.Portfolio.Builder portfolioToBuider = portfolio.toBuilder()
                        .setUsername(username)
                        .setNamePortfolio(namePortfolio)
                        .addAsset(asset);

                portfolioHashMap.put(portfolioToBuider.getNamePortfolio(), portfolioToBuider.build());
                MainApp.getPortfolioMaps().put(username, portfolioHashMap);

                BlotterMessage.PortfolioResponse portfolioResponse = BlotterMessage.PortfolioResponse.newBuilder()
                        .setStatusPortfolio(BlotterMessage.StatusPortfolio.ADD_ASSET)
                        .setUsername(username)
                        .setNamePortfolio(namePortfolio)
                        .setAsset(asset)
                        .build();

                sendMessages(portfolioResponse);

            } else if (portfolioRequest.getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.REMOVE_ASSET)) {

                try {
                    BlotterMessage.Asset asset = portfolioRequest.getAsset();
                    String namePortfolio = portfolioRequest.getNamePortfolio();
                    String username = portfolioRequest.getUsername();

                    if (isSystemManagedPortfolio(namePortfolio)
                            || (isProtectedPortfolio(namePortfolio) && !isPrincipalPortfolio(namePortfolio))) {
                        sendPortfolioBlockedNotification("El portafolio " + namePortfolio + " es generado por el sistema");
                        return;
                    }

                    HashMap<String, BlotterMessage.Portfolio> portfolioHashMap =
                            MainApp.getPortfolioMaps().get(username);

                    BlotterMessage.Portfolio portfolio = portfolioHashMap.get(namePortfolio);
                    if (portfolio == null) {
                        NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                                .setTitle("Asset Deleted")
                                .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                                .setMessage("El portafolio no existe")
                                .setLevel(NotificationMessage.Level.ERROR)
                                .build();
                        sendMessages(notification);
                        return;
                    }

                    BlotterMessage.Portfolio.Builder portfolioBuild = BlotterMessage.Portfolio.newBuilder()
                            .setNamePortfolio(portfolio.getNamePortfolio())
                            .setUsername(portfolio.getUsername())
                            .setId(portfolio.getId());

                    List<BlotterMessage.Asset> listaInmutable = new ArrayList<>(portfolio.getAssetList());
                    listaInmutable.removeIf(symbol -> symbol.getSymbol().equals(asset.getSymbol()));

                    portfolioBuild.addAllAsset(listaInmutable);
                    portfolioHashMap.put(namePortfolio, portfolioBuild.build());
                    MainApp.getPortfolioMaps().put(username, portfolioHashMap);

                    BlotterMessage.PortfolioResponse portfolioResponse = BlotterMessage.PortfolioResponse.newBuilder()
                            .setStatusPortfolio(BlotterMessage.StatusPortfolio.REMOVE_ASSET)
                            .setUsername(username)
                            .setNamePortfolio(namePortfolio)
                            .setAsset(asset)
                            .build();
                    sendMessages(portfolioResponse);

                    NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                            .setTitle("Asset Deleted")
                            .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                            .setMessage("Asset Deleted " + asset.getSymbol())
                            .setLevel(NotificationMessage.Level.SUCCESS)
                            .build();
                    sendMessages(notification);

                } catch (Exception e) {
                    NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                            .setTitle("Asset Deleted")
                            .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                            .setMessage("The asset couldn't be deleted")
                            .setLevel(NotificationMessage.Level.ERROR)
                            .build();
                    sendMessages(notification);
                }

            } else if (portfolioRequest.getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.DELETE_PORTFOLIO)) {

                try {
                    String username = portfolioRequest.getUsername();
                    String namePortfolio = portfolioRequest.getNamePortfolio();

                    if (isProtectedPortfolio(namePortfolio)) {
                        NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                                .setTitle("Portfolio")
                                .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                                .setMessage("No se puede eliminar el portafolio " + namePortfolio)
                                .setLevel(NotificationMessage.Level.ERROR)
                                .build();
                        sendMessages(notification);
                        return;
                    }

                    HashMap<String, BlotterMessage.Portfolio> portfolios =
                            MainApp.getPortfolioMaps().get(username);

                    if (portfolios == null || !portfolios.containsKey(namePortfolio)) {
                        NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                                .setTitle("Portfolio")
                                .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                                .setMessage("El portafolio no existe")
                                .setLevel(NotificationMessage.Level.ERROR)
                                .build();
                        sendMessages(notification);
                        return;
                    }

                    portfolios.remove(namePortfolio);
                    MainApp.getPortfolioMaps().put(username, portfolios);

                    BlotterMessage.PortfolioResponse resp = BlotterMessage.PortfolioResponse.newBuilder()
                            .setStatusPortfolio(BlotterMessage.StatusPortfolio.DELETE_PORTFOLIO)
                            .setUsername(username)
                            .setNamePortfolio(namePortfolio)
                            .setMarketdataControllerId(portfolioRequest.getMarketdataControllerId())
                            .build();
                    sendMessages(resp);

                    NotificationMessage.Notification ok = NotificationMessage.Notification.newBuilder()
                            .setTitle("Portfolio")
                            .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                            .setMessage("Portafolio \"" + namePortfolio + "\" eliminado")
                            .setLevel(NotificationMessage.Level.SUCCESS)
                            .build();
                    sendMessages(ok);

                } catch (Exception e) {
                    log.error("REMOVE_PORTFOLIO error", e);
                    NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                            .setTitle("Portfolio")
                            .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                            .setMessage("No se pudo eliminar el portafolio")
                            .setLevel(NotificationMessage.Level.ERROR)
                            .build();
                    sendMessages(notification);
                }
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void ensureSystemPortfolios(String username) {
        HashMap<String, BlotterMessage.Portfolio> portfolios = MainApp.getPortfolioMaps().get(username);
        if (portfolios == null) {
            portfolios = new HashMap<>();
        } else {
            portfolios = new HashMap<>(portfolios);
        }

        portfolios.remove(IpsaPortfolioService.DEFAULT_PRIMARY_PORTFOLIO_NAME);

        String ipsaPortfolioName = IpsaPortfolioService.getPortfolioName(MainApp.getProperties());
        if (IpsaPortfolioService.isEnabled(MainApp.getProperties())) {
            BlotterMessage.Portfolio ipsaPortfolio = IpsaPortfolioService.buildPortfolio(
                    username,
                    MainApp.getProperties(),
                    ipsaPortfolioName
            );
            portfolios.put(ipsaPortfolioName, ipsaPortfolio);
        } else {
            portfolios.remove(ipsaPortfolioName);
        }

        String igpaPortfolioName = IgpaPortfolioService.getPortfolioName(MainApp.getProperties());
        if (IgpaPortfolioService.isEnabled(MainApp.getProperties())) {
            BlotterMessage.Portfolio igpaPortfolio = IgpaPortfolioService.buildPortfolio(
                    username,
                    MainApp.getProperties(),
                    igpaPortfolioName
            );
            portfolios.put(igpaPortfolioName, igpaPortfolio);
        } else {
            portfolios.remove(igpaPortfolioName);
        }

        MainApp.getPortfolioMaps().put(username, portfolios);
    }

    private boolean isPrincipalPortfolio(String namePortfolio) {
        return namePortfolio != null && "Principal".equalsIgnoreCase(namePortfolio);
    }

    private boolean isProtectedPortfolio(String namePortfolio) {
        return namePortfolio != null && (isPrincipalPortfolio(namePortfolio)
                || IpsaPortfolioService.getPortfolioName(MainApp.getProperties()).equalsIgnoreCase(namePortfolio)
                || IgpaPortfolioService.getPortfolioName(MainApp.getProperties()).equalsIgnoreCase(namePortfolio));
    }

    private boolean isSystemManagedPortfolio(String namePortfolio) {
        return namePortfolio != null
                && (isPrincipalPortfolio(namePortfolio)
                || (IpsaPortfolioService.isEnabled(MainApp.getProperties())
                && IpsaPortfolioService.getPortfolioName(MainApp.getProperties()).equalsIgnoreCase(namePortfolio)));
    }

    private void sendPortfolioBlockedNotification(String message) {
        NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                .setTitle("Portfolio")
                .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                .setMessage(message)
                .setLevel(NotificationMessage.Level.ERROR)
                .build();
        sendMessages(notification);
    }


    @Data
    @AllArgsConstructor
    public static final class SnapshotPositionsAccount {
        private String account;
        private BlotterMessage.SnapshotPositions snapshotPositions;
    }

}
