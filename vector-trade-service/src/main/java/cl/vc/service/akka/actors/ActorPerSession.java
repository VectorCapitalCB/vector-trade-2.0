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
import cl.vc.service.admin.ClientLogDiagnosticsStore;
import cl.vc.service.akka.actors.mkd.ActorPerSubscriptionMkd;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import cl.vc.service.multibook.Multibook2Repository;
import cl.vc.service.util.MongoHistoryRepository;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.keycloak.representations.idm.UserRepresentation;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.zip.ZipException;


@Slf4j
public class ActorPerSession extends AbstractActor {

    private final Session session;

    private final HashMap<String, ActorRef> actorPerSubscriptionMkdHash = new HashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private SessionsMessage.Connect connect = SessionsMessage.Connect.newBuilder().build();
    private String idSession;

    // ── Debounce de onConnect ───────────────────────────────────────────────
    // Un ActorPerSession = una sesión WebSocket. El primer Connect inicializa todo
    // (Keycloak, addAccount, snapshots). Si el front reenvía Connect repetidamente
    // sobre el MISMO socket (loop de reconexión), no hay que rehacer ese trabajo caro:
    // el cliente ya tiene los datos y se mantiene al día por mensajes en vivo.
    private boolean connectInitialized = false;
    private String initializedUsername = null;
    private String sessionUsername = null;
    private final Set<String> sessionAccounts = new LinkedHashSet<>();
    private int debouncedConnects = 0;
    private long lastDebounceLogMs = 0L;

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
                .match(ActorPerSubscriptionMkd.RefreshPreviousClose.class, this::onRefreshPreviousClose)
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
                .match(HeartbeatTick.class, this::onHeartbeatTick)
                .match(BlotterMessage.User.class, this::onUser)
                .match(BlotterMessage.UserList.class, this::onUserList)
                .match(BlotterMessage.Multibook.class, this::onMultibook)
                .match(MarketDataMessage.BolsaStats.class, this::onBolsaStats)
                .match(BlotterMessage.SnapshotSimultaneas.class, this::onSnapshotSimultaneas)
                .match(BlotterMessage.SnapshotPrestamos.class, this::onSnapshotPrestamos)
                .match(BlotterMessage.HistoricalOrdersRequest.class, this::onHistoricalOrdersRequest)
                .match(BlotterMessage.HistoricalOrdersSnapshot.class, this::onHistoricalOrdersSnapshot)
                .match(BlotterMessage.ClientLogRequest.class, this::sendMessages)
                .match(BlotterMessage.ClientLogResponse.class, this::onClientLogResponse)
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
            Runnable task = () -> getSelf().tell(HeartbeatTick.INSTANCE, ActorRef.noSender());
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
            scheduler.shutdownNow();

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
        } finally {
            String username = sessionUsername;
            if ((username == null || username.isBlank()) && connect != null) {
                username = connect.getUsername();
            }
            if (username != null && !username.isBlank()) {
                boolean removedCurrentSession = MainApp.getUserActiveSessionsMap().remove(username, session);
                MainApp.getUserSessionActorsMap().remove(username, getSelf());
                if (removedCurrentSession) MainApp.getUserSessionConnectedAt().remove(username);
            }
        }
    }

    private void sendMessages(Message message) {
        try {

            if (session != null && session.isOpen() && message != null) {
                ByteBuffer messsage = MessageUtilVT.serializeMessageByteBuffer(message);
                session.getRemote().sendBytes(messsage);
                if (MainApp.isLogOutboundOrders()) logOutbound(message);
            }

        } catch (ZipException e) {
            log.error("[WebSocket] Error de compresión enviando mensaje a sesión {}: {}. Se cerrará la sesión.",
                    this.idSession, e.getMessage(), e);

            try {
                if (this.session != null && this.session.isOpen()) {
                    this.session.close(1011, "Error de compresión WebSocket");
                }
            } catch (Exception closeEx) {
                log.warn("[WebSocket] No se pudo cerrar la sesión {} tras ZipException: {}",
                        this.idSession, closeEx.getMessage());
            }
        } catch (ClosedChannelException e) {
            log.info("[WebSocket] Canal ya cerrado al enviar mensaje a sesión {}.", this.idSession);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /** Debug (flag log.outbound.orders=true): loguea SOLO Order/OrderCancelReject enviados al cliente,
     *  en el hilo del actor (orden real de envio). Default off -> sin costo en el hot path. */
    private void logOutbound(Message message) {
        if (message instanceof RoutingMessage.Order o) {
            log.info("[Outbound][Order] sesion={} id={} clOrdId={} sym={} acc={} side={} ordStatus={} execType={} lastQty={} lastPx={} cumQty={} leaves={} price={} execId={}",
                    idSession, o.getId(), o.getClOrdId(), o.getSymbol(), o.getAccount(), o.getSide(),
                    o.getOrdStatus(), o.getExecType(), o.getLastQty(), o.getLastPx(), o.getCumQty(),
                    o.getLeaves(), o.getPrice(), o.getExecId());
        } else if (message instanceof RoutingMessage.OrderCancelReject r) {
            log.info("[Outbound][OrderCancelReject] sesion={} id={} text={}", idSession, r.getId(), r.getText());
        }
    }

    private void onBolsaStats(MarketDataMessage.BolsaStats stats) {
        sendMessages(stats);
    }

    /**
     * Reparte a los hijos MKD el aviso de que el Admin recargo cierres en Mongo.
     *
     * Va por mensaje y no por acceso directo a proposito: actorPerSubscriptionMkdHash es un HashMap
     * sin sincronizar que solo debe tocarse desde el hilo de este actor. Cada hijo decide si el aviso
     * le corresponde (compara el simbolo) y si tiene que re-emitir.
     */
    private void onRefreshPreviousClose(ActorPerSubscriptionMkd.RefreshPreviousClose msg) {
        try {
            for (ActorRef mkd : actorPerSubscriptionMkdHash.values()) {
                if (mkd != null) mkd.tell(msg, ActorRef.noSender());
            }
        } catch (Exception ex) {
            log.error("[VarPct] error repartiendo refresh de cierre en sesion {}: {}",
                    idSession, ex.getMessage(), ex);
        }
    }

    public void onSnapshotPrestamos(BlotterMessage.SnapshotPrestamos snapshot) {
        sendMessages(snapshot);
    }

    public void onSnapshotSimultaneas(BlotterMessage.SnapshotSimultaneas snapshotSimultaneas) {
        sendMessages(snapshotSimultaneas);
    }

    private void onHistoricalOrdersSnapshot(BlotterMessage.HistoricalOrdersSnapshot snapshot) {
        sendMessages(snapshot);
    }

    private void onClientLogResponse(BlotterMessage.ClientLogResponse response) {
        ClientLogDiagnosticsStore.acceptAsync(sessionUsername, response);
    }

    private void onHistoricalOrdersRequest(BlotterMessage.HistoricalOrdersRequest request) {
        if (!MongoHistoryRepository.isConnected()) {
            sendMessages(historyError(request.getRequestId(),
                    "El historico de ordenes no esta conectado a Mongo"));
            return;
        }
        LinkedHashSet<String> allowed = new LinkedHashSet<>(sessionAccounts);
        if (allowed.isEmpty()) {
            sendMessages(historyError(request.getRequestId(), "No hay cuentas asignadas a esta sesion"));
            return;
        }

        LinkedHashSet<String> selected = new LinkedHashSet<>();
        if (request.getAccountsCount() == 0) {
            selected.addAll(allowed);
        } else {
            request.getAccountsList().stream().filter(allowed::contains).forEach(selected::add);
        }
        if (selected.isEmpty()) {
            sendMessages(historyError(request.getRequestId(), "La cuenta solicitada no esta autorizada"));
            return;
        }

        final java.time.LocalDate from;
        final java.time.LocalDate to;
        try {
            from = parseOptionalDate(request.getFrom());
            to = parseOptionalDate(request.getTo());
            if (from != null && to != null && from.isAfter(to)) {
                throw new IllegalArgumentException("La fecha desde no puede ser posterior a la fecha hasta");
            }
        } catch (IllegalArgumentException e) {
            sendMessages(historyError(request.getRequestId(), e.getMessage()));
            return;
        }

        ActorRef self = getSelf();
        java.util.concurrent.CompletableFuture
                .supplyAsync(() -> MongoHistoryRepository.queryHistoricalOrders(
                        selected, request.getSymbol(), from, to, request.getLimit()))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("[History] consulta fallida usuario={} cuentas={}: {}",
                                initializedUsername, selected, error.toString());
                        self.tell(historyError(request.getRequestId(), "No se pudo consultar el historico"),
                                ActorRef.noSender());
                        return;
                    }
                    BlotterMessage.HistoricalOrdersSnapshot.Builder snapshot =
                            BlotterMessage.HistoricalOrdersSnapshot.newBuilder()
                                    .setRequestId(request.getRequestId())
                                    .setTruncated(result.truncated());
                    for (MongoHistoryRepository.HistoricalOrderBundle bundle : result.orders()) {
                        snapshot.addOrders(BlotterMessage.HistoricalOrderGroup.newBuilder()
                                .setSummary(bundle.summary())
                                .addAllExecutions(bundle.executions()));
                    }
                    self.tell(snapshot.build(), ActorRef.noSender());
                });
    }

    private static java.time.LocalDate parseOptionalDate(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return java.time.LocalDate.parse(value.trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("Fecha invalida: " + value);
        }
    }

    private static BlotterMessage.HistoricalOrdersSnapshot historyError(String requestId, String message) {
        return BlotterMessage.HistoricalOrdersSnapshot.newBuilder()
                .setRequestId(requestId == null ? "" : requestId)
                .setError(message == null ? "Error consultando el historico" : message)
                .build();
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

    private void onHeartbeatTick(HeartbeatTick ignored) {
        SessionsMessage.Ping ping = SessionsMessage.Ping.newBuilder().setId(IDGenerator.getID()).build();
        sendMessages(ping);
    }

    public void onMultibook(BlotterMessage.Multibook multiBook) {

        try {

            if (connect.getUsername().isEmpty()) {
                return;
            }

            String username = connect.getUsername();
            if (!ensureRedisAvailable("multibook")) {
                return;
            }

            // Compatibilidad con fronts previos a Multibook 2.0: los nuevos guardan por HTTP.
            List<BlotterMessage.SubMultibook> list =
                    Multibook2Repository.mergeRows(username, multiBook.getSubmultibookList());

            MainApp.registerMultibookTopicsForStartupMkdWatch(username, list);
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
            if (!ensureRedisAvailable("preselect")) {
                return;
            }

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

    /**
     * Decide si un Connect es un duplicado sobre la MISMA sesión que ya fue inicializada
     * para el MISMO usuario. En ese caso se ignora el trabajo caro (debounce). Un cambio de
     * usuario sobre el mismo socket (re-login) NO se considera duplicado y re-inicializa.
     */
    static boolean isDuplicateConnect(boolean alreadyInitialized, String initializedUsername, String incomingUsername) {
        return alreadyInitialized
                && incomingUsername != null
                && incomingUsername.equals(initializedUsername);
    }

    public void onConnect(SessionsMessage.Connect onConnect) {

        try {

            this.connect = onConnect;
            String username = onConnect.getUsername();
            // Con aislamiento ON: usar la identidad AUTENTICADA (header validado por el filtro),
            // NO la que manda el cliente -> evita reclamar el username de otro y filtrar su token/cuentas.
            if (MainApp.isAccountIsolationEnabled()) {
                String auth = authenticatedUsername();
                if (auth != null && !auth.isEmpty()) {
                    username = auth;
                }
            }

            // Rechazar usuarios bloqueados por el admin antes de hacer cualquier otra cosa.
            // (Se mantiene en CADA Connect: si el admin bloquea a media sesión, el próximo
            //  keepalive/Connect lo expulsa casi en tiempo real. Es barato.)
            if (!username.isEmpty() && MainApp.isUserBlocked(username)) {
                log.warn("[Security] Usuario bloqueado intentó conectarse y fue rechazado: {}", username);
                if (session != null && session.isOpen()) {
                    try {
                        session.close(1008, "Acceso bloqueado por administrador");
                    } catch (Exception e) {
                        log.warn("[Security] Error cerrando sesión de usuario bloqueado '{}': {}", username, e.getMessage());
                    }
                }
                return;
            }

            // Registrar sesión activa para gestión desde el admin (idempotente).
            if (session != null && !username.isEmpty()) {
                sessionUsername = username;
                MainApp.getUserActiveSessionsMap().put(username, session);
                MainApp.getUserSessionActorsMap().put(username, getSelf());
            }

            // ── DEBOUNCE ───────────────────────────────────────────────────────
            // Si ya inicializamos esta sesión para este usuario, NO repetir el trabajo
            // caro (Keycloak getRolesUserKeycloak + addAccount + reenvío de snapshots).
            // Esto evita el martilleo cuando el front reenvía Connect cada ~7s sobre el
            // mismo socket (loop de reconexión) y la contención de escrituras en el WS.
            if (isDuplicateConnect(connectInitialized, initializedUsername, username)) {
                debouncedConnects++;
                long now = System.currentTimeMillis();
                if (now - lastDebounceLogMs > 60_000L) {   // log throttled: 1 vez/min
                    log.warn("[Session] Connect repetido sobre el mismo socket ignorado (debounce) usuario={} repeticiones={} " +
                            "(posible loop de reconexión en el front)", username, debouncedConnects);
                    lastDebounceLogMs = now;
                }
                return;
            }

            log.info("onConnect - procesando usuario: {}", username);

            List<BlotterMessage.ClientLogRequest> pendingLogRequests =
                    ClientLogDiagnosticsStore.pendingRequestsFor(username);
            pendingLogRequests.forEach(this::sendMessages);
            if (!pendingLogRequests.isEmpty()) {
                log.info("[ClientLogs] Se reenviaron {} solicitudes pendientes al reconectar usuario={}",
                        pendingLogRequests.size(), username);
            }

            // Primera inicialización real de esta sesión.
            if (session != null && !username.isEmpty()) {
                MainApp.getUserSessionConnectedAt().put(username, System.currentTimeMillis());
            }

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
                sessionAccounts.clear();
                sessionAccounts.addAll(userMsg.getAccountList());
                userMsg.getAccountList().forEach(this::addAccount);
            }

            if (username.isEmpty()) {
                return;
            }

            if (!ensureRedisAvailable("connect-multibook")) {
                return;
            }

            List<BlotterMessage.SubMultibook> list = Multibook2Repository.effectiveRows(username);
            if (!list.isEmpty()) {
                MainApp.registerMultibookTopicsForStartupMkdWatch(username, list);
                BlotterMessage.Multibook.Builder multibook = BlotterMessage.Multibook.newBuilder()
                        .addAllSubmultibook(list)
                        .setUsername(username);
                sendMessages(multibook.build());
            }

            // Inicialización completa: los próximos Connect sobre este mismo socket
            // se ignorarán (debounce) mientras el usuario no cambie.
            connectInitialized = true;
            initializedUsername = username;

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
                    // IDOR: con aislamiento ON, solo el propio usuario autenticado puede cambiar su clave.
                    if (MainApp.isAccountIsolationEnabled()) {
                        String auth = authenticatedUsername();
                        if (auth == null || !auth.equalsIgnoreCase(user.getUsername())) {
                            log.warn("[Security] Cambio de clave rechazado: '{}' intentó cambiar la de '{}'", auth, user.getUsername());
                            return;
                        }
                    }
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
        boolean created = false;
        ActorRef actorRef;
        if (!MainApp.getAccountGroupUser().containsKey(account)) {
            Double margin = MainApp.getAccountMarginCache().getOrDefault(account, 0.0);
            Double leverage = MainApp.getAccountLeverageCache().getOrDefault(account, 3.0);
            actorRef = MainApp.getSystem().actorOf(ActorGroupPerAccount.props(account, margin, leverage).withDispatcher("ActorperAccount"));
            MainApp.getAccountGroupUser().put(account, actorRef);
            created = true;
        } else {
            actorRef = MainApp.getAccountGroupUser().get(account);
        }

        actorRef.tell(new ActorGroupPerAccount.NewActorSession(getSelf(), idSession), ActorRef.noSender());
        if (created) {
            actorRef.tell(ActorGroupPerAccount.Initialize.INSTANCE, ActorRef.noSender());
        }

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
            // Rate-limit: demasiadas órdenes por segundo desde esta IP → auto-bloquear
            String clientIp = getClientIp();
            if (MainApp.recordIpOrderExceeded(clientIp)) {
                MainApp.blockIp(clientIp);
                log.warn("[IpSecurity] IP auto-bloqueada por exceso de órdenes: {}", clientIp);
                try { if (session != null && session.isOpen()) session.close(1008, "IP bloqueada: demasiadas órdenes"); } catch (Exception ignored) {}
                return;
            }

            final RoutingMessage.Order order = msg.getOrder();

            // Aislamiento de cuentas: el operador solo rutea en SUS cuentas (identidad autenticada, no la del mensaje).
            if (!isAccountAllowed(order.getAccount())) {
                log.warn("[Security] Orden rechazada: cuenta {} no autorizada para el usuario autenticado (id={})",
                        order.getAccount(), order.getId());
                sendMessages(order.toBuilder()
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setExecId(IDGenerator.getID())
                        .setText("Cuenta no autorizada para este usuario")
                        .setLeaves(0d)
                        .build());
                return;
            }

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

    private java.util.Set<String> allowedAccountsCache;

    /** Username autenticado por el filtro (del header Authorization de la sesión), NO el que manda el cliente. */
    private String authenticatedUsername() {
        try {
            if (session == null || session.getUpgradeRequest() == null) return null;
            String auth = session.getUpgradeRequest().getHeader("Authorization");
            if (auth == null || !auth.startsWith("Basic ")) return null;
            String decoded = new String(java.util.Base64.getDecoder().decode(auth.substring(6).trim()),
                    java.nio.charset.StandardCharsets.UTF_8);
            int sep = decoded.indexOf(':');
            if (sep < 0) return null;
            return cl.vc.module.protocolbuff.crypt.AESEncryption.decrypt(decoded.substring(0, sep));
        } catch (Exception e) {
            log.warn("[Security] no se pudo resolver la identidad autenticada: {}", e.toString());
            return null;
        }
    }

    /**
     * ¿La orden puede rutear en esa cuenta? Con el flag OFF -> siempre true (comportamiento actual).
     * Con ON -> la cuenta debe estar en las que Keycloak asigna al usuario AUTENTICADO (no al del mensaje).
     * Fail-closed: si el aislamiento está ON y no hay identidad, rechaza.
     */
    private boolean isAccountAllowed(String account) {
        if (!MainApp.isAccountIsolationEnabled()) return true;
        if (allowedAccountsCache == null) {
            String user = authenticatedUsername();
            if (user == null || user.isEmpty()) {
                log.warn("[Security] aislamiento ON pero sin identidad autenticada -> rechazo (fail-closed)");
                return false;
            }
            try {
                BlotterMessage.User u = MainApp.getKeycloakService().getRolesUserKeycloak(user);
                allowedAccountsCache = new java.util.HashSet<>(u.getAccountList());
            } catch (Exception e) {
                log.error("[Security] no se pudieron obtener las cuentas del usuario {}: {}", user, e.toString());
                return false;
            }
        }
        return allowedAccountsCache.contains(account);
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
            // Rate-limit de órdenes
            String clientIp = getClientIp();
            if (MainApp.recordIpOrderExceeded(clientIp)) {
                MainApp.blockIp(clientIp);
                log.warn("[IpSecurity] IP auto-bloqueada por exceso de órdenes (replace): {}", clientIp);
                try { if (session != null && session.isOpen()) session.close(1008, "IP bloqueada: demasiadas órdenes"); } catch (Exception ignored) {}
                return;
            }

            RoutingMessage.Order base = MainApp.getIdOrders().get(msg.getId());
            if (base == null) {
                log.warn("Replace sin orden base indexada: {}", msg.getId());
                return;
            }
            if (!isAccountAllowed(base.getAccount())) {
                log.warn("[Security] Replace rechazado: cuenta {} no autorizada (id={})", base.getAccount(), msg.getId());
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
            if (!isAccountAllowed(base.getAccount())) {
                log.warn("[Security] Cancel rechazado: cuenta {} no autorizada (id={})", base.getAccount(), msg.getId());
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

    /** Extrae la IP del cliente de la sesión WebSocket activa. */
    private String getClientIp() {
        try {
            if (session != null && session.getRemoteAddress() != null) {
                return session.getRemoteAddress().getAddress().getHostAddress();
            }
        } catch (Exception ignored) {}
        return "unknown";
    }

    private boolean ensureRedisAvailable(String operation) {
        if (MainApp.ensureRedisReady("websocket-" + operation)) {
            return true;
        }

        NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                .setTitle("Redis")
                .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                .setMessage("Redis no disponible para " + operation + ". Usa Admin > Redis Recovery para reconectar.")
                .setLevel(NotificationMessage.Level.ERROR)
                .build();
        sendMessages(notification);
        log.error("[Redis] Operación {} rechazada: Redis no disponible ({})", operation, MainApp.getRedisLastError());
        return false;
    }

    public void onPortfolioRequest(BlotterMessage.PortfolioRequest portfolioRequest) {

        try {
            if (!ensureRedisAvailable("portfolio")) {
                return;
            }
            // Asegura nodo de usuario con portafolio "Principal"
            if (!MainApp.getPortfolioMaps().containsKey(portfolioRequest.getUsername())) {
                BlotterMessage.Portfolio newPortfolio = BlotterMessage.Portfolio.newBuilder()
                        .setId("Principal")
                        .setNamePortfolio("Principal")
                        .setUsername(portfolioRequest.getUsername())
                        .build();

                HashMap<String, BlotterMessage.Portfolio> portfolios = new HashMap<>();
                portfolios.put(newPortfolio.getNamePortfolio(), newPortfolio);
                MainApp.getPortfolioMaps().put(portfolioRequest.getUsername(), portfolios);
                // Ojo: si aparece seguido para el mismo usuario, la clave con la que se guarda no es
                // la misma con la que se consulta y los portafolios anteriores quedan huerfanos en Redis.
                log.warn("[Portfolio] usuario '{}' no existia en Redis: creado desde cero con solo 'Principal'",
                        portfolioRequest.getUsername());
            }

            if (portfolioRequest.getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO)) {

                HashMap<String, BlotterMessage.Portfolio> porfolios =
                        MainApp.getPortfolioMaps().get(portfolioRequest.getUsername());
                MainApp.registerPortfolioTopicsForStartupMkdWatch(portfolioRequest.getUsername(), porfolios);

                log.info("[Portfolio][SNAPSHOT] usuario='{}' devuelve {} portafolios: {}",
                        portfolioRequest.getUsername(), porfolios.size(), porfolios.keySet());

                BlotterMessage.PortfolioResponse.Builder portfolioResponse = BlotterMessage.PortfolioResponse.newBuilder()
                        .addAllPostfolio(porfolios.values())
                        .setMarketdataControllerId(portfolioRequest.getMarketdataControllerId())
                        .setStatusPortfolio(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO);

                sendMessages(portfolioResponse.build());

            } else if (portfolioRequest.getStatusPortfolio().equals(BlotterMessage.StatusPortfolio.NEW_PORTFOLIO)) {

                HashMap<String, BlotterMessage.Portfolio> porfolios =
                        MainApp.getPortfolioMaps().get(portfolioRequest.getUsername());

                // Crea portafolio nuevo
                BlotterMessage.Portfolio newPortfolio = BlotterMessage.Portfolio.newBuilder()
                        .setId(IDGenerator.getID())
                        .setNamePortfolio(portfolioRequest.getNamePortfolio())
                        .setUsername(portfolioRequest.getUsername())
                        .build();

                porfolios.put(newPortfolio.getNamePortfolio(), newPortfolio);
                MainApp.getPortfolioMaps().put(portfolioRequest.getUsername(), porfolios);
                MainApp.registerPortfolioTopicsForStartupMkdWatch(portfolioRequest.getUsername(), porfolios);

                log.info("[Portfolio][NEW] usuario='{}' creo '{}'; persistidos ahora: {}",
                        portfolioRequest.getUsername(), newPortfolio.getNamePortfolio(),
                        MainApp.getPortfolioMaps().get(portfolioRequest.getUsername()).keySet());

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

                HashMap<String, BlotterMessage.Portfolio> portfolioHashMap =
                        MainApp.getPortfolioMaps().get(username);

                BlotterMessage.Portfolio portfolio = portfolioHashMap.get(namePortfolio);
                BlotterMessage.Portfolio.Builder portfolioToBuider = portfolio.toBuilder()
                        .setUsername(username)
                        .setNamePortfolio(namePortfolio)
                        .addAsset(asset);

                portfolioHashMap.put(portfolioToBuider.getNamePortfolio(), portfolioToBuider.build());
                MainApp.getPortfolioMaps().put(username, portfolioHashMap);
                MainApp.registerPortfolioTopicsForStartupMkdWatch(username, portfolioHashMap);

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
                    MainApp.registerPortfolioTopicsForStartupMkdWatch(username, portfolioHashMap);

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

                    if ("Principal".equalsIgnoreCase(namePortfolio)) {
                        NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                                .setTitle("Portfolio")
                                .setTime(TimeGenerator.getTimeGeneral(MainApp.getZoneId()))
                                .setMessage("No se puede eliminar el portafolio Principal")
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


    @Data
    @AllArgsConstructor
    public static final class SnapshotPositionsAccount {
        private String account;
        private BlotterMessage.SnapshotPositions snapshotPositions;
    }

    private enum HeartbeatTick {
        INSTANCE
    }

}
