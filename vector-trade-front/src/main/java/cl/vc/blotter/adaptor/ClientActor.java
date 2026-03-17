package cl.vc.blotter.adaptor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.blotter.Repository;
import cl.vc.blotter.controller.*;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Tab;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;


@Slf4j
public class ClientActor extends AbstractActor {

    private final static HashMap<String, RoutingMessage.Order> ordersById = new HashMap<>();

    public static Props props() {
        return Props.create(ClientActor.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onTransportingObjects)
                .match(BlotterMessage.PortfolioResponse.class, this::onPortfolioResponse)
                .match(BlotterMessage.PreselectResponse.class, this::onPreselectResponse)
                .match(BlotterMessage.SnapshotPositionHistory.class, this::onSnapshotPositionHistory)
                .match(BlotterMessage.SnapshotPositions.class, this::onPositionSnappshot)
                .match(BlotterMessage.Patrimonio.class, this::onPatrimonio)
                .match(BlotterMessage.Balance.class, this::onBalance)
                .match(NotificationMessage.Notification.class, this::onNotification)
                .match(MarketDataMessage.Snapshot.class, this::onSnappshot)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(SessionsMessage.Connect.class, this::onConnect)
                .match(SessionsMessage.Pong.class, this::onPong)
                .match(SessionsMessage.Ping.class, this::onPing)
                .match(SessionsMessage.Disconnect.class, this::onDisconnect)
                .match(RoutingMessage.OrderCancelReject.class, this::onCancelReject)
                .match(MarketDataMessage.IncrementalBook.class, this::onIncrementalBook)
                .match(MarketDataMessage.Rejected.class, this::onmkdReject)
                .match(MarketDataMessage.Statistic.class, this::onStatistic)
                .match(MarketDataMessage.Trade.class, this::onTrade)
                .match(MarketDataMessage.News.class, this::onNews)
                .match(MarketDataMessage.SnapshotNews.class, this::onSnapshotNews)
                .match(NotificationMessage.NotificationResponse.class, this::onNotificationResponse)
                .match(BktStrategyProtos.SnapshotBasket.class, this::onBasketMessage)
                .match(BlotterMessage.UserList.class, this::onUserList)
                .match(BlotterMessage.User.class, this::onUser)
                .match(BlotterMessage.Multibook.class, this::onMultibook)
                .match(MarketDataMessage.TradeGeneral.class, this::onTradeGeneral)
                .match(MarketDataMessage.SnapshotTradeGeneral.class, this::onSnapshotTradeGeneral)
                .match(BlotterMessage.SnapshotSimultaneas.class, this::onSnapshotSimultaneas)
                .match(MarketDataMessage.SecurityList.class, this::onSecurityList)
                .match(BlotterMessage.SnapshotPrestamos.class, this::onSnapshotPrestamos)
                .match(MarketDataMessage.BolsaStats.class, this::onBolsaStats)
                .build();


    }

    @Override
    public void preStart() {
    }

    private void onBolsaStats(MarketDataMessage.BolsaStats stats) {
        // Las estadisticas de mercado se toman exclusivamente del canal candle.
    }

    private void onSnapshotSimultaneas(BlotterMessage.SnapshotSimultaneas snapshotSimultaneas) {
        Repository.getPositionSimultaneasController().addSnapshot(snapshotSimultaneas);
    }

    private void onMultibook(BlotterMessage.Multibook multibook) {
        Repository.setMultibook(multibook);
    }

    private void onUser(BlotterMessage.User user) {

        try {

            Platform.runLater(() -> {

                Repository.enviasubscripcionAll();

                Repository.createSuscripcion(
                        Repository.getDolarSymbol(),
                        MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL,
                        RoutingMessage.SettlType.T2,
                        RoutingMessage.SecurityType.CS);

                Repository.setUser(user);

                if (user.getIsAdmin()) {
                    Repository.getFooterController().btnAdminUser.setDisable(false);
                    Repository.getFooterController().btnAdminUser.setVisible(true);
                    Repository.getFooterController().btnAdminUser.setManaged(true);

                    ImageView imageView = new ImageView(new Image(Objects.requireNonNull(getClass().getResourceAsStream("/blotter/img/admin.png"))));
                    imageView.setFitHeight(35);
                    imageView.setFitWidth(35);
                    Repository.getFooterController().btnAdminUser.setGraphic(imageView);
                }

                boolean isSmart = !user.getRoles().getPerfil().isEmpty() && user.getRoles().getPerfil().contains("avanzado");

                if (isSmart) {
                    Repository.setIsLight(false);
                } else {
                    Repository.setIsLight(true);
                    Platform.runLater(() -> {
                        var pc = Repository.getPrincipalController();
                        if (pc != null) {
                            pc.isLight();
                        } else {
                            log.warn("PrincipalController todavía no está disponible al aplicar LIGHT.");
                        }
                    });
                }


                ObservableList<RoutingMessage.StrategyOrder> strtate = FXCollections.observableArrayList();
                strtate.addAll(user.getRoles().getStrategyList());
                strtate.add(RoutingMessage.StrategyOrder.NONE_STRATEGY);
                Repository.getPrincipalController().getLanzadorController().getStrategOrder().setItems(strtate);


                ObservableList<RoutingMessage.ExecBroker> broker = FXCollections.observableArrayList();
                broker.addAll(user.getRoles().getBrokerList());
                List<String> defaultOrders = user.getRoles().getDefaultRoutingList();
                Repository.setDefaultRoutingList(defaultOrders);
                LanzadorController lanz = Repository.getPrincipalController().getLanzadorController();
                lanz.getDeaultoForm().clear();

                if (defaultOrders != null) {
                    Set<String> uniqueOrders = new LinkedHashSet<>(defaultOrders);
                    for (String order : uniqueOrders) {
                        String[] attributes = order.split("-");
                        if (attributes.length >= 9) {
                            String key = attributes[0]; // SecurityExchangeRouting.name()
                            lanz.getDeaultoForm().put(key, new ArrayList<>(Arrays.asList(attributes)));
                        } else {
                            log.warn("DefaultRouting inválido (se esperan 9 campos): {}", order);
                        }
                    }
                }



                ObservableList<MarketDataMessage.SecurityExchangeMarketData> desnitoMKD = FXCollections.observableArrayList();
                desnitoMKD.addAll(user.getRoles().getDestinoMKDList());

                Repository.getPrincipalController().getMarketDataPortfolioViewControllers().forEach((key, value) -> {
                    value.cbMarket.setItems(desnitoMKD);
                    value.cbMarket.getSelectionModel().select(MarketDataMessage.SecurityExchangeMarketData.BCS);
                });

                Repository.getPrincipalController().getLanzadorController().getBrokerOrder().setItems(broker);

                // Security Exchange del usuario
                ObservableList<RoutingMessage.SecurityExchangeRouting> securityExchangeUser = FXCollections.observableArrayList();
                securityExchangeUser.addAll(user.getRoles().getDestinoRoutingList());

                Repository.getPrincipalController().getLanzadorController().getSecExchOrder().setItems(securityExchangeUser);

                // Filtro de Security Exchange posiciones
                Repository.getPrincipalController().getPositionsViewController().getSecurityExchangeFilter().setItems(FXCollections.observableArrayList(securityExchangeUser));
                Repository.getPrincipalController().getPositionsViewController().getSecurityExchangeFilter().getItems().add(RoutingMessage.SecurityExchangeRouting.ALL_SECURITY_EXCHANGE);
                Repository.getPrincipalController().getPositionsViewController().getSecurityExchangeFilter().getSelectionModel().selectFirst();

                // Código operador
                ObservableList<String> codeOperator = FXCollections.observableArrayList();
                codeOperator.addAll(user.getRoles().getCodeOperatorList());

                Repository.getPrincipalController().getLanzadorController().getCOperador().setItems(codeOperator);
                Repository.getPrincipalController().getLanzadorController().getCOperador().getSelectionModel().selectFirst();


                ObservableList<String> allAccountUsers = FXCollections.observableArrayList();
                ObservableList<String> accountUsers = FXCollections.observableArrayList();
                allAccountUsers.addAll(user.getAccountList());

                if (!allAccountUsers.contains(Repository.getALL_ACCOUNT())) {
                    allAccountUsers.add(Repository.getALL_ACCOUNT());
                }

                accountUsers.addAll(user.getAccountList());

                if (accountUsers.contains(Repository.getALL_ACCOUNT())) {
                    accountUsers.remove(Repository.getALL_ACCOUNT());
                }

                Repository.getBalanceControllerList().forEach(s -> {
                    s.actualizarCuentas(accountUsers);
                });

                LanzadorController lanzadorController = Repository.getPrincipalController().getLanzadorController();
                if (lanzadorController != null) {
                    lanzadorController.getAcAccount().setItems(accountUsers);
                    lanzadorController.getAcAccount().getSelectionModel().selectFirst();
                    Platform.runLater(() -> lanzadorController.applyDefaultRoutingFromMapOrFirst());
                }


                Repository.getPrincipalController().getRoutingViewController().getAccountFilter().setItems(allAccountUsers);
                Repository.getPrincipalController().getRoutingViewController().getAccountFilter().getSelectionModel().select(Repository.getALL_ACCOUNT());

                if (Repository.getPrincipalController().getPositionsViewController().getAccountFilter() != null) {
                    Repository.getPrincipalController().getRoutingViewController().getAccountFilter().setItems(accountUsers);
                    Repository.getPrincipalController().getRoutingViewController().getAccountFilter().getSelectionModel().selectFirst();
                }

                Repository.getPrincipalController().getPositionsViewController().getAccountFilter().setItems(accountUsers);
                Repository.getPrincipalController().getPositionsViewController().getAccountFilter().getSelectionModel().selectFirst();


                Repository.getPrincipalController().getRoutingViewController().getAccountFilter().getItems().add(Repository.getALL_ACCOUNT());
                Repository.getPrincipalController().getRoutingViewController().getAccountFilter().getSelectionModel().select(Repository.getALL_ACCOUNT());



                Repository.getPositionHistoricalControllerList().forEach(s -> {
                    s.updateAllAccounts(accountUsers);
                    s.setUser(user);
                });


                Repository.setLoginControllerUser(user);
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            Repository.setLoginControllerUser(user);
        }
    }

    public void onSnapshotPositionHistory(BlotterMessage.SnapshotPositionHistory snapshotPositions) {

        if (snapshotPositions.getPositionsHistoryList().isEmpty()) {
            return;
        }

        Repository.getPositionHistoricalControllerList().forEach(s -> {
            s.addPositions(snapshotPositions);
        });
    }

    private void onUserList(BlotterMessage.UserList userList) {
        Repository.getAdminController().updateUsers(userList);
    }

    private void onTransportingObjects(TransportingObjects message) {
        getSelf().tell(message.getMessage(), ActorRef.noSender());
    }

    private void onPatrimonio(BlotterMessage.Patrimonio patrimonio) {
        Repository.getBalanceControllerList().forEach(s -> {
            s.updateTreeTableView(patrimonio);
        });
    }

    private void onSnapshotPrestamos(BlotterMessage.SnapshotPrestamos snapshot) {
        javafx.application.Platform.runLater(() ->
                Repository.getPrestamosControllerList()
                        .forEach(c -> c.updateTableView(snapshot))
        );
    }

    private void onBalance(BlotterMessage.Balance balance) {
        Repository.getBalanceControllerList().forEach(s -> {
            s.updateBalance(balance);
        });
    }

    private void onPing(SessionsMessage.Ping ping) {
    }

    private void onPong(SessionsMessage.Pong pong) {

    }

    private void onCancelReject(RoutingMessage.OrderCancelReject response) {
        Platform.runLater(() -> {
            Notifier.INSTANCE.notifyError("Rejected", response.getText());
        });
    }

    private void onNotificationResponse(NotificationMessage.NotificationResponse response) {
        Platform.runLater(() -> {
            Repository.getNotificationController().getData().addAll(response.getNotificationlistList());
        });
    }

    private synchronized void onBasketMessage(BktStrategyProtos.SnapshotBasket snapshot) {

        try {

            Platform.runLater(() -> {
                try {

                    if (Repository.getIsLight()) {
                        return;
                    }

                    if (!Repository.getBasketTabController().containsKey(snapshot.getBasket().getBasketID())) {

                        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/BasketMainTabView.fxml"));
                        AnchorPane anchorPane = loader.load();
                        BasketTabController tabBasketsController = loader.getController();
                        Repository.getBasketTabController().put(snapshot.getBasket().getBasketID(), tabBasketsController);
                        Tab tab = new Tab(snapshot.getBasket().getBasketID());
                        tab.setContent(anchorPane);

                        Repository.getBasketController().getTabBasket().getTabs().add(tab);

                        tabBasketsController.getData().add(snapshot.getBasket());
                        replaceBasketOrders(tabBasketsController, snapshot.getBasket().getOrdersList());

                    } else {

                        Repository.getBasketTabController().get(snapshot.getBasket().getBasketID()).getData().clear();
                        Repository.getBasketTabController().get(snapshot.getBasket().getBasketID()).getData().add(snapshot.getBasket());
                        BasketTabController tabBasketsController = Repository.getBasketTabController().get(snapshot.getBasket().getBasketID());

                        replaceBasketOrders(tabBasketsController, snapshot.getBasket().getOrdersList());

                        Repository.getBasketTabController().get(snapshot.getBasket().getBasketID()).getBasketMainTable().refresh();
                        tabBasketsController.getExecutionsOrderController().getTableExecutionReports().refresh();
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    private void onSecurityList(MarketDataMessage.SecurityList securityList) {

        Repository.createSuscripcion(
                Repository.getDolarSymbol(),
                MarketDataMessage.SecurityExchangeMarketData.DATATEC_XBCL,
                RoutingMessage.SettlType.T2,
                RoutingMessage.SecurityType.CS);


        securityList.getListSecuritiesList().forEach(s -> {
            Repository.getSecurityListMaps().put(s.getSymbol(), s.getSecurityExchange().name(), s);
        });
    }

    private void onPositionSnappshot(BlotterMessage.SnapshotPositions snapshot) {
        Repository.getPrincipalController().getPositionsViewController().addSnpashot(snapshot);
    }

    private void onmkdReject(MarketDataMessage.Rejected rejected) {
        Notifier.INSTANCE.notifyError("Rechazo MKD", rejected.getText());
    }

    private void onIncrementalBook(MarketDataMessage.IncrementalBook incremental) {
        try {

            if (!Repository.getBookPortMaps().containsKey(incremental.getId())) {
                log.error("llega informacion que no tengo onIncrementalBook {}", incremental.getId());
                return;
            }

            Platform.runLater(() -> {
                Repository.getBookPortMaps().get(incremental.getId()).update(incremental);
            });


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onNews(MarketDataMessage.News news) {
        try {

            if (!Repository.isNotification()) {
                return;
            }

            Platform.runLater(() -> {
                Repository.getNews().add(news);
                Notifier.INSTANCE.notifyInfo("Noticias", news.getLineoftext() + "\n" + news.getTexto()); //todo descoemtar
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onSnapshotNews(MarketDataMessage.SnapshotNews news) {
        Repository.getNews().addAll(news.getNewsList());
    }

    private void onSnapshotTradeGeneral(MarketDataMessage.SnapshotTradeGeneral snapshot) {
        Platform.runLater(() -> {
            Repository.replaceTradeGenerales(snapshot.getTradesList());
            Repository.getPrincipalController().getTabGenerales().setText("Trades Generales (" + Repository.getTradeGenerales().size() + ")");
        });

    }

    private void onTradeGeneral(MarketDataMessage.TradeGeneral tradeg) {
        Platform.runLater(() -> {
            Repository.addTradeGeneral(tradeg);
            Repository.getPrincipalController().getTabGenerales().setText("Trades Generales (" + Repository.getTradeGenerales().size() + ")");
        });

    }

    private void onTrade(MarketDataMessage.Trade trade) {

        try {


            Platform.runLater(() -> {

                String id = TopicGenerator.getTopicMKD(trade);
                if (Repository.getBookPortMaps().containsKey(id)) {
                    Repository.getBookPortMaps().get(id).addtrade(trade);
                }
            });


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }

    }

    private void onStatistic(MarketDataMessage.Statistic statistic) {

        try {

            String id = TopicGenerator.getTopicMKD(statistic);

            if (Repository.getBookPortMaps().containsKey(id)) {
                BookVO bookVO = Repository.getBookPortMaps().get(id);
                bookVO.updateStatistic(statistic);

                if (isActiveMultibookSubscription(id) && isZeroStatistic(statistic)) {
                    log.warn("MULTIBOOK zero statistic id={} symbol={} market={} settl={} securityType={} bidPx={} askPx={} last={} prevClose={} volume={}",
                            id,
                            statistic.getSymbol(),
                            statistic.getSecurityExchange(),
                            statistic.getSettlType(),
                            statistic.getSecurityType(),
                            statistic.getBidPx(),
                            statistic.getAskPx(),
                            statistic.getLast(),
                            statistic.getPreviusClose(),
                            statistic.getTradeVolume());
                }

                LibroEmergentePrincipalController.getMapsLibroMaps().entrySet().forEach(s -> {
                    s.getValue().update(bookVO);
                });

            }

            if (statistic.getSymbol().equals(Repository.getDolarSymbol())) {
                Platform.runLater(() -> {
                    try {
                        Repository.getFooterController().updateDollarStatistics(statistic);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }




        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onSnappshot(MarketDataMessage.Snapshot snapshot) {

        try {

            String id = TopicGenerator.getTopicMKD(snapshot);

            if(!Repository.getBookPortMaps().containsKey(id)){
                log.error("llega informacion que no tengo onSnappshot {} ", id);
                return;
            }

            if (isActiveMultibookSubscription(id)
                    && snapshot.getBidsList().isEmpty()
                    && snapshot.getAsksList().isEmpty()
                    && (snapshot.getStatistic() == null || isZeroStatistic(snapshot.getStatistic()))) {
                log.warn("MULTIBOOK empty snapshot id={} symbol={} market={} settl={} securityType={} bids={} asks={}",
                        id,
                        snapshot.getSymbol(),
                        snapshot.getSecurityExchange(),
                        snapshot.getSettlType(),
                        snapshot.getSecurityType(),
                        snapshot.getBidsCount(),
                        snapshot.getAsksCount());
            }

            Repository.getBookPortMaps().get(id).update(snapshot);


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private boolean isActiveMultibookSubscription(String id) {
        return Repository.getLibroEmergenteMap().values().stream()
                .filter(Objects::nonNull)
                .anyMatch(controller -> id.equals(controller.getIdSubscribeBook()));
    }

    private boolean isZeroStatistic(MarketDataMessage.Statistic statistic) {
        if (statistic == null) {
            return true;
        }
        return statistic.getBidPx() <= 0d
                && statistic.getAskPx() <= 0d
                && statistic.getLast() <= 0d
                && statistic.getPreviusClose() <= 0d
                && statistic.getTradeVolume() <= 0d
                && statistic.getIndicativeOpening() <= 0d
                && statistic.getReferencialPrice() <= 0d;
    }

    private void onConnect(SessionsMessage.Connect message) {
        try {

            SessionsMessage.Connect connect = message.toBuilder().setUsername(Repository.getUsername()).build();

            Repository.getClientService().sendMessage(connect);

            NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                    .setComments(message.getText())
                    .setComponent(message.getComponent())
                    .setTypeState(NotificationMessage.TypeState.CONNECTION)
                    .setLevel(NotificationMessage.Level.INFO)
                    .setTime(TimeGenerator.getTimeProto())
                    .setTitle("Services Connection").build();

            if (Repository.getNotificationController() != null) {
                Repository.getNotificationController().getData().add(notification);
            }

            Notifier.INSTANCE.notifyInfo("Service", "Service Connected");


            if (Repository.getPrincipalController() != null && Repository.getPrincipalController().getMarketDataController() != null) {
                Repository.getPrincipalController().getMarketDataPortfolioViewController().requestPortfolio();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onDisconnect(SessionsMessage.Disconnect message) {
        try {


            Repository.getPositionHistoricalControllerList().forEach(PositionHistoricalController::clear);

            Repository.getBalanceControllerList().forEach(BalanceController::removeData);

            NotificationMessage.Notification notification = NotificationMessage.Notification.newBuilder()
                    .setComments(message.getText())
                    .setComponent(message.getComponent())
                    .setTypeState(NotificationMessage.TypeState.DISCONNECTION)
                    .setLevel(NotificationMessage.Level.FATAL)
                    .setTime(TimeGenerator.getTimeProto())
                    .setTitle("Error Services").build();

            if (Repository.getNotificationController() != null) {
                Repository.getNotificationController().getData().add(notification);
            }


            Repository.getMarketDataController().getMarketDataTradeTableG().getItems().clear();
            Repository.getMarketDataController().getMarketDataTradeTable().getItems().clear();

            Repository.getPrincipalController().getMarketDataPortfolioViewControllers().forEach((key, value) -> value.getData().clear());
            Repository.getRoutingController().getWorkingOrderController().getData().clear();
            Repository.getRoutingController().getExecutionsOrderController().getData().clear();

            Platform.runLater(() -> {
                Repository.getPrincipalController().getMarketDataController().getTpMkData().getTabs().clear();
            });

            Repository.getSubscribeIdsMaps().clear();


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onPreselectResponse(BlotterMessage.PreselectResponse message) {
        Platform.runLater(() -> {
            try {
                Repository.setPendingPreselect(message.getOrdersList());
                var pc   = Repository.getPrincipalController();
                var ctrl = (pc != null) ? pc.getPreselectOrdersController() : null;

                if (ctrl != null) {
                    ctrl.getData().setAll(message.getOrdersList());
                    ctrl.getTableExecutionReports().refresh();
                    ctrl.getTableExecutionReports().sort();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
    }

    private void onNotification(NotificationMessage.Notification message) {
        try {

            if (message.getLevel().equals(NotificationMessage.Level.ERROR) ||
                    message.getLevel().equals(NotificationMessage.Level.FATAL)) {
                Notifier.INSTANCE.notifyError(message.getTitle(), message.getMessage());

            } else if (message.getLevel().equals(NotificationMessage.Level.INFO) ||
                    message.getLevel().equals(NotificationMessage.Level.SUCCESS)) {
                Notifier.INSTANCE.notifyInfo(message.getTitle(), message.getMessage());
            } else {
                Notifier.INSTANCE.notifyWarning(message.getTitle(), message.getMessage());
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onPortfolioResponse(BlotterMessage.PortfolioResponse response) {

        Repository.setPortfolioResponse(response);
        Repository.getPrincipalController().addDatosDeMercado();


    }

    private void replaceBasketOrders(BasketTabController tabBasketsController, List<RoutingMessage.Order> orders) {
        if (tabBasketsController == null) {
            return;
        }

        LinkedHashMap<String, RoutingMessage.Order> uniqueOrders = new LinkedHashMap<>();
        for (RoutingMessage.Order order : orders) {
            uniqueOrders.put(order.getId(), order);
        }

        ObservableList<RoutingMessage.Order> data = tabBasketsController.getExecutionsOrderController().getData();
        data.setAll(uniqueOrders.values());
        tabBasketsController.getExecutionsOrderController().getTableExecutionReports().refresh();
        updateTabText();
    }

    private void upsertBasketOrder(RoutingMessage.Order order) {
        BasketTabController basketTabController = Repository.getBasketTabController().get(order.getBasketID());
        if (basketTabController == null) {
            return;
        }

        ObservableList<RoutingMessage.Order> data = basketTabController.getExecutionsOrderController().getData();
        OptionalInt indexOptional = IntStream.range(0, data.size())
                .filter(i -> data.get(i).getId().equals(order.getId()))
                .findFirst();

        if (indexOptional.isPresent()) {
            data.set(indexOptional.getAsInt(), order);
        } else {
            data.add(order);
        }

        basketTabController.getExecutionsOrderController().getTableExecutionReports().refresh();
        updateTabText();
    }

    private void onOrder(RoutingMessage.Order order) {
        try {

            Platform.runLater(() -> {

                if (ordersById.containsKey(order.getId())) {

                    if (order.getBasketID().isEmpty()) {

                        ExecutionsController workingMKD = Repository.getRoutingController().getWorkingOrderController();

                        OptionalInt indiceEncontrado = IntStream.range(0, workingMKD.data.size()).filter(i -> workingMKD.data.get(i).getId().equals(order.getId())).findFirst();

                        RoutingMessage.Order orderOld = workingMKD.data.get(indiceEncontrado.getAsInt());

                        boolean validate = (orderOld.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                                || orderOld.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)
                                || orderOld.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)
                                || orderOld.getOrdStatus().equals(RoutingMessage.OrderStatus.DONE_FOR_DAY)
                                || orderOld.getOrdStatus().equals(RoutingMessage.OrderStatus.STOPPED));

                        if (!validate) {
                            workingMKD.data.set(indiceEncontrado.getAsInt(), order);
                        }

                    } else {

                        if (!Repository.getIsLight()) {
                            upsertBasketOrder(order);
                        }

                    }

                } else {

                    ordersById.put(order.getId(), order);

                    if (order.getBasketID().isEmpty()) {

                        ExecutionsController workingMKD = Repository.getRoutingController().getWorkingOrderController();
                        workingMKD.data.add(order);
                        workingMKD.getTableExecutionReports().refresh();

                        String text = "Trabajando (" + workingMKD.data.size() + ")";
                        Repository.getRoutingController().getTabRuteo().setText(text);
                        workingMKD.getTableExecutionReports().refresh();

                    } else {

                        if (!Repository.getIsLight()) {
                            upsertBasketOrder(order);
                        }
                    }

                }


                if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_NEW)) {
                    if (Repository.isSound()) {
                        Repository.getMediaPlayerNew().stop();
                        Repository.getMediaPlayerNew().play();
                    }

                } else if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {

                    Platform.runLater(() -> {

                        ExecutionsController execuMKD = Repository.getRoutingController().getExecutionsOrderController();

                        boolean validate = execuMKD.data.stream().anyMatch(orders -> orders.getExecId().equals(order.getExecId()));
                        if (!validate) {
                            execuMKD.data.add(order);
                            execuMKD.getTableExecutionReports().refresh();
                        }

                        if (Repository.isSound()) {
                            Repository.getMediaPlayerTrade().stop();
                            Repository.getMediaPlayerTrade().play();
                        }
                    });

                } else if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REJECTED)) {
                    if (Repository.isSound()) {
                        Repository.getMediaPlayerReject().stop();
                        Repository.getMediaPlayerReject().play();
                    }
                }


            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void updateTabText() {

    }


}
