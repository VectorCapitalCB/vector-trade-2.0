package cl.vc.blotter;

import cl.vc.blotter.utils.ColumnConfig;
import cl.vc.blotter.utils.ConfigManager;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import cl.vc.blotter.controller.*;
import cl.vc.blotter.model.BookVO;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.InterfaceTcp;
import dev.mccue.guava.collect.HashBasedTable;
import dev.mccue.guava.collect.Table;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.media.MediaPlayer;
import javafx.stage.Stage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.prefs.Preferences;


@Slf4j
public class Repository {

    private static final int MAX_TRADE_GENERALES = 5_000;
    private static final int MAX_CANDLE_TRADE_GENERALES = 5_000;


    @Getter
    @Setter
    private static StadisticsController statsController;

    private static volatile ColumnConfig columnConfig;

    public static ColumnConfig getColumnConfig() {
        if (columnConfig == null) {
            synchronized (Repository.class) {
                if (columnConfig == null) {
                    columnConfig = ConfigManager.loadConfig();
                }
            }
        }
        return columnConfig;
    }

    public static void saveColumnConfig() {
        ConfigManager.saveConfig(getColumnConfig());
    }

    private static int maxBook = 15;

    @Getter
    private static HashMap<String, RoutingMessage.SecurityType> staticSecurityType = new HashMap();

    @Getter
    public static final String ALL_ACCOUNT = "Todas";

    private static final Preferences prefs = Preferences.userNodeForPackage(Repository.class);

    private static final String DAY_MODE_KEY = "dayMode";

    @Getter
    @Setter
    private static MarketDataMessage.BolsaStats stats;
    @Getter
    private static final ObservableList<MarketDataMessage.BolsaStats> bolsaStatsHistory = FXCollections.observableArrayList();

    @Getter
    private final static Properties properties = new Properties();

    @Getter
    private static final ObservableList<MarketDataMessage.News> news = FXCollections.observableArrayList();
    @Getter
    private static final ObservableList<String> chatMessages = FXCollections.observableArrayList();
    @Getter
    private static final ObservableList<NewsItem> newsMessages = FXCollections.observableArrayList();
    private static final BooleanProperty serviceConnected = new SimpleBooleanProperty(false);
    private static final BooleanProperty candleConnected = new SimpleBooleanProperty(false);
    private static final BooleanProperty chatConnected = new SimpleBooleanProperty(false);
    private static final BooleanProperty newsConnected = new SimpleBooleanProperty(false);

    @Getter
    private final static String STYLE = "/blotter/css/style.css";

    @Getter
    private final static String STYLE_ESTADISTICAS = "/blotter/css/styleEstadisticas.css";

    @Getter
    @Setter
    private static BlotterMessage.PortfolioResponse portfolioResponse;

    @Getter
    @Setter
    private static MarketDataMessage.Statistic lastSelectedStatistic;

    @Getter
    @Setter
    private static PreDigitadosController preDigitadosController;

    @Getter
    @Setter
    private static MarketDataViewerController marketDataController;

    @Getter
    private static final Table<String, String, MarketDataMessage.Security> securityListMaps = HashBasedTable.create();

    @Getter
    private static final Map<String, MarketDataViewerController> controllerStageMap = new HashMap<>();

    @Getter
    private static final HashMap<String, BasketTabController> basketTabController = new HashMap<>();

    @Getter
    private static final HashMap<Integer, LibroEmergenteController> libroEmergenteMap = new HashMap<>();

    @Getter
    private static final ObservableList<MarketDataMessage.TradeGeneral> tradeGenerales = FXCollections.observableArrayList();
    @Getter
    private static final ObservableList<MarketDataMessage.TradeGeneral> candleTradeGenerales = FXCollections.observableArrayList();

    @Setter
    @Getter
    private static  FooterController footerController;

    @Setter
    @Getter
    private static  MarketDataPortfolioViewController marketDataPortfolioViewController;

    @Setter
    @Getter
    private static  RoutingController routingController;

    @Getter
    private static final Set<String> hashSet = new LinkedHashSet<>();

    @Getter
    @Setter
    private static boolean autologin = false;

    @Getter
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Getter
    private static final DecimalFormat formatter4dec = new DecimalFormat("#,##0.0000");

    @Getter
    private static final DecimalFormat formatter2dec = new DecimalFormat("#,##0.00");

    @Getter
    private static final DecimalFormat formatter0dec = new DecimalFormat("#,##0");

    @Getter
    @Setter
    public static LanzadorController lanzadorController;

    @Getter
    @Setter
    public static String credencial;
    @Getter
    @Setter
    public static Integer countMultibook = -1;
    @Getter
    public static ActorRef clientActor;
    @Getter
    @Setter
    public static String tokenKeycloak;
    @Getter
    @Setter
    public static String username;
    private static final String PREMIUM_LIQUIDATION_USER = "jtolosa";

    public static boolean isPremiumLiquidationUser() {
        String currentUsername = username;
        return currentUsername != null
                && currentUsername.trim().equalsIgnoreCase(PREMIUM_LIQUIDATION_USER);
    }
    @Getter
    @Setter
    public static String dolarSymbol;
    @Getter
    @Setter
    public static BlotterMessage.Multibook multibook;
    @Getter
    @Setter
    public static HashMap<String, LibroEmergentePrincipalController> controllerMultibook = new HashMap<>();
    @Getter
    @Setter
    public static String dolarSecurity;
    @Getter
    @Setter
    public static BlotterMessage.User user;
    @Getter
    @Setter
    public static String version;
    @Getter
    @Setter
    private static String selectedCandleSymbol;
    @Getter
    public static List<String> groups = new ArrayList<>();
    @Setter
    @Getter
    public static AdminController adminController;
    @Setter
    @Getter
    public static SessionsMessage.Enviroment enviroment;
    @Setter
    @Getter
    public static String enviromentKey;
    @Getter
    public static Scene login;
    @Getter
    public static Stage principal;
    @Getter
    @Setter
    public static ZoneId zoneID = ZoneId.of("America/Santiago");
    @Getter
    public static ActorSystem actorSystem = ActorSystem.create();
    @Getter
    @Setter
    public static String credencialPath;
    @Getter
    public static HashMap<String, MarketDataMessage.Subscribe> subscribeIdsMaps = new HashMap<>();
    @Getter
    @Setter
    private static HashMap<String, BookVO> bookPortMaps = new HashMap<>();
    @Getter
    @Setter
    private static HashMap<String, LanzadorController> mapLanzadores = new HashMap<>();
    @Getter
    @Setter
    private static InterfaceTcp clientService;
    @Getter
    @Setter
    private static InterfaceTcp candleClientService;
    @Getter
    @Setter
    private static InterfaceTcp chatClientService;
    @Getter
    @Setter
    private static InterfaceTcp newsClientService;
    @Getter
    @Setter
    private static LoginController loginController;

    @Getter
    @Setter
    private static Boolean isLight = false;

    @Getter
    @Setter
    private static RoutingMessage.Order orderSelected;

    @Getter
    @Setter
    private static NotificationController notificationController;
    @Getter
    @Setter
    private static BasketController basketController;
    @Getter
    @Setter
    private static PrincipalController principalController;
    @Getter
    @Setter
    private static boolean sound = false;
    @Getter
    @Setter
    private static boolean notification = true;
    @Getter
    @Setter
    private static List<BalanceController> balanceControllerList = new ArrayList<>();
    @Getter
    @Setter
    private static List<PrestamosController> prestamosControllerList = new ArrayList<>();
    @Getter
    @Setter
    private static BlotterMessage.User loginControllerUser;
    @Getter
    @Setter
    private static PositionSimultaneasController positionSimultaneasController;
    @Getter
    @Setter
    private static List<PositionHistoricalController> positionHistoricalControllerList = new ArrayList<>();
    @Getter
    @Setter
    private static MediaPlayer mediaPlayerNew;
    @Getter
    @Setter
    private static MediaPlayer mediaPlayerReject;
    @Getter
    @Setter
    private static MediaPlayer mediaPlayerTrade;
    @Getter
    @Setter
    private static List<String> defaultRoutingList = new ArrayList<>();
    @Getter
    private static boolean dayMode = prefs.getBoolean(DAY_MODE_KEY, false);

    @Getter
    private static List<String> userEnable = Arrays.asList("vnazar", "fricci");

    @Getter
    @Setter
    private static java.util.List<RoutingMessage.Order> pendingPreselect = null;

    private static String appVersion;

    public static String getAppVersion() { return appVersion; }
    public static void setAppVersion(String v) { appVersion = v; }


    public static synchronized void setPendingPreselect(java.util.List<RoutingMessage.Order> orders) {
        pendingPreselect = (orders == null) ? java.util.List.of() : new java.util.ArrayList<>(orders);
    }

    public static synchronized java.util.List<RoutingMessage.Order> takePendingPreselect() {
        var tmp = pendingPreselect;
        pendingPreselect = null;
        return tmp == null ? java.util.List.of() : tmp;
    }

    public static ObservableList<String> getAllSymbols() {

        ObservableList<String> symbols = FXCollections.observableArrayList();

        try {
            for (Table.Cell<String, String, MarketDataMessage.Security> cell : securityListMaps.cellSet()) {
                String symbol = cell.getRowKey();
                if (!symbols.contains(symbol)) {
                    symbols.add(symbol);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return symbols;
    }

    public static void setDayMode(boolean mode) {
        dayMode = mode;
        prefs.putBoolean(DAY_MODE_KEY, mode);
    }

    public static List<String> getAllAccounts() {
        return Arrays.asList("Cuenta1", "Cuenta2", "Cuenta3", "Cuenta4");
    }

    public static String createSuscripcion(String symbol,
                                           MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData,
                                           RoutingMessage.SettlType settlType,
                                           RoutingMessage.SecurityType securityType) {

        try {

            MarketDataMessage.Subscribe.Builder subscribe = MarketDataMessage.Subscribe.newBuilder()
                    .setSymbol(symbol.toUpperCase())
                    .setBook(true)
                    .setSecurityExchange(securityExchangeMarketData)
                    .setSecurityType(securityType)
                    .setSettlType(settlType)
                    .setStatistic(true)
                    .setTrade(true)
                    .setDepth(MarketDataMessage.Depth.FULL_BOOK);

            String id = TopicGenerator.getTopicMKD(subscribe);
            subscribe.setId(id);

            MarketDataMessage.Subscribe aux = subscribe.build();

            if (!bookPortMaps.containsKey(id)) {
                BookVO bookVO = new BookVO(aux);
                bookPortMaps.put(bookVO.getId(), bookVO);
            }

            if (!subscribeIdsMaps.containsKey(id)) {
                subscribeIdsMaps.put(id, aux);
                Repository.getClientService().sendMessage(aux);

            } else {
                BookVO bookVO =  bookPortMaps.get(id);
                if (bookVO != null && shouldRefreshSubscription(bookVO)) {
                    Repository.getClientService().sendMessage(aux);
                }

            }

            return id;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;

    }

    public static void refreshSubscription(MarketDataMessage.Subscribe subscribe, String reason) {
        try {
            if (subscribe == null || getClientService() == null) {
                return;
            }

            subscribeIdsMaps.put(subscribe.getId(), subscribe);
            log.warn("Reenviando suscripcion id={} symbol={} market={} settl={} securityType={} reason={}",
                    subscribe.getId(),
                    subscribe.getSymbol(),
                    subscribe.getSecurityExchange(),
                    subscribe.getSettlType(),
                    subscribe.getSecurityType(),
                    reason);
            getClientService().sendMessage(subscribe);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static boolean shouldRefreshSubscription(BookVO bookVO) {
        if (bookVO == null) {
            return true;
        }

        if (bookVO.getBidBook().isEmpty() && bookVO.getAskBook().isEmpty()) {
            if (bookVO.getStatisticVO() == null) {
                return true;
            }

            MarketDataMessage.Statistic statistic = bookVO.getStatisticVO().getStatistic();
            return statistic == null
                    || (statistic.getBidPx() <= 0d
                    && statistic.getAskPx() <= 0d
                    && statistic.getLast() <= 0d
                    && statistic.getPreviusClose() <= 0d
                    && statistic.getTradeVolume() <= 0d
                    && statistic.getIndicativeOpening() <= 0d
                    && statistic.getReferencialPrice() <= 0d);
        }

        return false;
    }

    public static void unSuscripcion(String id) {
        //todo falta la terrible logica si desuscribimos o no
    }


    public static BookVO createBook(MarketDataMessage.Statistic statistic) {

        try {

            String id = TopicGenerator.getTopicMKD(statistic);

            if (!bookPortMaps.containsKey(id)) {
                BookVO bookVO = new BookVO(statistic);
                bookPortMaps.put(id, bookVO);
                return bookVO;
            } else {
                return bookPortMaps.get(id);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public static BookVO createBook(MarketDataMessage.Subscribe subscribe) {

        try {

            String id = TopicGenerator.getTopicMKD(subscribe);

            if (!bookPortMaps.containsKey(id)) {
                BookVO bookVO = new BookVO(subscribe);
                bookPortMaps.put(id, bookVO);
                return bookVO;
            } else {
                return bookPortMaps.get(id);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }


    public static void enviasubscripcionAll() {
        subscribeIdsMaps.values().forEach(s -> Repository.getClientService().sendMessage(s));
    }

    public static void appendChatMessage(String message) {
        if (message == null || message.isBlank()) {
            return;
        }
        javafx.application.Platform.runLater(() -> {
            chatMessages.add(message);
            if (chatMessages.size() > 500) {
                chatMessages.remove(0, chatMessages.size() - 500);
            }
        });
    }

    public static ReadOnlyBooleanProperty serviceConnectedProperty() {
        return serviceConnected;
    }

    public static void addBolsaStatsHistory(MarketDataMessage.BolsaStats item) {
        if (item == null) {
            return;
        }
        javafx.application.Platform.runLater(() -> {
            boolean exists = bolsaStatsHistory.stream().anyMatch(s -> java.util.Objects.equals(s.getId(), item.getId()));
            if (!exists) {
                bolsaStatsHistory.add(item);
                bolsaStatsHistory.sort(java.util.Comparator.comparing(Repository::extractHistorySortKey));
                if (bolsaStatsHistory.size() > 2000) {
                    bolsaStatsHistory.remove(0, bolsaStatsHistory.size() - 2000);
                }
            }
        });
    }

    private static String extractHistorySortKey(MarketDataMessage.BolsaStats s) {
        String id = s == null ? "" : s.getId();
        if (id != null && id.startsWith("hist:")) {
            String[] parts = id.split(":", 3);
            if (parts.length == 3) {
                return parts[2];
            }
        }
        return s == null ? "" : s.getHoraFin();
    }

    public static ReadOnlyBooleanProperty candleConnectedProperty() {
        return candleConnected;
    }

    public static ReadOnlyBooleanProperty chatConnectedProperty() {
        return chatConnected;
    }

    public static ReadOnlyBooleanProperty newsConnectedProperty() {
        return newsConnected;
    }

    public static void setChannelConnected(String channelName, boolean connected) {
        javafx.application.Platform.runLater(() -> {
            String ch = channelName == null ? "service" : channelName.toLowerCase();
            switch (ch) {
                case "candle" -> candleConnected.set(connected);
                case "chat" -> chatConnected.set(connected);
                case "news" -> newsConnected.set(connected);
                default -> serviceConnected.set(connected);
            }
        });
    }

    public static void appendNewsMessage(String message) {
        appendNewsMessage(message, "", System.currentTimeMillis(), "NORMAL");
    }

    public static void appendNewsMessage(String message, String url) {
        appendNewsMessage(message, url, System.currentTimeMillis(), "NORMAL");
    }

    public static void appendNewsMessage(String message, String url, long publishedAt) {
        appendNewsMessage(message, url, publishedAt, "NORMAL");
    }

    public static void appendNewsMessage(String message, String url, long publishedAt, String impact) {
        if (message == null || message.isBlank()) {
            return;
        }
        javafx.application.Platform.runLater(() -> {
            newsMessages.add(new NewsItem(
                    message.trim(),
                    url == null ? "" : url.trim(),
                    publishedAt,
                    impact == null || impact.isBlank() ? "NORMAL" : impact.trim().toUpperCase()
            ));
            if (newsMessages.size() > 2000) {
                newsMessages.remove(0, newsMessages.size() - 2000);
            }
        });
    }

    public static void replaceTradeGenerales(List<MarketDataMessage.TradeGeneral> trades) {
        tradeGenerales.clear();
        hashSet.clear();
        if (trades == null || trades.isEmpty()) {
            return;
        }

        int start = Math.max(0, trades.size() - MAX_TRADE_GENERALES);
        for (int i = start; i < trades.size(); i++) {
            addTradeGeneral(trades.get(i));
        }
    }

    public static void addTradeGeneral(MarketDataMessage.TradeGeneral trade) {
        if (trade == null) {
            return;
        }

        String tradeId = trade.getIdGenerico();
        if (tradeId != null && !tradeId.isBlank()) {
            if (!hashSet.add(tradeId)) {
                return;
            }
        }

        tradeGenerales.add(trade);
        trimTradeGenerales();
    }

    public static void replaceCandleTradeGenerales(List<MarketDataMessage.TradeGeneral> trades) {
        candleTradeGenerales.clear();
        if (trades == null || trades.isEmpty()) {
            return;
        }

        int start = Math.max(0, trades.size() - MAX_CANDLE_TRADE_GENERALES);
        candleTradeGenerales.addAll(trades.subList(start, trades.size()));
    }

    public static void addCandleTradeGeneral(MarketDataMessage.TradeGeneral trade) {
        if (trade == null) {
            return;
        }

        candleTradeGenerales.add(trade);
        if (candleTradeGenerales.size() > MAX_CANDLE_TRADE_GENERALES) {
            candleTradeGenerales.remove(0, candleTradeGenerales.size() - MAX_CANDLE_TRADE_GENERALES);
        }
    }

    private static void trimTradeGenerales() {
        if (tradeGenerales.size() <= MAX_TRADE_GENERALES) {
            return;
        }

        int removeCount = tradeGenerales.size() - MAX_TRADE_GENERALES;
        tradeGenerales.remove(0, removeCount);
        rebuildTradeGeneralIds();
    }

    private static void rebuildTradeGeneralIds() {
        hashSet.clear();
        for (MarketDataMessage.TradeGeneral trade : tradeGenerales) {
            if (trade == null) {
                continue;
            }
            String tradeId = trade.getIdGenerico();
            if (tradeId != null && !tradeId.isBlank()) {
                hashSet.add(tradeId);
            }
        }
    }

    public static class NewsItem {
        private final String message;
        private final String url;
        private final long publishedAt;
        private final String impact;

        public NewsItem(String message, String url, long publishedAt, String impact) {
            this.message = message == null ? "" : message;
            this.url = url == null ? "" : url;
            this.publishedAt = publishedAt;
            this.impact = impact == null ? "NORMAL" : impact;
        }

        public String getMessage() {
            return message;
        }

        public String getUrl() {
            return url;
        }

        public long getPublishedAt() {
            return publishedAt;
        }

        public String getImpact() {
            return impact;
        }
    }

}
