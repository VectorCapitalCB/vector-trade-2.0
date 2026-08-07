package cl.vc.service;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.routing.RoundRobinPool;
import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.keycloak.KeycloakService;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.module.protocolbuff.tcp.NettyProtobufServer;
import cl.vc.service.akka.actors.SellsideConnect;
import cl.vc.service.akka.actors.routing.ActorGroupPerAccount;
import cl.vc.service.admin.AccountLoadTracker;
import cl.vc.service.admin.AdminServer;
import cl.vc.service.admin.ManualSaldoOverride;
import cl.vc.service.admin.OdAttempt;
import cl.vc.service.security.IpRateLimiter;
import cl.vc.service.akka.actors.websocket.WebSocketServer;
import cl.vc.service.util.BookSnapshot;
import cl.vc.service.util.CalculoCreasys;
import cl.vc.service.util.ConnectionsVO;
import cl.vc.service.util.SQLServerConnection;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.netty.channel.Channel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.GroupResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.AccessToken;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.eclipse.jetty.websocket.api.Session;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

@Slf4j
@Data
public class MainApp {

    @Getter private static ActorRef bolsaStatsActor;

    private static final List<Channel> frontChannels = new ArrayList<>();
    @Getter
    private static final Properties properties = new Properties();
    @Getter
    private static String propertiesPath;

    /**
     * Id de ESTA conexión de servicio (routing/XRO). Mapeado desde application.properties
     * ('idconnection') para que cada despliegue tenga identidad única y ningún componente
     * se confunda. Si falta la propiedad usa el valor legacy hardcodeado.
     */
    public static String getIdConnection() {
        return properties.getProperty("idconnection", "service-vector-trade");
    }

    /**
     * Aislamiento de cuentas: si está ON, un operador SOLO puede rutear/cancelar/reemplazar
     * órdenes en las cuentas que Keycloak le asigna (identidad autenticada del header, NO la
     * que manda el cliente). Default OFF (comportamiento actual) para habilitarlo recién tras
     * validarlo en staging — evita dejar operadores reales afuera si el Keycloak no está completo.
     */
    public static boolean isAccountIsolationEnabled() {
        return Boolean.parseBoolean(properties.getProperty("security.account.isolation", "false"));
    }

    public static Map<RoutingMessage.Order, ActorRef> orderTracker;
    public static String ENVIRONMENT;
    @Getter
    public static boolean requiereCreasys = true;
    @Getter
    public static NettyProtobufServer nettyProtobufServer;
    @Getter
    private static ActorSystem system;
    @Getter
    private static ActorRef sellSideManager;
    @Getter
    private static Map<String, NotificationMessage.Notification> notificationConectionMap = new HashMap<>();
    @Getter
    private static List<NotificationMessage.Notification> notificationMap = new ArrayList<>();
    @Getter
    private static RMap<String, List<RoutingMessage.Order>> preSelectordersMap;
    @Getter
    private static RMap<String, List<BlotterMessage.SubMultibook>> multiBookMaps;
    /** Multibook 2.0: username -> documento JSON con las configuraciones nombradas del usuario. */
    @Getter
    private static RMap<String, String> multibook2Maps;
    @Getter
    private static HashMap<String, List<RoutingMessage.Order>> AllordersMap = new HashMap<>();
    @Getter
    private static HashMap<String, HashMap<String, RoutingMessage.Order>> tradesMapAll = new HashMap<>();
    @Getter
    private static RMap<String, HashMap<String, RoutingMessage.Order>> tradesMapReddis;

    @Getter
    private static RedissonClient redissonClient;
    @Getter
    private static volatile boolean redisConnected = false;
    @Getter
    private static volatile Instant redisLastAttemptAt;
    @Getter
    private static volatile Instant redisLastSuccessAt;
    @Getter
    private static volatile String redisLastError = "";
    @Getter
    private static volatile String redisLastReason = "";
    private static final Object redisReconnectLock = new Object();

    @Getter
    private static RMap<String, HashMap<String, RoutingMessage.Order>> ordersMapRedis;
    @Getter
    private static RMap<String, Map<String, BlotterMessage.Position>> positionsMapsRedis;
    @Getter
    private static RMap<String, BlotterMessage.Patrimonio> patrimonioMapsRedis;
    @Getter
    private static RMap<String, BlotterMessage.SnapshotPositionHistory> snapshotPositionHistoryRedis;
    @Getter
    private static RMap<String, BlotterMessage.Balance> balanceRedis;
    @Getter
    private static RMap<String, BlotterMessage.SnapshotSimultaneas> snapshotSimultaneasRedis;
    @Getter
    private static RMap<String, BlotterMessage.SnapshotPrestamos> snapshotPrestamosRedis;
    @Getter
    private static HashMap<String, BookSnapshot> snapshotHashMap = new HashMap<>();
    @Getter
    @Setter
    private static ZoneId zoneId;
    @Getter
    private static AccessToken accessToken;
    @Getter
    private static RMap<String, HashMap<String, BlotterMessage.Portfolio>> portfolioMaps;
    @Getter
    private static Instant redisExpirationInstant;
    @Getter
    private static MessageEventBus messageEventBus = new MessageEventBus();
    @Getter
    private static Map<RoutingMessage.SecurityExchangeRouting, NettyProtobufClient> connections = new EnumMap<>(RoutingMessage.SecurityExchangeRouting.class);
    @Getter
    private static Map<MarketDataMessage.SecurityExchangeMarketData, NettyProtobufClient> connections_mkd = new EnumMap<>(MarketDataMessage.SecurityExchangeMarketData.class);
    @Getter
    private static WebSocketServer websockerServer = new WebSocketServer();
    @Getter
    private static List<MarketDataMessage.News> listNews = new ArrayList<>();
    @Getter
    private static HashMap<MarketDataMessage.SecurityExchangeMarketData, MarketDataMessage.SecurityList> securityExchangeMaps = new HashMap<>();
    @Getter
    private static HashMap<String, MarketDataMessage.Security> securityExchangeSymbolsMaps = new HashMap<>();
    @Getter
    private static int maxBook = 100;
    @Getter
    private static int maxTradeNemo = 500;
    @Getter
    private static int maxTradeGeneral = 4000;
    @Getter
    private static HashMap<String, BlotterMessage.Position> positionsMaps = new HashMap<>();
    @Getter
    private static volatile LinkedList<MarketDataMessage.TradeGeneral> tradeGeneral = new LinkedList<>();
    @Getter
    private static HashMap<String, ActorRef> groupActors = new HashMap<>();
    @Getter
    private static ActorRef buySide;
    @Getter
    private static ActorRef sellSideMKD;
    @Getter
    private static ActorRef sellSideRouting;
    @Getter
    private static Boolean sendsecuritylist = true;

    private static NettyProtobufServer buySideServer;
    @Getter
    private static KeycloakService keycloakService;
    @Getter
    private static List<ActorRef> sessionAdminList = new ArrayList<>();
    @Getter
    private static Table<String, String, Integer> groupByUsers = HashBasedTable.create();
    @Getter
    private static ConcurrentHashMap<String, ActorRef> accountGroupUser = new ConcurrentHashMap<>();
    @Getter
    private static boolean subscribeSecuritylist_bcs = false;
    @Getter
    private static Map<String, MarketDataMessage.Subscribe> idSymbolsSubscrib = new ConcurrentHashMap<>();
    @Getter
    private static HashMap<String, RoutingMessage.Order> idOrders = new HashMap<>();
    @Getter
    private static ActorRef clientManager;
    @Getter
    private static List<BlotterMessage.Simultaneas> allSimultaneas;
    private Map<RoutingMessage.SecurityExchangeRouting, ActorRef> conMap;
    @Getter private static boolean voultechExcluded = false;
    @Getter private static boolean suppressActorsForVoultech = true;
    /** Debug: loguea sincronicamente cada Order/OrderCancelReject enviado al cliente. Default false. */
    @Getter private static boolean logOutboundOrders = false;
    @Getter private static int voultechAccountCountRaw = 0;
    @Getter private static int voultechAccountCountValid = 0;
    @Getter private static Set<String> voultechAccountsSet = Collections.emptySet();
    private static final Set<String> processedUsers = ConcurrentHashMap.newKeySet();
    private static final long ACCOUNT_INIT_TIMEOUT_SECONDS = 120L;
    private static final Object accountValidationSchedulerLock = new Object();
    private static ScheduledExecutorService accountValidationScheduler;
    private static ScheduledFuture<?> accountValidationFuture;
    private static Runnable accountValidationTask;
    private static volatile int accountValidationMinutes = 5;
    private static volatile int accountValidationInitialDelayMinutes = 60;
    private static final Object redisRetrySchedulerLock = new Object();
    private static ScheduledExecutorService redisReconnectScheduler;
    private static ScheduledFuture<?> redisReconnectFuture;
    private static volatile int redisReconnectAttempts = 0;
    // Escrituras a Redis fuera del hilo del actor. Striped por cuenta: la misma cuenta cae siempre
    // en el mismo hilo, para que un snapshot viejo no pise a uno mas nuevo.
    private static final ExecutorService[] redisWriters = newRedisWriters(4);
    private static final Object startupMkdWatchdogLock = new Object();
    private static ScheduledExecutorService startupMkdWatchdogScheduler;
    private static ScheduledFuture<?> startupMkdWatchdogFuture;
    private static final EnumSet<RoutingMessage.SettlType> STARTUP_MKD_SETTL_TYPES =
            EnumSet.of(RoutingMessage.SettlType.CASH, RoutingMessage.SettlType.T2, RoutingMessage.SettlType.NEXT_DAY);
    @Getter
    private static final ConcurrentHashMap<String, MarketDataMessage.Subscribe> startupMkdWatchTopics = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> startupMkdTrackedAtMs = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> startupMkdLastRetryAtMs = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> marketDataActivityAtMs = new ConcurrentHashMap<>();
    // Estado de custodia por cuenta para la validación visual del admin: [positions, saldoDisponible, atMillis]
    private static final ConcurrentHashMap<String, double[]> accountCustodyStatus = new ConcurrentHashMap<>();
    @Getter
    private static final ConcurrentHashMap<String, Double> accountMarginCache = new ConcurrentHashMap<>();
    @Getter
    private static final ConcurrentHashMap<String, Double> accountLeverageCache = new ConcurrentHashMap<>();
    @Getter
    private static final AccountLoadTracker accountLoadTracker = new AccountLoadTracker();
    @Getter
    private static final ConcurrentHashMap<String, List<BlotterMessage.Simultaneas>> manualSimultaneasOverrides = new ConcurrentHashMap<>();
    @Getter
    private static final ConcurrentHashMap<String, List<BlotterMessage.Prestamos>> manualPrestamosOverrides = new ConcurrentHashMap<>();
    @Getter
    private static final ConcurrentHashMap<String, ManualSaldoOverride> manualSaldoOverrides = new ConcurrentHashMap<>();
    private static final int MAX_OD_ATTEMPTS = 500;
    private static final String ADMIN_FLAG_OD_PROTECTION = "odProtectionEnabled";
    @Getter
    private static final Deque<OdAttempt> odAttempts = new ConcurrentLinkedDeque<>();
    private static volatile boolean odProtectionEnabled = true;

    /** Símbolos bloqueados por riesgo. Órdenes sobre estos símbolos son rechazadas inmediatamente. */
    @Getter
    private static Set<String> blockedSymbols = ConcurrentHashMap.newKeySet();
    @Getter
    private static RSet<String> blockedSymbolsRedis;

    /** Sesiones WebSocket activas: username → Session (para desconexión desde el admin). */
    @Getter
    private static ConcurrentHashMap<String, Session> userActiveSessionsMap = new ConcurrentHashMap<>();
    @Getter
    private static ConcurrentHashMap<String, Long> userSessionConnectedAt = new ConcurrentHashMap<>();

    /** Usuarios bloqueados por el admin. Sus conexiones son rechazadas al instante. */
    @Getter
    private static Set<String> blockedUsers = ConcurrentHashMap.newKeySet();
    @Getter
    private static RSet<String> blockedUsersRedis;

    /**
     * Límites de monto nominal (precio × cantidad) por destino de routing.
     * Las órdenes que superen el límite son rechazadas con mensaje descriptivo.
     * Defaults: XSGO/NUAM = 30.000.000 · IB_SMART/ALPACA = 10.000
     */
    @Getter
    private static ConcurrentHashMap<String, Double> notionalLimits = new ConcurrentHashMap<>();
    @Getter
    private static RMap<String, Double> notionalLimitsRedis;
    @Getter
    private static RMap<String, Boolean> adminFlagsRedis;

    private static final Map<String, Double> DEFAULT_NOTIONAL_LIMITS = Map.of(
            "XSGO",    30_000_000.0,
            "NUAM",    30_000_000.0,
            "IB_SMART", 10_000.0,
            "ALPACA",   10_000.0
    );

    /** IPs bloqueadas manualmente por el admin o auto-bloqueadas por rate-limiting. Persiste en Redis. */
    @Getter
    private static Set<String> blockedIps = ConcurrentHashMap.newKeySet();
    @Getter
    private static RSet<String> blockedIpsRedis;

    /** Rate-limiter per IP (en memoria, se reinicia al arrancar el servicio). */
    @Getter
    private static IpRateLimiter ipRateLimiter = new IpRateLimiter();
    private static volatile boolean ipSecurityEnabled = false;
    private static volatile boolean webSocketCompressionEnabled = false;

    private static final class GroupUsersBatch {
        private final GroupRepresentation rootGroup;
        private final List<UserRepresentation> users;

        private GroupUsersBatch(GroupRepresentation rootGroup, List<UserRepresentation> users) {
            this.rootGroup = rootGroup;
            this.users = users;
        }
    }

    public static void main(String[] args) {

        try (FileInputStream fis = new FileInputStream(args[0])) {
            propertiesPath = args[0];



            log.info("Leyendo parametros.");
            properties.load(fis);
            properties.putIfAbsent("min_validate_account", "5");
            properties.putIfAbsent("initial_delay_validate_account", "60");
            properties.putIfAbsent("ip.security.enabled", "false");
            properties.putIfAbsent("websocket.compression.enabled", "false");
            properties.putIfAbsent("startup.mkd.watchdog.enabled", "true");
            properties.putIfAbsent("startup.mkd.watchdog.initial.delay.seconds", "120");
            properties.putIfAbsent("startup.mkd.watchdog.period.seconds", "60");
            properties.putIfAbsent("startup.mkd.watchdog.stale.seconds", "180");
            properties.putIfAbsent("startup.mkd.watchdog.retry.cooldown.seconds", "600");
            properties.putIfAbsent("startup.mkd.watchdog.max.retries.per.cycle", "10");
            properties.putIfAbsent("redis.reconnect.startup.retry.enabled", "true");
            properties.putIfAbsent("redis.reconnect.startup.retry.seconds", "10");
            properties.putIfAbsent("redis.reconnect.startup.max.attempts", "60");
            properties.putIfAbsent("redis.reconnect.runtime.retry.enabled", "true");
            properties.putIfAbsent("redis.connect.timeout.ms", "2000");
            properties.putIfAbsent("redis.command.timeout.ms", "2000");
            properties.putIfAbsent("redis.retry.attempts", "1");
            properties.putIfAbsent("redis.retry.interval.ms", "500");
            ipSecurityEnabled = Boolean.parseBoolean(properties.getProperty("ip.security.enabled", "false"));
            webSocketCompressionEnabled = Boolean.parseBoolean(properties.getProperty("websocket.compression.enabled", "false"));
            logOutboundOrders = Boolean.parseBoolean(properties.getProperty("log.outbound.orders", "false"));

            // ── Alertas a Telegram para errores graves (ERROR+). Si falta token/chatId, no hace nada. ──
            properties.putIfAbsent("telegram.enabled", "false");
            properties.putIfAbsent("telegram.bot.token", "");
            properties.putIfAbsent("telegram.chat.id", "");
            properties.putIfAbsent("telegram.min.interval.seconds", "300");
            cl.vc.service.util.TelegramNotifier.configure(
                    Boolean.parseBoolean(properties.getProperty("telegram.enabled")),
                    properties.getProperty("telegram.bot.token"),
                    properties.getProperty("telegram.chat.id"),
                    Long.parseLong(properties.getProperty("telegram.min.interval.seconds", "300")));
            cl.vc.service.util.TelegramAppender.attachToRoot();

            // ── Admin HTTP arranca primero para estar disponible durante toda la inicialización ──
            new AdminServer(properties).start();

            // Precarga del cierre de ayer (Mongo) al arrancar, en background: asi la cache ya esta lista
            // cuando el primer cliente se conecta y la var% sale correcta desde el primer Statistic.
            cl.vc.service.util.MongoCloseRepository.warmStart();
            cl.vc.service.util.MongoHistoryRepository.warmStart();

            if (properties.getProperty("subscribeSecuritylist_BCS") != null) {
                subscribeSecuritylist_bcs = Boolean.parseBoolean(properties.getProperty("subscribeSecuritylist_BCS"));
            }

            if (properties.getProperty("subscribeSecuritylist") != null) {
                sendsecuritylist = Boolean.valueOf(properties.getProperty("subscribeSecuritylist"));
            }


            requiereCreasys = Boolean.parseBoolean(MainApp.getProperties().getProperty("requiere.sql"));

            if (requiereCreasys) {
                SQLServerConnection.getConnection(properties);
                allSimultaneas = CalculoCreasys.getAllSimultaneas();
            }

            maxBook = Integer.parseInt(properties.getProperty("max.book"));
            maxTradeNemo = Integer.parseInt(properties.getProperty("max.trade.nemo"));
            maxTradeGeneral = Integer.parseInt(properties.getProperty("max.trade.general"));

            system = ActorSystem.create();
            zoneId = ZoneId.of(properties.getProperty("zoneId"));
            TimeZone.setDefault(TimeZone.getTimeZone(zoneId));

            if (Boolean.parseBoolean(MainApp.getProperties().getProperty("bolsa.stats"))) {
                startBolsaStatsActor();
            }

            ENVIRONMENT = properties.getProperty("ENVIRONMENT");

            List<String> symbols = Arrays.asList(properties.getProperty("IB.SECURITYLIST").split(","));
            List<String> symbolsBCS = Arrays.asList(properties.getProperty("BCS.SECURITYLIST").split(","));

            MarketDataMessage.SecurityList.Builder securityListBCS = MarketDataMessage.SecurityList.newBuilder()
                    .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS);

            symbolsBCS.forEach(s -> {
                MarketDataMessage.Security security = MarketDataMessage.Security.newBuilder()
                        .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                        .setSymbol(s)
                        .setCurrency(RoutingMessage.Currency.CLP.name())
                        .setSecurityType(RoutingMessage.SecurityType.CS.name())
                        .build();
                securityListBCS.addListSecurities(security);
            });

            securityExchangeMaps.put(securityListBCS.getSecurityExchange(), securityListBCS.build());

            MarketDataMessage.SecurityList.Builder securityList = MarketDataMessage.SecurityList.newBuilder()
                    .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.FH_IBKR);

            symbols.forEach(s -> {
                MarketDataMessage.Security security = MarketDataMessage.Security.newBuilder()
                        .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.FH_IBKR)
                        .setSymbol(s)
                        .setCurrency(RoutingMessage.Currency.USD.name())
                        .setSecurityType(RoutingMessage.SecurityType.CS.name())
                        .build();
                securityList.addListSecurities(security);
            });

            securityExchangeMaps.put(securityList.getSecurityExchange(), securityList.build());

            initSellSideManager();
            boolean redisReady = initRedis("startup", true);
            if (!redisReady) {
                startRedisReconnectRetry("startup", true);
            }
            startStartupMkdWatchdog();

            log.info("#################################################################################################################################");
            log.info("############################################## WEB SOCKET #######################################################################");

            websockerServer.setProperties(properties);
            new Thread(websockerServer).start();

            log.info("#################################################################################################################################");
            log.info("############################################## ADMIN HTTP ########################################################################");
            // Ya iniciado al comienzo — no se inicia de nuevo
            log.info("#################################################################################################################################");

            initKeycloak();

        } catch (Exception exc) {
            log.error("Error al leer parametros:", exc);
        }


    }

    private static List<String> extractAccountsTokens(UserRepresentation user) {
        if (user == null || user.getAttributes() == null) return Collections.emptyList();
        Map<String, List<String>> attrs = user.getAttributes();
        List<String> rawValues = null;
        for (String k : attrs.keySet()) {
            if ("account".equalsIgnoreCase(k)) { rawValues = attrs.get(k); break; }
        }
        if (rawValues == null || rawValues.isEmpty()) return Collections.emptyList();

        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (String v : rawValues) {
            if (v == null) continue;
            for (String t : v.split("[,;|\\s]+")) {
                String x = t.trim();
                if (!x.isEmpty()) out.add(x);
            }
        }
        return new ArrayList<>(out);
    }

    private static boolean isValidAccountFmt(String s) {

        return s != null && s.matches("\\d{1,9}/\\d{1,3}");
    }

    private static Set<String> filterValidAccounts(Collection<String> tokens) {
        LinkedHashSet<String> ok = new LinkedHashSet<>();
        for (String t : tokens) if (isValidAccountFmt(t)) ok.add(t);
        return ok;
    }

    private static Set<String> extractDeclaredAccounts(UserRepresentation user) {
        return filterValidAccounts(extractAccountsTokens(user));
    }

    private static void logVoultechSummaryOnce(String username) {
        if (!voultechExcluded) {
            log.info("🔒 [Keycloak] EXCLUIDO usuario '{}'", username);
            log.info("📦 [Keycloak] 'account' tokens={} | ✅ cuentas válidas={}",
                    voultechAccountCountRaw, voultechAccountCountValid);
            log.info("🛑 [Actors] Bloqueada creación/restauración de actores para cuentas de '{}'", username);
            voultechExcluded = true;
        }
    }

    private static void initKeycloak() {
        try {

            keycloakService = new KeycloakService(properties.getProperty("username"), properties.getProperty("password"),
                    properties.getProperty("url"), properties.getProperty("realm"), properties.getProperty("clientid"), properties.getProperty("secret"));

            initAccount(true);
            configureAccountValidationSchedulerOnStartup();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static int readPositiveIntProperty(String key, int defaultValue) {
        String raw = properties.getProperty(key, String.valueOf(defaultValue));
        int parsed;
        try {
            parsed = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            parsed = defaultValue;
            log.warn("{} inválido ('{}'), usando {}", key, raw, defaultValue);
        }
        parsed = Math.max(1, parsed);
        properties.setProperty(key, String.valueOf(parsed));
        return parsed;
    }

    private static int readNonNegativeIntProperty(String key, int defaultValue) {
        String raw = properties.getProperty(key, String.valueOf(defaultValue));
        int parsed;
        try {
            parsed = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            parsed = defaultValue;
            log.warn("{} inválido ('{}'), usando {}", key, raw, defaultValue);
        }
        parsed = Math.max(0, parsed);
        properties.setProperty(key, String.valueOf(parsed));
        return parsed;
    }

    private static void ensureAccountValidationTask() {
        if (accountValidationTask == null) {
            accountValidationTask = () -> {
                try {
                    log.info("evaluamos si se agregaron cuentas");
                    initAccount(false);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            };
        }
    }

    private static void ensureAccountValidationScheduler() {
        if (accountValidationScheduler == null || accountValidationScheduler.isShutdown()) {
            accountValidationScheduler = Executors.newScheduledThreadPool(1);
        }
    }

    private static boolean isAccountValidationEnabled() {
        return Boolean.parseBoolean(properties.getProperty("account.validation.enabled", "false"));
    }

    private static void stopAccountValidationScheduler(String reason) {
        synchronized (accountValidationSchedulerLock) {
            if (accountValidationFuture != null) {
                accountValidationFuture.cancel(false);
                accountValidationFuture = null;
            }
            log.info("🛑 [Keycloak] Scheduler automático desactivado ({})", reason);
        }
    }

    private static void scheduleAccountValidationTask(int initialDelayMinutes, int delayMinutes, String reason) {
        synchronized (accountValidationSchedulerLock) {
            if (!isAccountValidationEnabled()) {
                stopAccountValidationScheduler(reason);
                return;
            }
            ensureAccountValidationTask();
            ensureAccountValidationScheduler();
            if (accountValidationFuture != null) {
                accountValidationFuture.cancel(false);
            }
            accountValidationFuture = accountValidationScheduler.scheduleWithFixedDelay(
                    accountValidationTask,
                    Math.max(1, initialDelayMinutes),
                    Math.max(1, delayMinutes),
                    TimeUnit.MINUTES
            );
            log.info("🕒 [Keycloak] Scheduler reprogramado ({}) → primer disparo en {} min, luego cada {} min",
                    reason, Math.max(1, initialDelayMinutes), Math.max(1, delayMinutes));
        }
    }

    private static void configureAccountValidationSchedulerOnStartup() {
        accountValidationMinutes = readPositiveIntProperty("min_validate_account", 5);
        accountValidationInitialDelayMinutes = readPositiveIntProperty("initial_delay_validate_account", 60);
        if (!isAccountValidationEnabled()) {
            stopAccountValidationScheduler("startup");
            return;
        }
        scheduleAccountValidationTask(accountValidationInitialDelayMinutes, accountValidationMinutes, "startup");
    }

    public static void initAccount(Boolean isloog) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(keycloakService.getKeycloakUrl())
                .realm(keycloakService.getRealm())
                .clientId(keycloakService.getClientId())
                .username(keycloakService.getAdminUsername())
                .password(keycloakService.getAdminPassword())
                .clientSecret(keycloakService.getClientSecret())
                .build();

        RealmResource realmResource = keycloak.realm(keycloakService.getRealm());
        List<GroupRepresentation> grupos = keycloakService.getAllGroups();
        List<GroupUsersBatch> batches = new ArrayList<>();
        int totalUsers = 0;
        int maxResults = 50;

        for (GroupRepresentation s : grupos) {
            for (GroupRepresentation t : s.getSubGroups()) {
                GroupResource groupResource = realmResource.groups().group(t.getId());
                int first = 0;
                List<UserRepresentation> usersInGroup = new ArrayList<>();
                List<UserRepresentation> users;

                do {
                    users = groupResource.members(first, maxResults);
                    usersInGroup.addAll(users);
                    first += maxResults;
                } while (!users.isEmpty());

                totalUsers += usersInGroup.size();
                batches.add(new GroupUsersBatch(s, usersInGroup));
            }
        }

        log.info("[Keycloak] Usuarios totales a procesar: {}", totalUsers);
        if (totalUsers == 0) {
            log.info("[Keycloak] No hay usuarios para procesar.");
            keycloak.close();
            return;
        }

        Set<String> priorityUsers = parsePriorityUsers();
        log.info("[Keycloak] Usuarios prioritarios configurados: {}", priorityUsers.size());
        long cycleId = accountLoadTracker.startCycle(isloog ? "startup" : "scheduled", totalUsers, priorityUsers.size());
        long globalStartNanos = System.nanoTime();
        int processedUsers = 0;
        boolean cycleSuccess = false;
        String cycleError = null;

        try {
            for (GroupUsersBatch batch : batches) {
                List<UserRepresentation> prioritized = selectUsersByPriority(batch.users, priorityUsers, true);
                if (!prioritized.isEmpty()) {
                    processedUsers = interval(batch.rootGroup, prioritized, totalUsers, processedUsers, globalStartNanos, priorityUsers, cycleId);
                }
            }

            for (GroupUsersBatch batch : batches) {
                List<UserRepresentation> nonPrioritized = selectUsersByPriority(batch.users, priorityUsers, false);
                if (!nonPrioritized.isEmpty()) {
                    processedUsers = interval(batch.rootGroup, nonPrioritized, totalUsers, processedUsers, globalStartNanos, priorityUsers, cycleId);
                }
            }

            cycleSuccess = true;
        } catch (Exception e) {
            cycleError = e.getMessage();
            throw e;
        } finally {
            accountLoadTracker.finishCycle(cycleId, cycleSuccess, cycleError);
            keycloak.close();
        }
    }

    private static int interval(GroupRepresentation s, List<UserRepresentation> users, int totalUsers, int processedUsers,
                                long globalStartNanos, Set<String> priorityUsers, long cycleId) {
        for (UserRepresentation r : users) {
            long userStartNanos = System.nanoTime();
            int currentUserIndex = processedUsers + 1;
            String username = Optional.ofNullable(r.getUsername()).orElse("");
            boolean isPriorityUser = priorityUsers.contains(normalizeUsername(username));

            try {
                boolean isExcludedUser = "voultech".equalsIgnoreCase(username);
                if (isExcludedUser && !voultechExcluded) {
                    List<String> tokens = extractAccountsTokens(r);
                    voultechAccountCountRaw = tokens.size();
                    voultechAccountsSet = filterValidAccounts(tokens);
                    voultechAccountCountValid = voultechAccountsSet.size();
                    logVoultechSummaryOnce(username);
                }

                groupByUsers.put(s.getName(), username, 1);
                int declaredAccounts = countDistinctAccountsFromAttributes(r);
                accountLoadTracker.onUserStart(cycleId, username, s.getName(), isPriorityUser, declaredAccounts);
                log.info("[Keycloak] Iniciando usuario {}/{} '{}' (grupo='{}', cuentas declaradas={})",
                        currentUserIndex, totalUsers, username, s.getName(), declaredAccounts);

                if (r.getAttributes() == null) {
                    continue;
                }

                Set<String> declaredAccountSet = extractDeclaredAccounts(r);
                declaredAccountSet.forEach(account -> accountLoadTracker.onAccountDeclared(cycleId, account, username));
                if (isPriorityUser && !isExcludedUser) {
                    for (String account : declaredAccountSet) {
                        initializeDeclaredPriorityAccountIfNeeded(account, username, currentUserIndex, totalUsers, cycleId);
                    }
                }

                for (Map.Entry<String, List<String>> entry : r.getAttributes().entrySet()) {
                    String key = entry.getKey().toLowerCase(Locale.ROOT);

                    if (key.contains("marginaccount")) {
                        for (String raw : entry.getValue()) {
                            try {
                                if (!raw.contains("|")) {
                                    continue;
                                }
                                String[] value = raw.split("\\|");
                                String account = value[0].trim().replace(" ", "");
                                Double margin = Double.valueOf(value[1].trim().replace(" ", ""));
                                log.info("[Keycloak] Usuario {}/{} '{}' procesando cuenta '{}' (margin={})",
                                        currentUserIndex, totalUsers, username, account, margin);

                                accountMarginCache.put(account, margin);
                                accountLoadTracker.onAccountSeen(cycleId, account, username);
                                accountLoadTracker.onMarginDeclared(account, username);

                                ActorRef ref = accountGroupUser.get(account);
                                if (ref != null) {
                                    if (!isExcludedUser) {
                                        accountLoadTracker.onMarginUpdated(cycleId, account, username);
                                        ref.tell(new ActorGroupPerAccount.UpdateMargin(margin, username, cycleId), ActorRef.noSender());
                                    }
                                } else if (!(suppressActorsForVoultech && (isExcludedUser || voultechAccountsSet.contains(account)))) {
                                    ActorRef actorRef = system.actorOf(ActorGroupPerAccount.props(account, margin, 3.0).withDispatcher("ActorperAccount"));
                                    accountGroupUser.put(account, actorRef);
                                    accountLoadTracker.onActorCreated(cycleId, account, username);
                                    initializeAccountActorSync(account, actorRef, username, currentUserIndex, totalUsers, cycleId);
                                }
                            } catch (Exception e) {
                                log.warn("No se pudo parsear marginaccount '{}' para usuario '{}': {}", raw, username, e.getMessage());
                            }
                        }
                    }

                    if (key.contains("palanca")) {
                        for (String raw : entry.getValue()) {
                            try {
                                if (!raw.contains("|")) {
                                    continue;
                                }
                                String[] value = raw.split("\\|");
                                String account = value[0].trim().replace(" ", "");
                                Double leverage = Double.valueOf(value[1].trim().replace(" ", ""));
                                log.info("[Keycloak] Usuario {}/{} '{}' procesando cuenta '{}' (palanca={})",
                                        currentUserIndex, totalUsers, username, account, leverage);

                                accountLeverageCache.put(account, leverage);
                                accountLoadTracker.onAccountSeen(cycleId, account, username);
                                accountLoadTracker.onLeverageDeclared(account, username);

                                ActorRef ref = accountGroupUser.get(account);
                                if (ref != null) {
                                    if (!isExcludedUser) {
                                        accountLoadTracker.onLeverageUpdated(cycleId, account, username);
                                        ref.tell(new ActorGroupPerAccount.UpdateLeverage(leverage, username, cycleId), ActorRef.noSender());
                                    }
                                } else if (!(suppressActorsForVoultech && (isExcludedUser || voultechAccountsSet.contains(account)))) {
                                    ActorRef actorRef = system.actorOf(ActorGroupPerAccount.props(account, 0.0, leverage).withDispatcher("ActorperAccount"));
                                    accountGroupUser.put(account, actorRef);
                                    accountLoadTracker.onActorCreated(cycleId, account, username);
                                    initializeAccountActorSync(account, actorRef, username, currentUserIndex, totalUsers, cycleId);
                                }
                            } catch (Exception e) {
                                log.error("No se pudo parsear palanca '{}' para usuario '{}': {}", raw, username, e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                accountLoadTracker.onUserError(cycleId, username, e.getMessage());
                log.error("[Keycloak] Error procesando usuario '{}': {}", username, e.getMessage(), e);
            } finally {
                markUserProcessed(username);
                long userElapsedMs = (System.nanoTime() - userStartNanos) / 1_000_000L;
                processedUsers++;
                long elapsedGlobalMs = (System.nanoTime() - globalStartNanos) / 1_000_000L;
                long avgPerUserMs = processedUsers == 0 ? 0 : elapsedGlobalMs / processedUsers;
                long etaMs = Math.max(0, (totalUsers - processedUsers) * avgPerUserMs);
                accountLoadTracker.onUserFinished(cycleId, username, isPriorityUser, userElapsedMs, processedUsers, totalUsers, etaMs);
                log.info("[Keycloak] Usuario {}/{} '{}' procesado en {} ms. ETA restante aprox: {}",
                        processedUsers, totalUsers, username, userElapsedMs, formatMillis(etaMs));
            }
        }

        return processedUsers;
    }

    private static Set<String> parsePriorityUsers() {
        if (!isPriorityUsersEnabled()) {
            return Collections.emptySet();
        }
        String raw = properties.getProperty("prioridad.users", "");
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new LinkedHashSet<>();
        for (String token : raw.split(",")) {
            String normalized = normalizeUsername(token);
            if (!normalized.isEmpty()) {
                result.add(normalized);
            }
        }
        return result;
    }

    private static boolean isPriorityUsersEnabled() {
        return Boolean.parseBoolean(properties.getProperty("prioridad.users.enabled", "false"));
    }

    private static List<UserRepresentation> selectUsersByPriority(List<UserRepresentation> users, Set<String> priorityUsers, boolean onlyPriority) {
        if (users == null || users.isEmpty()) {
            return Collections.emptyList();
        }
        if (priorityUsers == null || priorityUsers.isEmpty()) {
            return onlyPriority ? Collections.emptyList() : users;
        }

        List<UserRepresentation> selected = new ArrayList<>();
        for (UserRepresentation user : users) {
            String username = normalizeUsername(user == null ? null : user.getUsername());
            boolean isPriority = priorityUsers.contains(username);
            if (onlyPriority == isPriority) {
                selected.add(user);
            }
        }
        return selected;
    }

    private static void markUserProcessed(String username) {
        String normalized = normalizeUsername(username);
        if (normalized.isEmpty()) {
            return;
        }
        processedUsers.add(normalized);
    }

    public static void ensureUserProcessed(String username) {
        markUserProcessed(username);
    }

    public static boolean isUserProcessed(String username) {
        String normalized = normalizeUsername(username);
        return !normalized.isEmpty() && processedUsers.contains(normalized);
    }

    public static int getProcessedUsersCount() {
        return processedUsers.size();
    }

    public static int getPriorityUsersConfiguredCount() {
        return parsePriorityUsers().size();
    }

    private static String normalizeUsername(String username) {
        return username == null ? "" : username.trim().toLowerCase(Locale.ROOT);
    }

    private static int countDistinctAccountsFromAttributes(UserRepresentation user) {
        if (user.getAttributes() == null || user.getAttributes().isEmpty()) {
            return 0;
        }
        Set<String> accounts = new HashSet<>(extractDeclaredAccounts(user));
        for (Map.Entry<String, List<String>> entry : user.getAttributes().entrySet()) {
            String key = Optional.ofNullable(entry.getKey()).orElse("").toLowerCase(Locale.ROOT);
            if (!key.contains("marginaccount") && !key.contains("palanca")) {
                continue;
            }
            for (String raw : entry.getValue()) {
                if (raw == null || !raw.contains("|")) {
                    continue;
                }
                String account = raw.split("\\|")[0].trim().replace(" ", "");
                if (!account.isEmpty()) {
                    accounts.add(account);
                }
            }
        }
        return accounts.size();
    }

    private static String formatMillis(long millis) {
        long hours = millis / 3_600_000L;
        long minutes = (millis % 3_600_000L) / 60_000L;
        long seconds = (millis % 60_000L) / 1_000L;
        return String.format("%02dh:%02dm:%02ds", hours, minutes, seconds);
    }

    private static void initializeDeclaredPriorityAccountIfNeeded(String account, String username, int currentUserIndex,
                                                                  int totalUsers, long cycleId) {
        if (account == null || account.isBlank() || accountGroupUser.containsKey(account)) {
            return;
        }
        Double margin = accountMarginCache.getOrDefault(account, 0.0);
        Double leverage = accountLeverageCache.getOrDefault(account, 3.0);
        log.info("[Actors] Usuario prioritario {}/{} '{}' precalentando cuenta declarada '{}' (margin={}, palanca={})",
                currentUserIndex, totalUsers, username, account, margin, leverage);
        ActorRef actorRef = system.actorOf(ActorGroupPerAccount.props(account, margin, leverage).withDispatcher("ActorperAccount"));
        accountGroupUser.put(account, actorRef);
        accountLoadTracker.onActorCreated(cycleId, account, username);
        initializeAccountActorSync(account, actorRef, username, currentUserIndex, totalUsers, cycleId);
    }

    public static ActorRef ensureAccountActor(String account) {
        if (account == null || account.isBlank()) {
            return null;
        }

        ActorRef existing = accountGroupUser.get(account);
        if (existing != null) {
            return existing;
        }

        Double margin = accountMarginCache.getOrDefault(account, 0.0);
        Double leverage = accountLeverageCache.getOrDefault(account, 3.0);
        ActorRef created = system.actorOf(ActorGroupPerAccount.props(account, margin, leverage).withDispatcher("ActorperAccount"));
        ActorRef previous = accountGroupUser.putIfAbsent(account, created);
        if (previous != null) {
            created.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
            return previous;
        }
        return created;
    }

    public static List<BlotterMessage.Simultaneas> getManualSimultaneasOverride(String account) {
        if (account == null || account.isBlank()) {
            return Collections.emptyList();
        }
        List<BlotterMessage.Simultaneas> rows = manualSimultaneasOverrides.get(account);
        return rows != null ? rows : Collections.emptyList();
    }

    public static boolean hasManualSimultaneasOverride(String account) {
        return account != null && !account.isBlank() && manualSimultaneasOverrides.containsKey(account);
    }

    public static void replaceManualSimultaneasOverride(String account, List<BlotterMessage.Simultaneas> rows) {
        if (account == null || account.isBlank()) {
            return;
        }
        List<BlotterMessage.Simultaneas> safeRows = rows == null ? Collections.emptyList() : new ArrayList<>(rows);
        manualSimultaneasOverrides.put(account, Collections.unmodifiableList(safeRows));
    }

    public static void clearManualSimultaneasOverride(String account) {
        if (account == null || account.isBlank()) {
            return;
        }
        manualSimultaneasOverrides.remove(account);
    }

    public static List<BlotterMessage.Prestamos> getManualPrestamosOverride(String account) {
        if (account == null || account.isBlank()) {
            return Collections.emptyList();
        }
        List<BlotterMessage.Prestamos> rows = manualPrestamosOverrides.get(account);
        return rows != null ? rows : Collections.emptyList();
    }

    public static boolean hasManualPrestamosOverride(String account) {
        return account != null && !account.isBlank() && manualPrestamosOverrides.containsKey(account);
    }

    public static void replaceManualPrestamosOverride(String account, List<BlotterMessage.Prestamos> rows) {
        if (account == null || account.isBlank()) {
            return;
        }
        List<BlotterMessage.Prestamos> safeRows = rows == null ? Collections.emptyList() : new ArrayList<>(rows);
        manualPrestamosOverrides.put(account, Collections.unmodifiableList(safeRows));
    }

    public static void clearManualPrestamosOverride(String account) {
        if (account == null || account.isBlank()) {
            return;
        }
        manualPrestamosOverrides.remove(account);
    }

    public static ManualSaldoOverride getManualSaldoOverride(String account) {
        if (account == null || account.isBlank()) {
            return null;
        }
        return manualSaldoOverrides.get(account);
    }

    public static boolean hasManualSaldoOverride(String account) {
        return account != null && !account.isBlank() && manualSaldoOverrides.containsKey(account);
    }

    public static void replaceManualSaldoOverride(String account, ManualSaldoOverride override) {
        if (account == null || account.isBlank()) {
            return;
        }
        if (override == null) {
            manualSaldoOverrides.remove(account);
            return;
        }
        manualSaldoOverrides.put(account, override);
    }

    public static void clearManualSaldoOverride(String account) {
        if (account == null || account.isBlank()) {
            return;
        }
        manualSaldoOverrides.remove(account);
    }

    private static void initializeAccountActorSync(String account, ActorRef actorRef, String username, int currentUserIndex,
                                                   int totalUsers, long cycleId) {
        long startedAt = System.nanoTime();
        try {
            log.info("[Actors] Usuario {}/{} '{}' inicializando cuenta '{}' en modo sincronizado...",
                    currentUserIndex, totalUsers, username, account);
            Future<Object> initFuture = Patterns.ask(
                    actorRef,
                    ActorGroupPerAccount.Initialize.INSTANCE,
                    ACCOUNT_INIT_TIMEOUT_SECONDS * 1000L
            );

            Object response = Await.result(
                    initFuture,
                    Duration.create(ACCOUNT_INIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            );

            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
            if (response instanceof ActorGroupPerAccount.Initialized) {
                accountLoadTracker.onAccountInitialized(cycleId, account, username, durationMs, true);
                log.info("[Actors] Usuario {}/{} '{}' cuenta '{}' inicializada.",
                        currentUserIndex, totalUsers, username, account);
            } else {
                accountLoadTracker.onAccountInitialized(cycleId, account, username, durationMs, false);
                log.warn("[Actors] Respuesta inesperada al inicializar cuenta {}: {}", account, response);
            }
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
            accountLoadTracker.onAccountInitialized(cycleId, account, username, durationMs, false);
            log.error("[Actors] Error/timeout inicializando cuenta '{}' (usuario='{}' {}/{}): {}",
                    account, username, currentUserIndex, totalUsers, e.getMessage(), e);
        }
    }

    private static boolean initRedis(String reason, boolean restoreActorState) {
        synchronized (redisReconnectLock) {
            redisLastAttemptAt = Instant.now();
            redisLastReason = reason;
            RedissonClient newRedisson = null;

            try {
                Calendar expirationDate = Calendar.getInstance();
                expirationDate.set(Calendar.HOUR_OF_DAY, 17);
                expirationDate.set(Calendar.MINUTE, 59);
                expirationDate.set(Calendar.SECOND, 0);
                Instant expirationInstant = expirationDate.toInstant();

                String connections = properties.getProperty("redis.host") + ":" + Integer.parseInt(properties.getProperty("redis.port"));
                Config redis = new Config();
                SingleServerConfig redisServer = redis.useSingleServer()
                        .setAddress("redis://" + connections)
                        .setPassword(properties.getProperty("redis.password"));
                redisServer
                        .setConnectTimeout(readPositiveIntProperty("redis.connect.timeout.ms", 2000))
                        .setTimeout(readPositiveIntProperty("redis.command.timeout.ms", 2000))
                        .setRetryAttempts(readNonNegativeIntProperty("redis.retry.attempts", 1))
                        .setRetryInterval(readPositiveIntProperty("redis.retry.interval.ms", 500));

                // Codec seguro (River + whitelist) para cerrar el RCE por deserializacion. Detras de flag
                // (default OFF) porque toca el path del restore: validar en staging que lee los datos existentes.
                if (Boolean.parseBoolean(properties.getProperty("redis.codec.secure", "false"))) {
                    org.jboss.marshalling.MarshallingConfiguration mc = new org.jboss.marshalling.MarshallingConfiguration();
                    mc.setClassResolver(new cl.vc.service.util.SecureRiverClassResolver());
                    redis.setCodec(new org.redisson.codec.MarshallingCodec(
                            org.redisson.codec.MarshallingCodec.Protocol.RIVER, mc));
                    log.info("[Redis] codec seguro (River + whitelist) ACTIVADO");
                }

                newRedisson = Redisson.create(redis);
                newRedisson.getBucket("__vt_admin_healthcheck").isExists();

                RMap<String, HashMap<String, RoutingMessage.Order>> newOrdersMapRedis = newRedisson.getMap("Orders");
                newOrdersMapRedis.expire(expirationInstant);

                RMap<String, HashMap<String, RoutingMessage.Order>> newTradesMapReddis = newRedisson.getMap("Trades");
                newTradesMapReddis.expire(expirationInstant);

                RMap<String, Map<String, BlotterMessage.Position>> newPositionsMapsRedis = newRedisson.getMap("Positions");
                newPositionsMapsRedis.expire(expirationInstant);

                RMap<String, BlotterMessage.Patrimonio> newPatrimonioMapsRedis = newRedisson.getMap("Patrimonio");
                newPatrimonioMapsRedis.expire(expirationInstant);

                RMap<String, BlotterMessage.SnapshotPositionHistory> newSnapshotPositionHistoryRedis = newRedisson.getMap("SnapshotPositionHistory");
                newSnapshotPositionHistoryRedis.expire(expirationInstant);

                RMap<String, BlotterMessage.Balance> newBalanceRedis = newRedisson.getMap("Balance");
                newBalanceRedis.expire(expirationInstant);

                RMap<String, BlotterMessage.SnapshotSimultaneas> newSnapshotSimultaneasRedis = newRedisson.getMap("SnapshotSimultaneas");
                newSnapshotSimultaneasRedis.expire(expirationInstant);

                RMap<String, BlotterMessage.SnapshotPrestamos> newSnapshotPrestamosRedis = newRedisson.getMap("SnapshotPrestamos");
                newSnapshotPrestamosRedis.expire(expirationInstant);

                RMap<String, List<RoutingMessage.Order>> newPreSelectordersMap = newRedisson.getMap("PreSelectOrders");
                newPreSelectordersMap.expire(expirationInstant);

                RMap<String, HashMap<String, BlotterMessage.Portfolio>> newPortfolioMaps = newRedisson.getMap("Portfolio");
                RMap<String, List<BlotterMessage.SubMultibook>> newMultiBookMaps = newRedisson.getMap("MultiBook");
                RMap<String, String> newMultibook2Maps = newRedisson.getMap("MultiBook2.0",
                        org.redisson.client.codec.StringCodec.INSTANCE);

                RSet<String> newBlockedSymbolsRedis = newRedisson.getSet("RiskBlockedSymbols");
                Set<String> newBlockedSymbols = new HashSet<>(newBlockedSymbolsRedis.readAll());

                RSet<String> newBlockedUsersRedis = newRedisson.getSet("BlockedUsers");
                Set<String> newBlockedUsers = new HashSet<>(newBlockedUsersRedis.readAll());

                RMap<String, Double> newNotionalLimitsRedis = newRedisson.getMap("NotionalLimits");
                ConcurrentHashMap<String, Double> newNotionalLimits = new ConcurrentHashMap<>();
                DEFAULT_NOTIONAL_LIMITS.forEach((exchange, defaultLimit) -> {
                    Double stored = newNotionalLimitsRedis.get(exchange);
                    newNotionalLimits.put(exchange, stored != null ? stored : defaultLimit);
                });
                DEFAULT_NOTIONAL_LIMITS.forEach((exchange, defaultLimit) -> {
                    if (!newNotionalLimitsRedis.containsKey(exchange)) {
                        newNotionalLimitsRedis.put(exchange, defaultLimit);
                    }
                });

                RMap<String, Boolean> newAdminFlagsRedis = newRedisson.getMap("AdminFlags");
                Boolean storedOdProtection = newAdminFlagsRedis.get(ADMIN_FLAG_OD_PROTECTION);
                boolean newOdProtectionEnabled = storedOdProtection != null ? storedOdProtection : true;
                if (!newAdminFlagsRedis.containsKey(ADMIN_FLAG_OD_PROTECTION)) {
                    newAdminFlagsRedis.put(ADMIN_FLAG_OD_PROTECTION, newOdProtectionEnabled);
                }

                RSet<String> newBlockedIpsRedis = newRedisson.getSet("BlockedIps");
                Set<String> newBlockedIps = new HashSet<>(newBlockedIpsRedis.readAll());

                RedissonClient previousClient = redissonClient;
                redissonClient = newRedisson;
                redisExpirationInstant = expirationInstant;

                ordersMapRedis = newOrdersMapRedis;
                tradesMapReddis = newTradesMapReddis;
                positionsMapsRedis = newPositionsMapsRedis;
                patrimonioMapsRedis = newPatrimonioMapsRedis;
                snapshotPositionHistoryRedis = newSnapshotPositionHistoryRedis;
                balanceRedis = newBalanceRedis;
                snapshotSimultaneasRedis = newSnapshotSimultaneasRedis;
                snapshotPrestamosRedis = newSnapshotPrestamosRedis;
                preSelectordersMap = newPreSelectordersMap;
                portfolioMaps = newPortfolioMaps;
                multiBookMaps = newMultiBookMaps;
                multibook2Maps = newMultibook2Maps;

                blockedSymbolsRedis = newBlockedSymbolsRedis;
                blockedSymbols.clear();
                blockedSymbols.addAll(newBlockedSymbols);

                blockedUsersRedis = newBlockedUsersRedis;
                blockedUsers.clear();
                blockedUsers.addAll(newBlockedUsers);

                notionalLimitsRedis = newNotionalLimitsRedis;
                notionalLimits.clear();
                notionalLimits.putAll(newNotionalLimits);

                adminFlagsRedis = newAdminFlagsRedis;
                odProtectionEnabled = newOdProtectionEnabled;
                log.info("[OD] Protección OD cargada desde Redis: {}", odProtectionEnabled);

                blockedIpsRedis = newBlockedIpsRedis;
                blockedIps.clear();
                blockedIps.addAll(newBlockedIps);

                purgeRedisDailyState();

                if (Boolean.parseBoolean(properties.getProperty("redis.enable.persistencia")) && restoreActorState) {
                    try {
                        restoreRedisStateIntoExistingActors();
                    } catch (Exception restoreError) {
                        log.error("[Redis] Conexión OK, pero falló restore a actores existentes: {}",
                                restoreError.getMessage(), restoreError);
                    }
                }

                redisConnected = true;
                redisLastSuccessAt = Instant.now();
                redisLastError = "";
                log.info("[Redis] Conexión OK reason={} host={} maps portfolioUsers={} multibookUsers={}",
                        reason, connections, safeRedisSize(portfolioMaps), safeRedisSize(multiBookMaps));

                if (previousClient != null && previousClient != newRedisson) {
                    shutdownRedisClient(previousClient);
                }
                return true;

            } catch (Exception e) {
                if (newRedisson != null && newRedisson != redissonClient) {
                    shutdownRedisClient(newRedisson);
                }
                redisConnected = false;
                redisLastError = e.getMessage() != null ? e.getMessage() : e.toString();
                log.error("Could not start the redis connection", e);
                cl.vc.service.util.TelegramNotifier.alert("redis-down",
                        "Redis no disponible\nhost: " + properties.getProperty("redis.host")
                                + ":" + properties.getProperty("redis.port") + "\nerror: " + redisLastError);
                return false;
            }
        }
    }

    private static ExecutorService[] newRedisWriters(int size) {
        ExecutorService[] writers = new ExecutorService[size];
        for (int i = 0; i < size; i++) {
            final int index = i;
            writers[i] = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "redis-writer-" + index);
                t.setDaemon(true);
                return t;
            });
        }
        return writers;
    }

    /**
     * Encola una escritura a Redis sin bloquear al llamador. El {@code stripeKey} (normalmente la
     * cuenta) fija el hilo, de modo que las escrituras de una misma cuenta conservan su orden.
     */
    public static void submitRedisWrite(String stripeKey, Runnable write) {
        ExecutorService writer = redisWriters[Math.floorMod(stripeKey.hashCode(), redisWriters.length)];
        try {
            writer.execute(() -> {
                try {
                    write.run();
                } catch (Exception e) {
                    log.error("[Redis] escritura asincrona fallo para {}: {}", stripeKey, e.getMessage());
                }
            });
        } catch (RejectedExecutionException e) {
            log.error("[Redis] escritura rechazada para {}: {}", stripeKey, e.getMessage());
        }
    }

    private static void purgeRedisDailyState() {
        purgePreSelectOrdersNotFromToday();
        purgeDailyMapNotFromToday(patrimonioMapsRedis, "Patrimonio");
        purgeDailyMapNotFromToday(snapshotPositionHistoryRedis, "SnapshotPositionHistory");
        purgeDailyMapNotFromToday(balanceRedis, "Balance");
        purgeDailyMapNotFromToday(snapshotSimultaneasRedis, "SnapshotSimultaneas");
        purgeDailyMapNotFromToday(snapshotPrestamosRedis, "SnapshotPrestamos");
        purgeDailyMapNotFromToday(positionsMapsRedis, "Positions");

        if (Boolean.parseBoolean(properties.getProperty("redis.enable.persistencia"))) {
            purgeOrdersNotFromToday();
            purgeTradesNotFromToday();
        }
    }

    private static void restoreRedisStateIntoExistingActors() {
        if (ordersMapRedis != null && !ordersMapRedis.isEmpty()) {
            ordersMapRedis.forEach((key, value) -> {
                if (suppressActorsForVoultech && voultechAccountsSet.contains(key)) {
                    log.info("Restauración omitida para cuenta {} (voultech)", key);
                    return;
                }
                if (accountGroupUser.containsKey(key)) {
                    log.info("se restaura la cuenta {}", key);
                    accountGroupUser.get(key).tell(new ActorGroupPerAccount.RestoreOrder(value), ActorRef.noSender());
                } else {
                    log.info("actor no creado para la cuenta {}", key);
                }
            });
        }

        if (tradesMapReddis != null && !tradesMapReddis.isEmpty()) {
            tradesMapReddis.forEach((key, value) -> {
                if (suppressActorsForVoultech && voultechAccountsSet.contains(key)) {
                    log.info("Restauración TRADES omitida para cuenta {} (voultech)", key);
                    return;
                }
                if (accountGroupUser.containsKey(key)) {
                    accountGroupUser.get(key).tell(new ActorGroupPerAccount.RestoreTrade(value), ActorRef.noSender());
                }
            });
        }

        if (positionsMapsRedis != null && !positionsMapsRedis.isEmpty()) {
            positionsMapsRedis.forEach((key, value) -> {
                String account = key != null && key.contains("|") ? key.substring(0, key.indexOf("|")) : key;
                if (suppressActorsForVoultech && voultechAccountsSet.contains(account)) {
                    log.info("Restauración POSITIONS omitida para cuenta {} (voultech)", account);
                    return;
                }
                if (accountGroupUser.containsKey(account)) {
                    accountGroupUser.get(account).tell(new ActorGroupPerAccount.RestorePositions(value), ActorRef.noSender());
                }
            });
        }
    }

    public static Map<String, Object> reconnectRedisFromAdmin() {
        boolean ok = initRedis("admin", false);
        if (ok) {
            stopRedisReconnectRetry();
        }
        Map<String, Object> status = getRedisStatusSnapshot();
        status.put("ok", ok);
        status.put("message", ok
                ? "Redis reconectado y mapas/config recargados. No se restauraron posiciones intradía automáticamente."
                : "No se pudo reconectar Redis: " + redisLastError);
        return status;
    }

    public static boolean ensureRedisReady(String reason) {
        if (isRedisReady()) {
            return true;
        }
        boolean ok = initRedis(reason, false);
        if (!ok) {
            startRedisReconnectRetry(reason, false);
        }
        return ok;
    }

    public static boolean isRedisReady() {
        return hasRedisMaps() && isRedisClientHealthy(false);
    }

    public static Map<String, Object> getRedisStatusSnapshot() {
        boolean clientHealthy = isRedisClientHealthy(true);
        Map<String, Boolean> maps = redisMapAvailability();
        boolean mapsReady = maps.values().stream().allMatch(Boolean.TRUE::equals);
        boolean connected = clientHealthy && mapsReady;
        if (!connected) {
            startRedisReconnectRetry("status-check", false);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("connected", connected);
        out.put("clientHealthy", clientHealthy);
        out.put("mapsReady", mapsReady);
        out.put("clientCreated", redissonClient != null);
        out.put("host", properties.getProperty("redis.host", ""));
        out.put("port", properties.getProperty("redis.port", ""));
        out.put("lastAttemptAt", redisLastAttemptAt != null ? redisLastAttemptAt.toString() : "");
        out.put("lastSuccessAt", redisLastSuccessAt != null ? redisLastSuccessAt.toString() : "");
        out.put("lastError", redisLastError != null ? redisLastError : "");
        out.put("lastReason", redisLastReason != null ? redisLastReason : "");
        out.put("retryAttempts", redisReconnectAttempts);
        out.put("retryActive", redisReconnectFuture != null && !redisReconnectFuture.isDone());
        out.put("expirationAt", redisExpirationInstant != null ? redisExpirationInstant.toString() : "");
        out.put("maps", maps);
        out.put("ordersAccounts", connected ? safeRedisSize(ordersMapRedis) : -1);
        out.put("tradesAccounts", connected ? safeRedisSize(tradesMapReddis) : -1);
        out.put("portfolioUsers", connected ? safeRedisSize(portfolioMaps) : -1);
        out.put("multibookUsers", connected ? safeRedisSize(multiBookMaps) : -1);
        out.put("preselectUsers", connected ? safeRedisSize(preSelectordersMap) : -1);
        out.put("positionsKeys", connected ? safeRedisSize(positionsMapsRedis) : -1);
        out.put("balanceKeys", connected ? safeRedisSize(balanceRedis) : -1);
        return out;
    }

    private static Map<String, Boolean> redisMapAvailability() {
        Map<String, Boolean> maps = new LinkedHashMap<>();
        maps.put("Orders", ordersMapRedis != null);
        maps.put("Trades", tradesMapReddis != null);
        maps.put("Positions", positionsMapsRedis != null);
        maps.put("Patrimonio", patrimonioMapsRedis != null);
        maps.put("SnapshotPositionHistory", snapshotPositionHistoryRedis != null);
        maps.put("Balance", balanceRedis != null);
        maps.put("SnapshotSimultaneas", snapshotSimultaneasRedis != null);
        maps.put("SnapshotPrestamos", snapshotPrestamosRedis != null);
        maps.put("PreSelectOrders", preSelectordersMap != null);
        maps.put("Portfolio", portfolioMaps != null);
        maps.put("MultiBook", multiBookMaps != null);
        maps.put("RiskBlockedSymbols", blockedSymbolsRedis != null);
        maps.put("BlockedUsers", blockedUsersRedis != null);
        maps.put("NotionalLimits", notionalLimitsRedis != null);
        maps.put("AdminFlags", adminFlagsRedis != null);
        maps.put("BlockedIps", blockedIpsRedis != null);
        return maps;
    }

    private static boolean hasRedisMaps() {
        return redisMapAvailability().values().stream().allMatch(Boolean.TRUE::equals);
    }

    private static boolean isRedisClientHealthy(boolean updateLastError) {
        RedissonClient client = redissonClient;
        if (client == null || client.isShutdown() || client.isShuttingDown()) {
            return false;
        }
        try {
            client.getBucket("__vt_admin_healthcheck").isExists();
            redisConnected = true;
            return true;
        } catch (Exception e) {
            if (updateLastError) {
                markRedisUnavailable("healthcheck", e);
            } else {
                redisConnected = false;
            }
            return false;
        }
    }

    private static int safeRedisSize(Map<?, ?> map) {
        if (map == null) {
            return 0;
        }
        try {
            return map.size();
        } catch (Exception e) {
            log.warn("[Redis] No se pudo leer size de mapa: {}", e.getMessage());
            markRedisUnavailable("map-size", e);
            return -1;
        }
    }

    public static void reportRedisFailure(String context, Exception e) {
        markRedisUnavailable(context, e);
    }

    private static void markRedisUnavailable(String context, Exception e) {
        redisConnected = false;
        redisLastReason = context;
        redisLastError = e != null && e.getMessage() != null ? e.getMessage() : String.valueOf(e);
        startRedisReconnectRetry(context, false);
    }

    private static void shutdownRedisClient(RedissonClient client) {
        try {
            if (client != null && !client.isShutdown()) {
                client.shutdown();
            }
        } catch (Exception e) {
            log.warn("[Redis] No se pudo cerrar cliente anterior: {}", e.getMessage());
        }
    }

    private static void startRedisReconnectRetry(String trigger, boolean restoreActorState) {
        boolean startupTrigger = trigger != null && trigger.startsWith("startup");
        String enabledProperty = startupTrigger
                ? "redis.reconnect.startup.retry.enabled"
                : "redis.reconnect.runtime.retry.enabled";
        if (!Boolean.parseBoolean(properties.getProperty(enabledProperty, "true"))) {
            log.warn("[Redis] Retry automático deshabilitado por {}.", enabledProperty);
            return;
        }

        synchronized (redisRetrySchedulerLock) {
            if (redisReconnectFuture != null && !redisReconnectFuture.isDone()) {
                return;
            }

            int periodSeconds = readPositiveIntProperty("redis.reconnect.startup.retry.seconds", 10);
            int maxAttempts = readPositiveIntProperty("redis.reconnect.startup.max.attempts", 60);
            redisReconnectAttempts = 0;

            if (redisReconnectScheduler == null || redisReconnectScheduler.isShutdown()) {
                redisReconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "redis-reconnect-retry");
                    t.setDaemon(true);
                    return t;
                });
            }

            redisReconnectFuture = redisReconnectScheduler.scheduleWithFixedDelay(() -> {
                try {
                    boolean wasMarkedConnected = redisConnected;
                    if (wasMarkedConnected && isRedisReady()) {
                        stopRedisReconnectRetry();
                        return;
                    }
                    redisReconnectAttempts++;
                    if (redisReconnectAttempts > maxAttempts) {
                        log.error("[Redis] Retry automático agotado tras {} intentos.", maxAttempts);
                        stopRedisReconnectRetry();
                        return;
                    }
                    log.warn("[Redis] Retry automático ({}) intento {}/{}...", trigger, redisReconnectAttempts, maxAttempts);
                    if (initRedis(trigger + "-retry-" + redisReconnectAttempts, restoreActorState)) {
                        log.info("[Redis] Retry automático ({}) recuperó la conexión en intento {}.",
                                trigger, redisReconnectAttempts);
                        stopRedisReconnectRetry();
                    }
                } catch (Exception e) {
                    log.error("[Redis] Error en retry automático", e);
                }
            }, periodSeconds, periodSeconds, TimeUnit.SECONDS);

            log.warn("[Redis] Retry automático ({}) programado: cada {}s, máximo {} intentos.",
                    trigger, periodSeconds, maxAttempts);
        }
    }

    private static void stopRedisReconnectRetry() {
        synchronized (redisRetrySchedulerLock) {
            if (redisReconnectFuture != null) {
                redisReconnectFuture.cancel(false);
                redisReconnectFuture = null;
            }
            if (redisReconnectScheduler != null) {
                redisReconnectScheduler.shutdown();
                redisReconnectScheduler = null;
            }
        }
    }

    public static boolean isOrderFromToday(RoutingMessage.Order order) {
        if (order == null || !order.hasTime()) {
            return false;
        }

        Instant orderInstant = Instant.ofEpochSecond(order.getTime().getSeconds(), order.getTime().getNanos());
        LocalDate orderDate = orderInstant.atZone(zoneId).toLocalDate();
        LocalDate today = LocalDate.now(zoneId);
        return today.equals(orderDate);
    }

    private static void purgeOrdersNotFromToday() {
        try {
            if (ordersMapRedis == null || ordersMapRedis.isEmpty()) {
                return;
            }

            int removedOrders = 0;
            int cleanedAccounts = 0;
            List<String> accounts = new ArrayList<>(ordersMapRedis.keySet());

            for (String account : accounts) {
                HashMap<String, RoutingMessage.Order> accountOrders = ordersMapRedis.get(account);
                if (accountOrders == null || accountOrders.isEmpty()) {
                    continue;
                }

                HashMap<String, RoutingMessage.Order> todayOrders = new HashMap<>();
                for (Map.Entry<String, RoutingMessage.Order> entry : accountOrders.entrySet()) {
                    RoutingMessage.Order order = entry.getValue();
                    if (isOrderFromToday(order)) {
                        todayOrders.put(entry.getKey(), order);
                    } else {
                        removedOrders++;
                    }
                }

                if (todayOrders.isEmpty()) {
                    ordersMapRedis.fastRemove(account);
                    cleanedAccounts++;
                } else if (todayOrders.size() != accountOrders.size()) {
                    ordersMapRedis.put(account, todayOrders);
                    cleanedAccounts++;
                }
            }

            if (removedOrders > 0) {
                log.info("[Redis] Limpieza de órdenes por fecha: {} órdenes removidas en {} cuentas (se conserva sólo el día actual).",
                        removedOrders, cleanedAccounts);
            }
        } catch (Exception e) {
            log.error("[Redis] Error limpiando órdenes antiguas", e);
        }
    }

    private static void purgeTradesNotFromToday() {
        try {
            if (tradesMapReddis == null || tradesMapReddis.isEmpty()) {
                return;
            }

            int removedTrades = 0;
            int cleanedAccounts = 0;
            List<String> accounts = new ArrayList<>(tradesMapReddis.keySet());

            for (String account : accounts) {
                HashMap<String, RoutingMessage.Order> accountTrades = tradesMapReddis.get(account);
                if (accountTrades == null || accountTrades.isEmpty()) {
                    continue;
                }

                HashMap<String, RoutingMessage.Order> todayTrades = new HashMap<>();
                for (Map.Entry<String, RoutingMessage.Order> entry : accountTrades.entrySet()) {
                    RoutingMessage.Order trade = entry.getValue();
                    if (isOrderFromToday(trade)) {
                        todayTrades.put(entry.getKey(), trade);
                    } else {
                        removedTrades++;
                    }
                }

                if (todayTrades.isEmpty()) {
                    tradesMapReddis.fastRemove(account);
                    cleanedAccounts++;
                } else if (todayTrades.size() != accountTrades.size()) {
                    tradesMapReddis.put(account, todayTrades);
                    cleanedAccounts++;
                }
            }

            if (removedTrades > 0) {
                log.info("[Redis] Limpieza de trades por fecha: {} trades removidos en {} cuentas (se conserva sólo el día actual).",
                        removedTrades, cleanedAccounts);
            }
        } catch (Exception e) {
            log.error("[Redis] Error limpiando trades antiguos", e);
        }
    }

    private static void purgePreSelectOrdersNotFromToday() {
        try {
            if (preSelectordersMap == null || preSelectordersMap.isEmpty()) {
                return;
            }

            int removedOrders = 0;
            List<String> usernames = new ArrayList<>(preSelectordersMap.keySet());

            for (String username : usernames) {
                List<RoutingMessage.Order> orders = preSelectordersMap.get(username);
                if (orders == null || orders.isEmpty()) {
                    preSelectordersMap.fastRemove(username);
                    continue;
                }

                List<RoutingMessage.Order> todayOrders = new ArrayList<>();
                for (RoutingMessage.Order order : orders) {
                    if (isOrderFromToday(order)) {
                        todayOrders.add(order);
                    } else {
                        removedOrders++;
                    }
                }

                if (todayOrders.isEmpty()) {
                    preSelectordersMap.fastRemove(username);
                } else if (todayOrders.size() != orders.size()) {
                    preSelectordersMap.put(username, todayOrders);
                }
            }

            if (removedOrders > 0) {
                log.info("[Redis] Limpieza de preSelectOrders por fecha: {} órdenes removidas (se conserva sólo el día actual).",
                        removedOrders);
            }
        } catch (Exception e) {
            log.error("[Redis] Error limpiando preSelectOrders antiguos", e);
        }
    }

    private static void purgeDailyMapNotFromToday(RMap<String, ?> map, String mapName) {
        try {
            if (map == null || map.isEmpty()) {
                return;
            }

            String todaySuffix = "|" + LocalDate.now(zoneId);
            int removed = 0;

            List<String> keys = new ArrayList<>(map.keySet());
            for (String key : keys) {
                if (key == null || !key.endsWith(todaySuffix)) {
                    map.fastRemove(key);
                    removed++;
                }
            }

            if (removed > 0) {
                log.info("[Redis] Limpieza {} por fecha: {} keys removidas (se conserva sólo el día actual).",
                        mapName, removed);
            }
        } catch (Exception e) {
            log.error("[Redis] Error limpiando {} por fecha", mapName, e);
        }
    }

    private static void initSellSideManager() {

        try {

            String jsonString = new String(Files.readAllBytes(Paths.get(properties.getProperty("connections"))));
            String mkdString = new String(Files.readAllBytes(Paths.get(properties.getProperty("connectionsmkd"))));

            Map<String, ConnectionsVO> retMap = new Gson().fromJson(jsonString, new TypeToken<Map<String, ConnectionsVO>>() {}.getType());
            Map<String, ConnectionsVO> mkdMap = new Gson().fromJson(mkdString, new TypeToken<Map<String, ConnectionsVO>>() {}.getType());

            sellSideMKD = system.actorOf(new RoundRobinPool(20).props(SellsideConnect.props()));
            sellSideRouting = system.actorOf(new RoundRobinPool(10).props(SellsideConnect.props()));


            Boolean islog = Boolean.valueOf(properties.getProperty("islogs"));

            mkdMap.forEach((key, value) -> {
                if (value.status) {
                    log.info(key);
                    try {

                        String host = value.host + ":" + value.port;

                        MarketDataMessage.SecurityExchangeMarketData exch = MarketDataMessage.SecurityExchangeMarketData.valueOf(key);
                        String pathlogs = properties.getProperty("path.logs") + File.separator + exch.name();
                        NettyProtobufClient nettyProtobufClient = new NettyProtobufClient(host, sellSideMKD, pathlogs, exch.name(), NotificationMessage.Component.ORB, islog);
                        new Thread(nettyProtobufClient).start();
                        connections_mkd.put(exch, nettyProtobufClient);

                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                    }
                }
            });

            retMap.forEach((key, value) -> {
                if (value.status) {
                    log.info(key);
                    try {
                        String host = value.host + ":" + value.port;
                        RoutingMessage.SecurityExchangeRouting exch = RoutingMessage.SecurityExchangeRouting.valueOf(key);
                        String pathlogs = properties.getProperty("path.logs") + File.separator + exch.name();
                        NettyProtobufClient nettyProtobufClient = new NettyProtobufClient(host, sellSideRouting, pathlogs, exch.name(), NotificationMessage.Component.XRO, true,
                                getIdConnection());
                        new Thread(nettyProtobufClient).start();
                        connections.put(exch, nettyProtobufClient);
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                    }
                }
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Bloquea un símbolo por riesgo. Las órdenes que lleguen con este símbolo serán rechazadas.
     * Persiste el cambio en Redis.
     */
    public static void blockSymbol(String symbol) {
        blockedSymbols.add(symbol);
        if (blockedSymbolsRedis != null) {
            blockedSymbolsRedis.add(symbol);
        }
        log.info("[Risk] Símbolo bloqueado: {}", symbol);
    }

    /**
     * Desbloquea un símbolo previamente bloqueado por riesgo.
     * Persiste el cambio en Redis.
     */
    public static void unblockSymbol(String symbol) {
        blockedSymbols.remove(symbol);
        if (blockedSymbolsRedis != null) {
            blockedSymbolsRedis.remove(symbol);
        }
        log.info("[Risk] Símbolo desbloqueado: {}", symbol);
    }

    /** Retorna true si el usuario está bloqueado por el admin. */
    public static boolean isUserBlocked(String username) {
        return blockedUsers.contains(username);
    }

    /**
     * Bloquea un usuario. Impide que se conecte vía WebSocket.
     * Persiste el cambio en Redis (sin expiración diaria).
     */
    public static void blockUser(String username) {
        blockedUsers.add(username);
        if (blockedUsersRedis != null) {
            blockedUsersRedis.add(username);
        }
        log.info("[UserBlock] Usuario bloqueado: {}", username);
    }

    /**
     * Desbloquea un usuario previamente bloqueado.
     * Persiste el cambio en Redis.
     */
    public static void unblockUser(String username) {
        blockedUsers.remove(username);
        if (blockedUsersRedis != null) {
            blockedUsersRedis.remove(username);
        }
        log.info("[UserBlock] Usuario desbloqueado: {}", username);
    }



    /** Retorna {@code true} si la IP está bloqueada (manual o auto-bloqueada). */
    public static boolean isIpBlocked(String ip) {
        return ipSecurityEnabled && ip != null && blockedIps.contains(ip);
    }

    public static boolean isIpSecurityEnabled() {
        return ipSecurityEnabled;
    }

    public static boolean isWebSocketCompressionEnabled() {
        return webSocketCompressionEnabled;
    }

    public static boolean recordIpMessageExceeded(String ip) {
        return ipSecurityEnabled && ipRateLimiter.recordMessage(ip);
    }

    public static boolean recordIpOrderExceeded(String ip) {
        return ipSecurityEnabled && ipRateLimiter.recordOrder(ip);
    }

    /**
     * Bloquea una IP. Las conexiones WebSocket desde esta IP serán rechazadas.
     * Persiste el cambio en Redis (sin expiración diaria).
     */
    public static void blockIp(String ip) {
        if (ip == null || ip.isBlank()) return;
        blockedIps.add(ip);
        if (blockedIpsRedis != null) {
            blockedIpsRedis.add(ip);
        }
        log.warn("[IpSecurity] 🚫 IP bloqueada: {}", ip);
    }

    /**
     * Desbloquea una IP previamente bloqueada y limpia sus contadores de rate-limiting.
     * Persiste el cambio en Redis.
     */
    public static void unblockIp(String ip) {
        if (ip == null || ip.isBlank()) return;
        blockedIps.remove(ip);
        if (blockedIpsRedis != null) {
            blockedIpsRedis.remove(ip);
        }
        ipRateLimiter.clearIpStats(ip);
        log.info("[IpSecurity] ✅ IP desbloqueada: {}", ip);
    }

    /**
     * Retorna el límite de monto nominal (precio × cantidad) para el destino dado.
     * Si el destino no tiene límite configurado devuelve Double.MAX_VALUE (sin restricción).
     */
    public static double getNotionalLimit(RoutingMessage.SecurityExchangeRouting exchange) {
        return notionalLimits.getOrDefault(exchange.name(), Double.MAX_VALUE);
    }

    /**
     * Actualiza el límite de monto nominal para un destino.
     * Persiste el cambio en Redis.
     */
    public static void setNotionalLimit(RoutingMessage.SecurityExchangeRouting exchange, double limit) {
        notionalLimits.put(exchange.name(), limit);
        if (notionalLimitsRedis != null) {
            notionalLimitsRedis.put(exchange.name(), limit);
        }
        log.info("[NotionalLimit] Límite actualizado: {} → {}", exchange.name(), limit);
    }

    /**
     * Valida el monto nominal de una orden.
     *
     * @return Optional con el texto de rechazo si supera el límite; empty si está dentro del rango.
     */
    public static Optional<String> checkNotionalLimit(
            RoutingMessage.SecurityExchangeRouting exchange, double price, double qty) {
        double notional = price * qty;
        double limit    = getNotionalLimit(exchange);
        if (notional > limit) {
            return Optional.of(String.format(
                    "Orden rechazada: monto nominal (%.2f × %.2f = %,.2f) supera el límite de %,.2f para destino %s",
                    price, qty, notional, limit, exchange.name()));
        }
        return Optional.empty();
    }

    public static void recordOdAttempt(OdAttempt attempt) {
        if (attempt == null) {
            return;
        }
        odAttempts.addFirst(attempt);
        while (odAttempts.size() > MAX_OD_ATTEMPTS) {
            odAttempts.pollLast();
        }
    }

    public static List<OdAttempt> getOdAttemptsSnapshot() {
        return new ArrayList<>(odAttempts);
    }

    public static void clearOdAttempts() {
        odAttempts.clear();
    }

    public static boolean isOdProtectionEnabled() {
        return odProtectionEnabled;
    }

    public static void setOdProtectionEnabled(boolean enabled) {
        odProtectionEnabled = enabled;
        if (adminFlagsRedis != null) {
            adminFlagsRedis.put(ADMIN_FLAG_OD_PROTECTION, enabled);
        }
        log.warn("[OD] Protección OD {}", enabled ? "ACTIVADA" : "DESACTIVADA");
    }

    public synchronized static void subscribeSymbol(MarketDataMessage.Subscribe subscribe, String id) {

        try {

            if (!idSymbolsSubscrib.containsKey(id)) {
                MarketDataMessage.Subscribe subscribeaux = subscribe.toBuilder().setStatistic(true).setBook(true).setTrade(true).build();
                idSymbolsSubscrib.put(id, subscribeaux);
                if(MainApp.getConnections_mkd().containsKey(subscribe.getSecurityExchange())){
                    MainApp.getConnections_mkd().get(subscribe.getSecurityExchange()).sendMessage(subscribeaux);
                } else {
                    log.error("destino no conectado {}", subscribe.getSecurityExchange().name());
                }

            } else if(!MainApp.getSnapshotHashMap().containsKey(id)){
                idSymbolsSubscrib.remove(id);
                subscribeSymbol(subscribe, id);

            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    /** Registra el estado de custodia de una cuenta (posiciones + saldo) para la validación del admin. */
    public static void updateAccountCustody(String account, int positions, double saldoDisponible) {
        if (account == null || account.isBlank()) {
            return;
        }
        accountCustodyStatus.put(account, new double[]{positions, saldoDisponible, System.currentTimeMillis()});
    }

    public static Map<String, double[]> getAccountCustodyStatus() {
        return accountCustodyStatus;
    }

    public static void markMarketDataActivity(String id) {
        if (id == null || id.isBlank()) {
            return;
        }
        marketDataActivityAtMs.put(id, System.currentTimeMillis());
    }

    public static void registerPortfolioTopicsForStartupMkdWatch(String username, Map<String, BlotterMessage.Portfolio> portfolios) {
        if (portfolios == null || portfolios.isEmpty()) {
            return;
        }
        int added = 0;
        for (BlotterMessage.Portfolio portfolio : portfolios.values()) {
            for (BlotterMessage.Asset asset : portfolio.getAssetList()) {
                String symbol = asset.getSymbol();
                if (!isCfiSymbol(symbol)) {
                    continue;
                }
                RoutingMessage.SecurityType securityType = resolveMkdSecurityType(
                        symbol,
                        MarketDataMessage.SecurityExchangeMarketData.BCS,
                        RoutingMessage.SecurityType.CFI
                );
                for (RoutingMessage.SettlType settlType : STARTUP_MKD_SETTL_TYPES) {
                    if (registerStartupMkdWatchTopic(buildStartupMkdSubscribe(
                            symbol,
                            MarketDataMessage.SecurityExchangeMarketData.BCS,
                            settlType,
                            securityType
                    ))) {
                        added++;
                    }
                }
            }
        }
        if (added > 0) {
            log.info("[MKD/StartupWatch] usuario={} portfolio registró {} tópicos nuevos (total={})",
                    username, added, startupMkdWatchTopics.size());
        }
    }

    public static void registerMultibookTopicsForStartupMkdWatch(String username, List<BlotterMessage.SubMultibook> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        int added = 0;
        for (BlotterMessage.SubMultibook row : rows) {
            if (!row.hasSubscribeBook()) {
                continue;
            }
            MarketDataMessage.Subscribe subscribe = row.getSubscribeBook();
            if (subscribe.getSecurityExchange() == MarketDataMessage.SecurityExchangeMarketData.BCS
                    && isCfiSymbol(subscribe.getSymbol())) {
                subscribe = subscribe.toBuilder()
                        .setSecurityType(resolveMkdSecurityType(
                                subscribe.getSymbol(),
                                subscribe.getSecurityExchange(),
                                subscribe.getSecurityType()
                        ))
                        .build();
            }
            if (registerStartupMkdWatchTopic(subscribe)) {
                added++;
            }
        }
        if (added > 0) {
            log.info("[MKD/StartupWatch] usuario={} multibook registró {} tópicos nuevos (total={})",
                    username, added, startupMkdWatchTopics.size());
        }
    }

    private static MarketDataMessage.Subscribe buildStartupMkdSubscribe(
            String symbol,
            MarketDataMessage.SecurityExchangeMarketData exchange,
            RoutingMessage.SettlType settlType,
            RoutingMessage.SecurityType securityType
    ) {
        MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setSecurityExchange(exchange)
                .setSettlType(settlType)
                .setSecurityType(securityType)
                .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                .setBook(true)
                .setStatistic(true)
                .setTrade(true)
                .build();
        String topicId = cl.vc.module.protocolbuff.generator.TopicGenerator.getTopicMKD(subscribe);
        return subscribe.toBuilder().setId(topicId).build();
    }

    private static boolean registerStartupMkdWatchTopic(MarketDataMessage.Subscribe subscribe) {
        if (!isEligibleStartupMkdWatch(subscribe)) {
            return false;
        }
        String topicId = cl.vc.module.protocolbuff.generator.TopicGenerator.getTopicMKD(subscribe);
        MarketDataMessage.Subscribe normalized = subscribe.toBuilder()
                .setId(topicId)
                .setBook(true)
                .setStatistic(true)
                .setTrade(true)
                .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                .build();
        if (startupMkdWatchTopics.putIfAbsent(topicId, normalized) == null) {
            startupMkdTrackedAtMs.putIfAbsent(topicId, System.currentTimeMillis());
            return true;
        }
        return false;
    }

    private static boolean isEligibleStartupMkdWatch(MarketDataMessage.Subscribe subscribe) {
        return subscribe != null
                && isCfiSymbol(subscribe.getSymbol())
                && STARTUP_MKD_SETTL_TYPES.contains(subscribe.getSettlType());
    }

    public static RoutingMessage.SecurityType resolveMkdSecurityType(
            String symbol,
            MarketDataMessage.SecurityExchangeMarketData exchange,
            RoutingMessage.SecurityType fallback
    ) {
        MarketDataMessage.Security security = securityExchangeSymbolsMaps.get(symbol + exchange.name());
        if (security == null) {
            MarketDataMessage.SecurityList securityList = securityExchangeMaps.get(exchange);
            if (securityList != null) {
                security = securityList.getListSecuritiesList().stream()
                        .filter(candidate -> symbol.equals(candidate.getSymbol()))
                        .findFirst()
                        .orElse(null);
            }
        }

        if (security == null || security.getSecurityType().isBlank()) {
            return fallback;
        }

        try {
            return RoutingMessage.SecurityType.valueOf(security.getSecurityType());
        } catch (IllegalArgumentException e) {
            log.warn("[MKD] securityType desconocido '{}' para {}; usando {}.",
                    security.getSecurityType(), symbol, fallback);
            return fallback;
        }
    }

    private static boolean isCfiSymbol(String symbol) {
        return symbol != null && symbol.startsWith("CFI");
    }

    private static void startStartupMkdWatchdog() {
        if (!Boolean.parseBoolean(properties.getProperty("startup.mkd.watchdog.enabled", "true"))) {
            log.info("[MKD/StartupWatch] watchdog desactivado por propiedad.");
            return;
        }
        long initialDelaySeconds = Long.parseLong(properties.getProperty("startup.mkd.watchdog.initial.delay.seconds", "120"));
        long periodSeconds = Long.parseLong(properties.getProperty("startup.mkd.watchdog.period.seconds", "60"));

        synchronized (startupMkdWatchdogLock) {
            if (startupMkdWatchdogScheduler == null || startupMkdWatchdogScheduler.isShutdown()) {
                startupMkdWatchdogScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "startup-mkd-watchdog");
                    t.setDaemon(true);
                    return t;
                });
            }
            if (startupMkdWatchdogFuture != null && !startupMkdWatchdogFuture.isCancelled()) {
                startupMkdWatchdogFuture.cancel(false);
            }
            startupMkdWatchdogFuture = startupMkdWatchdogScheduler.scheduleAtFixedRate(
                    MainApp::runStartupMkdWatchdogCycle,
                    initialDelaySeconds,
                    periodSeconds,
                    TimeUnit.SECONDS
            );
        }

        log.info("[MKD/StartupWatch] watchdog programado: {} tópicos iniciales, primer chequeo en {}s, luego cada {}s.",
                startupMkdWatchTopics.size(), initialDelaySeconds, periodSeconds);
    }

    private static void runStartupMkdWatchdogCycle() {
        try {
            long now = System.currentTimeMillis();
            long staleMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(
                    properties.getProperty("startup.mkd.watchdog.stale.seconds", "180")));
            long retryCooldownMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(
                    properties.getProperty("startup.mkd.watchdog.retry.cooldown.seconds", "600")));
            int maxRetriesPerCycle = Integer.parseInt(
                    properties.getProperty("startup.mkd.watchdog.max.retries.per.cycle", "10"));

            int healthy = 0;
            int pending = 0;
            int retried = 0;

            for (Map.Entry<String, MarketDataMessage.Subscribe> entry : startupMkdWatchTopics.entrySet()) {
                String topicId = entry.getKey();
                MarketDataMessage.Subscribe subscribe = entry.getValue();

                if (marketDataActivityAtMs.containsKey(topicId) || snapshotHashMap.containsKey(topicId)) {
                    healthy++;
                    continue;
                }

                pending++;
                long trackedAt = startupMkdTrackedAtMs.getOrDefault(topicId, now);
                if ((now - trackedAt) < staleMillis) {
                    continue;
                }

                long lastRetryAt = startupMkdLastRetryAtMs.getOrDefault(topicId, 0L);
                if ((now - lastRetryAt) < retryCooldownMillis) {
                    continue;
                }

                if (retried >= maxRetriesPerCycle) {
                    continue;
                }

                idSymbolsSubscrib.remove(topicId);
                startupMkdLastRetryAtMs.put(topicId, now);
                subscribeSymbol(subscribe, topicId);
                retried++;

                log.warn("[MKD/StartupWatch] re-suscripción controlada topic={} symbol={} exchange={} settlType={} pending={} retried={}",
                        topicId,
                        subscribe.getSymbol(),
                        subscribe.getSecurityExchange(),
                        subscribe.getSettlType(),
                        pending,
                        retried);
            }

            if (pending > 0 || retried > 0) {
                log.info("[MKD/StartupWatch] ciclo: total={} healthy={} pending={} retried={}",
                        startupMkdWatchTopics.size(), healthy, pending, retried);
            }
        } catch (Exception e) {
            log.error("[MKD/StartupWatch] error en ciclo: {}", e.getMessage(), e);
        }
    }

    /**
     * Aplica una propiedad en caliente:
     *   1. Actualiza el objeto Properties en memoria.
     *   2. Para propiedades cacheadas en campos estáticos, actualiza el campo directamente.
     *
     * Llamado desde PropertiesServlet para que el cambio sea efectivo sin reinicio.
     */
    public static void applyProperty(String key, String value) {
        properties.setProperty(key, value);
        try {
            switch (key) {
                case "max.book":
                    maxBook = Integer.parseInt(value);
                    log.info("[Admin/Properties] maxBook actualizado en caliente: {}", maxBook);
                    break;
                case "max.trade.nemo":
                    maxTradeNemo = Integer.parseInt(value);
                    log.info("[Admin/Properties] maxTradeNemo actualizado en caliente: {}", maxTradeNemo);
                    break;
                case "max.trade.general":
                    maxTradeGeneral = Integer.parseInt(value);
                    log.info("[Admin/Properties] maxTradeGeneral actualizado en caliente: {}", maxTradeGeneral);
                    break;
                case "ENVIRONMENT":
                    ENVIRONMENT = value;
                    log.info("[Admin/Properties] ENVIRONMENT actualizado en caliente: {}", ENVIRONMENT);
                    break;
                case "prioridad.users":
                    log.info("[Admin/Properties] prioridad.users actualizado en caliente: {}", value);
                    break;
                case "prioridad.users.enabled":
                    log.info("[Admin/Properties] prioridad.users.enabled actualizado en caliente: {}", value);
                    break;
                case "account.validation.enabled":
                    properties.setProperty(key, value);
                    if (Boolean.parseBoolean(value)) {
                        scheduleAccountValidationTask(accountValidationInitialDelayMinutes, accountValidationMinutes, "property:account.validation.enabled");
                    } else {
                        stopAccountValidationScheduler("property:account.validation.enabled");
                    }
                    log.info("[Admin/Properties] account.validation.enabled actualizado en caliente: {}", value);
                    break;
                case "min_validate_account":
                    accountValidationMinutes = Math.max(1, Integer.parseInt(value));
                    properties.setProperty(key, String.valueOf(accountValidationMinutes));
                    scheduleAccountValidationTask(accountValidationMinutes, accountValidationMinutes, "property:min_validate_account");
                    log.info("[Admin/Properties] min_validate_account actualizado en caliente: {}", accountValidationMinutes);
                    break;
                case "initial_delay_validate_account":
                    accountValidationInitialDelayMinutes = Math.max(1, Integer.parseInt(value));
                    properties.setProperty(key, String.valueOf(accountValidationInitialDelayMinutes));
                    scheduleAccountValidationTask(accountValidationInitialDelayMinutes, accountValidationMinutes, "property:initial_delay_validate_account");
                    log.info("[Admin/Properties] initial_delay_validate_account actualizado en caliente: {}", accountValidationInitialDelayMinutes);
                    break;
                case "ip.security.enabled":
                    ipSecurityEnabled = Boolean.parseBoolean(value);
                    properties.setProperty(key, String.valueOf(ipSecurityEnabled));
                    if (!ipSecurityEnabled) {
                        ipRateLimiter.resetProtectionMode();
                    }
                    log.info("[Admin/Properties] ip.security.enabled actualizado en caliente: {}", ipSecurityEnabled);
                    break;
                case "websocket.compression.enabled":
                    webSocketCompressionEnabled = Boolean.parseBoolean(value);
                    properties.setProperty(key, String.valueOf(webSocketCompressionEnabled));
                    log.info("[Admin/Properties] websocket.compression.enabled actualizado{}: {}",
                            " (requiera reinicio del WebSocket)", webSocketCompressionEnabled);
                    break;
                default:
                    break;
            }
        } catch (NumberFormatException e) {
            log.warn("[Admin/Properties] No se pudo convertir '{}' para clave '{}': {}", value, key, e.getMessage());
        }
    }

    private static void startBolsaStatsActor() {
        try {

            bolsaStatsActor = system.actorOf(
                    cl.vc.service.akka.actors.ActorTradeGeneralStats.props()
                            .withDispatcher("ActorperMkd"),
                    "market-stats"
            );

            log.info("✅ ActorTradeGeneralStats iniciado: {}", bolsaStatsActor);

        } catch (Exception e) {
            log.error("No se pudo iniciar ActorTradeGeneralStats", e);
        }
    }

    public static int getAccountValidationMinutes() {
        return accountValidationMinutes;
    }

    public static int getAccountValidationInitialDelayMinutes() {
        return accountValidationInitialDelayMinutes;
    }

}
