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
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    @Getter
    private static HashMap<String, List<RoutingMessage.Order>> AllordersMap = new HashMap<>();
    @Getter
    private static HashMap<String, HashMap<String, RoutingMessage.Order>> tradesMapAll = new HashMap<>();
    @Getter
    private static RMap<String, HashMap<String, RoutingMessage.Order>> tradesMapReddis;

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
    private static HashMap<String, ActorRef> accountGroupUser = new HashMap<>();
    @Getter
    private static boolean subscribeSecuritylist_bcs = false;
    @Getter
    private static HashMap<String, MarketDataMessage.Subscribe> idSymbolsSubscrib = new HashMap<>();
    @Getter
    private static HashMap<String, RoutingMessage.Order> idOrders = new HashMap<>();
    @Getter
    private static ActorRef clientManager;
    @Getter
    private static List<BlotterMessage.Simultaneas> allSimultaneas;
    private Map<RoutingMessage.SecurityExchangeRouting, ActorRef> conMap;
    @Getter private static boolean voultechExcluded = false;
    @Getter private static boolean suppressActorsForVoultech = true;
    @Getter private static int voultechAccountCountRaw = 0;
    @Getter private static int voultechAccountCountValid = 0;
    @Getter private static Set<String> voultechAccountsSet = Collections.emptySet();
    private static final Set<String> processedUsers = ConcurrentHashMap.newKeySet();
    private static final long ACCOUNT_INIT_TIMEOUT_SECONDS = 120L;

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



            log.info("Leyendo parametros.");
            properties.load(fis);

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
            initRedis();

            log.info("#################################################################################################################################");
            log.info("############################################## WEB SOCKET #######################################################################");

            websockerServer.setProperties(properties);
            new Thread(websockerServer).start();

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

            java.util.concurrent.ScheduledExecutorService scheduler = java.util.concurrent.Executors.newScheduledThreadPool(1);

            Runnable task = new Runnable() {
                @Override
                public void run() {
                    try {
                        log.info("evaluamos si se agregaron cuentas");
                        initAccount(false);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            };

            int time;
            try {
                time = Integer.parseInt(properties.getProperty("min_validate_account", "5"));
            } catch (NumberFormatException e) {
                time = 5;
                log.warn("min_validate_account inválido, usando {} min por defecto", time);
            }
            time = Math.max(1, time);
            log.info("🕒 [Keycloak] Scheduler activo (withFixedDelay): {} min después de cada corrida", time);

            scheduler.scheduleWithFixedDelay(task, 60, time, java.util.concurrent.TimeUnit.MINUTES);




        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
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
                List<UserRepresentation> users;
                List<UserRepresentation> usersInGroup = new ArrayList<>();

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

        long globalStartNanos = System.nanoTime();
        int processedUsers = 0;

        for (GroupUsersBatch batch : batches) {
            List<UserRepresentation> prioritized = selectUsersByPriority(batch.users, priorityUsers, true);
            if (!prioritized.isEmpty()) {
                processedUsers = interval(batch.rootGroup, prioritized, isloog, totalUsers, processedUsers, globalStartNanos);
            }
        }

        for (GroupUsersBatch batch : batches) {
            List<UserRepresentation> nonPrioritized = selectUsersByPriority(batch.users, priorityUsers, false);
            if (!nonPrioritized.isEmpty()) {
                processedUsers = interval(batch.rootGroup, nonPrioritized, isloog, totalUsers, processedUsers, globalStartNanos);
            }
        }

        keycloak.close();
    }

    private static int interval(GroupRepresentation s, List<UserRepresentation> users, Boolean isloog, int totalUsers, int processedUsers, long globalStartNanos) {

        HashMap<String, String> marginByAccount = new HashMap<>();
        HashMap<String, Double> leverageByAccount = new HashMap<>();

        for (UserRepresentation r : users) {
            long userStartNanos = System.nanoTime();
            int currentUserIndex = processedUsers + 1;
            final String username = Optional.ofNullable(r.getUsername()).orElse("");

            try {
                final boolean isExcludedUser = "voultech".equalsIgnoreCase(username);
                if (isExcludedUser && !voultechExcluded) {
                    List<String> tokens = extractAccountsTokens(r);
                    voultechAccountCountRaw   = tokens.size();
                    voultechAccountsSet       = filterValidAccounts(tokens);
                    voultechAccountCountValid = voultechAccountsSet.size();
                    logVoultechSummaryOnce(username);
                }

                groupByUsers.put(s.getName(), username, 1);
                int declaredAccounts = countDistinctAccountsFromAttributes(r);
                log.info("[Keycloak] Iniciando usuario {}/{} '{}' (grupo='{}', cuentas declaradas={})",
                        currentUserIndex, totalUsers, username, s.getName(), declaredAccounts);

                if(!username.contains("dbarrios")){
                    continue; //todo eliminar esto
                }

                if (r.getAttributes() == null) continue;

                for (Map.Entry<String, List<String>> l : r.getAttributes().entrySet()) {

                    String key = l.getKey().toLowerCase();

                    if (key.contains("marginaccount")) {

                        for (String x : l.getValue()) {
                            try {

                                if (x.contains("|")) {

                                    String[] valor  = x.split("\\|");
                                    String account  = valor[0].trim().replace(" ", "");
                                    Double margen   = Double.valueOf(valor[1].trim().replace(" ", ""));
                                    log.info("[Keycloak] Usuario {}/{} '{}' procesando cuenta '{}' (margin={})", currentUserIndex, totalUsers, username, account, margen);

                                    marginByAccount.put(account, valor[1].trim().replace(" ", ""));

                                    ActorRef ref = accountGroupUser.get(account);

                                    if (ref != null) {

                                        if (isExcludedUser) {
                                            //log.info("?? [Actors] Skip UpdateMargin desde 'voultech' para {}", account);
                                        } else {
                                            ref.tell(new ActorGroupPerAccount.UpdateMargin(margen, username), ActorRef.noSender());
                                        }

                                    } else {

                                        if (suppressActorsForVoultech && (isExcludedUser || voultechAccountsSet.contains(account))) {
                                            //log.info("?? [Actors] No se crea actor para {} (usuario/cuenta 'voultech'). Omito UpdateMargin.", account);

                                        } else {

                                            ActorRef actorRef = system.actorOf(ActorGroupPerAccount.props(account, margen, 3.0).withDispatcher("ActorperAccount"));
                                            accountGroupUser.put(account, actorRef);
                                            initializeAccountActorSync(account, actorRef, username, currentUserIndex, totalUsers);

                                        }
                                    }
                                }
                            } catch (Exception e) {
                                log.warn("No se pudo parsear marginaccount '{}' para usuario '{}': {}", x, username, e.getMessage());
                            }
                        }

                    }


                    if (key.contains("palanca")) {

                        for (String x : l.getValue()) { //l.getValue() es el rut

                            try {

                                if(!username.contains("dbarrios")){
                                    continue; //todo eliminar esto
                                }

                                if (x.contains("|")) {

                                    String[] valor  = x.split("\\|");
                                    String account  = valor[0].trim().replace(" ", "");
                                    Double leverage = Double.valueOf(valor[1].trim().replace(" ", ""));

                                    log.info("[Keycloak] Usuario {}/{} '{}' procesando cuenta '{}' (palanca={})", currentUserIndex, totalUsers, username, account, leverage);

                                    leverageByAccount.put(account, leverage);

                                    ActorRef ref = accountGroupUser.get(account);

                                    if (ref != null) {
                                        if (isExcludedUser) {
                                            log.info("?? [Actors] Skip UpdateLeverage desde 'voultech' para {}", account);
                                        } else {
                                            ref.tell(new ActorGroupPerAccount.UpdateLeverage(leverage, username), ActorRef.noSender());
                                        }

                                    } else {

                                        if (suppressActorsForVoultech && (isExcludedUser || voultechAccountsSet.contains(account))) {
                                            log.info("?? [Actors] No se crea actor para {} (usuario/cuenta 'voultech'). Omito UpdateLeverage.", account);

                                        } else {

                                            ActorRef actorRef = system.actorOf(ActorGroupPerAccount
                                                    .props(account, 0.0, leverage).withDispatcher("ActorperAccount"));

                                            accountGroupUser.put(account, actorRef);


                                            initializeAccountActorSync(account, actorRef, username, currentUserIndex, totalUsers);
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                log.error("No se pudo parsear palanca '{}' para usuario '{}': {}", x, username, e.getMessage());
                            }
                        }
                    }
                }

            } finally {
                markUserProcessed(username);

                long userElapsedMs = (System.nanoTime() - userStartNanos) / 1_000_000L;
                processedUsers++;

                long elapsedGlobalMs = (System.nanoTime() - globalStartNanos) / 1_000_000L;
                long avgPerUserMs = processedUsers == 0 ? 0 : elapsedGlobalMs / processedUsers;
                long etaMs = Math.max(0, (totalUsers - processedUsers) * avgPerUserMs);

                log.info("[Keycloak] Usuario {}/{} '{}' procesado en {} ms. ETA restante aprox: {}",
                        processedUsers, totalUsers, username, userElapsedMs, formatMillis(etaMs));
            }
        }

        return processedUsers;
    }

    private static Set<String> parsePriorityUsers() {
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
        if (normalized.isEmpty()) return;
        processedUsers.add(normalized);
    }

    public static boolean isUserProcessed(String username) {
        String normalized = normalizeUsername(username);
        if (normalized.isEmpty()) return false;
        return processedUsers.contains(normalized);
    }

    private static String normalizeUsername(String username) {
        return username == null ? "" : username.trim().toLowerCase(Locale.ROOT);
    }

    private static int countDistinctAccountsFromAttributes(UserRepresentation user) {
        if (user.getAttributes() == null || user.getAttributes().isEmpty()) {
            return 0;
        }
        Set<String> accounts = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : user.getAttributes().entrySet()) {
            String key = Optional.ofNullable(entry.getKey()).orElse("").toLowerCase();
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

    private static void initializeAccountActorSync(String account, ActorRef actorRef, String username, int currentUserIndex, int totalUsers) {
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

            if (response instanceof ActorGroupPerAccount.Initialized) {
                log.info("[Actors] Usuario {}/{} '{}' cuenta '{}' inicializada.",
                        currentUserIndex, totalUsers, username, account);
            } else {
                log.warn("[Actors] Respuesta inesperada al inicializar cuenta {}: {}", account, response);
            }
        } catch (Exception e) {
            log.error("[Actors] Error/timeout inicializando cuenta '{}' (usuario='{}' {}/{}): {}",
                    account, username, currentUserIndex, totalUsers, e.getMessage(), e);
        }
    }

    private static void initRedis() {
        try {

            Calendar expirationDate = Calendar.getInstance();
            expirationDate.set(Calendar.HOUR_OF_DAY, 17);
            expirationDate.set(Calendar.MINUTE, 59);
            expirationDate.set(Calendar.SECOND, 0);

            redisExpirationInstant = expirationDate.toInstant();

            String connections = properties.getProperty("redis.host") + ":" + Integer.valueOf(properties.getProperty("redis.port"));
            Config redis = new Config();

            redis.useSingleServer().setAddress("redis://" + connections).setPassword(properties.getProperty("redis.password"));
            RedissonClient redisson = Redisson.create(redis);

            ordersMapRedis = redisson.getMap("Orders");
            ordersMapRedis.expire(expirationDate.toInstant());

            tradesMapReddis = redisson.getMap("Trades");
            tradesMapReddis.expire(expirationDate.toInstant());

            positionsMapsRedis = redisson.getMap("Positions");
            positionsMapsRedis.expire(expirationDate.toInstant());

            patrimonioMapsRedis = redisson.getMap("Patrimonio");
            patrimonioMapsRedis.expire(expirationDate.toInstant());

            snapshotPositionHistoryRedis = redisson.getMap("SnapshotPositionHistory");
            snapshotPositionHistoryRedis.expire(expirationDate.toInstant());

            balanceRedis = redisson.getMap("Balance");
            balanceRedis.expire(expirationDate.toInstant());

            snapshotSimultaneasRedis = redisson.getMap("SnapshotSimultaneas");
            snapshotSimultaneasRedis.expire(expirationDate.toInstant());

            snapshotPrestamosRedis = redisson.getMap("SnapshotPrestamos");
            snapshotPrestamosRedis.expire(expirationDate.toInstant());

            if(Boolean.parseBoolean(properties.getProperty("redis.enable.persistencia"))) {
                purgeOrdersNotFromToday();
                purgeTradesNotFromToday();
                purgeDailyMapNotFromToday(patrimonioMapsRedis, "Patrimonio");
                purgeDailyMapNotFromToday(snapshotPositionHistoryRedis, "SnapshotPositionHistory");
                purgeDailyMapNotFromToday(balanceRedis, "Balance");
                purgeDailyMapNotFromToday(snapshotSimultaneasRedis, "SnapshotSimultaneas");
                purgeDailyMapNotFromToday(snapshotPrestamosRedis, "SnapshotPrestamos");

                if (!ordersMapRedis.isEmpty()) {
                    ordersMapRedis.forEach((key, value) -> {
                        if (suppressActorsForVoultech && voultechAccountsSet.contains(key)) {
                            log.info("⛔️ [Restore] Omitida restauración para cuenta {} (voultech)", key);
                            return;
                        }
                        if (accountGroupUser.containsKey(key)) {
                            log.info("🔁 se restaura la cuenta {}", key);
                            accountGroupUser.get(key).tell(new ActorGroupPerAccount.RestoreOrder(value), ActorRef.noSender());
                        } else {
                            log.info("actor no creado para la cuenta {}", key);
                        }
                    });
                }

                if (!tradesMapReddis.isEmpty()) {
                    tradesMapReddis.forEach((key, value) -> {
                        if (suppressActorsForVoultech && voultechAccountsSet.contains(key)) {
                            log.info("⛔️ [Restore] Omitida restauración TRADES para cuenta {} (voultech)", key);
                            return;
                        }
                        if (accountGroupUser.containsKey(key)) {
                            accountGroupUser.get(key).tell(new ActorGroupPerAccount.RestoreTrade(value), ActorRef.noSender());
                        }
                    });
                }

                if (!positionsMapsRedis.isEmpty()) {
                    positionsMapsRedis.forEach((key, value) -> {
                        if (suppressActorsForVoultech && voultechAccountsSet.contains(key)) {
                            log.info("⛔️ [Restore] Omitida restauración POSITIONS para cuenta {} (voultech)", key);
                            return;
                        }
                        if (accountGroupUser.containsKey(key)) {
                            accountGroupUser.get(key).tell(new ActorGroupPerAccount.RestorePositions(value), ActorRef.noSender());
                        }
                    });
                }

            }


            portfolioMaps = redisson.getMap("Portfolio");
            preSelectordersMap = redisson.getMap("PreSelectOrders");
            multiBookMaps = redisson.getMap("MultiBook");


        } catch (Exception e) {
            log.error("Could not start the redis connection", e);
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
                        NettyProtobufClient nettyProtobufClient = new NettyProtobufClient(host, sellSideRouting, pathlogs, exch.name(), NotificationMessage.Component.XRO, true, "service-vector-trade");
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

}



