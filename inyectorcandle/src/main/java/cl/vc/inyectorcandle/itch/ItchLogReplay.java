package cl.vc.inyectorcandle.itch;

import cl.vc.inyectorcandle.model.BrokerDailyStats;
import cl.vc.inyectorcandle.model.InstrumentDailyStats;
import cl.vc.inyectorcandle.model.MarketDaySummary;
import cl.vc.inyectorcandle.model.MarketSession;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Reconstruye la estadistica de la bolsa COMPLETA desde los logs del consumidor ITCH.
 * <p>
 * A diferencia del feed FIX (que solo cubre los instrumentos suscritos), aca llegan los ~16.700
 * order books de la plaza, el detalle de cada ejecucion y la corredora de ambas puntas.
 * <p>
 * El punto no obvio: {@code ORDER_EXECUTED} (el 73% de las ejecuciones) NO trae precio, solo el
 * {@code orderId}. Hay que mantener el libro vivo desde {@code ADD_ORDER}/{@code ORDER_DELETE}
 * para poder valorizarlas. Verificado sobre un dia completo: el 100% de los orderId se resuelven.
 */
public final class ItchLogReplay {
    private static final Logger LOG = LoggerFactory.getLogger(ItchLogReplay.class);
    private static final long PROGRESS_EVERY_LINES = 1_000_000L;
    private static final int RANKING_SIZE = 20;

    /** Fases en las que una ejecucion cuenta para el cierre; AfterMarket queda fuera a proposito. */
    private static final String PHASE_CLOSING_AUCTION = "SubastaCierreCL";
    private static final String PHASE_AFTER_MARKET = "AfterMarket";

    private final MongoMarketRepository repository;
    private final String market;
    private final String currency;
    private final MarketSession session;

    private long processedLines;
    private long executions;
    private long duplicates;
    private long unpriced;

    public ItchLogReplay(MongoMarketRepository repository, String market, String currency, MarketSession session) {
        this.repository = repository;
        this.market = market;
        this.currency = currency;
        this.session = session;
    }

    public void replay(String inputPath) {
        if (inputPath == null || inputPath.isBlank()) {
            throw new IllegalArgumentException("itch.replay.input.path no puede estar vacio");
        }

        List<Path> files = resolveFiles(Paths.get(inputPath.trim()));
        if (files.isEmpty()) {
            LOG.warn("Sin archivos ITCH en {}", inputPath);
            return;
        }

        long startMs = System.currentTimeMillis();
        LOG.info("Replay ITCH iniciado. archivos={} market={} currency={}", files.size(), market, currency);
        for (Path file : files) {
            replayFile(file);
        }
        LOG.info("Replay ITCH finalizado. lineas={} ejecuciones={} duplicadasPorMatchId={} sinPrecio={} duracionMs={}",
                processedLines, executions, duplicates, unpriced, System.currentTimeMillis() - startMs);
    }

    private List<Path> resolveFiles(Path input) {
        try {
            if (Files.isRegularFile(input)) {
                return List.of(input);
            }
            if (Files.isDirectory(input)) {
                try (Stream<Path> stream = Files.list(input)) {
                    return stream.filter(Files::isRegularFile)
                            .filter(p -> {
                                String n = p.getFileName().toString().toLowerCase();
                                boolean known = n.startsWith("itch_data.") || n.startsWith("itch-messages-");
                                return known && (n.endsWith(".log") || n.endsWith(".log.gz"));
                            })
                            .sorted()
                            .toList();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("No se pudo resolver " + input, e);
        }
        return Collections.emptyList();
    }

    /** Un archivo es un dia. El estado del libro y los acumuladores viven solo dentro del archivo. */
    private void replayFile(Path file) {
        DayState state = new DayState();
        long fileStartMs = System.currentTimeMillis();
        long fileStartLines = processedLines;
        LOG.info("Leyendo {}", file);

        try (InputStream in = openInput(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 20)) {

            String line;
            while ((line = reader.readLine()) != null) {
                processedLines++;
                processLine(line, state);
                if (processedLines % PROGRESS_EVERY_LINES == 0) {
                    LOG.info("Progreso lineas={} ejecuciones={} librosVivos={} ordenesVivas={}",
                            processedLines, executions, state.books.size(), state.livePrice.size());
                }
            }
        } catch (Exception e) {
            LOG.error("Error leyendo {}", file, e);
        }

        flushDay(state);
        LOG.info("Terminado {} lineas={} instrumentos={} corredoras={} duracionMs={}",
                file.getFileName(), processedLines - fileStartLines, state.instruments.size(),
                state.brokers.size(), System.currentTimeMillis() - fileStartMs);
    }

    private InputStream openInput(Path file) throws IOException {
        InputStream fileIn = Files.newInputStream(file);
        return file.getFileName().toString().toLowerCase().endsWith(".gz") ? new GZIPInputStream(fileIn) : fileIn;
    }

    /**
     * El consumidor ITCH escribe el mismo stream en dos formatos segun el logger que este activo:
     * {@code itch_data.*} en JSON ({@code ... ITCH[SOUP] TRADE {json}}) e {@code itch-messages-*} con
     * el toString de los records ({@code ... TradeReport[matchId=..., qty=...]}). Ambos traen los
     * mismos campos, asi que se resuelven al mismo acumulador en vez de duplicar el replay.
     */
    private void processLine(String line, DayState state) {
        if (state.day == null && line.length() >= 10) {
            try {
                state.day = LocalDate.parse(line.substring(0, 10));
            } catch (Exception ignored) {
                return;
            }
        }

        int typeStart = line.indexOf("ITCH[SOUP] ");
        if (typeStart < 0) {
            processRecordLine(line, state);
            return;
        }
        typeStart += 11;

        int jsonStart = line.indexOf('{');
        int typeEnd = line.indexOf(' ', typeStart);
        if (typeEnd < 0 || jsonStart < 0) {
            return;
        }

        switch (line.substring(typeStart, typeEnd)) {
            case "ADD_ORDER" -> state.livePrice.put(jsonLong(line, "orderId", jsonStart), jsonLong(line, "price", jsonStart));
            case "ORDER_DELETE" -> state.livePrice.remove(jsonLong(line, "orderId", jsonStart));
            case "ORDER_BOOK_DIR" -> state.books.put(jsonLong(line, "orderBookId", jsonStart), new Book(
                    jsonString(line, "symbol", jsonStart),
                    jsonString(line, "isin", jsonStart),
                    jsonString(line, "tradingCurrency", jsonStart),
                    (int) jsonLong(line, "decimalsInPrice", jsonStart),
                    (int) jsonLong(line, "decimalsInQuantity", jsonStart)));
            case "ORDER_BOOK_STATE" -> state.phase.put(jsonLong(line, "orderBookId", jsonStart), jsonString(line, "stateName", jsonStart));
            case "ORDER_EXECUTED" -> onExecuted(state, jsonLong(line, "matchId", jsonStart),
                    jsonLong(line, "orderBookId", jsonStart), jsonLong(line, "orderId", jsonStart),
                    Long.MIN_VALUE, jsonLong(line, "quantity", jsonStart), true,
                    jsonString(line, "owner", jsonStart), jsonString(line, "counterparty", jsonStart),
                    jsonString(line, "side", jsonStart));
            case "ORDER_EXECUTED_PRICE", "TRADE" -> onExecuted(state, jsonLong(line, "matchId", jsonStart),
                    jsonLong(line, "orderBookId", jsonStart), Long.MIN_VALUE,
                    jsonLong(line, "price", jsonStart), jsonLong(line, "quantity", jsonStart), false,
                    jsonString(line, "owner", jsonStart), jsonString(line, "counterparty", jsonStart),
                    jsonString(line, "side", jsonStart));
            default -> { }
        }
    }

    /**
     * Formato {@code 2026-08-04T13:05:00,099 OrderExecuted[time=..., bookId=70675, executedQty=1801]}.
     * Sin decimales de cantidad: el Directory de este logger no los publica y en el JSON vienen
     * siempre en 0.
     * <p>
     * Ojo con el prefijo: aca es UTC (el logger JSON lo escribe en hora local). Como el consumidor
     * arranca antes de la apertura la fecha coincide igual, pero un log que empiece despues de las
     * 20:00 de Santiago le pondria el dia siguiente a {@code state.day}.
     */
    private void processRecordLine(String line, DayState state) {
        int open = line.indexOf('[');
        if (open < 0) {
            return;
        }
        int typeStart = line.lastIndexOf(' ', open) + 1;
        if (typeStart <= 0) {
            return;
        }

        switch (line.substring(typeStart, open)) {
            case "AddOrder" -> state.livePrice.put(recLong(line, "orderId", open), recLong(line, "priceRaw", open));
            case "DeleteOrder" -> state.livePrice.remove(recLong(line, "orderId", open));
            case "Directory" -> state.books.put(recLong(line, "bookId", open), new Book(
                    recString(line, "symbol", open),
                    recString(line, "isin", open),
                    recString(line, "currency", open),
                    (int) recLong(line, "priceDecimals", open),
                    0));
            case "BookState" -> state.phase.put(recLong(line, "bookId", open), recString(line, "state", open));
            case "OrderExecuted" -> onExecuted(state, recLong(line, "matchId", open),
                    recLong(line, "bookId", open), recLong(line, "orderId", open),
                    Long.MIN_VALUE, recLong(line, "executedQty", open), true,
                    recString(line, "owner", open), recString(line, "counterparty", open),
                    recSide(line, open));
            case "ExecutedWithPrice" -> onExecuted(state, recLong(line, "matchId", open),
                    recLong(line, "bookId", open), Long.MIN_VALUE,
                    recLong(line, "priceRaw", open), recLong(line, "executedQty", open), false,
                    recString(line, "owner", open), recString(line, "counterparty", open),
                    recSide(line, open));
            case "TradeReport" -> onExecuted(state, recLong(line, "matchId", open),
                    recLong(line, "bookId", open), Long.MIN_VALUE,
                    recLong(line, "priceRaw", open), recLong(line, "qty", open), false,
                    recString(line, "owner", open), recString(line, "counterparty", open),
                    recSide(line, open));
            default -> { }
        }
    }

    /**
     * {@code rawPrice} solo se usa cuando el mensaje lo trae; con {@code priceFromBook} se resuelve
     * desde la orden en el libro, que es el 73% de las ejecuciones.
     * <p>
     * {@code side} es el lado de la orden que estaba en el libro: si es B, el owner compro y la
     * contraparte vendio. En los mensajes de trade viene en blanco y solo se acumula el total.
     */
    private void onExecuted(DayState state, long matchId, long orderBookId, long orderId,
                            long rawPrice, long rawQty, boolean priceFromBook,
                            String owner, String counterparty, String side) {
        if (matchId != Long.MIN_VALUE && !state.seenMatch.add(matchId)) {
            duplicates++;
            return;
        }

        Book book = state.books.get(orderBookId);
        if (book == null) {
            return;
        }

        if (priceFromBook) {
            Long resting = state.livePrice.get(orderId);
            if (resting == null) {
                unpriced++;
                return;
            }
            rawPrice = resting;
        }
        if (rawPrice == Long.MIN_VALUE || rawPrice <= 0) {
            unpriced++;
            return;
        }
        if (rawQty == Long.MIN_VALUE || rawQty <= 0) {
            return;
        }

        BigDecimal price = BigDecimal.valueOf(rawPrice, book.decimalsInPrice);
        BigDecimal qty = BigDecimal.valueOf(rawQty, book.decimalsInQuantity);
        BigDecimal amount = price.multiply(qty);

        state.instruments
                .computeIfAbsent(book.symbol, s -> new InstrumentAccum(book))
                .apply(price, qty, amount, state.phase.getOrDefault(orderBookId, ""));

        boolean ownerBuys = "B".equals(side);
        boolean ownerSells = "S".equals(side);
        if (!owner.isEmpty()) {
            state.broker(owner).apply(qty, amount, ownerBuys, ownerSells);
        }
        if (!counterparty.isEmpty()) {
            state.broker(counterparty).apply(qty, amount, ownerSells, ownerBuys);
        }
        executions++;
    }

    private void flushDay(DayState state) {
        if (state.day == null || state.instruments.isEmpty()) {
            LOG.info("Dia sin ejecuciones utilizables, nada que escribir");
            return;
        }

        for (Map.Entry<String, InstrumentAccum> entry : state.instruments.entrySet()) {
            InstrumentAccum acc = entry.getValue();
            repository.upsertInstrumentDailyFromItch(acc.toStats(market, currency, entry.getKey(), state.day), acc.trades);
        }

        for (Map.Entry<String, BrokerAccum> entry : state.brokers.entrySet()) {
            repository.upsertBrokerDaily(entry.getValue().toStats(market, entry.getKey(), state.day));
        }

        LOG.info("[ITCH][{}] instrumentos={} corredoras={}",
                state.day, state.instruments.size(), state.brokers.size());
    }

    // ---- parseo JSON plano: los mensajes son de una linea y sin anidamiento ----

    private static long jsonLong(String line, String key, int from) {
        int i = fieldValueStart(line, key, from);
        if (i < 0) {
            return Long.MIN_VALUE;
        }
        boolean negative = line.charAt(i) == '-';
        if (negative) {
            i++;
        }
        long out = 0;
        int start = i;
        for (int n = line.length(); i < n; i++) {
            char c = line.charAt(i);
            if (c < '0' || c > '9') {
                break;
            }
            out = out * 10 + (c - '0');
        }
        if (i == start) {
            return Long.MIN_VALUE;
        }
        return negative ? -out : out;
    }

    private static String jsonString(String line, String key, int from) {
        int i = fieldValueStart(line, key, from);
        if (i < 0 || line.charAt(i) != '"') {
            return "";
        }
        int end = line.indexOf('"', i + 1);
        if (end < 0) {
            return "";
        }
        return line.substring(i + 1, end).trim();
    }

    private static int fieldValueStart(String line, String key, int from) {
        int i = line.indexOf('"' + key + "\":", from);
        return i < 0 ? -1 : i + key.length() + 3;
    }

    // ---- parseo del toString de records: key=valor, separados por ", " y cerrados con ']' ----

    private static long recLong(String line, String key, int from) {
        int i = recValueStart(line, key, from);
        if (i < 0) {
            return Long.MIN_VALUE;
        }
        boolean negative = line.charAt(i) == '-';
        if (negative) {
            i++;
        }
        long out = 0;
        int start = i;
        for (int n = line.length(); i < n; i++) {
            char c = line.charAt(i);
            if (c < '0' || c > '9') {
                break;
            }
            out = out * 10 + (c - '0');
        }
        if (i == start) {
            return Long.MIN_VALUE;
        }
        return negative ? -out : out;
    }

    private static String recString(String line, String key, int from) {
        int i = recValueStart(line, key, from);
        if (i < 0) {
            return "";
        }
        int end = i;
        for (int n = line.length(); end < n; end++) {
            char c = line.charAt(end);
            if (c == ',' || c == ']') {
                break;
            }
        }
        return line.substring(i, end).trim();
    }

    /** El JSON usa B/S y este logger BUY/SELL; se normaliza al primero para no tocar el acumulador. */
    private static String recSide(String line, int from) {
        String side = recString(line, "side", from);
        return side.isEmpty() ? "" : side.substring(0, 1);
    }

    /**
     * La clave debe empezar justo despues de '[' o de ", ": buscar el literal suelto haria que
     * {@code qty=} calzara dentro de {@code executedQty=} y {@code price=} dentro de otro campo.
     */
    private static int recValueStart(String line, String key, int from) {
        int i = from;
        int n = line.length();
        while (i < n) {
            i = line.indexOf(key, i + 1);
            if (i < 0) {
                return -1;
            }
            int end = i + key.length();
            char before = line.charAt(i - 1);
            if (end < n && line.charAt(end) == '=' && (before == '[' || before == ' ')) {
                return end + 1;
            }
            i = end;
        }
        return -1;
    }

    // ---- estado ----

    private record Book(String symbol, String isin, String currency, int decimalsInPrice, int decimalsInQuantity) {
    }

    private static final class DayState {
        LocalDate day;
        final Map<Long, Book> books = new HashMap<>(20_000);
        final Map<Long, Long> livePrice = new HashMap<>(65_536);
        final Map<Long, String> phase = new HashMap<>(20_000);
        final Set<Long> seenMatch = new HashSet<>(131_072);
        final Map<String, InstrumentAccum> instruments = new HashMap<>(20_000);
        final Map<String, BrokerAccum> brokers = new HashMap<>(64);

        BrokerAccum broker(String code) {
            return brokers.computeIfAbsent(code, k -> new BrokerAccum());
        }
    }

    private static final class InstrumentAccum {
        private final Book book;
        private BigDecimal open;
        private BigDecimal high;
        private BigDecimal low;
        private BigDecimal last;
        private BigDecimal close;
        private BigDecimal auctionClose;
        private BigDecimal volume = BigDecimal.ZERO;
        private BigDecimal turnover = BigDecimal.ZERO;
        private long trades;

        InstrumentAccum(Book book) {
            this.book = book;
        }

        void apply(BigDecimal price, BigDecimal qty, BigDecimal amount, String phase) {
            if (open == null) {
                open = price;
                high = price;
                low = price;
            } else {
                if (price.compareTo(high) > 0) {
                    high = price;
                }
                if (price.compareTo(low) < 0) {
                    low = price;
                }
            }
            last = price;
            volume = volume.add(qty);
            turnover = turnover.add(amount);
            trades++;

            if (PHASE_CLOSING_AUCTION.equals(phase)) {
                auctionClose = price;
            }
            if (!PHASE_AFTER_MARKET.equals(phase)) {
                close = price;
            }
        }

        InstrumentDailyStats toStats(String market, String fallbackCurrency, String symbol, LocalDate day) {
            String ccy = book.currency == null || book.currency.isBlank() ? fallbackCurrency : book.currency;
            BigDecimal officialClose = auctionClose != null ? auctionClose : close;
            return new InstrumentDailyStats(market, symbol, book.isin, ccy, day,
                    open, high, low, last, officialClose, null, volume, turnover, auctionClose,
                    null, null, null, 0L, trades, Instant.now());
        }
    }

    private static final class BrokerAccum {
        private long trades;
        private BigDecimal volume = BigDecimal.ZERO;
        private BigDecimal turnover = BigDecimal.ZERO;
        private BigDecimal buyVolume = BigDecimal.ZERO;
        private BigDecimal sellVolume = BigDecimal.ZERO;
        private BigDecimal buyTurnover = BigDecimal.ZERO;
        private BigDecimal sellTurnover = BigDecimal.ZERO;

        void apply(BigDecimal qty, BigDecimal amount, boolean buys, boolean sells) {
            trades++;
            volume = volume.add(qty);
            turnover = turnover.add(amount);
            if (buys) {
                buyVolume = buyVolume.add(qty);
                buyTurnover = buyTurnover.add(amount);
            } else if (sells) {
                sellVolume = sellVolume.add(qty);
                sellTurnover = sellTurnover.add(amount);
            }
        }

        BrokerDailyStats toStats(String market, String broker, LocalDate day) {
            return new BrokerDailyStats(market, broker, day, trades, volume, turnover,
                    buyVolume, sellVolume, buyTurnover, sellTurnover);
        }
    }
}
