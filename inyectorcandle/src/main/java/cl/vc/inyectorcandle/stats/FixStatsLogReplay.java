package cl.vc.inyectorcandle.stats;

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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Lee logs de mensajes de QuickFIX y extrae la estadistica oficial que la bolsa publica en los
 * MarketDataSnapshotFullRefresh (35=W): apertura, maximo, minimo, ultimo, cierre, volumen y monto.
 * <p>
 * Cada 35=W es un snapshot completo del instrumento, no un delta: el ultimo del dia gana. El unico
 * valor que se toma del primero es 269=5, que a esa hora todavia es el cierre de la jornada anterior.
 * <p>
 * Los 35=X quedan fuera a proposito: solo transportan libro (269=0/1), ninguna estadistica.
 */
public final class FixStatsLogReplay {
    private static final Logger LOG = LoggerFactory.getLogger(FixStatsLogReplay.class);
    private static final char SOH = (char) 1;
    private static final String SNAPSHOT_MARKER = SOH + "35=W" + SOH;
    private static final DateTimeFormatter LOG_DAY = DateTimeFormatter.BASIC_ISO_DATE;
    private static final long PROGRESS_EVERY_LINES = 250_000L;

    /** 1178=101 es el monto transado acumulado; el 102 viene siempre en cero en este feed. */
    private static final String TURNOVER_STAT = "101";

    /** Digitos de Long.MIN_VALUE: el centinela de "sin dato" del feed, con cualquier escala. */
    private static final String SENTINEL_DIGITS = "9223372036854775808";

    private static final int RANKING_SIZE = 20;

    private final MongoMarketRepository repository;
    private final String market;
    private final String currency;
    private final MarketSession session;
    private final Map<String, Accum> byInstrumentDay = new LinkedHashMap<>();

    private long processedLines;
    private long snapshots;

    public FixStatsLogReplay(MongoMarketRepository repository, String market, String currency, MarketSession session) {
        this.repository = repository;
        this.market = market;
        this.currency = currency;
        this.session = session;
    }

    public void replay(String inputPath) {
        if (inputPath == null || inputPath.isBlank()) {
            throw new IllegalArgumentException("stats.replay.input.path no puede estar vacio");
        }

        List<Path> files = resolveFiles(Paths.get(inputPath.trim()));
        if (files.isEmpty()) {
            LOG.warn("Sin archivos de mensajes FIX en {}", inputPath);
            return;
        }

        long startMs = System.currentTimeMillis();
        LOG.info("Estadistica FIX iniciada. archivos={} market={} currency={}", files.size(), market, currency);
        for (Path file : files) {
            replayFile(file);
        }

        int written = flush();
        LOG.info("Estadistica FIX finalizada. lineas={} snapshots35W={} instrumentosDia={} duracionMs={}",
                processedLines, snapshots, written, System.currentTimeMillis() - startMs);
    }

    private List<Path> resolveFiles(Path input) {
        try {
            if (Files.isRegularFile(input)) {
                return List.of(input);
            }
            if (Files.isDirectory(input)) {
                try (Stream<Path> stream = Files.list(input)) {
                    return stream.filter(Files::isRegularFile)
                            .filter(path -> {
                                String name = path.getFileName().toString().toLowerCase();
                                // los .events solo traen logon/logout, no mensajes de aplicacion
                                return name.contains(".messages") && (name.endsWith(".log") || name.endsWith(".log.gz"));
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

    private void replayFile(Path file) {
        long fileSnapshots = snapshots;
        LOG.info("Leyendo {}", file);

        try (InputStream in = openInput(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 20)) {

            String line;
            while ((line = reader.readLine()) != null) {
                processedLines++;
                if (line.contains(SNAPSHOT_MARKER)) {
                    parseSnapshot(line);
                }
                if (processedLines % PROGRESS_EVERY_LINES == 0) {
                    LOG.info("Progreso lineas={} snapshots={}", processedLines, snapshots);
                }
            }
        } catch (Exception e) {
            LOG.error("Error leyendo {}", file, e);
        }

        LOG.info("Terminado {} snapshots={}", file.getFileName(), snapshots - fileSnapshots);
    }

    private InputStream openInput(Path file) throws IOException {
        InputStream fileIn = Files.newInputStream(file);
        return file.getFileName().toString().toLowerCase().endsWith(".gz") ? new GZIPInputStream(fileIn) : fileIn;
    }

    /**
     * Formato de linea: {@code yyyyMMdd HH:mm:ss.SSS: 8=FIXT.1.1<SOH>9=...}. El dia se toma del
     * prefijo del log, que es hora local del proceso que capturo el mensaje.
     */
    private void parseSnapshot(String line) {
        int bodyStart = line.indexOf(": 8=");
        if (bodyStart < 0 || bodyStart < 8) {
            return;
        }

        LocalDate day;
        try {
            day = LocalDate.parse(line.substring(0, 8), LOG_DAY);
        } catch (Exception e) {
            return;
        }

        Accum accum = null;
        char entryType = 0;
        String pendingStat = null;
        BigDecimal price = null;
        BigDecimal qty = null;

        int i = bodyStart + 2;
        int n = line.length();
        while (i < n) {
            int eq = line.indexOf('=', i);
            if (eq < 0) {
                break;
            }
            int end = line.indexOf(SOH, eq + 1);
            if (end < 0) {
                end = n;
            }
            int tag = parseInt(line, i, eq);
            String value = line.substring(eq + 1, end);
            i = end + 1;

            switch (tag) {
                case 55 -> accum = accumFor(day, value.trim());
                case 48 -> { if (accum != null) accum.securityId = value.trim(); }
                case 269 -> {
                    if (accum != null && entryType != 0) {
                        accum.apply(entryType, price, qty);
                    }
                    entryType = value.isEmpty() ? 0 : value.charAt(0);
                    price = null;
                    qty = null;
                }
                case 270 -> price = decimal(value);
                case 271 -> qty = decimal(value);
                case 1178 -> pendingStat = value.trim();
                case 1179 -> {
                    if (accum != null && TURNOVER_STAT.equals(pendingStat)) {
                        accum.turnover = decimal(value);
                    }
                }
                case 1148 -> { if (accum != null) accum.lowLimit = decimal(value); }
                case 1149 -> { if (accum != null) accum.highLimit = decimal(value); }
                case 1150 -> { if (accum != null) accum.referencePrice = decimal(value); }
                default -> { }
            }
        }

        if (accum != null && entryType != 0) {
            accum.apply(entryType, price, qty);
        }
        if (accum != null) {
            accum.updates++;
            snapshots++;
        }
    }

    private Accum accumFor(LocalDate day, String symbol) {
        return byInstrumentDay.computeIfAbsent(day + "|" + symbol, k -> new Accum(day, symbol));
    }

    private int flush() {
        List<InstrumentDailyStats> rows = new ArrayList<>(byInstrumentDay.size());
        for (Accum accum : byInstrumentDay.values()) {
            rows.add(accum.toStats(market, currency));
        }
        rows.forEach(repository::upsertInstrumentDaily);
        logSample(rows);
        return rows.size();
    }

    private void logSample(List<InstrumentDailyStats> rows) {
        rows.stream()
                .filter(r -> r.turnover() != null)
                .sorted((a, b) -> b.turnover().compareTo(a.turnover()))
                .limit(10)
                .forEach(r -> LOG.info("[STATS][{}] {} open={} high={} low={} last={} close={} prevClose={} vol={} monto={} var={}%",
                        r.tradingDay(), r.symbol(), r.open(), r.high(), r.low(), r.last(), r.close(),
                        r.previousClose(), r.volume(), r.turnover(), r.variationPct()));
    }

    private static int parseInt(String s, int from, int to) {
        int out = 0;
        for (int i = from; i < to; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            out = out * 10 + (c - '0');
        }
        return out;
    }

    /**
     * El feed escribe "campo ausente" como Long.MIN_VALUE escalado por los decimales del
     * instrumento (-9223372036854775.808 con 3 decimales), no como tag vacio. Sin descartarlo
     * termina en Mongo como un Double de 16 digitos y el front lo pinta como precio.
     */
    private static BigDecimal decimal(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            BigDecimal parsed = new BigDecimal(value.trim());
            return isAbsentSentinel(parsed) ? null : parsed;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean isAbsentSentinel(BigDecimal value) {
        return SENTINEL_DIGITS.equals(value.unscaledValue().abs().toString());
    }

    private static final class Accum {
        private final LocalDate day;
        private final String symbol;
        private String securityId;
        private BigDecimal open;
        private BigDecimal high;
        private BigDecimal low;
        private BigDecimal last;
        private BigDecimal close;
        private BigDecimal previousClose;
        private BigDecimal volume;
        private BigDecimal turnover;
        private BigDecimal auctionPrice;
        private BigDecimal lowLimit;
        private BigDecimal highLimit;
        private BigDecimal referencePrice;
        private long updates;

        Accum(LocalDate day, String symbol) {
            this.day = day;
            this.symbol = symbol;
        }

        void apply(char entryType, BigDecimal price, BigDecimal qty) {
            switch (entryType) {
                case '4' -> open = price != null ? price : open;
                case '7' -> high = price != null ? price : high;
                case '8' -> low = price != null ? price : low;
                case '2' -> last = price != null ? price : last;
                case '5' -> {
                    if (price != null) {
                        // el primer 35=W del dia todavia trae el cierre de la jornada anterior
                        if (previousClose == null) {
                            previousClose = price;
                        }
                        close = price;
                    }
                }
                case 'B' -> volume = qty != null ? qty : volume;
                case 'Q' -> auctionPrice = price != null ? price : auctionPrice;
                default -> { }
            }
        }

        InstrumentDailyStats toStats(String market, String currency) {
            return new InstrumentDailyStats(market, symbol, securityId, currency, day,
                    open, high, low, last, close, previousClose, volume, turnover, auctionPrice,
                    lowLimit, highLimit, referencePrice, updates, 0L, Instant.now());
        }
    }
}
