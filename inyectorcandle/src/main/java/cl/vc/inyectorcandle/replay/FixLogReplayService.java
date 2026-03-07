package cl.vc.inyectorcandle.replay;

import cl.vc.inyectorcandle.actor.MarketActorSystem;
import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.TradeEvent;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class FixLogReplayService {
    private static final Logger LOG = LoggerFactory.getLogger(FixLogReplayService.class);
    private static final long PROGRESS_LOG_EVERY_LINES = 1_000L;

    private static final Pattern LINE_PATTERN = Pattern.compile("^(\\d{8} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}):\\s+(.*)$");
    private static final DateTimeFormatter LINE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");
    private static final char SOH = '\u0001';

    private static final int TAG_MSGTYPE = 35;
    private static final int TAG_MDREQID = 262;
    private static final int TAG_NOMDENTRIES = 268;
    private static final int TAG_MDENTRYTYPE = 269;
    private static final int TAG_MDENTRYPX = 270;
    private static final int TAG_MDENTRYSIZE = 271;
    private static final int TAG_MDENTRYID = 278;
    private static final int TAG_MDUPDATEACTION = 279;
    private static final int TAG_SYMBOL = 55;
    private static final int TAG_SECURITY_TYPE = 167;
    private static final int TAG_CURRENCY = 15;
    private static final int TAG_SECURITY_ID = 48;
    private static final int TAG_DESTINATION = 207;
    private static final int TAG_SIDE = 54;
    private static final int TAG_SETTL_TYPE = 63;
    private static final int TAG_SETTL_BCS_ALT = 446;
    private static final int TAG_SETTL_BCS = 466;
    private static final int TAG_SETTL_ALT = 876;
    private static final int TAG_AMOUNT = 10124;

    private final MarketActorSystem actorSystem;
    private final ZoneId logZone;
    private final long sleepMs;
    private final long maxLines;
    private final boolean preserveTiming;
    private final double timingSpeed;
    private final long timingMaxSleepMs;
    private final boolean purgeDayBeforeInject;

    private final Map<String, InstrumentKey> mdReqToKey = new ConcurrentHashMap<>();
    private final Set<LocalDate> purgedDays = ConcurrentHashMap.newKeySet();

    private long processedLines;
    private long parsedTrades;
    private long rawTradeMarkers;
    private Instant previousLineTime;
    private long replayStartEpochMs;

    public FixLogReplayService(MarketActorSystem actorSystem,
                               String zoneId,
                               long sleepMs,
                               long maxLines,
                               boolean preserveTiming,
                               double timingSpeed,
                               long timingMaxSleepMs,
                               boolean purgeDayBeforeInject) {
        this.actorSystem = actorSystem;
        this.logZone = ZoneId.of(zoneId);
        this.sleepMs = Math.max(0, sleepMs);
        this.maxLines = Math.max(0, maxLines);
        this.preserveTiming = preserveTiming;
        this.timingSpeed = timingSpeed <= 0 ? 1.0d : timingSpeed;
        this.timingMaxSleepMs = Math.max(0, timingMaxSleepMs);
        this.purgeDayBeforeInject = purgeDayBeforeInject;
    }

    public void replay(String inputPath) {
        if (inputPath == null || inputPath.isBlank()) {
            throw new IllegalArgumentException("replay.input.path no puede estar vacio cuando replay.enabled=true");
        }

        List<Path> files = resolveFiles(Paths.get(inputPath.trim()));
        if (files.isEmpty()) {
            LOG.warn("Replay sin archivos: {}", inputPath);
            return;
        }

        replayStartEpochMs = System.currentTimeMillis();
        LOG.info("Replay FIX iniciado. archivos={}, zona={}, purgeDayBeforeInject={}, preserveTiming={}, speed={}, sleepMs={}, maxSleepMs={}, maxLines={}",
                files.size(), logZone, purgeDayBeforeInject, preserveTiming, timingSpeed, sleepMs, timingMaxSleepMs, maxLines);

        for (Path file : files) {
            if (maxLines > 0 && processedLines >= maxLines) {
                break;
            }
            replayFile(file);
        }

        long elapsedMs = Math.max(1L, System.currentTimeMillis() - replayStartEpochMs);
        long linesPerSec = (processedLines * 1000L) / elapsedMs;
        LOG.info("Replay FIX finalizado. lineasProcesadas={}, tradesParseados={}, marcadores269_trade(2|B)={}, instrumentosIndexados={}, duracionMs={}, lineasPorSegundo={}",
                processedLines, parsedTrades, rawTradeMarkers, mdReqToKey.size(), elapsedMs, linesPerSec);
        LOG.info("REPLAY_COMPLETADO inputPath={} lineasProcesadas={} tradesParseados={}",
                inputPath, processedLines, parsedTrades);
    }

    private List<Path> resolveFiles(Path input) {
        try {
            if (Files.isRegularFile(input)) {
                return List.of(input);
            }

            if (Files.isDirectory(input)) {
                List<Path> files;
                try (Stream<Path> stream = Files.list(input)) {
                    files = stream
                            .filter(Files::isRegularFile)
                            .filter(path -> {
                                String name = path.getFileName().toString().toLowerCase();
                                return name.endsWith(".log") || name.endsWith(".log.gz") || name.endsWith(".gz");
                            })
                            .sorted()
                            .toList();
                }
                return files;
            }
        } catch (IOException e) {
            throw new RuntimeException("No se pudo resolver replay.input.path " + input, e);
        }

        return Collections.emptyList();
    }

    private void replayFile(Path file) {
        long fileStartLines = processedLines;
        long fileStartTrades = parsedTrades;
        long fileStartMs = System.currentTimeMillis();
        LOG.info("Replay archivo iniciado: {}", file);

        try (InputStream in = openInput(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (maxLines > 0 && processedLines >= maxLines) {
                    break;
                }

                processedLines++;
                processLine(line);
                logProgressCheckpoint();

                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Replay interrumpido en archivo {}", file);
        } catch (Exception e) {
            LOG.error("Error procesando archivo replay {}", file, e);
        } finally {
            long fileElapsedMs = Math.max(1L, System.currentTimeMillis() - fileStartMs);
            long fileLines = processedLines - fileStartLines;
            long fileTrades = parsedTrades - fileStartTrades;
            long fileLinesPerSec = (fileLines * 1000L) / fileElapsedMs;
            LOG.info("Replay archivo finalizado: {} | lineas={} trades={} duracionMs={} lineasPorSegundo={}",
                    file, fileLines, fileTrades, fileElapsedMs, fileLinesPerSec);
        }
    }

    private void logProgressCheckpoint() {
        if (processedLines <= 0 || processedLines % PROGRESS_LOG_EVERY_LINES != 0) {
            return;
        }
        long elapsedMs = Math.max(1L, System.currentTimeMillis() - replayStartEpochMs);
        long linesPerSec = (processedLines * 1000L) / elapsedMs;
        LOG.info("Replay progreso: lineasProcesadas={} tradesParseados={} marcadores269_trade(2|B)={} instrumentosIndexados={} lineasPorSegundo={}",
                processedLines, parsedTrades, rawTradeMarkers, mdReqToKey.size(), linesPerSec);
    }

    private InputStream openInput(Path file) throws IOException {
        InputStream fileIn = Files.newInputStream(file);
        String name = file.getFileName().toString().toLowerCase();
        if (name.endsWith(".gz")) {
            return new GZIPInputStream(fileIn);
        }
        return fileIn;
    }

    private void processLine(String line) throws InterruptedException {
        Matcher matcher = LINE_PATTERN.matcher(line);
        if (!matcher.matches()) {
            return;
        }

        Instant lineTimestamp;
        try {
            lineTimestamp = LocalDateTime.parse(matcher.group(1), LINE_TIME_FORMAT).atZone(logZone).toInstant();
        } catch (Exception ex) {
            return;
        }

        if (purgeDayBeforeInject) {
            LocalDate day = lineTimestamp.atZone(logZone).toLocalDate();
            if (purgedDays.add(day)) {
                LOG.info("Replay purge dia {} antes de reinyectar", day);
                actorSystem.purgeDay(day, logZone);
            }
        }

        maybeSleepForTiming(lineTimestamp);

        String fixBody = matcher.group(2);
        List<TagValue> tags = parseTags(fixBody);
        if (tags.isEmpty()) {
            return;
        }

        String msgType = valueOf(tags, TAG_MSGTYPE);
        if ("V".equals(msgType)) {
            indexSubscription(tags);
            return;
        }

        if (!"X".equals(msgType) && !"W".equals(msgType)) {
            return;
        }

        if (containsTradeMarker(fixBody)) {
            rawTradeMarkers++;
            if (rawTradeMarkers == 1) {
                LOG.info("Replay primer marcador 269 trade (2|B) detectado en linea {}", processedLines);
            }
        }

        Integer noMdEntries = intOrNull(valueOf(tags, TAG_NOMDENTRIES));
        if (noMdEntries == null || noMdEntries <= 0) {
            return;
        }

        int startIndex = indexOf(tags, TAG_NOMDENTRIES);
        if (startIndex < 0 || startIndex + 1 >= tags.size()) {
            return;
        }

        Map<Integer, String> messageLevel = new LinkedHashMap<>();
        for (int i = 0; i < startIndex; i++) {
            messageLevel.put(tags.get(i).tag, tags.get(i).value);
        }

        List<Map<Integer, String>> entries = parseEntries(tags.subList(startIndex + 1, tags.size()));
        String msgMdReqId = valueOf(tags, TAG_MDREQID);

        for (Map<Integer, String> entry : entries) {
            String entryType = fallback(entry.get(TAG_MDENTRYTYPE), messageLevel.get(TAG_MDENTRYTYPE));
            if (!isTradeEntry(entryType)) {
                continue;
            }

            String mdReqId = fallback(entry.get(TAG_MDREQID), msgMdReqId);
            InstrumentKey indexedKey = mdReqId == null ? null : mdReqToKey.get(mdReqId);

            String symbol = fallback(entry.get(TAG_SYMBOL), messageLevel.get(TAG_SYMBOL));
            String destination = fallback(entry.get(TAG_DESTINATION), messageLevel.get(TAG_DESTINATION));
            String securityType = fallback(entry.get(TAG_SECURITY_TYPE), messageLevel.get(TAG_SECURITY_TYPE));
            String currency = resolveCurrency(
                    fallback(entry.get(TAG_CURRENCY), messageLevel.get(TAG_CURRENCY)),
                    fallback(entry.get(TAG_SECURITY_ID), messageLevel.get(TAG_SECURITY_ID))
            );
            String settlement = resolveSettlement(
                    fallback(entry.get(TAG_SETTL_BCS_ALT), messageLevel.get(TAG_SETTL_BCS_ALT)),
                    fallback(entry.get(TAG_SETTL_BCS), messageLevel.get(TAG_SETTL_BCS)),
                    fallback(entry.get(TAG_SETTL_ALT), messageLevel.get(TAG_SETTL_ALT)),
                    fallback(entry.get(TAG_SETTL_TYPE), messageLevel.get(TAG_SETTL_TYPE))
            );

            InstrumentKey key;
            if (indexedKey != null) {
                key = InstrumentKey.fromValues(
                        isBlank(symbol) ? indexedKey.symbol() : symbol,
                        preferIndexed(indexedKey.settlement(), settlement),
                        isBlank(destination) ? indexedKey.destination() : destination,
                        preferIndexed(indexedKey.currency(), currency),
                        preferIndexed(indexedKey.securityType(), securityType)
                );
            } else {
                key = InstrumentKey.fromValues(symbol, settlement, destination, currency, securityType);
            }

            BigDecimal price = decimalOrNull(fallback(entry.get(TAG_MDENTRYPX), messageLevel.get(TAG_MDENTRYPX)));
            BigDecimal quantity = decimalOrNull(fallback(entry.get(TAG_MDENTRYSIZE), messageLevel.get(TAG_MDENTRYSIZE)));
            BigDecimal amount = decimalOrNull(fallback(entry.get(TAG_AMOUNT), messageLevel.get(TAG_AMOUNT)));

            if (price == null) {
                continue;
            }

            TradeEvent trade = new TradeEvent(
                    key,
                    lineTimestamp,
                    price,
                    quantity,
                    amount,
                    fallback(entry.get(TAG_SIDE), messageLevel.get(TAG_SIDE)),
                    fallback(entry.get(TAG_MDENTRYID), messageLevel.get(TAG_MDENTRYID)),
                    mdReqId,
                    msgType
            );

            actorSystem.onTrade(trade);
            parsedTrades++;
            if (parsedTrades == 1) {
                LOG.info("Replay primer trade parseado en linea {} key={} price={} qty={} amount={}",
                        processedLines, key.id(), price, quantity, amount);
            }
        }
    }

    private void maybeSleepForTiming(Instant current) throws InterruptedException {
        if (!preserveTiming) {
            previousLineTime = current;
            return;
        }

        if (previousLineTime == null) {
            previousLineTime = current;
            return;
        }

        long deltaMs = Duration.between(previousLineTime, current).toMillis();
        previousLineTime = current;

        if (deltaMs <= 0) {
            return;
        }

        long adjusted = (long) (deltaMs / timingSpeed);
        if (timingMaxSleepMs > 0) {
            adjusted = Math.min(adjusted, timingMaxSleepMs);
        }

        if (adjusted > 0) {
            Thread.sleep(adjusted);
        }
    }

    private void indexSubscription(List<TagValue> tags) {
        String mdReqId = valueOf(tags, TAG_MDREQID);
        if (isBlank(mdReqId)) {
            return;
        }

        String symbol = valueOf(tags, TAG_SYMBOL);
        String destination = valueOf(tags, TAG_DESTINATION);
        String securityType = valueOf(tags, TAG_SECURITY_TYPE);
        String currency = resolveCurrency(valueOf(tags, TAG_CURRENCY), valueOf(tags, TAG_SECURITY_ID));
        String settlement = resolveSettlement(
                valueOf(tags, TAG_SETTL_BCS_ALT),
                valueOf(tags, TAG_SETTL_BCS),
                valueOf(tags, TAG_SETTL_ALT),
                valueOf(tags, TAG_SETTL_TYPE)
        );

        InstrumentKey key = InstrumentKey.fromValues(symbol, settlement, destination, currency, securityType);
        mdReqToKey.put(mdReqId, key);
    }

    private String preferIndexed(String indexedValue, String candidateValue) {
        if (!isBlank(indexedValue) && !indexedValue.startsWith("UNKNOWN_")) {
            return indexedValue;
        }
        return candidateValue;
    }

    private String resolveSettlement(String from446, String from466, String from876, String from63) {
        String settlement = normalizeSettlement(from446);
        if (isBlank(settlement)) {
            settlement = normalizeSettlement(from466);
        }
        if (isBlank(settlement)) {
            settlement = normalizeSettlement(from876);
        }
        if (isBlank(settlement)) {
            settlement = normalizeSettlement(from63);
        }
        return settlement;
    }

    private String resolveCurrency(String from15, String from48) {
        String from48Currency = normalizeCurrencyFrom48(from48);
        if (!isBlank(from48Currency)) {
            return from48Currency;
        }
        return from15;
    }

    private String normalizeCurrencyFrom48(String raw48) {
        if (isBlank(raw48)) {
            return null;
        }
        String candidate = raw48.trim().toUpperCase();
        String[] allowed = {"USD", "CLP", "SOL", "ARS", "COP"};
        for (String ccy : allowed) {
            if (candidate.equals(ccy) || candidate.startsWith(ccy)) {
                return ccy;
            }
            if (candidate.contains("|" + ccy + "|")
                    || candidate.contains("-" + ccy + "-")
                    || candidate.endsWith("-" + ccy)
                    || candidate.endsWith("_" + ccy)) {
                return ccy;
            }
        }
        return null;
    }

    private String normalizeSettlement(String raw) {
        if (isBlank(raw)) {
            return raw;
        }

        String value = raw.trim().toUpperCase();
        if ("|||".equals(value)) {
            return "T2";
        }
        if ("PH|||".equals(value)) {
            return "CASH";
        }
        if ("PM|||".equals(value)) {
            return "NEXT_DAY";
        }
        if ("T+3|||".equals(value)) {
            return "T3";
        }
        if ("T+5|||".equals(value)) {
            return "T5";
        }
        if ("0".equals(value)) {
            return "REGULAR";
        }
        if ("1".equals(value)) {
            return "CASH";
        }
        if ("2".equals(value)) {
            return "NEXT_DAY";
        }
        if ("3".equals(value)) {
            return "T2";
        }
        if ("4".equals(value)) {
            return "T3";
        }
        if ("5".equals(value)) {
            return "T4";
        }
        if ("6".equals(value)) {
            return "FUTURE";
        }
        if ("7".equals(value)) {
            return "WHEN_ISSUED";
        }
        if ("8".equals(value)) {
            return "SELLERS_OPTION";
        }
        if ("9".equals(value)) {
            return "T5";
        }
        if ("B".equals(value)) {
            return "BROKEN_DATE";
        }
        return value;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private List<Map<Integer, String>> parseEntries(List<TagValue> tail) {
        List<Map<Integer, String>> out = new ArrayList<>();
        Map<Integer, String> current = null;

        for (TagValue tagValue : tail) {
            int tag = tagValue.tag;

            boolean newEntryBy269 = tag == TAG_MDENTRYTYPE && current != null && current.containsKey(TAG_MDENTRYTYPE);
            boolean newEntryBy279 = tag == TAG_MDUPDATEACTION && current != null
                    && (current.containsKey(TAG_MDUPDATEACTION) || current.containsKey(TAG_MDENTRYTYPE));

            if (newEntryBy269 || newEntryBy279) {
                out.add(current);
                current = new LinkedHashMap<>();
            }

            if (current == null) {
                current = new LinkedHashMap<>();
            }

            current.put(tag, tagValue.value);
        }

        if (current != null && !current.isEmpty()) {
            out.add(current);
        }

        return out;
    }

    private List<TagValue> parseTags(String fixBody) {
        List<TagValue> out = new ArrayList<>();
        if (fixBody == null || fixBody.isBlank()) {
            return out;
        }

        int i = 0;
        int n = fixBody.length();
        while (i < n) {
            while (i < n && (fixBody.charAt(i) == SOH || fixBody.charAt(i) == '|' || Character.isWhitespace(fixBody.charAt(i)))) {
                i++;
            }
            if (i >= n) {
                break;
            }

            int tagStart = i;
            while (i < n && Character.isDigit(fixBody.charAt(i))) {
                i++;
            }
            if (i >= n || fixBody.charAt(i) != '=' || tagStart == i) {
                i++;
                continue;
            }

            Integer tag = intOrNull(fixBody.substring(tagStart, i));
            i++; // '='
            int valueStart = i;

            while (i < n) {
                char c = fixBody.charAt(i);
                if (c == SOH) {
                    break;
                }
                if (c == '|' && looksLikeNextTag(fixBody, i + 1)) {
                    break;
                }
                i++;
            }

            if (tag != null) {
                out.add(new TagValue(tag, fixBody.substring(valueStart, i)));
            }

            if (i < n && (fixBody.charAt(i) == SOH || fixBody.charAt(i) == '|')) {
                i++;
            }
        }
        return out;
    }

    private boolean looksLikeNextTag(String text, int index) {
        int n = text.length();
        int i = index;
        int digits = 0;
        while (i < n && Character.isDigit(text.charAt(i))) {
            i++;
            digits++;
        }
        return digits > 0 && i < n && text.charAt(i) == '=';
    }

    private int indexOf(List<TagValue> tags, int field) {
        for (int i = 0; i < tags.size(); i++) {
            if (tags.get(i).tag == field) {
                return i;
            }
        }
        return -1;
    }

    private String valueOf(List<TagValue> tags, int field) {
        for (TagValue tagValue : tags) {
            if (tagValue.tag == field) {
                return tagValue.value;
            }
        }
        return null;
    }

    private boolean isTradeEntry(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        return "2".equals(value) || "B".equalsIgnoreCase(value);
    }

    private boolean containsTradeMarker(String fixBody) {
        return fixBody.contains("269=2") || fixBody.contains("269=B");
    }

    private String fallback(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        return second;
    }

    private BigDecimal decimalOrNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(value.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private Integer intOrNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static final class TagValue {
        private final int tag;
        private final String value;

        private TagValue(int tag, String value) {
            this.tag = tag;
            this.value = value;
        }
    }
}
