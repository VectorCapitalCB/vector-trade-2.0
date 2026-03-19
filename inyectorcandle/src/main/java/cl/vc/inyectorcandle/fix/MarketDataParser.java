package cl.vc.inyectorcandle.fix;

import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.MarketDataEvent;
import cl.vc.inyectorcandle.model.TradeEvent;
import quickfix.FieldMap;
import quickfix.field.*;
import quickfix.fix44.MarketDataIncrementalRefresh;
import quickfix.fix44.MarketDataSnapshotFullRefresh;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class MarketDataParser {

    public record ParsedBatch(List<MarketDataEvent> events, List<TradeEvent> trades) {
    }

    private MarketDataParser() {
    }

    public static ParsedBatch parseSnapshot(MarketDataSnapshotFullRefresh msg, InstrumentKey key) throws Exception {
        List<MarketDataEvent> events = new ArrayList<>();
        List<TradeEvent> trades = new ArrayList<>();
        String mdReqId = msg.isSetField(MDReqID.FIELD) ? msg.getString(MDReqID.FIELD) : null;

        int count = msg.getInt(NoMDEntries.FIELD);
        for (int i = 1; i <= count; i++) {
            MarketDataSnapshotFullRefresh.NoMDEntries group = new MarketDataSnapshotFullRefresh.NoMDEntries();
            msg.getGroup(i, group);
            ParsedEntry entry = parseEntry(group, key, mdReqId, "W");
            events.add(entry.event());
            if (entry.trade() != null) {
                trades.add(entry.trade());
            }
        }

        return new ParsedBatch(events, trades);
    }

    public static ParsedBatch parseIncremental(MarketDataIncrementalRefresh msg, Map<String, InstrumentKey> mdReqIndex) throws Exception {
        List<MarketDataEvent> events = new ArrayList<>();
        List<TradeEvent> trades = new ArrayList<>();
        String msgSymbol = read(msg, Symbol.FIELD);
        String msgSettlement = normalizeSettlement(firstPresent(
                read(msg, 446),
                read(msg, 466),
                read(msg, 876),
                read(msg, SettlType.FIELD)
        ));
        String msgDestination = read(msg, SecurityExchange.FIELD);
        String msgCurrency = resolveCurrency(read(msg, Currency.FIELD), read(msg, SecurityID.FIELD));
        String msgSecurityType = read(msg, SecurityType.FIELD);

        int count = msg.getInt(NoMDEntries.FIELD);
        for (int i = 1; i <= count; i++) {
            MarketDataIncrementalRefresh.NoMDEntries group = new MarketDataIncrementalRefresh.NoMDEntries();
            msg.getGroup(i, group);

            String mdReqId = read(group, MDReqID.FIELD);
            InstrumentKey indexedKey = mdReqId == null ? null : mdReqIndex.get(mdReqId);
            String symbol = firstPresent(read(group, Symbol.FIELD), msgSymbol, known(indexedKey, Component.SYMBOL));
            String settlement = preferKnown(
                    known(indexedKey, Component.SETTLEMENT),
                    normalizeSettlement(firstPresent(
                            read(group, 446),
                            read(group, 466),
                            read(group, 876),
                            read(group, SettlType.FIELD),
                            msgSettlement
                    ))
            );
            String destination = firstPresent(read(group, SecurityExchange.FIELD), msgDestination, known(indexedKey, Component.DESTINATION));
            String currency = firstPresent(
                    resolveCurrency(read(group, Currency.FIELD), read(group, SecurityID.FIELD)),
                    msgCurrency,
                    known(indexedKey, Component.CURRENCY)
            );
            String securityType = firstPresent(read(group, SecurityType.FIELD), msgSecurityType, known(indexedKey, Component.SECURITY_TYPE));
            InstrumentKey key = InstrumentKey.fromValues(symbol, settlement, destination, currency, securityType);

            ParsedEntry entry = parseEntry(group, key, mdReqId, "X");
            events.add(entry.event());
            if (entry.trade() != null) {
                trades.add(entry.trade());
            }
        }

        return new ParsedBatch(events, trades);
    }

    private static ParsedEntry parseEntry(FieldMap group, InstrumentKey key, String mdReqId, String sourceMsgType) {
        char entryType = readChar(group, MDEntryType.FIELD, '?');
        BigDecimal px = readDecimal(group, MDEntryPx.FIELD);
        BigDecimal size = readDecimal(group, MDEntrySize.FIELD);
        String mdEntryId = read(group, MDEntryID.FIELD);
        char mdUpdateAction = readChar(group, MDUpdateAction.FIELD, '0');
        Instant eventTime = readTimestamp(group);

        MarketDataEvent event = new MarketDataEvent(
                key,
                eventTime,
                entryType,
                px,
                size,
                mdEntryId,
                mdReqId,
                sourceMsgType
        );

        TradeEvent trade = null;
        if (entryType == MDEntryType.TRADE) {
            BigDecimal qty = size;
            BigDecimal amount = readDecimal(group, MDEntryForwardPoints.FIELD);
            trade = new TradeEvent(
                    key,
                    eventTime,
                    px,
                    qty,
                    amount,
                    read(group, Side.FIELD),
                    mdEntryId,
                    mdUpdateAction,
                    mdReqId,
                    sourceMsgType
            );
        }

        return new ParsedEntry(event, trade);
    }

    private static Instant readTimestamp(FieldMap fieldMap) {
        if (fieldMap.isSetField(MDEntryDate.FIELD) && fieldMap.isSetField(MDEntryTime.FIELD)) {
            LocalDate date = LocalDate.parse(read(fieldMap, MDEntryDate.FIELD));
            LocalTime time = LocalTime.parse(read(fieldMap, MDEntryTime.FIELD));
            return date.atTime(time).toInstant(ZoneOffset.UTC);
        }
        return Instant.now();
    }

    private static String read(FieldMap fieldMap, int field) {
        try {
            return fieldMap.isSetField(field) ? fieldMap.getString(field) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static char readChar(FieldMap fieldMap, int field, char fallback) {
        try {
            if (!fieldMap.isSetField(field)) {
                return fallback;
            }
            return fieldMap.getChar(field);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static BigDecimal readDecimal(FieldMap fieldMap, int field) {
        try {
            if (!fieldMap.isSetField(field)) {
                return null;
            }
            return BigDecimal.valueOf(fieldMap.getDouble(field));
        } catch (Exception e) {
            return null;
        }
    }

    private static String firstPresent(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String normalizeSettlement(String raw) {
        if (raw == null || raw.isBlank()) {
            return raw;
        }
        return switch (raw.trim().toUpperCase()) {
            case "|||" -> "T2";
            case "PH|||" -> "CASH";
            case "PM|||" -> "NEXT_DAY";
            case "T+3|||" -> "T3";
            case "T+5|||" -> "T5";
            case "0" -> "REGULAR";
            case "1" -> "CASH";
            case "2" -> "NEXT_DAY";
            case "3" -> "T2";
            case "4" -> "T3";
            case "5" -> "T4";
            case "6" -> "FUTURE";
            case "7" -> "WHEN_ISSUED";
            case "8" -> "SELLERS_OPTION";
            case "9" -> "T5";
            case "B" -> "BROKEN_DATE";
            default -> raw.trim().toUpperCase();
        };
    }

    private static String resolveCurrency(String from15, String from48) {
        String from48Currency = normalizeCurrencyFrom48(from48);
        if (from48Currency != null) {
            return from48Currency;
        }
        return from15;
    }

    private static String normalizeCurrencyFrom48(String raw48) {
        if (raw48 == null || raw48.isBlank()) {
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

    private static String preferKnown(String preferred, String fallback) {
        if (preferred != null && !preferred.isBlank()) {
            return preferred;
        }
        return fallback;
    }

    private static String known(InstrumentKey key, Component component) {
        if (key == null) {
            return null;
        }
        String value = switch (component) {
            case SYMBOL -> key.symbol();
            case SETTLEMENT -> key.settlement();
            case DESTINATION -> key.destination();
            case CURRENCY -> key.currency();
            case SECURITY_TYPE -> key.securityType();
        };
        if (value == null || value.isBlank() || value.startsWith("UNKNOWN_")) {
            return null;
        }
        return value;
    }

    private enum Component {
        SYMBOL,
        SETTLEMENT,
        DESTINATION,
        CURRENCY,
        SECURITY_TYPE
    }

    private record ParsedEntry(MarketDataEvent event, TradeEvent trade) {
    }
}
