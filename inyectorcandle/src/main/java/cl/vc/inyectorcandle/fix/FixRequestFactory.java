package cl.vc.inyectorcandle.fix;

import quickfix.field.*;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.SecurityListRequest;

import java.util.UUID;

public final class FixRequestFactory {
    private static final String SETTLEMENT_T2 = "T2";
    private static final String SETTLEMENT_CASH = "CASH";
    private static final String SETTLEMENT_NEXT_DAY = "NEXT_DAY";

    private FixRequestFactory() {
    }

    public static SecurityListRequest securityListRequest(String requestType, String scope) {
        SecurityListRequest req = new SecurityListRequest();
        req.set(new SecurityReqID("SEC-" + UUID.randomUUID()));
        req.set(new SecurityListRequestType(Integer.parseInt(requestType)));
        if (scope != null && !scope.isBlank()) {
            req.setString(58, "scope=" + scope);
        }
        req.setField(new OptAttribute('A'));
        return req;
    }

    public static MarketDataRequest subscribeAllDepth(String symbol, String bookingRefId, String settlement, String destination, String currency) {
        MarketDataRequest request = new MarketDataRequest();
        request.set(new MDReqID("MD-" + UUID.randomUUID()));
        request.set(new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_UPDATES));
        request.set(new MarketDepth(0));
        request.set(new MDUpdateType(MDUpdateType.INCREMENTAL_REFRESH));
        request.set(new AggregatedBook(false));

        MarketDataRequest.NoRelatedSym group = new MarketDataRequest.NoRelatedSym();
        group.set(new Symbol(symbol));
        if (bookingRefId != null) {
            group.setField(new BookingRefID(bookingRefId));
        }
        if (settlement != null) {
            group.setField(new SettlType(settlement));
        }
        if (destination != null) {
            group.setField(new SecurityExchange(destination));
        }
        request.addGroup(group);

        addEntryType(request, MDEntryType.BID);
        addEntryType(request, MDEntryType.OFFER);
        addEntryType(request, MDEntryType.TRADE);
        addEntryType(request, MDEntryType.TRADE_VOLUME);
        addEntryType(request, MDEntryType.OPENING_PRICE);
        addEntryType(request, MDEntryType.CLOSING_PRICE);
        addEntryType(request, MDEntryType.TRADING_SESSION_HIGH_PRICE);
        addEntryType(request, MDEntryType.TRADING_SESSION_LOW_PRICE);
        addEntryType(request, MDEntryType.IMBALANCE);
        addEntryType(request, MDEntryType.TRADING_SESSION_VWAP_PRICE);
        addEntryType(request, MDEntryType.SETTLEMENT_PRICE);

        return request;
    }

    public static MarketDataRequest subscribeTrades(String symbol, String settlement, String destination, String currency) {
        MarketDataRequest request = new MarketDataRequest();
        request.set(new MDReqID("MD-" + UUID.randomUUID()));
        request.set(new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_UPDATES));
        request.set(new MarketDepth(0));
        request.set(new AggregatedBook(false));

        MarketDataRequest.NoRelatedSym group = new MarketDataRequest.NoRelatedSym();
        group.set(new Symbol(symbol));

        String bookingRefId = convertBookingRefId(settlement);
        if (bookingRefId != null) {
            group.setField(new BookingRefID(bookingRefId));
        }
        if (destination != null) {
            group.setField(new SecurityExchange(destination));
        }
        request.addGroup(group);

        addEntryType(request, MDEntryType.TRADE);
        return request;
    }

    public static String convertBookingRefId(String settlement) {
        if (settlement == null || settlement.isBlank()) {
            return null;
        }
        return switch (settlement.trim().toUpperCase()) {
            case SETTLEMENT_T2, "REGULAR", "3", "|||" -> "|||";
            case SETTLEMENT_CASH, "1", "PH|||" -> "PH|||";
            case SETTLEMENT_NEXT_DAY, "2", "PM|||" -> "PM|||";
            case "T3", "4", "T+3|||" -> "T+3|||";
            case "T5", "9", "T+5|||" -> "T+5|||";
            default -> settlement.trim().toUpperCase();
        };
    }

    public static String convertFixSettlType(String settlement) {
        if (settlement == null || settlement.isBlank()) {
            return null;
        }
        return switch (settlement.trim().toUpperCase()) {
            case SETTLEMENT_T2, "REGULAR", "3", "|||" -> "3";
            case SETTLEMENT_CASH, "1", "PH|||" -> "1";
            case SETTLEMENT_NEXT_DAY, "2", "PM|||" -> "2";
            case "T3", "4", "T+3|||" -> "4";
            case "T5", "9", "T+5|||" -> "9";
            default -> settlement.trim().toUpperCase();
        };
    }

    private static void addEntryType(MarketDataRequest request, char type) {
        MarketDataRequest.NoMDEntryTypes entryType = new MarketDataRequest.NoMDEntryTypes();
        entryType.set(new MDEntryType(type));
        request.addGroup(entryType);
    }
}
