package cl.vc.inyectorcandle.fix;

import quickfix.field.*;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.SecurityListRequest;

import java.util.UUID;

public final class FixRequestFactory {

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
        if (currency != null) {
            group.setField(new Currency(currency));
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

    private static void addEntryType(MarketDataRequest request, char type) {
        MarketDataRequest.NoMDEntryTypes entryType = new MarketDataRequest.NoMDEntryTypes();
        entryType.set(new MDEntryType(type));
        request.addGroup(entryType);
    }
}
