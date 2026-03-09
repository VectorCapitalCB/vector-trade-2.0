package cl.vc.inyectorcandle.utils;


import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.extern.slf4j.Slf4j;
import quickfix.field.*;
import quickfix.fix44.MarketDataRequest;

@Slf4j
public class Helper {

    public static final String LOGON_FIX = "/LOGON_FIX/";

    public static String convertSettlType(RoutingMessage.SettlType settlType) {
        return switch (settlType) {
            case REGULAR, T2 -> "|||";
            case CASH -> "PH|||";
            case NEXT_DAY -> "PM|||";
            case T3 -> "T+3|||";
            case T5 -> "T+5|||";
            default -> settlType.name();
        };
    }

    public static String convertFixSettlType(RoutingMessage.SettlType settlType) {
        return switch (settlType) {
            case REGULAR, T2 -> "3";
            case CASH -> "1";
            case NEXT_DAY -> "2";
            case T3 -> "4";
            case T5 -> "9";
            default -> settlType.name();
        };
    }

    public static MarketDataRequest createMarketDataRequest(String symbol, RoutingMessage.SettlType settlType, boolean subscribe) {

        try {

            MarketDataRequest request = new MarketDataRequest();
            request.set(new MDReqID(IDGenerator.getID()));
            request.set(new MarketDepth(0));
            request.set(new AggregatedBook(false));

            if (subscribe) {
                request.set(new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_UPDATES));
            } else {
                request.set(new SubscriptionRequestType(SubscriptionRequestType.DISABLE_PREVIOUS_SNAPSHOT_UPDATE_REQUEST));
            }

            MarketDataRequest.NoRelatedSym noRelatedSym = new MarketDataRequest.NoRelatedSym();
            noRelatedSym.set(new Symbol(symbol));

            noRelatedSym.setField(new BookingRefID(convertSettlType(settlType)));
            noRelatedSym.setField(new SettlType(convertFixSettlType(settlType)));
            request.addGroup(noRelatedSym);


            MarketDataRequest.NoMDEntryTypes noMDEntryTypesImbalance = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesImbalance.set(new MDEntryType(MDEntryType.IMBALANCE));
            request.addGroup(noMDEntryTypesImbalance);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesVWAP = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesVWAP.set(new MDEntryType(MDEntryType.TRADING_SESSION_VWAP_PRICE));
            request.addGroup(noMDEntryTypesVWAP);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesCompise = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesCompise.set(new MDEntryType(MDEntryType.COMPOSITE_UNDERLYING_PRICE));
            request.addGroup(noMDEntryTypesCompise);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesMargin = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesMargin.set(new MDEntryType(MDEntryType.MARGIN_RATE));
            request.addGroup(noMDEntryTypesMargin);


            MarketDataRequest.NoMDEntryTypes noMDEntryTypesS = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesS.set(new MDEntryType('S'));
            request.addGroup(noMDEntryTypesS);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesVlolume = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesVlolume.set(new MDEntryType(MDEntryType.TRADE_VOLUME));
            request.addGroup(noMDEntryTypesVlolume);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesVHIgh = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesVHIgh.set(new MDEntryType(MDEntryType.TRADING_SESSION_HIGH_PRICE));
            request.addGroup(noMDEntryTypesVHIgh);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesLow = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesLow.set(new MDEntryType(MDEntryType.TRADING_SESSION_LOW_PRICE));
            request.addGroup(noMDEntryTypesLow);


            // Request bid and ask
            MarketDataRequest.NoMDEntryTypes noMDEntryTypesBid = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesBid.set(new MDEntryType(MDEntryType.BID));
            request.addGroup(noMDEntryTypesBid);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesOffer = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesOffer.set(new MDEntryType(MDEntryType.OFFER));
            request.addGroup(noMDEntryTypesOffer);

            // Request trades
            MarketDataRequest.NoMDEntryTypes noMDEntryTypesTrade = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesTrade.set(new MDEntryType(MDEntryType.TRADE));
            request.addGroup(noMDEntryTypesTrade);

            // Request OHLCV
            MarketDataRequest.NoMDEntryTypes noMDEntryTypesClose = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesClose.set(new MDEntryType(MDEntryType.CLOSING_PRICE));
            request.addGroup(noMDEntryTypesClose);

            MarketDataRequest.NoMDEntryTypes noMDEntryTypesOpen = new MarketDataRequest.NoMDEntryTypes();
            noMDEntryTypesOpen.set(new MDEntryType(MDEntryType.OPENING_PRICE));
            request.addGroup(noMDEntryTypesOpen);


            return request;

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            return null;
        }
    }


}
