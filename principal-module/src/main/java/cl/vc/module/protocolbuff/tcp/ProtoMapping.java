
package cl.vc.module.protocolbuff.tcp;

import cl.vc.algos.adr.proto.PairsStrategyProtos;
import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generalstrategy.GeneralStrategy;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;

public class ProtoMapping {

    enum MessageCodes {
        // Routing Requests
        NEW_ORDER_REQUEST,
        ORDER_REPLACE_REQUEST,
        ORDER_CANCEL_REQUEST,
        ORDERS_CANCEL_REQUEST_LIST,
        ORDERS_CANCEL_UNSOLICITED,

        // Routing Responses
        ORDER,
        REPLACE_CANCEL_EXECUTION,
        TRADE_EXECUTION,
        ORDER_CANCEL_REJECT,

        // Session Messages
        PONG,
        PING,
        DISCONNECT,
        CONNECT,

        // Notifications
        NOTIFICATION,
        NOTIFICATION_REQUEST,
        NOTIFICATION_RESPONSE,

        // Market Data Requests
        SUBSCRIBE,
        UNSUBSCRIBE,
        SECURITY_LIST_REQUEST,

        // Market Data Responses
        NEWS,
        SNAPSHOT,

        SNAPSHOT_NEWS,
        INCREMENTAL,
        TRADE,
        SECURITY_LIST,
        REJECTED,
        STATISTIC,
        OHLCV,

        //blotter
        PORTFOLIO_REQUES,
        PORTFOLIO_RESPONSE,
        PRESELECT_RESPONSE,
        PRESELECT_REQUEST,
        SNAPSHOT_POSITIONS,
        POSITIONS,
        SUBSCRIBE_TRADE_MONITOR,
        USER,

        //ALGO PAIRS
        PAIRS_STRATEGY,
        DOLLAR_RISK_OF_PAIRS,
        PAIRS_STRATEGY_STATUS,
        OPERATIONS_CONTROL,
        INCREMENTAL_BOOK,

        // ETF
        BKT_SYMBOL_LIST,
        BKT_CONFIG,
        BKT_SYMBOL,
        SNAPSHOT_BASKET,
        SNAPSHOT_BASKET_REQUEST,
        DELETE_BASKET,
        BOLSA_STATS;




    }

    private static final MessageCodes[] messageCodes = MessageCodes.values();

    public static ByteBuf packMessage(Message message) {
        int messageCode;

        // Routing Requests
        if (message instanceof RoutingMessage.NewOrderRequest) {
            messageCode = MessageCodes.NEW_ORDER_REQUEST.ordinal();
        } else if (message instanceof RoutingMessage.OrderReplaceRequest) {
            messageCode = MessageCodes.ORDER_REPLACE_REQUEST.ordinal();
        } else if (message instanceof RoutingMessage.OrderCancelRequest) {
            messageCode = MessageCodes.ORDER_CANCEL_REQUEST.ordinal();
        } else if (message instanceof RoutingMessage.OrdersCancelRequestList) {
            messageCode = MessageCodes.ORDERS_CANCEL_REQUEST_LIST.ordinal();
        } else if (message instanceof RoutingMessage.OrdersCancelUnsolicited) {
            messageCode = MessageCodes.ORDERS_CANCEL_UNSOLICITED.ordinal();

        // Routing Responses
        } else if (message instanceof RoutingMessage.Order) {
            messageCode = MessageCodes.ORDER.ordinal();
        } else if (message instanceof RoutingMessage.TradeExecution) {
            messageCode = MessageCodes.TRADE_EXECUTION.ordinal();
        } else if (message instanceof RoutingMessage.OrderCancelReject) {
            messageCode = MessageCodes.ORDER_CANCEL_REJECT.ordinal();

        // Session Messages
        } else if (message instanceof SessionsMessage.Pong) {
            messageCode = MessageCodes.PONG.ordinal();
        } else if (message instanceof SessionsMessage.Ping) {
            messageCode = MessageCodes.PING.ordinal();
        } else if (message instanceof SessionsMessage.Disconnect) {
            messageCode = MessageCodes.DISCONNECT.ordinal();
        } else if (message instanceof SessionsMessage.Connect) {
            messageCode = MessageCodes.CONNECT.ordinal();

        // Notifications
        } else if (message instanceof NotificationMessage.Notification) {
            messageCode = MessageCodes.NOTIFICATION.ordinal();
        } else if (message instanceof NotificationMessage.NotificationRequest) {
            messageCode = MessageCodes.NOTIFICATION_REQUEST.ordinal();
        } else if (message instanceof NotificationMessage.NotificationResponse) {
            messageCode = MessageCodes.NOTIFICATION_RESPONSE.ordinal();

        // Market Data Requests
        } else if (message instanceof MarketDataMessage.Subscribe) {
            messageCode = MessageCodes.SUBSCRIBE.ordinal();
        } else if (message instanceof MarketDataMessage.Unsubscribe) {
            messageCode = MessageCodes.UNSUBSCRIBE.ordinal();
        } else if (message instanceof MarketDataMessage.SecurityListRequest) {
            messageCode = MessageCodes.SECURITY_LIST_REQUEST.ordinal();
        } else if (message instanceof MarketDataMessage.SnapshotNews) {
            messageCode = MessageCodes.SNAPSHOT_NEWS.ordinal();


        // Market Data Responses
        } else if (message instanceof MarketDataMessage.News) {
            messageCode = MessageCodes.NEWS.ordinal();
        } else if (message instanceof MarketDataMessage.Snapshot) {
            messageCode = MessageCodes.SNAPSHOT.ordinal();
        } else if (message instanceof MarketDataMessage.Trade) {
            messageCode = MessageCodes.TRADE.ordinal();
        } else if (message instanceof MarketDataMessage.SecurityList) {
            messageCode = MessageCodes.SECURITY_LIST.ordinal();
        } else if (message instanceof MarketDataMessage.Rejected) {
            messageCode = MessageCodes.REJECTED.ordinal();
        } else if (message instanceof MarketDataMessage.Statistic) {
            messageCode = MessageCodes.STATISTIC.ordinal();
        } else if (message instanceof MarketDataMessage.Ohlcv) {
            messageCode = MessageCodes.OHLCV.ordinal();
        } else if (message instanceof MarketDataMessage.BolsaStats) {
            messageCode = MessageCodes.BOLSA_STATS.ordinal();
        } else if (message instanceof MarketDataMessage.IncrementalBook) {
            messageCode = MessageCodes.INCREMENTAL_BOOK.ordinal();

            // Blotter
        } else if (message instanceof BlotterMessage.PortfolioRequest) {
            messageCode = MessageCodes.PORTFOLIO_REQUES.ordinal();
        } else if (message instanceof BlotterMessage.PortfolioResponse) {
            messageCode = MessageCodes.PORTFOLIO_RESPONSE.ordinal();
        } else if (message instanceof BlotterMessage.PreselectRequest) {
            messageCode = MessageCodes.PRESELECT_REQUEST.ordinal();
        } else if (message instanceof BlotterMessage.PreselectResponse) {
            messageCode = MessageCodes.PRESELECT_RESPONSE.ordinal();
        } else if (message instanceof BlotterMessage.Position) {
            messageCode = MessageCodes.POSITIONS.ordinal();
        } else if (message instanceof BlotterMessage.SnapshotPositions) {
            messageCode = MessageCodes.SNAPSHOT_POSITIONS.ordinal();
        } else if (message instanceof BlotterMessage.SubscribeTradeMonitor) {
            messageCode = MessageCodes.SUBSCRIBE_TRADE_MONITOR.ordinal();
        } else if (message instanceof BlotterMessage.User) {
            messageCode = MessageCodes.USER.ordinal();


            //PAIRS
        } else if (message instanceof PairsStrategyProtos.PairsStrategy) {
            messageCode = MessageCodes.PAIRS_STRATEGY.ordinal();
        } else if (message instanceof GeneralStrategy.DollarRiskOfPairs) {
            messageCode = MessageCodes.DOLLAR_RISK_OF_PAIRS.ordinal();
        } else if (message instanceof GeneralStrategy.PairsStrategyStatus) {
            messageCode = MessageCodes.PAIRS_STRATEGY_STATUS.ordinal();
        } else if (message instanceof GeneralStrategy.OperationsControl) {
            messageCode = MessageCodes.OPERATIONS_CONTROL.ordinal();


        //BASKET
        } else if (message instanceof BktStrategyProtos.BktSymbolList) {
            messageCode = MessageCodes.BKT_SYMBOL_LIST.ordinal();
        } else if (message instanceof BktStrategyProtos.EtfConfig) {
            messageCode = MessageCodes.BKT_CONFIG.ordinal();
        } else if (message instanceof BktStrategyProtos.BktSymbol) {
            messageCode = MessageCodes.BKT_SYMBOL.ordinal();
        } else {
            messageCode = -1;
        }

        return Unpooled.wrappedBuffer(new byte[]{(byte) messageCode}, message.toByteArray());
    }

    public static Message unpackMessage(byte[] content) throws InvalidProtocolBufferException {
        int messageCode = content[0];
        byte[] messageBytes = Arrays.copyOfRange(content, 1, content.length);

        MessageCodes messageType = messageCodes[messageCode];

        switch (messageType) {
            // Routing Requests
            case NEW_ORDER_REQUEST:
                return RoutingMessage.NewOrderRequest.parseFrom(messageBytes);
            case ORDER_REPLACE_REQUEST:
                return RoutingMessage.OrderReplaceRequest.parseFrom(messageBytes);
            case ORDER_CANCEL_REQUEST:
                return RoutingMessage.OrderCancelRequest.parseFrom(messageBytes);
            case ORDERS_CANCEL_REQUEST_LIST:
                return RoutingMessage.OrdersCancelRequestList.parseFrom(messageBytes);
            case ORDERS_CANCEL_UNSOLICITED:
                return RoutingMessage.OrdersCancelUnsolicited.parseFrom(messageBytes);

            // Routing Responses
            case ORDER:
                return RoutingMessage.Order.parseFrom(messageBytes);
            case TRADE_EXECUTION:
                return RoutingMessage.TradeExecution.parseFrom(messageBytes);
            case ORDER_CANCEL_REJECT:
                return RoutingMessage.OrderCancelReject.parseFrom(messageBytes);

            // Session Messages
            case PONG:
                return SessionsMessage.Pong.parseFrom(messageBytes);
            case PING:
                return SessionsMessage.Ping.parseFrom(messageBytes);
            case DISCONNECT:
                return SessionsMessage.Disconnect.parseFrom(messageBytes);
            case CONNECT:
                return SessionsMessage.Connect.parseFrom(messageBytes);
            // Notifications
            case NOTIFICATION:
                return NotificationMessage.Notification.parseFrom(messageBytes);
            case NOTIFICATION_REQUEST:
                return NotificationMessage.NotificationRequest.parseFrom(messageBytes);
            case NOTIFICATION_RESPONSE:
                return NotificationMessage.NotificationResponse.parseFrom(messageBytes);

            // Market Data Requests
            case SUBSCRIBE:
                return MarketDataMessage.Subscribe.parseFrom(messageBytes);
            case UNSUBSCRIBE:
                return MarketDataMessage.Unsubscribe.parseFrom(messageBytes);
            case SECURITY_LIST_REQUEST:
                return MarketDataMessage.SecurityListRequest.parseFrom(messageBytes);

            // Market Data Responses
            case NEWS:
                return MarketDataMessage.News.parseFrom(messageBytes);
            case SNAPSHOT:
                return MarketDataMessage.Snapshot.parseFrom(messageBytes);
            case SNAPSHOT_NEWS:
                return MarketDataMessage.SnapshotNews.parseFrom(messageBytes);

            case INCREMENTAL_BOOK:
                return MarketDataMessage.IncrementalBook.parseFrom(messageBytes);
            case TRADE:
                return MarketDataMessage.Trade.parseFrom(messageBytes);
            case SECURITY_LIST:
                return MarketDataMessage.SecurityList.parseFrom(messageBytes);
            case REJECTED:
                return MarketDataMessage.Rejected.parseFrom(messageBytes);
            case STATISTIC:
                return MarketDataMessage.Statistic.parseFrom(messageBytes);
            case OHLCV:
                return MarketDataMessage.Ohlcv.parseFrom(messageBytes);
            case BOLSA_STATS:
                return MarketDataMessage.BolsaStats.parseFrom(messageBytes);


            // Blotter Messages
            case USER:
                return BlotterMessage.User.parseFrom(messageBytes);
            case PORTFOLIO_REQUES:
                return BlotterMessage.PortfolioRequest.parseFrom(messageBytes);
            case PORTFOLIO_RESPONSE:
                return BlotterMessage.PortfolioResponse.parseFrom(messageBytes);
            case PRESELECT_RESPONSE:
                return BlotterMessage.PreselectResponse.parseFrom(messageBytes);
            case PRESELECT_REQUEST:
                return BlotterMessage.PreselectRequest.parseFrom(messageBytes);
            case POSITIONS:
                return BlotterMessage.Position.parseFrom(messageBytes);
            case SNAPSHOT_POSITIONS:
                return BlotterMessage.SnapshotPositions.parseFrom(messageBytes);
            case SUBSCRIBE_TRADE_MONITOR:
                return BlotterMessage.SubscribeTradeMonitor.parseFrom(messageBytes);

            case PAIRS_STRATEGY:
                return PairsStrategyProtos.PairsStrategy.parseFrom(messageBytes);
            case DOLLAR_RISK_OF_PAIRS:
                return GeneralStrategy.DollarRiskOfPairs.parseFrom(messageBytes);
            case PAIRS_STRATEGY_STATUS:
                return GeneralStrategy.PairsStrategyStatus.parseFrom(messageBytes);
            case OPERATIONS_CONTROL:
                return GeneralStrategy.OperationsControl.parseFrom(messageBytes);
            case BKT_SYMBOL_LIST:
                return BktStrategyProtos.BktSymbolList.parseFrom(messageBytes);
            case BKT_CONFIG:
                return BktStrategyProtos.EtfConfig.parseFrom(messageBytes);
            case BKT_SYMBOL:
                return BktStrategyProtos.BktSymbol.parseFrom(messageBytes);
            default:
                return Any.parseFrom(messageBytes);
        }
    }

}
