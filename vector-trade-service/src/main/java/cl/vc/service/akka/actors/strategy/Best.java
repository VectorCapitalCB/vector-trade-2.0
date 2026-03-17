package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;

import java.math.BigDecimal;

public class Best implements StrategyI {

    private final Logger log;
    private final ActorRef actorGroupPerOrder;
    private RoutingMessage.Order order;
    private BookSnapshot snapshot;
    private Double limit = 0d;
    private Double spread = 0d;
    private Double maxfloor = 0d;
    private String icebergperc = "";
    private Double blockQty = 0d;
    private Boolean blockOrders = false;
    private int blockrejected = 0;

    public Best(RoutingMessage.Order order, String idSubscribe, Logger fileLog, ActorRef actorGroupPerOrder) {

        this.order = order;

        if (order.getMaxFloor() <= 0d) {
            maxfloor = order.getOrderQty();
        } else {
            maxfloor = order.getMaxFloor();
        }

        icebergperc = order.getIcebergPercentage();
        limit = order.getLimit();
        spread = order.getSpread();
        this.log = fileLog;
        this.actorGroupPerOrder = actorGroupPerOrder;
    }


    @Override
    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {

        try {

            BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(incrementalBook.getId());
            onSnapshot(bookSnapshot);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }


    @Override
    public void onSnapshot(BookSnapshot snapshot) {

        if (blockOrders) {
            return;
        }

        if (snapshot == null) {
            log.warn("BEST onSnapshot null order={}", order.getId());
            return;
        }

        this.snapshot = snapshot;

        if (order.getLimit() <= 0) {

            blockOrders = true;
            RoutingMessage.Order order1 = order.toBuilder().setText("Best Strategy!!!! Limit must not be Zero")
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED).setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setTime(TimeGenerator.getTimeProto())
                    .setExecId(IDGenerator.getID()).build();
            MainApp.getMessageEventBus().publish(new Envelope(order1.getId(), order1));
            return;
        }

        if (order.getSide().equals(RoutingMessage.Side.BUY)) {

            if (snapshot.getBid() == null || snapshot.getBid().isEmpty()) {
                log.warn("BEST snapshot without bid levels order={} symbol={} idSubscribe={}",
                        order.getId(), order.getSymbol(), snapshot.getId());
                return;
            }

            MarketDataMessage.DataBook dataBook = snapshot.getBid().get(0);

            if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

                if (dataBook.getPrice() >= order.getLimit()) {
                    blockOrders = true;

                    order = order.toBuilder().setPrice(order.getLimit()).build();

                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                } else {
                    blockOrders = true;
                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).add(tick).doubleValue();
                    order = order.toBuilder().setPrice(newPrice).build();
                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                }

            } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)) {

                if (dataBook.getPrice() <= order.getLimit()) {

                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).add(tick).doubleValue();

                    if (newPrice != order.getPrice() && order.getPrice() != dataBook.getPrice() && order.getLimit() >= newPrice && !blockOrders) {
                        blockOrders = true;
                        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(newPrice)
                                .setQuantity(order.getOrderQty()).build();
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);

                    } else if (order.getPrice() == dataBook.getPrice()) {
                        if (snapshot.getBid().size() < 2) {
                            log.warn("BEST snapshot without second bid level order={} symbol={} idSubscribe={}",
                                    order.getId(), order.getSymbol(), snapshot.getId());
                            return;
                        }

                        MarketDataMessage.DataBook dataBookSecond = snapshot.getBid().get(1);

                        BigDecimal tick2 = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBookSecond.getPrice()));
                        double newPrice2 = BigDecimal.valueOf(dataBookSecond.getPrice()).add(tick2).doubleValue();

                        if (order.getPrice() != newPrice2 && order.getLimit() >= newPrice2 && !blockOrders) {
                            blockOrders = true;
                            RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                    .setId(order.getId())
                                    .setPrice(newPrice2)
                                    .setQuantity(order.getOrderQty()).build();
                            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
                        }
                    } else if (dataBook.getPrice() >= order.getLimit() && !blockOrders) {
                        blockOrders = true;
                        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(order.getLimit())
                                .setQuantity(order.getOrderQty()).build();
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
                    }

                } else if (order.getPrice() != dataBook.getPrice() && !blockOrders) {

                    if (order.getLimit() != order.getPrice()) {
                        blockOrders = true;
                        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(order.getLimit())
                                .setQuantity(order.getOrderQty()).build();
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
                    }

                }
            }


        } else if (order.getSide().equals(RoutingMessage.Side.SELL) || order.getSide().equals(RoutingMessage.Side.SELL_SHORT)) {

            if (snapshot.getAsk() == null || snapshot.getAsk().isEmpty()) {
                log.warn("BEST snapshot without ask levels order={} symbol={} idSubscribe={}",
                        order.getId(), order.getSymbol(), snapshot.getId());
                return;
            }

            MarketDataMessage.DataBook dataBook = snapshot.getAsk().get(0);

            if (!blockOrders && order.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {

                if (dataBook.getPrice() <= order.getLimit()) {
                    blockOrders = true;
                    order = order.toBuilder().setPrice(order.getLimit()).build();
                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                } else {
                    blockOrders = true;
                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).subtract(tick).doubleValue();
                    order = order.toBuilder().setPrice(newPrice).build();
                    RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                    MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
                }

            } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                    || order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                    | order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)) {

                if (dataBook.getPrice() >= order.getLimit() && !blockOrders) {

                    BigDecimal tick = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBook.getPrice()));
                    double newPrice = BigDecimal.valueOf(dataBook.getPrice()).subtract(tick).doubleValue();

                    if (newPrice != order.getPrice() && order.getPrice() != dataBook.getPrice() && newPrice >= order.getLimit() && !blockOrders) {
                        blockOrders = true;
                        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(newPrice)
                                .setMaxFloor(maxfloor)
                                .setIcebergPercentage(icebergperc)
                                .setQuantity(order.getOrderQty()).build();
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);

                    } else if (order.getPrice() == dataBook.getPrice() && !blockOrders) {
                        if (snapshot.getAsk().size() < 2) {
                            log.warn("BEST snapshot without second ask level order={} symbol={} idSubscribe={}",
                                    order.getId(), order.getSymbol(), snapshot.getId());
                            return;
                        }

                        MarketDataMessage.DataBook dataBookSecond = snapshot.getAsk().get(1);

                        BigDecimal tick2 = Ticks.getTick(order.getSecurityExchange(), BigDecimal.valueOf(dataBookSecond.getPrice()));
                        double newPrice2 = BigDecimal.valueOf(dataBookSecond.getPrice()).subtract(tick2).doubleValue();

                        if (order.getPrice() != newPrice2 && order.getLimit() <= newPrice2 && !blockOrders) {
                            blockOrders = true;
                            RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                    .setId(order.getId())
                                    .setPrice(newPrice2)
                                    .setMaxFloor(maxfloor)
                                    .setIcebergPercentage(icebergperc)
                                    .setQuantity(order.getOrderQty()).build();
                            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
                        }

                    } else if (dataBook.getPrice() >= order.getLimit() && !blockOrders) {
                        blockOrders = true;
                        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setPrice(order.getLimit())
                                .setMaxFloor(maxfloor)
                                .setQuantity(order.getOrderQty()).build();
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
                    }

                } else if (dataBook.getPrice() <= order.getLimit() && order.getPrice() != dataBook.getPrice() && !blockOrders) {
                    if (order.getLimit() != order.getPrice()) {
                        blockOrders = true;
                        RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(order.getId())
                                .setMaxFloor(maxfloor)
                                .setIcebergPercentage(icebergperc)
                                .setPrice(order.getLimit())
                                .setQuantity(order.getOrderQty()).build();
                        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
                    }
                }
            }
        }

    }

    @Override
    public void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {

        double price = order.getPrice();
        limit = orderReplaceRequest.getLimit();
        spread = orderReplaceRequest.getSpread();
        icebergperc = orderReplaceRequest.getIcebergPercentage();

        if (orderReplaceRequest.getMaxFloor() <= 0d) {
            maxfloor = orderReplaceRequest.getQuantity();
        } else {
            maxfloor = orderReplaceRequest.getMaxFloor();
        }

        if ((orderReplaceRequest.getQuantity() != orderReplaceRequest.getQuantity() || orderReplaceRequest.getMaxFloor() != order.getMaxFloor())
                && orderReplaceRequest.getSpread() == order.getSpread() && orderReplaceRequest.getLimit() == order.getLimit()) {

            RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId(order.getId())
                    .setPrice(order.getLimit())
                    .setIcebergPercentage(icebergperc)
                    .setMaxFloor(maxfloor)
                    .setQuantity(orderReplaceRequest.getQuantity()).build();
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(replace);
            return;
        }


        order = order.toBuilder()
                .setPrice(price)
                .setMaxFloor(maxfloor)
                .setIcebergPercentage(icebergperc)
                .setOrderQty(orderReplaceRequest.getQuantity())
                .setSpread(orderReplaceRequest.getSpread())
                .setLimit(orderReplaceRequest.getLimit()).build();

        blockOrders = false;
        onSnapshot(snapshot);

    }

    @Override
    public void onCancelRequest(RoutingMessage.OrderCancelRequest orderCancelRequest) {
        MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);
    }

    @Override
    public void onOrders(RoutingMessage.Order order) {


        if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE) ||
                order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
            return;
        }


        if (!order.getTif().equals(RoutingMessage.Tif.FILL_OR_KILL)) {
            this.order = order.toBuilder().setLimit(limit).setSpread(spread).setIcebergPercentage(String.valueOf(maxfloor)).setIcebergPercentage(icebergperc).build();
        }


        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

            blockOrders = false;
            blockrejected = 0;


        } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED) ||
                order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)) {

            blockOrders = true;

        } else if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.DONE_FOR_DAY)) {
            blockQty = blockQty - order.getOrderQty();
        }

        this.actorGroupPerOrder.tell(this.order, ActorRef.noSender());

    }

    @Override
    public void onRejected(RoutingMessage.OrderCancelReject rejected) {

        blockOrders = false;
        blockrejected++;

        log.info("received rejected {}", rejected.toString());

        if (blockrejected >= 5) {

            blockOrders = true;

            RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(order.getId()).build();
            log.info("order cancel by rejected {} {}", blockrejected, order.getId());
            MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(orderCancelRequest);

        }

    }


    @Override
    public void onStatistic(MarketDataMessage.Statistic statistic) {

    }

}
