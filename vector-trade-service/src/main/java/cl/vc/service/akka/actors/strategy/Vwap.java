package cl.vc.service.akka.actors.strategy;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.service.MainApp;
import cl.vc.service.util.BookSnapshot;
import com.google.protobuf.Timestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Vwap implements StrategyI {

    private final Logger log;
    private final ActorRef actorGroupPerOrder;
    private final ActorRef actorStrategy;
    private MarketDataMessage.Statistic statistic;

    private RoutingMessage.Order.Builder parentOrder;
    private RoutingMessage.Order childOrder;

    private final double totalQty;



    private final long startMillis;
    private final long endMillis;
    private final int totalSlices;

    private boolean blockOrders = false;
    private final Object lockVwap = new Object();
    private final List<RoutingMessage.Order> tradesList = new ArrayList<>();

    private int blockRejected = 0;

    private double assignedQty = 0d;

    private double limitPrice = 0d;

    private final HashMap<String, ActorRef> strategyActors;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public Vwap(RoutingMessage.Order order,
                Logger fileLog,
                ActorRef actorGroupPerOrder,
                ActorRef actorStrategy,
                HashMap<String, ActorRef> strategyActors) {

        this.log = fileLog;
        this.actorGroupPerOrder = actorGroupPerOrder;
        this.actorStrategy = actorStrategy;
        this. strategyActors = strategyActors;
        this.limitPrice = order.getLimit();

        this.parentOrder = order.toBuilder();
        this.totalQty = order.getOrderQty();

        Timestamp eff = order.getEffectiveTime();
        Timestamp exp = order.getExpireTime();
        long now = System.currentTimeMillis();

        if (eff.getSeconds() == 0L) {
            this.startMillis = now;
        } else {
            this.startMillis = eff.getSeconds() * 1000L + eff.getNanos() / 1_000_000L;
        }

        if (exp.getSeconds() == 0L) {
            this.endMillis = this.startMillis + 30L * 60L * 1000L; // default 30 min
        } else {
            this.endMillis = exp.getSeconds() * 1000L + exp.getNanos() / 1_000_000L;
        }


        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZonedDateTime startZdt = Instant.ofEpochMilli(this.startMillis).atZone(MainApp.getZoneId());
        ZonedDateTime endZdt = Instant.ofEpochMilli(this.endMillis).atZone(MainApp.getZoneId());

        log.info("VWAP ventana inicio: {}  fin: {}", fmt.format(startZdt), fmt.format(endZdt));

        long durationMs = Math.max(this.endMillis - this.startMillis, 60_000L);

        double intervalMinutes = order.getRiskRate() > 0d ? order.getRiskRate() : 5d;
        long intervalMs = (long) (intervalMinutes * 60_000L);

        int estSlices = (int) (durationMs / Math.max(intervalMs, 60_000L));
        if (estSlices < 1) {
            estSlices = 1;
        }

        this.totalSlices = estSlices +1;

        if (!riskOrder()) {
            scheduler.shutdownNow();
            return;
        }

        parentOrder = parentOrder
                .setCumQty(0d)
                .setLeaves(totalQty)
                .setAvgPrice(0d)
                .setLastPx(0d)
                .setLastQty(0d)
                .setTime(TimeGenerator.getTimeXSGO())
                .setText("Orden Padre VWAP " + parentOrder.getId())
                .setOrdStatus(RoutingMessage.OrderStatus.NEW)
                .setExecType(RoutingMessage.ExecutionType.EXEC_NEW)
                .setExecId(IDGenerator.getID());

        actorGroupPerOrder.tell(parentOrder.build(), ActorRef.noSender());

        long periodSeconds = (long) (intervalMinutes * 60L);

        if (periodSeconds <= 0L) {
            periodSeconds = 30L;
        }

        log.info("VWAP scheduler cada {} segundos para orden {}", periodSeconds, parentOrder.getId());

        long nowMs = System.currentTimeMillis();
        long initialDelayMs = Math.max(0L, startMillis - nowMs);
        long periodMs = Math.max(1L, intervalMs);

        log.info("VWAP scheduler: initialDelay={} ms (~{} s), period={} ms (~{} s), totalSlices={}",
                initialDelayMs,
                initialDelayMs / 1000.0,
                periodMs,
                periodMs / 1000.0,
                totalSlices);


        scheduler.scheduleAtFixedRate(() -> {
            try {
                process();
            } catch (Exception e) {
                log.error("Error en scheduler VWAP", e);
            }
        }, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
    }


    private boolean riskOrder() {

        try {

            if (totalQty <= 0d) {
                blockOrders = true;
                RoutingMessage.Order reject = parentOrder
                        .setText("VWAP Strategy!!! Quantity must be > 0")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeXSGO())
                        .setExecId(IDGenerator.getID())
                        .build();
                MainApp.getMessageEventBus().publish(new Envelope(reject.getId(), reject));
                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                actorGroupPerOrder.tell(reject, ActorRef.noSender());
                return false;
            }

            if (endMillis <= startMillis) {
                blockOrders = true;
                RoutingMessage.Order reject = parentOrder
                        .setText("VWAP Strategy!!! invalid time window")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeXSGO())
                        .setExecId(IDGenerator.getID())
                        .build();
                MainApp.getMessageEventBus().publish(new Envelope(reject.getId(), reject));
                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                actorGroupPerOrder.tell(reject, ActorRef.noSender());
                return false;
            }

            if (totalSlices > parentOrder.getOrderQty()) {
                blockOrders = true;
                RoutingMessage.Order reject = parentOrder
                        .setText("VWAP Strategy!!! La Qty de la orden padre es menor que la cantidad de slices")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeXSGO())
                        .setExecId(IDGenerator.getID())
                        .build();

                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                actorGroupPerOrder.tell(reject, ActorRef.noSender());
                return false;
            }

            if (limitPrice <= 0d) {
                blockOrders = true;
                RoutingMessage.Order reject = parentOrder
                        .setText("VWAP Strategy!!! Limit  0 no soportado")
                        .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                        .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                        .setTime(TimeGenerator.getTimeXSGO())
                        .setExecId(IDGenerator.getID())
                        .build();

                actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                actorGroupPerOrder.tell(reject, ActorRef.noSender());
                return false;
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }

        return true;
    }

    private void process() {
        try {

            synchronized (lockVwap) {


                if (blockOrders) {
                    log.info(" ##### blockOrders retorno, orden bloqeuda");
                    return;
                }


                long now = System.currentTimeMillis();

                if (now < startMillis) {
                    return;
                }


                double progress = (double) (now - startMillis) / (double) (endMillis - startMillis);
                if (progress < 0d) progress = 0d;
                if (progress > 1d) progress = 1d;


                int sliceIndex = (int) Math.floor(progress * totalSlices);

                int remainingSlices = totalSlices - sliceIndex;

                double sliceQty = calculateSliceQty(sliceIndex);
                assignedQty += sliceQty;



                if (sliceIndex > totalSlices) {
                    log.info("RETURN sliceIndex > totalSlices");
                    return;
                }

                log.info("sliceIndex: {} totalSlices: {} assignedQty {}" ,sliceIndex ,totalSlices , assignedQty);


                if(statistic.getVwap() <= 0d){

                    log.error("No hay vwap para procesar orden {}", parentOrder.getId());
                    RoutingMessage.Order.Builder childBuilder = parentOrder.clone()
                            .setText("Error hija VWAP Precio en cero " + parentOrder.getId())
                            .setId(IDGenerator.getID())
                            .setPrice(0d)
                            .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                            .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                            .setTime(TimeGenerator.getTimeXSGO())
                            .setOrderQty(sliceIndex);
                    actorGroupPerOrder.tell(childBuilder.build(), ActorRef.noSender());
                    return;
                }


                BigDecimal px = BigDecimal.valueOf(statistic.getVwap());
                BigDecimal ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);

                double pxRound = Ticks.roundToTick(px, ticks).doubleValue();

                if(parentOrder.getSide().equals(RoutingMessage.Side.BUY)){
                    if(pxRound >= this.statistic.getAskPx()){
                        px = BigDecimal.valueOf(this.statistic.getAskPx());
                        ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                        pxRound = Ticks.roundToTick(px, ticks).doubleValue();
                    }

                } else if(parentOrder.getSide().equals(RoutingMessage.Side.SELL)){
                    if(pxRound <= this.statistic.getBidPx()){
                        px = BigDecimal.valueOf(this.statistic.getBidPx());
                        ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                        pxRound = Ticks.roundToTick(px, ticks).doubleValue();
                    }
                }


                if(parentOrder.getSide().equals(RoutingMessage.Side.BUY) && statistic.getVwap() >= limitPrice){
                    px = BigDecimal.valueOf(limitPrice);
                    ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                    pxRound = Ticks.roundToTick(px, ticks).doubleValue();
                }

                if(parentOrder.getSide().equals(RoutingMessage.Side.SELL) && statistic.getVwap() <= limitPrice){
                    px = BigDecimal.valueOf(limitPrice);
                    ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                    pxRound = Ticks.roundToTick(px, ticks).doubleValue();

                }


                if(remainingSlices <= 0){

                    log.info("finishStrategy");
                    log.info("sliceIndex  " + sliceIndex + " sliceQty " + sliceQty + " totalSlices " + totalSlices + " finishStrategy");

                    finishStrategy("Cantidad total alcanzada dentro de process()");


                    // CALCULAMOS PRECIO FINAL

                    if(parentOrder.getSide().equals(RoutingMessage.Side.BUY)){
                        px = BigDecimal.valueOf(this.statistic.getAskPx());
                        ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                        pxRound = Ticks.roundToTick(px, ticks).doubleValue();
                        log.info("se a buscar la punta px {}", pxRound);


                    } else if(parentOrder.getSide().equals(RoutingMessage.Side.SELL)){
                        px = BigDecimal.valueOf(this.statistic.getBidPx());
                        ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                        pxRound = Ticks.roundToTick(px, ticks).doubleValue();
                        log.info("se a buscar la punta px {}", pxRound);
                    }


                    //CALCULAMOS QTY FINAL

                    if(childOrder == null){
                        log.info("childOrder null final qty poendiente {} ", parentOrder.getLeaves());
                        sliceQty = parentOrder.getLeaves();

                    } else {
                        sliceQty = parentOrder.getLeaves() - childOrder.getOrderQty();
                        log.info("childOrder final qty poendiente {} {} ", parentOrder.getLeaves(), childOrder.getOrderQty());

                    }


                }


                if (childOrder == null) {

                    if (!blockOrders) {
                        handleNewSlice(sliceQty, pxRound);
                    }


                } else if (childOrder.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                        || childOrder.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                        || childOrder.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)) {

                    if (!blockOrders) {
                        replaceActiveChildOrder(sliceQty, pxRound);
                    }
                }

            }



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    private double calculateSliceQty(int sliceIndex) {

        // Normalizamos el índice
        if (sliceIndex < 0) {
            sliceIndex = 0;
        }
        if (sliceIndex > totalSlices) {
            return 0d;
        }

        // Asumimos que totalQty viene como entero lógico (cantidad de acciones)
        long totalInt = (long) Math.floor(totalQty);

        // Reparto base y resto
        long base = totalInt / totalSlices;       // cantidad base por slice
        long remainder = totalInt % totalSlices;  // sobran 'remainder' unidades

        // Por diseño: a los primeros 'remainder' slices les sumamos 1
        long qty = base;
        if (sliceIndex < remainder) {
            qty += 1;
        }

        // Seguridad: no pasarse de lo que queda por asignar
        double remaining = totalQty - assignedQty;
        if (qty > remaining) {
            qty = (long) Math.floor(remaining);
            if (qty < 0L) {
                qty = 0L;
            }
        }

        return qty;
    }


    private void finishStrategy(String reason) {
        try {

            log.info("VWAP finaliza para orden {}. Motivo: {}.  totalQty={}", parentOrder.getId(), reason, totalQty);
            scheduler.shutdownNow();

        } catch (Exception e) {
            log.error("Error al finalizar estrategia VWAP", e);
        }
    }


    private synchronized void handleNewSlice(double sliceIndex, double pxRound) {

        try {

            synchronized (lockVwap) {

                blockOrders = true;

                RoutingMessage.Order.Builder childBuilder = parentOrder.clone()
                        .setText("VWAP " + parentOrder.getId())
                        .setLastQty(0d)
                        .setCumQty(0d)
                        .setLastPx(0d)
                        .setAvgPrice(0d)
                        .setId(IDGenerator.getID())
                        .setPrice(pxRound)
                        .setOrderQty(sliceIndex);

                MainApp.getMessageEventBus().subscribe(actorStrategy, childBuilder.getId());

                MainApp.getIdOrders().put(childBuilder.getId(), childBuilder.build());
                strategyActors.put(childBuilder.getId(), actorStrategy);

                RoutingMessage.NewOrderRequest newOrderRequest =
                        RoutingMessage.NewOrderRequest.newBuilder()
                                .setOrder(childBuilder)
                                .build();

                MainApp.getConnections().get(parentOrder.getSecurityExchange()).sendMessage(newOrderRequest);

                log.info("VWAP send NEW child {} qty {} px {}", childBuilder.getId(), sliceIndex, childBuilder.getPrice());

            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }


    private void replaceActiveChildOrder(double sliceQty, double pxRound) {

        try {

            synchronized (lockVwap) {
                if (childOrder == null) {
                    handleNewSlice(sliceQty, pxRound);
                    return;
                }

                blockOrders = true;

                double qty = childOrder.getOrderQty() + sliceQty;

                RoutingMessage.OrderReplaceRequest replace =
                        RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(childOrder.getId())
                                .setPrice(pxRound)
                                .setQuantity(qty)
                                .build();

                MainApp.getConnections().get(childOrder.getSecurityExchange()).sendMessage(replace);
                log.info("VWAP send REPLACE child {} qty {} px {}", childOrder.getId(), qty, pxRound);
            }



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void updatePxWvap(double qty, double px) {

        try {

            synchronized (lockVwap) {
                blockOrders = true;
                RoutingMessage.OrderReplaceRequest replace =
                        RoutingMessage.OrderReplaceRequest.newBuilder()
                                .setId(childOrder.getId())
                                .setPrice(px)
                                .setQuantity(qty)
                                .build();

                MainApp.getConnections().get(childOrder.getSecurityExchange()).sendMessage(replace);
                log.info("VWAP send REPLACE child {} qty {} px {}", childOrder.getId(), qty, parentOrder.getPrice());
            }



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void onStatistic(MarketDataMessage.Statistic statistic) {

        BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(statistic.getId());
        boolean isReplace = false;

        if(bookSnapshot.getStatistic().getVwap() != this.statistic.getVwap()){
            isReplace = true;
            log.info("VWAP statistic: {} -> {}", this.statistic.getVwap(), bookSnapshot.getStatistic().getVwap());
        }

        this.statistic = bookSnapshot.getStatistic();

        if(childOrder == null){
            return;
        }

        synchronized (lockVwap) {

            if(isReplace && childOrder != null){

                BigDecimal px = BigDecimal.valueOf(this.statistic.getVwap());
                BigDecimal ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                double pxRound = Ticks.roundToTick(px, ticks).doubleValue();


                if(childOrder.getPrice() != pxRound){

                    log.info("VWAP replace child By mkd  Order px {} new Px {}", childOrder.getPrice(), pxRound);

                    if(childOrder.getSide().equals(RoutingMessage.Side.BUY)){

                        if(blockOrders){
                            return;
                        }

                        if(pxRound >= this.statistic.getAskPx()){
                            px = BigDecimal.valueOf(this.statistic.getAskPx());
                            ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                            pxRound = Ticks.roundToTick(px, ticks).doubleValue();
                            updatePxWvap(childOrder.getOrderQty(), pxRound);
                        } else {
                            updatePxWvap(childOrder.getOrderQty(), pxRound);
                        }

                    } else if(childOrder.getSide().equals(RoutingMessage.Side.SELL)){

                        if(blockOrders){
                            return;
                        }

                        if(pxRound <= this.statistic.getBidPx()){
                            px = BigDecimal.valueOf(this.statistic.getBidPx());
                            ticks = Ticks.getTick(parentOrder.getSecurityExchange(), px);
                            pxRound = Ticks.roundToTick(px, ticks).doubleValue();
                            updatePxWvap(childOrder.getOrderQty(), pxRound);
                        } else {
                            updatePxWvap(childOrder.getOrderQty(), pxRound);
                        }
                    }

                }

            }

        }




    }

    @Override
    public void onSnapshot(BookSnapshot snapshot) {
        BookSnapshot bookSnapshot = MainApp.getSnapshotHashMap().get(snapshot.getId());
        this.statistic = bookSnapshot.getStatistic();
    }

    @Override
    public void onIncrementalBook(MarketDataMessage.IncrementalBook incrementalBook) {
    }

    @Override
    public void onOrders(RoutingMessage.Order order) {

        try {

            synchronized (lockVwap) {

                if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE) ||
                        order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL)) {
                    return;
                }

                childOrder = order;
                blockOrders = false;
                blockRejected = 0;

                if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)) {
                    childOrder = null;
                    log.info("childOrder null {}", order.getId());
                }


                if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE) && parentOrder != null) {

                    double lastQty = order.getLastQty();
                    double lastPx = order.getLastPx();

                    if (lastQty > 0d) {

                        tradesList.add(order);

                        double totalQty = 0d;
                        double totalValue = 0d;

                        for (RoutingMessage.Order f : tradesList) {
                            totalQty += f.getLastQty();
                            totalValue += f.getLastQty() * f.getLastPx();
                        }

                        double wap = totalValue / totalQty;
                        double avgPx = Math.round(wap * 10000d) / 10000d;

                        double prevCum = parentOrder.getCumQty() + order.getLastQty();
                        double leaves = parentOrder.getOrderQty() - prevCum;


                        double prevAvg = parentOrder.getAvgPrice();

                        RoutingMessage.OrderStatus newStatus = (leaves <= 0d)
                                ? RoutingMessage.OrderStatus.FILLED
                                : RoutingMessage.OrderStatus.PARTIALLY_FILLED;

                        parentOrder
                                .setCumQty(prevCum)                               // cum total padre
                                .setLeaves(leaves)                               // lo que falta
                                .setLastPx(lastPx)                               // último precio trade
                                .setLastQty(lastQty)                             // última cantidad trade
                                .setAvgPrice(avgPx)                             // promedio ponderado
                                .setOrdStatus(newStatus)                         // FILLED / PARTIALLY
                                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                                .setTime(TimeGenerator.getTimeXSGO())
                                .setExecId(order.getExecId());                   // puedes reutilizar el ExecId de la hija

                        RoutingMessage.Order parentExec = parentOrder.build();
                        actorGroupPerOrder.tell(parentExec, ActorRef.noSender());

                        log.info("VWAP PADRE {} TRADE agg: lastQty={}, lastPx={}, cumQty={}, avgPrice={}", parentExec.getId(), lastQty, lastPx, order.getLastQty(), prevAvg);

                        if (leaves <= 0d) {
                            finishStrategy("Padre FILLED por fills de hija");
                            actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                        }
                    }
                }




                if(parentOrder != null){
                    parentOrder.setTime(TimeGenerator.getTimeXSGO()).setExecId(IDGenerator.getID());
                    this.actorGroupPerOrder.tell(parentOrder.build(), ActorRef.noSender());
                    log.info("se envia update de la orden padre {}", parentOrder.getId());
                }

                this.actorGroupPerOrder.tell(order, ActorRef.noSender());

            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void onReplace(RoutingMessage.OrderReplaceRequest orderReplaceRequest) {
        this.limitPrice = orderReplaceRequest.getLimit();
    }

    @Override
    public void onCancelRequest(RoutingMessage.OrderCancelRequest orderCancelRequest) {

        try {

            if (childOrder != null && orderCancelRequest.getId().equals(childOrder.getId())) {

                blockOrders = true;

                RoutingMessage.OrderCancelRequest cancelChild = RoutingMessage.OrderCancelRequest.newBuilder().setId(childOrder.getId()).build();

                MainApp.getConnections()
                        .get(childOrder.getSecurityExchange())
                        .sendMessage(cancelChild);

            } else {

                blockOrders = true;

                if (childOrder != null) {
                    RoutingMessage.OrderCancelRequest cancelChild =
                            RoutingMessage.OrderCancelRequest.newBuilder()
                                    .setId(childOrder.getId())
                                    .build();
                    MainApp.getConnections()
                            .get(childOrder.getSecurityExchange())
                            .sendMessage(cancelChild);
                }

                if (parentOrder != null) {
                    parentOrder
                            .setOrdStatus(RoutingMessage.OrderStatus.CANCELED)
                            .setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED)
                            .setTime(TimeGenerator.getTimeXSGO())
                            .setExecId(IDGenerator.getID());

                    RoutingMessage.Order canceled = parentOrder.build();
                    MainApp.getMessageEventBus().publish(new Envelope(canceled.getId(), canceled));
                    actorGroupPerOrder.tell(canceled, ActorRef.noSender());
                }

                scheduler.shutdownNow();
                //actorStrategy.tell(PoisonPill.getInstance(), ActorRef.noSender());
                blockOrders = true;

            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void onRejected(RoutingMessage.OrderCancelReject rejected) {

        try {

            blockOrders = false;
            blockRejected++;

            log.info("VWAP received rejected {} {}", rejected.toString(), blockRejected);

            if(rejected.getText().contains("finished")){
                log.info("VWAP received rejected FINISHED {}", rejected.getText());
                childOrder = null;
                return;
            }

            if (blockRejected >= 5) {

                blockOrders = true;

                if (childOrder != null) {

                    RoutingMessage.OrderCancelRequest orderCancelRequest =
                            RoutingMessage.OrderCancelRequest.newBuilder()
                                    .setId(childOrder.getId())
                                    .build();

                    log.info("VWAP child order cancel by rejected {} {}", blockRejected, childOrder.getId());

                    MainApp.getConnections()
                            .get(childOrder.getSecurityExchange())
                            .sendMessage(orderCancelRequest);
                }

            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
