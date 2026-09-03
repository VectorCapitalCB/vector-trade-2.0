package cl.vc.module.protocolbuff.tcp;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import ch.qos.logback.classic.Logger;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;@Slf4j
public class LoggerActor extends AbstractActor {

    /** Nombres unicos: varias conexiones (XSGO, BCS, ALPACA...) viven en el mismo ActorSystem. */
    private static final java.util.concurrent.atomic.AtomicInteger NOMBRE =
            new java.util.concurrent.atomic.AtomicInteger();

    private final Logger fileLog;
    private final JsonFormat.Printer printer;
    private final Boolean islog;

    public LoggerActor(Logger fileLog, Boolean islog) {
        this.fileLog = fileLog;
        this.islog = islog;
        this.printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();
    }

    public static Props props(Logger fileLog, Boolean islog) {
        return Props.create(LoggerActor.class, fileLog, islog);
    }

    /**
     * Crea el actor de log del protocolo. Es UNO SOLO a proposito.
     *
     * <p>Antes se instanciaba como {@code new RoundRobinPool(10).props(...)}: diez actores
     * escribiendo el mismo archivo en paralelo, con lo que el log dejaba de reflejar el orden en que
     * llegaron los mensajes. Eso invalida cualquier forense de secuencia (caso orden 92b29dc1,
     * 2026-09-01: el archivo mostraba un orden y el servicio procesaba otro).
     *
     * <p>El pool tampoco daba throughput: FileAppender serializa con un lock interno, asi que los
     * diez actores solo se bloqueaban entre si. Un actor unico es mas rapido y ademas ordenado.
     *
     * <p>Sigue sin bloquear al productor: quien llama hace {@code tell}, que es asincrono; el disco
     * se toca en el hilo del actor, nunca en el de netty.
     */
    public static ActorRef create(ActorSystem system, Logger fileLog, Boolean islog) {
        return system.actorOf(props(fileLog, islog), "protoLogger-" + NOMBRE.incrementAndGet());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.class, this::getMessage)
                .build();
    }

    private void getMessage(Message message) throws InvalidProtocolBufferException {
        if (islog) {
            if (message instanceof RoutingMessage.NewOrderRequest) {
                RoutingMessage.Order order = ((RoutingMessage.NewOrderRequest) message).getOrder();
                fileLog.info("NewOrderRequest : {{\"order\":{}}}", formatOrder(order));
            } else if (message instanceof RoutingMessage.Order) {
                fileLog.info("{} : {}", message.getClass().getSimpleName(), formatOrder((RoutingMessage.Order) message));
            } else if (isOrderLike(message)) {
                // Handles Order-like messages from external modules (e.g. OutboundOrder)
                fileLog.info("{} : {}", message.getClass().getSimpleName(), formatOrderDescriptor(message));
            } else {
                fileLog.info("{} : {}", message.getClass().getSimpleName(), printer.print(message));
            }
        }
    }

    private boolean isOrderLike(Message message) {
        Descriptors.Descriptor d = message.getDescriptorForType();
        return d.findFieldByName("id") != null
            && d.findFieldByName("account") != null
            && d.findFieldByName("price") != null
            && d.findFieldByName("orderQty") != null;
    }

    private String getStr(Message msg, Descriptors.Descriptor d, String name) {
        Descriptors.FieldDescriptor fd = d.findFieldByName(name);
        if (fd == null) return "";
        Object val = msg.getField(fd);
        if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
            return val instanceof Descriptors.EnumValueDescriptor
                ? ((Descriptors.EnumValueDescriptor) val).getName() : "";
        }
        return val == null ? "" : val.toString();
    }

    private double getDbl(Message msg, Descriptors.Descriptor d, String name) {
        Descriptors.FieldDescriptor fd = d.findFieldByName(name);
        if (fd == null) return 0.0;
        Object val = msg.getField(fd);
        if (val instanceof Double) return (Double) val;
        if (val instanceof Float) return ((Float) val).doubleValue();
        return 0.0;
    }

    private boolean getBool(Message msg, Descriptors.Descriptor d, String name) {
        Descriptors.FieldDescriptor fd = d.findFieldByName(name);
        if (fd == null) return false;
        Object val = msg.getField(fd);
        return val instanceof Boolean && (Boolean) val;
    }

    private String formatOrderDescriptor(Message message) {
        Descriptors.Descriptor d = message.getDescriptorForType();
        return String.format(
            // Identity
            "{\"id\":\"%s\",\"account\":\"%s\",\"symbol\":\"%s\"," +
            // Price & quantity
            "\"price\":%s,\"orderQty\":%s,\"side\":\"%s\"," +
            // Trade quantities
            "\"lastQty\":%s,\"lastPx\":%s,\"leaves\":%s,\"cumQty\":%s,\"avgPrice\":%s,\"amount\":%s," +
            // States
            "\"ordStatus\":\"%s\",\"execType\":\"%s\",\"ordType\":\"%s\"," +
            // Main IDs
            "\"clOrdId\":\"%s\",\"origClOrdID\":\"%s\",\"orderID\":\"%s\",\"execId\":\"%s\",\"clOrdLinkID\":\"%s\"," +
            // Remaining fields
            "\"operator\":\"%s\",\"codeOperator\":\"%s\",\"handlInst\":\"%s\",\"broker\":\"%s\"," +
            "\"currency\":\"%s\",\"tif\":\"%s\",\"settlType\":\"%s\",\"settlDate\":\"%s\"," +
            "\"riskRate\":%s,\"prefixID\":\"%s\",\"folio\":\"%s\"," +
            "\"contraTrader\":\"%s\",\"contraBroker\":\"%s\"," +
            "\"basketID\":\"%s\",\"exStrategy\":\"%s\",\"strategyOrder\":\"%s\"," +
            "\"spread\":%s,\"limit\":%s,\"maxFloor\":%s,\"hideOrder\":%s,\"chkIndivisible\":%s," +
            "\"securityType\":\"%s\",\"securityExchange\":\"%s\",\"securityID\":\"%s\"," +
            "\"icebergPercentage\":\"%s\",\"icebergValue\":\"%s\"," +
            "\"commission\":\"%s\",\"commissionType\":\"%s\",\"text\":\"%s\"}",
            // Identity
            getStr(message, d, "id"), getStr(message, d, "account"), getStr(message, d, "symbol"),
            // Price & quantity
            getDbl(message, d, "price"), getDbl(message, d, "orderQty"), getStr(message, d, "side"),
            // Trade quantities
            getDbl(message, d, "lastQty"), getDbl(message, d, "lastPx"), getDbl(message, d, "leaves"),
            getDbl(message, d, "cumQty"), getDbl(message, d, "avgPrice"), getDbl(message, d, "amount"),
            // States
            getStr(message, d, "ordStatus"), getStr(message, d, "execType"), getStr(message, d, "ordType"),
            // Main IDs
            getStr(message, d, "clOrdId"), getStr(message, d, "origClOrdID"), getStr(message, d, "orderID"),
            getStr(message, d, "execId"), getStr(message, d, "clOrdLinkID"),
            // Remaining fields
            getStr(message, d, "operator"), getStr(message, d, "codeOperator"),
            getStr(message, d, "handlInst"), getStr(message, d, "broker"),
            getStr(message, d, "currency"), getStr(message, d, "tif"),
            getStr(message, d, "settlType"), getStr(message, d, "settlDate"),
            getDbl(message, d, "riskRate"), getStr(message, d, "prefixID"), getStr(message, d, "folio"),
            getStr(message, d, "contraTrader"), getStr(message, d, "contraBroker"),
            getStr(message, d, "basketID"), getStr(message, d, "exStrategy"), getStr(message, d, "strategyOrder"),
            getDbl(message, d, "spread"), getDbl(message, d, "limit"), getDbl(message, d, "maxFloor"),
            getBool(message, d, "hideOrder"), getBool(message, d, "chkIndivisible"),
            getStr(message, d, "securityType"), getStr(message, d, "securityExchange"), getStr(message, d, "securityID"),
            getStr(message, d, "icebergPercentage"), getStr(message, d, "icebergValue"),
            getStr(message, d, "commission"), getStr(message, d, "commission_type"), getStr(message, d, "text")
        );
    }

    private String formatOrder(RoutingMessage.Order o) {
        return String.format(
            // Identity
            "{\"id\":\"%s\",\"account\":\"%s\",\"symbol\":\"%s\"," +
            // Price & quantity
            "\"price\":%s,\"orderQty\":%s,\"side\":\"%s\"," +
            // Trade quantities
            "\"lastQty\":%s,\"lastPx\":%s,\"leaves\":%s,\"cumQty\":%s,\"avgPrice\":%s,\"amount\":%s," +
            // States
            "\"ordStatus\":\"%s\",\"execType\":\"%s\",\"ordType\":\"%s\"," +
            // Main IDs
            "\"clOrdId\":\"%s\",\"origClOrdID\":\"%s\",\"orderID\":\"%s\",\"execId\":\"%s\",\"clOrdLinkID\":\"%s\"," +
            // Remaining fields
            "\"operator\":\"%s\",\"codeOperator\":\"%s\",\"handlInst\":\"%s\",\"broker\":\"%s\"," +
            "\"currency\":\"%s\",\"tif\":\"%s\",\"settlType\":\"%s\",\"settlDate\":\"%s\"," +
            "\"riskRate\":%s,\"prefixID\":\"%s\",\"folio\":\"%s\"," +
            "\"contraTrader\":\"%s\",\"contraBroker\":\"%s\"," +
            "\"basketID\":\"%s\",\"exStrategy\":\"%s\",\"strategyOrder\":\"%s\"," +
            "\"spread\":%s,\"limit\":%s,\"maxFloor\":%s,\"hideOrder\":%s,\"chkIndivisible\":%s," +
            "\"securityType\":\"%s\",\"securityExchange\":\"%s\",\"securityID\":\"%s\"," +
            "\"icebergPercentage\":\"%s\",\"icebergValue\":\"%s\"," +
            "\"commission\":\"%s\",\"commissionType\":\"%s\",\"text\":\"%s\"}",
            // Identity
            o.getId(), o.getAccount(), o.getSymbol(),
            // Price & quantity
            o.getPrice(), o.getOrderQty(), o.getSide().name(),
            // Trade quantities
            o.getLastQty(), o.getLastPx(), o.getLeaves(), o.getCumQty(), o.getAvgPrice(), o.getAmount(),
            // States
            o.getOrdStatus().name(), o.getExecType().name(), o.getOrdType().name(),
            // Main IDs
            o.getClOrdId(), o.getOrigClOrdID(), o.getOrderID(), o.getExecId(), o.getClOrdLinkID(),
            // Remaining fields
            o.getOperator(), o.getCodeOperator(), o.getHandlInst().name(), o.getBroker().name(),
            o.getCurrency().name(), o.getTif().name(), o.getSettlType().name(), o.getSettlDate(),
            o.getRiskRate(), o.getPrefixID(), o.getFolio(),
            o.getContraTrader(), o.getContraBroker(),
            o.getBasketID(), o.getExStrategy().name(), o.getStrategyOrder().name(),
            o.getSpread(), o.getLimit(), o.getMaxFloor(), o.getHideOrder(), o.getChkIndivisible(),
            o.getSecurityType().name(), o.getSecurityExchange().name(), o.getSecurityID(),
            o.getIcebergPercentage(), o.getIcebergValue(),
            o.getCommission(), o.getCommissionType(), o.getText()
        );
    }

}
