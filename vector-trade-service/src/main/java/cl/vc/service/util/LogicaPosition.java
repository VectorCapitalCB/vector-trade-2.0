package cl.vc.service.util;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class LogicaPosition {

    private Double margin;
    private ActorRef self;
    private BlotterMessage.Balance.Builder balance;
    private HashMap<String, BlotterMessage.PositionHistory.Builder> snapshotPositionHistoryMaps;
    private HashMap<String, RoutingMessage.OrderReplaceRequest> replaceAux = new HashMap<>();
    private HashMap<String, RoutingMessage.Order> orderAux = new HashMap<>();
    private HashMap<String, RoutingMessage.Order> tradesAux = new HashMap<>();
    private HashMap<String, BlotterMessage.Simultaneas> simultaneasHashMap;

    public LogicaPosition(Double margin, ActorRef self, BlotterMessage.Balance.Builder balance,
                          HashMap<String, BlotterMessage.PositionHistory.Builder> snapshotPositionHistoryMaps,
                          HashMap<String, BlotterMessage.Simultaneas> simultaneasHashMap) {
        this.margin = margin;
        this.self = self;
        this.balance = balance;
        this.snapshotPositionHistoryMaps = snapshotPositionHistoryMaps;
        this.simultaneasHashMap = simultaneasHashMap;
    }

    public boolean calculateBalanceReplace(RoutingMessage.NewOrderRequest orders) {

        if (margin == -1) {
            return true; //quiere decir que no tiene limites
        }

        //validamos que tenga caja
        if (orders.getOrder().getSide().equals(RoutingMessage.Side.BUY)) {

            double amount = 0d;

            String id = orders.getOrder().getSymbol() + IDGenerator.conversorExdestination(orders.getOrder().getSecurityExchange()).name();

            if (MainApp.getSecurityExchangeSymbolsMaps().containsKey(id)) {
                if (MainApp.getSecurityExchangeSymbolsMaps().get(id).getCurrency().equals(RoutingMessage.Currency.USD.name())) {
                    String dolar = "USD/CLP" + "DATATEC_XBCL" + "T2" + "CS";
                    BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(dolar);
                    amount = orders.getOrder().getPrice() * orders.getOrder().getOrderQty() * snapshot.getStatistic().getAskPx();

                } else {
                    amount = orders.getOrder().getPrice() * orders.getOrder().getOrderQty();
                }
            } else {
                amount = orders.getOrder().getPrice() * orders.getOrder().getOrderQty();
            }


            if (balance.getSaldoDisponible() > amount) {
                return true;

            } else {
                RoutingMessage.Order.Builder orderRejected = orders.getOrder().toBuilder();
                orderRejected.setText("Saldo Insuficiente");
                orderRejected.setExecId(IDGenerator.getID());
                orderRejected.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                orderRejected.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                orderRejected.setLeaves(0d);
                self.tell(orderRejected.build(), ActorRef.noSender());
                log.info("Saldo Insuficiente {} {} {}",orders.getOrder().getAccount(),orders.getOrder().getSymbol(),orders.getOrder().getPrice());
                return false;
            }


        } else if (orders.getOrder().getSide().equals(RoutingMessage.Side.SELL)) {


            if (snapshotPositionHistoryMaps.containsKey(orders.getOrder().getSymbol())) {

                BlotterMessage.PositionHistory.Builder accion = snapshotPositionHistoryMaps.get(orders.getOrder().getSymbol());

                if (accion.getAvailableQuantity() >= orders.getOrder().getOrderQty()) {
                    return true;

                } else {
                    RoutingMessage.Order.Builder orderRejected = orders.getOrder().toBuilder();
                    orderRejected.setText("No tienes tantas acciones para vender qty maxima " + accion.getAvailableQuantity());
                    orderRejected.setExecId(IDGenerator.getID());
                    orderRejected.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                    orderRejected.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                    orderRejected.setLeaves(0d);
                    self.tell(orderRejected.build(), ActorRef.noSender());
                    return false;
                }

            } else {
                RoutingMessage.Order.Builder orderRejected = orders.getOrder().toBuilder();
                orderRejected.setText("No tienes acciones disponibles para vender");
                orderRejected.setExecId(IDGenerator.getID());
                orderRejected.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                orderRejected.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                orderRejected.setLeaves(0d);
                self.tell(orderRejected.build(), ActorRef.noSender());
                return false;
            }
        }

        return true;

    }

    public boolean calculateBalanceReplace(RoutingMessage.OrderReplaceRequest msg, RoutingMessage.Order orders) {

        replaceAux.put(msg.getId(), msg);

        if (margin == -1) {
            return true;
        }

        if (orders.getSide().equals(RoutingMessage.Side.BUY)) {

            Double amountOld = orders.getPrice() * orders.getOrderQty();
            Double amountNew = msg.getPrice() * msg.getQuantity();

            Double balanceAuxMayor = balance.getSaldoDisponible() + (amountOld - amountNew);

            if (amountOld > amountNew) {
                return true;
            } else if (balance.getSaldoDisponible() + balanceAuxMayor >= 0 && balance.getSaldoDisponible() >= 0) {
                return true;
            } else {
                RoutingMessage.OrderCancelReject.Builder orderRejected = RoutingMessage.OrderCancelReject.newBuilder();
                orderRejected.setText("El Replazo supera el balance disponible");
                orderRejected.setExecId(IDGenerator.getID());
                orderRejected.setId(msg.getId());
                self.tell(orderRejected.build(), ActorRef.noSender());
                return false;
            }

        } else if (orders.getSide().equals(RoutingMessage.Side.SELL)) {


            if (snapshotPositionHistoryMaps.containsKey(orders.getSymbol())) {

                BlotterMessage.PositionHistory.Builder accion = snapshotPositionHistoryMaps.get(orders.getSymbol());

                if (orders.getOrderQty() == msg.getQuantity()) {
                    return true;
                } else if (msg.getQuantity() < orders.getOrderQty()) {
                    return true;

                } else if (accion.getAvailableQuantity() >= (msg.getQuantity() - orders.getOrderQty())) {
                    return true;

                } else {
                    RoutingMessage.OrderCancelReject.Builder orderRejected = RoutingMessage.OrderCancelReject.newBuilder();
                    orderRejected.setText("No tienes tantas acciones para vender qty maxima " + accion.getAvailableQuantity());
                    orderRejected.setExecId(IDGenerator.getID());
                    orderRejected.setId(msg.getId());
                    self.tell(orderRejected.build(), ActorRef.noSender());
                    return false;
                }

            } else {

                RoutingMessage.OrderCancelReject.Builder orderRejected = RoutingMessage.OrderCancelReject.newBuilder();
                orderRejected.setText("No tienes acciones disponibles para vender");
                orderRejected.setExecId(IDGenerator.getID());
                orderRejected.setId(msg.getId());
                self.tell(orderRejected.build(), ActorRef.noSender());
                return false;

            }
        }

        return true;
    }

    public void orderUpdate(RoutingMessage.Order order, RoutingMessage.Order orderOld) {

        if (margin == -1) {
            return;
        }

        orderAux.put(order.getId(), order);

        //validamos ordenes Activas
        balance.setOrdenesActivasCompras(0d);
        balance.setOrdenesActivasVentas(0d);

        orderAux.values().forEach(s -> {

            if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                    || s.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)) {

                if (s.getSide().equals(RoutingMessage.Side.BUY)) {
                    Double aux = balance.getOrdenesActivasCompras() + (order.getPrice() * order.getOrderQty());
                    balance.setOrdenesActivasCompras(aux);
                } else if (s.getSide().equals(RoutingMessage.Side.SELL)) {
                    Double aux = balance.getOrdenesActivasVentas() + (order.getPrice() * order.getOrderQty());
                    balance.setOrdenesActivasVentas(aux);
                }

            } else if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
            || s.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)) {

                if (s.getSide().equals(RoutingMessage.Side.BUY)) {
                    Double aux = balance.getOrdenesActivasCompras() + (order.getPrice() * order.getLeaves());
                    balance.setOrdenesActivasCompras(aux);
                } else if (s.getSide().equals(RoutingMessage.Side.SELL)) {
                    Double aux = balance.getOrdenesActivasVentas() + (order.getPrice() * order.getLeaves());
                    balance.setOrdenesActivasVentas(aux);
                }
            }

        });


        if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_NEW)) {


            BlotterMessage.PositionHistory.Builder positionHIstory = snapshotPositionHistoryMaps.get(order.getSymbol());

            if (order.getSide().equals(RoutingMessage.Side.BUY) && order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)) {

                String id = order.getSymbol() + IDGenerator.conversorExdestination(order.getSecurityExchange()).name();
                Double amount = 0d;

                if (MainApp.getSecurityExchangeSymbolsMaps().containsKey(id)) {
                    if (MainApp.getSecurityExchangeSymbolsMaps().get(id).getCurrency().equals(RoutingMessage.Currency.USD.name())) {
                        String dolar = "USD/CLP" + "DATATEC_XBCL" + "T2" + "CS";
                        BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(dolar);
                        amount = order.getPrice() * order.getOrderQty() * snapshot.getStatistic().getAskPx();

                    } else {
                        amount = order.getPrice() * order.getOrderQty();
                    }
                } else {
                    amount = order.getPrice() * order.getOrderQty();
                }

                Double auxSaldo = balance.getSaldoDisponible() - (amount);
                balance.setSaldoDisponible(auxSaldo);

            } else if (order.getSide().equals(RoutingMessage.Side.SELL) && order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)) {

                Double qtyvalida = positionHIstory.getAvailableQuantity() - order.getOrderQty();
                positionHIstory.setAvailableQuantity(qtyvalida);
                snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);
            }


        } else if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REPLACED)) {

            if (order.getSide().equals(RoutingMessage.Side.BUY)) {

                RoutingMessage.OrderReplaceRequest replace = replaceAux.get(order.getId());

                Double amountOld = orderOld.getPrice() * orderOld.getOrderQty();
                Double amountnew = order.getPrice() * order.getOrderQty();

                Double balancs = balance.getSaldoDisponible() + amountOld - amountnew;
                balance.setSaldoDisponible(balancs);

            } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

                RoutingMessage.OrderReplaceRequest replace = replaceAux.get(order.getId());

                if (orderOld.getOrderQty() == replace.getQuantity()) {
                    //no se hace nada

                } else if (orderOld.getOrderQty() < order.getOrderQty()) {

                    BlotterMessage.PositionHistory.Builder positionHIstory = snapshotPositionHistoryMaps.get(order.getSymbol());
                    Double aux = positionHIstory.getAvailableQuantity() - (order.getOrderQty() - orderOld.getOrderQty());
                    positionHIstory.setAvailableQuantity(aux);
                    snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);


                } else if (orderOld.getOrderQty() > order.getOrderQty()) {
                    //aumentar la diferencia

                    BlotterMessage.PositionHistory.Builder positionHIstory = snapshotPositionHistoryMaps.get(order.getSymbol());
                    Double aux = positionHIstory.getAvailableQuantity() + (orderOld.getOrderQty() - order.getOrderQty());
                    positionHIstory.setAvailableQuantity(aux);
                    snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);
                }

            }


        } else if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {

            balance.setOrdenesCalzadasCompras(0d);
            balance.setOrdenesCalzadasVentas(0d);

            tradesAux.put(order.getExecId(), order);

            tradesAux.values().forEach(s -> {
                if (s.getSide().equals(RoutingMessage.Side.BUY)) {
                    Double aux = balance.getOrdenesCalzadasCompras() + (order.getLastPx() * order.getLastQty());
                    balance.setOrdenesCalzadasCompras(aux);
                } else if (s.getSide().equals(RoutingMessage.Side.SELL)) {
                    Double aux = balance.getOrdenesCalzadasVentas() + (order.getLastPx() * order.getLastQty());
                    balance.setOrdenesCalzadasVentas(aux);
                }
            });


            if (order.getSide().equals(RoutingMessage.Side.BUY)) {

                if (snapshotPositionHistoryMaps.containsKey(order.getSymbol())) {
                    BlotterMessage.PositionHistory.Builder positionHIstory = snapshotPositionHistoryMaps.get(order.getSymbol());
                    Double aux = positionHIstory.getAvailableQuantity() + order.getLastQty();
                    positionHIstory.setAvailableQuantity(aux);
                    snapshotPositionHistoryMaps.put(order.getSymbol(), positionHIstory);

                } else {

                    BlotterMessage.PositionHistory.Builder positionHIstory = BlotterMessage.PositionHistory.newBuilder();
                    ProtoDateProcessor.setDateProcesorIfMissing(positionHIstory);
                    positionHIstory.setAccount(order.getAccount());
                    positionHIstory.setMarketPrice(order.getLastPx());
                    positionHIstory.setPurchaseAmount(order.getLastPx() * order.getLastQty());
                    positionHIstory.setInstrument(order.getSymbol());
                    positionHIstory.setAvailableQuantity(order.getLastQty());
                    snapshotPositionHistoryMaps.put(order.getSymbol(), positionHIstory);
                }

            } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

                Double balancesum = (order.getLastPx() * order.getLastQty()) + balance.getSaldoDisponible();
                balance.setSaldoDisponible(balancesum);

            }


        } else if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_CANCELED)) {

            if (order.getSide().equals(RoutingMessage.Side.BUY)) {

                String id = order.getSymbol() + IDGenerator.conversorExdestination(order.getSecurityExchange()).name();
                Double amount = 0d;

                if (MainApp.getSecurityExchangeSymbolsMaps().containsKey(id)) {
                    if (MainApp.getSecurityExchangeSymbolsMaps().get(id).getCurrency().equals(RoutingMessage.Currency.USD.name())) {
                        String dolar = "USD/CLP" + "DATATEC_XBCL" + "T2" + "CS";
                        BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(dolar);
                        amount = order.getPrice() * (order.getOrderQty() - order.getCumQty()) * snapshot.getStatistic().getAskPx();

                    } else {
                        amount = order.getPrice() * (order.getOrderQty() - order.getCumQty());
                    }
                } else {
                    amount = order.getPrice() * order.getOrderQty();
                }

                balance.setSaldoDisponible(balance.getSaldoDisponible() + amount);

            } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

                BlotterMessage.PositionHistory.Builder positionHIstory = snapshotPositionHistoryMaps.get(order.getSymbol());
                double qtyPositions = positionHIstory.getAvailableQuantity() + order.getOrderQty() - order.getCumQty();
                positionHIstory.setAvailableQuantity(qtyPositions);
                snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);


            }

        }


        BlotterMessage.SnapshotPositionHistory.Builder snapshotPositionHistory = BlotterMessage.SnapshotPositionHistory.newBuilder();
        snapshotPositionHistoryMaps.values().forEach(s -> snapshotPositionHistory.addPositionsHistory(s));
        self.tell(snapshotPositionHistory.build(), ActorRef.noSender());
        self.tell(balance.build(), ActorRef.noSender());

    }
}

