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

        if (margin == -1 || orders.getOrder().getOperator().toLowerCase().contains("voultech")) {
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


            if (balance.getSaldoDisponible() > amount || orders.getOrder().getOperator().toLowerCase().contains("voultech")) {
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

        if (margin == -1 || orders.getOperator().toLowerCase().contains("voultech")) {
            return true;
        }

        if (orders.getSide().equals(RoutingMessage.Side.BUY)) {

            double oldActiveQty = activeQuantity(orders);
            double newActiveQty = Math.max(0d, msg.getQuantity() - orders.getCumQty());
            double amountOld = orders.getPrice() * oldActiveQty;
            double amountNew = msg.getPrice() * newActiveQty;
            double balanceAfterReplace = balance.getSaldoDisponible() + amountOld - amountNew;

            if (amountOld > amountNew) {
                return true;
            } else if (balanceAfterReplace >= 0d && balance.getSaldoDisponible() >= 0d) {
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

                double oldActiveQty = activeQuantity(orders);
                double newActiveQty = Math.max(0d, msg.getQuantity() - orders.getCumQty());

                if (Double.compare(oldActiveQty, newActiveQty) == 0) {
                    return true;
                } else if (newActiveQty < oldActiveQty) {
                    return true;

                } else if (accion.getAvailableQuantity() >= (newActiveQty - oldActiveQty)) {
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

    /**
     * Posicion historica del instrumento, o {@code null} si la cuenta todavia no la tiene cargada.
     *
     * <p>Existe por un incidente real: al reiniciar el core con la rueda abierta, los updates de
     * orden llegan mientras la cuenta aun esta restaurando desde Redis/SQL y
     * {@code snapshotPositionHistoryMaps} esta vacio. Los cuatro accesos de los caminos de VENTA
     * hacian {@code get(...).getAvailableQuantity()} directo y lanzaban NullPointerException, que
     * abortaba TODO el orderUpdate: se perdia tambien la actualizacion de saldo, no solo la de
     * posicion, y el log se llenaba de stack traces.
     *
     * <p>Se limita a avisar y devolver null: NO crea la entrada. Crearla en cero desbalancearia la
     * cuenta, porque en el alta de una venta se RESTA la reserva y al cancelarla se SUMA de vuelta;
     * sin la resta previa, el cancel inflaria la cantidad disponible. Que la posicion no exista
     * significa que la cuenta no tiene ese papel, y entonces no hay nada que ajustar.
     * El camino de COMPRA en EXEC_TRADE si crea la entrada, porque una compra genera posicion.
     */
    private BlotterMessage.PositionHistory.Builder positionHistoryOrNull(
            RoutingMessage.Order order, String contexto) {
        BlotterMessage.PositionHistory.Builder positionHistory =
                snapshotPositionHistoryMaps.get(order.getSymbol());
        if (positionHistory == null) {
            log.warn("[Position] sin posicion historica para {} cuenta={} execType={} status={} - se omite el ajuste ({})",
                    order.getSymbol(), order.getAccount(), order.getExecType(), order.getOrdStatus(), contexto);
        }
        return positionHistory;
    }

    public boolean orderUpdate(RoutingMessage.Order order, RoutingMessage.Order orderOld) {

        if (margin == -1 || order.getOperator().toLowerCase().contains("voultech")) {
            return false;
        }

        orderAux.put(order.getId(), order);

        //validamos ordenes Activas
        balance.setOrdenesActivasCompras(0d);
        balance.setOrdenesActivasVentas(0d);

        orderAux.values().forEach(s -> {

            if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                    || s.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                    || s.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

                double activeQty = activeQuantity(s);
                if (activeQty <= 0d) {
                    return;
                }
                double activeNotional = s.getPrice() * activeQty;

                if (s.getSide().equals(RoutingMessage.Side.BUY)) {
                    Double aux = balance.getOrdenesActivasCompras() + activeNotional;
                    balance.setOrdenesActivasCompras(aux);
                } else if (s.getSide().equals(RoutingMessage.Side.SELL)) {
                    Double aux = balance.getOrdenesActivasVentas() + activeNotional;
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

                if (positionHIstory == null) {
                    log.warn("[Position] sin posicion historica para {} cuenta={} execType={} status={} - se omite el ajuste (EXEC_NEW/SELL)",
                            order.getSymbol(), order.getAccount(), order.getExecType(), order.getOrdStatus());
                } else {
                    Double qtyvalida = positionHIstory.getAvailableQuantity() - order.getOrderQty();
                    positionHIstory.setAvailableQuantity(qtyvalida);
                    snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);
                }
            }


        } else if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REPLACED)) {

            if (order.getSide().equals(RoutingMessage.Side.BUY)) {

                double amountOld = orderOld.getPrice() * activeQuantity(orderOld);
                double amountnew = order.getPrice() * activeQuantity(order);

                Double balancs = balance.getSaldoDisponible() + amountOld - amountnew;
                balance.setSaldoDisponible(balancs);

            } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

                double oldActiveQty = activeQuantity(orderOld);
                double newActiveQty = activeQuantity(order);

                if (Double.compare(oldActiveQty, newActiveQty) == 0) {
                    //no se hace nada

                } else if (oldActiveQty < newActiveQty) {

                    BlotterMessage.PositionHistory.Builder positionHIstory =
                            positionHistoryOrNull(order, "EXEC_REPLACED/aumenta");
                    if (positionHIstory != null) {
                        Double aux = positionHIstory.getAvailableQuantity() - (newActiveQty - oldActiveQty);
                        positionHIstory.setAvailableQuantity(aux);
                        snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);
                    }


                } else if (oldActiveQty > newActiveQty) {
                    //aumentar la diferencia

                    BlotterMessage.PositionHistory.Builder positionHIstory =
                            positionHistoryOrNull(order, "EXEC_REPLACED/disminuye");
                    if (positionHIstory != null) {
                        Double aux = positionHIstory.getAvailableQuantity() + (oldActiveQty - newActiveQty);
                        positionHIstory.setAvailableQuantity(aux);
                        snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);
                    }
                }

            }


        } else if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {

            balance.setOrdenesCalzadasCompras(0d);
            balance.setOrdenesCalzadasVentas(0d);

            tradesAux.put(order.getExecId(), order);

            tradesAux.values().forEach(s -> {
                if (s.getSide().equals(RoutingMessage.Side.BUY)) {
                    Double aux = balance.getOrdenesCalzadasCompras() + (s.getLastPx() * s.getLastQty());
                    balance.setOrdenesCalzadasCompras(aux);
                } else if (s.getSide().equals(RoutingMessage.Side.SELL)) {
                    Double aux = balance.getOrdenesCalzadasVentas() + (s.getLastPx() * s.getLastQty());
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
                double canceledQty = activeQuantity(order);

                if (MainApp.getSecurityExchangeSymbolsMaps().containsKey(id)) {
                    if (MainApp.getSecurityExchangeSymbolsMaps().get(id).getCurrency().equals(RoutingMessage.Currency.USD.name())) {
                        String dolar = "USD/CLP" + "DATATEC_XBCL" + "T2" + "CS";
                        BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(dolar);
                        amount = order.getPrice() * canceledQty * snapshot.getStatistic().getAskPx();

                    } else {
                        amount = order.getPrice() * canceledQty;
                    }
                } else {
                    amount = order.getPrice() * canceledQty;
                }

                balance.setSaldoDisponible(balance.getSaldoDisponible() + amount);

            } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {

                BlotterMessage.PositionHistory.Builder positionHIstory =
                        positionHistoryOrNull(order, "cancel/SELL");
                if (positionHIstory != null) {
                    double qtyPositions = positionHIstory.getAvailableQuantity() + activeQuantity(order);
                    positionHIstory.setAvailableQuantity(qtyPositions);
                    snapshotPositionHistoryMaps.put(positionHIstory.getInstrument(), positionHIstory);
                }


            }

        }


        return true;

    }

    private double activeQuantity(RoutingMessage.Order order) {
        if (order.getLeaves() > 0d) {
            return order.getLeaves();
        }
        double remaining = order.getOrderQty() - order.getCumQty();
        if (remaining > 0d) {
            return remaining;
        }
        if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)) {
            return order.getOrderQty();
        }
        return 0d;
    }
}
