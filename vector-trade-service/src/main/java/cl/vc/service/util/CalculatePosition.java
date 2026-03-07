package cl.vc.service.util;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class CalculatePosition {

    private static final Object lock = new Object();

    private final String account;

    // Guarda el último CumQty visto por orden (clave estable: msg.id / clOrdId / orderID)
    private final Map<String, Double> lastCumQtyByOrderKey = new ConcurrentHashMap<>();

    public CalculatePosition(String account) {
        this.account = account;
    }

    public synchronized BlotterMessage.Position onOrder(RoutingMessage.Order msg,
                                                        Map<String, BlotterMessage.Position> positionsMaps) {
        try {
            synchronized (lock) {

                String posId = msg.getSymbol() + msg.getSecurityExchange().name() + msg.getAccount();

                if (!positionsMaps.containsKey(posId)) {
                    positionsMaps.put(posId,
                            BlotterMessage.Position.newBuilder()
                                    .setAccount(account)
                                    .setSymbol(msg.getSymbol())
                                    .setId(posId)
                                    .setSecurityexchange(msg.getSecurityExchange())
                                    .build()
                    );
                }

                BlotterMessage.Position.Builder builder = positionsMaps.get(posId).toBuilder();

                if (msg.getSide().equals(RoutingMessage.Side.BUY)) {
                    calculateBuyValues(msg, builder);
                    calculateNetValues(builder);

                } else if (msg.getSide().equals(RoutingMessage.Side.SELL)
                        || msg.getSide().equals(RoutingMessage.Side.SELL_SHORT)) {
                    calculateSellValues(msg, builder);
                    calculateNetValues(builder);
                }

                return builder.build();
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
        return null;
    }

    // ---------------------- helpers ----------------------

    private String orderKey(RoutingMessage.Order o) {
        // id es lo más estable en tu flujo (replace usa ese id)
        if (o.getId() != null && !o.getId().isEmpty()) return o.getId();
        if (o.getClOrdId() != null && !o.getClOrdId().isEmpty()) return o.getClOrdId();
        if (o.getOrderID() != null && !o.getOrderID().isEmpty()) return o.getOrderID();

        // fallback (último recurso)
        return o.getSymbol() + "|" + o.getSecurityExchange().name() + "|" + o.getAccount();
    }

    /**
     * Cantidad realmente ejecutada en ESTE evento:
     * - si lastQty > 0 => úsala
     * - si lastQty == 0 => usa delta(cumQty - prevCumQty)
     */
    private double resolveExecQty(RoutingMessage.Order o) {
        if (!o.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) return 0d;

        double lastQty = o.getLastQty();
        if (lastQty < 0) lastQty = Math.abs(lastQty);

        double cumQty = o.getCumQty();
        if (cumQty < 0) cumQty = Math.abs(cumQty);

        String k = orderKey(o);
        double prevCum = lastCumQtyByOrderKey.getOrDefault(k, 0d);

        // si por algún motivo cumQty “resetea” (nuevo día/restart), reseteamos base
        if (cumQty < prevCum) prevCum = 0d;

        double delta = cumQty - prevCum;

        // actualizamos el último cumQty visto (siempre)
        lastCumQtyByOrderKey.put(k, Math.max(cumQty, prevCum));

        if (lastQty > 0d) return lastQty;
        if (delta > 0d) return delta;

        // lastQty=0 y delta=0 => duplicado / evento inútil
        return 0d;
    }

    // ---------------------- calculations ----------------------

    private synchronized void calculateBuyValues(RoutingMessage.Order executionReport,
                                                 BlotterMessage.Position.Builder position) {
        try {
            if (executionReport.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {

                double execQty = resolveExecQty(executionReport);
                if (execQty <= 0d) {
                    // No hay fill real, no tocamos cálculos
                    return;
                }

                double newTradeBuy = position.getTradeBuy() + execQty;
                position.setTradeBuy(newTradeBuy);

                double newAuxBuy = position.getAuxBuy() + (executionReport.getLastPx() * execQty);
                position.setAuxBuy(BigDecimal.valueOf(newAuxBuy).setScale(6, RoundingMode.HALF_UP).doubleValue());

                BigDecimal auxBuyBD = BigDecimal.valueOf(position.getAuxBuy());
                BigDecimal tradeBuyBD = BigDecimal.valueOf(position.getTradeBuy());

                double pxBuy = 0d;
                if (tradeBuyBD.compareTo(BigDecimal.ZERO) != 0) {
                    pxBuy = auxBuyBD.divide(tradeBuyBD, 6, RoundingMode.HALF_UP).doubleValue();
                } else {
                    // Esto ya no debería pasar, pero lo dejamos por seguridad
                    log.warn("BUY EXEC_TRADE con tradeBuy=0 (raro): symbol={} exch={} account={} id={} execId={} lastQty={} cumQty={}",
                            executionReport.getSymbol(),
                            executionReport.getSecurityExchange(),
                            executionReport.getAccount(),
                            executionReport.getId(),
                            executionReport.getExecId(),
                            executionReport.getLastQty(),
                            executionReport.getCumQty());
                }
                position.setPxBuy(pxBuy);

                position.setCashBoughtBuy(
                        BigDecimal.valueOf(position.getTradeBuy())
                                .multiply(BigDecimal.valueOf(position.getPxBuy()))
                                .multiply(BigDecimal.valueOf(-1d))
                                .doubleValue()
                );
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            // (tu lógica actual deja esto en 0)
            position.setSentBuy(0d);
            position.setWorkingBuy(0d);
        }
    }

    private synchronized void calculateSellValues(RoutingMessage.Order executionReport,
                                                  BlotterMessage.Position.Builder position) {
        try {
            if (executionReport.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {

                double execQty = resolveExecQty(executionReport);
                if (execQty <= 0d) {
                    return;
                }

                // tradeSell lo mantienes negativo
                double newTradeSell = position.getTradeSell() - execQty;
                position.setTradeSell(newTradeSell);

                if (executionReport.getSide().equals(RoutingMessage.Side.SELL_SHORT)) {
                    position.setTradeSellShort(position.getTradeSellShort() - execQty);
                }

                double newAuxSell = position.getAuxBSell() + (executionReport.getLastPx() * execQty);
                position.setAuxBSell(BigDecimal.valueOf(newAuxSell).setScale(6, RoundingMode.HALF_UP).doubleValue());

                BigDecimal auxSellBD = BigDecimal.valueOf(position.getAuxBSell());
                BigDecimal qtySellBD = BigDecimal.valueOf(position.getTradeSell()).abs();

                double pxSell = 0d;
                if (qtySellBD.compareTo(BigDecimal.ZERO) != 0) {
                    pxSell = auxSellBD.divide(qtySellBD, 6, RoundingMode.HALF_UP).doubleValue();
                } else {
                    log.warn("SELL EXEC_TRADE con qtySell=0 (raro): symbol={} exch={} account={} id={} execId={} lastQty={} cumQty={}",
                            executionReport.getSymbol(),
                            executionReport.getSecurityExchange(),
                            executionReport.getAccount(),
                            executionReport.getId(),
                            executionReport.getExecId(),
                            executionReport.getLastQty(),
                            executionReport.getCumQty());
                }
                position.setPxSell(pxSell);

                position.setCashSell(
                        BigDecimal.valueOf(position.getTradeSell())
                                .abs()
                                .multiply(BigDecimal.valueOf(position.getPxSell()))
                                .doubleValue()
                );
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            position.setSentSell(0d);
            position.setWorkingSell(0d);
        }
    }

    private synchronized void calculateNetValues(BlotterMessage.Position.Builder position) {
        try {
            position.setQtyNet(position.getTradeBuy() + position.getTradeSell());
            position.setAmountNet(position.getCashBoughtBuy() + position.getCashSell());

            if (position.getQtyNet() != 0) {
                BigDecimal aux = BigDecimal.valueOf(position.getAmountNet())
                        .divide(BigDecimal.valueOf(position.getQtyNet()), 6, RoundingMode.HALF_UP)
                        .abs();
                position.setPxNet(aux.doubleValue());
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }
}
