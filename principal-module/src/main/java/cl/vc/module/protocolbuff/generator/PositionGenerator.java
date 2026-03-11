package cl.vc.module.protocolbuff.generator;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class PositionGenerator {

    public static BlotterMessage.Position.Builder calculate(BlotterMessage.Position.Builder position, RoutingMessage.Order order) throws Exception {

        if (order.getSide().equals(RoutingMessage.Side.BUY)) {
            calculateBuyValues(position, order);

        } else if (order.getSide().equals(RoutingMessage.Side.SELL) || order.getSide().equals(RoutingMessage.Side.SELL_SHORT)) {
            calculateSellValues(position , order);
        }

        if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {
            calculateNetValues(position);
        }

        return position;

    }

    private static void calculateBuyValues(BlotterMessage.Position.Builder position, RoutingMessage.Order order) throws Exception {
        if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {

            position.setTradeBuy(BigDecimal.valueOf(order.getLastQty()).add(BigDecimal.valueOf(position.getTradeBuy())).doubleValue());
            BigDecimal pxq = (BigDecimal.valueOf(order.getLastPx()).multiply(BigDecimal.valueOf(order.getLastQty()))).add(BigDecimal.valueOf(position.getAuxBuy()));
            position.setAuxBuy(pxq.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());

            position.setPxBuy((BigDecimal.valueOf(position.getAuxBuy()).compareTo(BigDecimal.valueOf(0d)) == 0) ? BigDecimal.ZERO.doubleValue()
                    : BigDecimal.valueOf(position.getAuxBuy()).divide(BigDecimal.valueOf(position.getTradeBuy()), 6, RoundingMode.HALF_UP).doubleValue());
            position.setCashBoughtBuy(
                    BigDecimal.valueOf(position.getTradeBuy()).multiply(BigDecimal.valueOf(position.getPxBuy())).multiply(BigDecimal.valueOf(-1d)).doubleValue());
        }
    }

    private static void calculateSellValues(BlotterMessage.Position.Builder position, RoutingMessage.Order order) throws Exception {
        if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {

            BigDecimal auxNegative = BigDecimal.valueOf(order.getLastQty()).multiply(new BigDecimal(-1));
            position.setTradeSell(auxNegative.add(BigDecimal.valueOf(position.getTradeSell())).doubleValue());

            if (order.getSide().equals(RoutingMessage.Side.SELL_SHORT)) {
                position.setTradeSellShort(BigDecimal.valueOf(order.getLastQty()).multiply(new BigDecimal(-1))
                        .add(BigDecimal.valueOf(position.getTradeSellShort())).doubleValue());
            }

            BigDecimal pxq = (BigDecimal.valueOf(order.getLastPx()).multiply(BigDecimal.valueOf(order.getLastQty())))
                    .add(BigDecimal.valueOf(position.getAuxBSell()));

            position.setAuxBSell(pxq.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());

            position.setPxSell((position.getAuxBSell() == 0d) ? 0d
                    : BigDecimal.valueOf(position.getAuxBSell()).divide(BigDecimal.valueOf(position.getTradeSell()).abs(), 6, RoundingMode.HALF_UP).doubleValue());
            position.setCashSell(BigDecimal.valueOf(position.getTradeSell()).abs().multiply(BigDecimal.valueOf(position.getPxSell())).doubleValue());

        }
    }

    private static void calculateNetValues(BlotterMessage.Position.Builder position) {
        position.setQtyNet((BigDecimal.valueOf(position.getTradeBuy()).add(BigDecimal.valueOf(position.getTradeSell())).doubleValue()));
        position.setAmountNet(BigDecimal.valueOf(position.getCashBoughtBuy()).add(BigDecimal.valueOf(position.getCashSell())).doubleValue());
        if (BigDecimal.valueOf(position.getQtyNet()).compareTo(BigDecimal.valueOf(0d)) != 0) {
            position.setPxNet(BigDecimal.valueOf(position.getAmountNet()).divide(BigDecimal.valueOf(position.getQtyNet()), 6, RoundingMode.HALF_UP).abs().doubleValue());
        }
    }
}
