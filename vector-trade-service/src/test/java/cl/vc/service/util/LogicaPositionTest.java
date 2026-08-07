package cl.vc.service.util;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class LogicaPositionTest {

    @Test
    void sellNewOrderReservesHistoricalQuantityAndBlocksSecondSell() {
        ActorRef self = mock(ActorRef.class);
        BlotterMessage.Balance.Builder balance = BlotterMessage.Balance.newBuilder()
                .setCuenta("12336718/9")
                .setSaldoDisponible(1_000_000d)
                .setCupo(1_000_000d);
        HashMap<String, BlotterMessage.PositionHistory.Builder> history = new HashMap<>();
        history.put("HIPERMARC", position("12336718/9", "HIPERMARC", 100_000d));

        LogicaPosition logic = new LogicaPosition(0d, self, balance, history, new HashMap<>());
        RoutingMessage.Order firstSell = order("sell-1", "HIPERMARC", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 16.09d, 100_000d, 100_000d, 0d);

        assertTrue(logic.calculateBalanceReplace(RoutingMessage.NewOrderRequest.newBuilder().setOrder(firstSell).build()));
        logic.orderUpdate(firstSell, null);

        assertEquals(0d, history.get("HIPERMARC").getAvailableQuantity(), 0.0001d);
        assertEquals(1_609_000d, balance.getOrdenesActivasVentas(), 0.0001d);

        RoutingMessage.Order secondSell = order("sell-2", "HIPERMARC", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 16.09d, 1d, 1d, 0d);

        assertFalse(logic.calculateBalanceReplace(RoutingMessage.NewOrderRequest.newBuilder().setOrder(secondSell).build()));
        verify(self).tell(any(RoutingMessage.Order.class), isNull());
    }

    @Test
    void activeOrderTotalsUseEachStoredOrder() {
        ActorRef self = mock(ActorRef.class);
        BlotterMessage.Balance.Builder balance = BlotterMessage.Balance.newBuilder()
                .setCuenta("12336718/9")
                .setSaldoDisponible(10_000d)
                .setCupo(10_000d);

        LogicaPosition logic = new LogicaPosition(0d, self, balance, new HashMap<>(), new HashMap<>());
        RoutingMessage.Order buyOne = order("buy-1", "ECL", RoutingMessage.Side.BUY,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 10d, 10d, 10d, 0d);
        RoutingMessage.Order buyTwo = order("buy-2", "ILC", RoutingMessage.Side.BUY,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 50d, 5d, 5d, 0d);

        logic.orderUpdate(buyOne, null);
        logic.orderUpdate(buyTwo, null);

        assertEquals(350d, balance.getOrdenesActivasCompras(), 0.0001d);
        verify(self, never()).tell(any(BlotterMessage.Balance.class), any());
        verify(self, never()).tell(any(BlotterMessage.SnapshotPositionHistory.class), any());
    }

    @Test
    void partialBuyFollowedByTenPriceChangesReservesOnlyLeaves() {
        ActorRef self = mock(ActorRef.class);
        BlotterMessage.Balance.Builder balance = BlotterMessage.Balance.newBuilder()
                .setCuenta("12336718/9")
                .setSaldoDisponible(1_000_000d)
                .setCupo(1_000_000d);
        LogicaPosition logic = new LogicaPosition(0d, self, balance, new HashMap<>(), new HashMap<>());

        RoutingMessage.Order current = order("buy-partial", "ECL", RoutingMessage.Side.BUY,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW, 10d, 100d, 100d, 0d);
        logic.orderUpdate(current, null);
        assertEquals(999_000d, balance.getSaldoDisponible(), 0.0001d);

        current = current.toBuilder()
                .setExecId("trade-1")
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .setLastPx(10d)
                .setLastQty(40d)
                .setCumQty(40d)
                .setLeaves(60d)
                .build();
        logic.orderUpdate(current, null);

        for (int price = 11; price <= 20; price++) {
            RoutingMessage.OrderReplaceRequest replace = RoutingMessage.OrderReplaceRequest.newBuilder()
                    .setId(current.getId())
                    .setPrice(price)
                    .setQuantity(100d)
                    .build();

            assertTrue(logic.calculateBalanceReplace(replace, current));

            RoutingMessage.Order replaced = current.toBuilder()
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED)
                    .setOrdStatus(RoutingMessage.OrderStatus.REPLACED)
                    .setPrice(price)
                    .setOrderQty(100d)
                    .setCumQty(40d)
                    .setLeaves(60d)
                    .build();
            logic.orderUpdate(replaced, current);

            assertEquals(1_000_000d - 400d - (60d * price),
                    balance.getSaldoDisponible(), 0.0001d);
            current = replaced;
        }
    }

    @Test
    void partialSellFollowedByTenPriceChangesKeepsCustodyReservedUntilCancel() {
        ActorRef self = mock(ActorRef.class);
        BlotterMessage.Balance.Builder balance = BlotterMessage.Balance.newBuilder()
                .setCuenta("47024924/0")
                .setSaldoDisponible(1_000_000d)
                .setCupo(1_000_000d);
        HashMap<String, BlotterMessage.PositionHistory.Builder> history = new HashMap<>();
        history.put("CFIETFIPSA", position("47024924/0", "CFIETFIPSA", 300_000d));
        LogicaPosition logic = new LogicaPosition(0d, self, balance, history, new HashMap<>());

        RoutingMessage.Order current = order("let2orcnlov6", "CFIETFIPSA", RoutingMessage.Side.SELL,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW,
                1328d, 300_000d, 300_000d, 0d);
        logic.orderUpdate(current, null);
        assertEquals(0d, history.get("CFIETFIPSA").getAvailableQuantity(), 0.0001d);

        current = current.toBuilder()
                .setExecId("trade-1")
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                .setLastPx(1334.9d)
                .setLastQty(37_549d)
                .setCumQty(37_549d)
                .setLeaves(262_451d)
                .build();
        logic.orderUpdate(current, null);

        for (int i = 0; i < 10; i++) {
            RoutingMessage.Order replaced = current.toBuilder()
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED)
                    .setOrdStatus(RoutingMessage.OrderStatus.REPLACED)
                    .setPrice(1335d + i)
                    .setOrderQty(300_000d)
                    .setCumQty(37_549d)
                    .setLeaves(262_451d)
                    .build();
            logic.orderUpdate(replaced, current);

            assertEquals(0d, history.get("CFIETFIPSA").getAvailableQuantity(), 0.0001d,
                    "un repricing no debe liberar ni volver a descontar custodia");
            current = replaced;
        }

        RoutingMessage.Order canceled = current.toBuilder()
                .setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED)
                .setOrdStatus(RoutingMessage.OrderStatus.CANCELED)
                .build();
        logic.orderUpdate(canceled, current);

        assertEquals(262_451d, history.get("CFIETFIPSA").getAvailableQuantity(), 0.0001d,
                "la cancelación debe devolver únicamente el saldo no ejecutado");
    }

    @Test
    void buyCancelRestoresReservedCashExactlyOnce() {
        ActorRef self = mock(ActorRef.class);
        BlotterMessage.Balance.Builder balance = BlotterMessage.Balance.newBuilder()
                .setCuenta("16936145/9")
                .setSaldoDisponible(1_000d)
                .setCupo(1_000d);
        LogicaPosition logic = new LogicaPosition(0d, self, balance, new HashMap<>(), new HashMap<>());

        RoutingMessage.Order active = order("buy-cancel", "AGUAS-A", RoutingMessage.Side.BUY,
                RoutingMessage.ExecutionType.EXEC_NEW, RoutingMessage.OrderStatus.NEW,
                10d, 50d, 50d, 0d);
        logic.orderUpdate(active, null);

        RoutingMessage.Order canceled = active.toBuilder()
                .setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED)
                .setOrdStatus(RoutingMessage.OrderStatus.CANCELED)
                .build();
        logic.orderUpdate(canceled, active);

        assertEquals(1_000d, balance.getSaldoDisponible(), 0.0001d,
                "un único CANCEL debe devolver exactamente la reserva");
    }

    private static BlotterMessage.PositionHistory.Builder position(String account, String symbol, double quantity) {
        return BlotterMessage.PositionHistory.newBuilder()
                .setAccount(account)
                .setInstrument(symbol)
                .setAvailableQuantity(quantity);
    }

    private static RoutingMessage.Order order(String id,
                                              String symbol,
                                              RoutingMessage.Side side,
                                              RoutingMessage.ExecutionType execType,
                                              RoutingMessage.OrderStatus status,
                                              double price,
                                              double orderQty,
                                              double leaves,
                                              double cumQty) {
        return RoutingMessage.Order.newBuilder()
                .setId(id)
                .setAccount("12336718/9")
                .setSymbol(symbol)
                .setSide(side)
                .setSecurityExchange(RoutingMessage.SecurityExchangeRouting.XSGO)
                .setExecType(execType)
                .setOrdStatus(status)
                .setPrice(price)
                .setOrderQty(orderQty)
                .setLeaves(leaves)
                .setCumQty(cumQty)
                .build();
    }
}
