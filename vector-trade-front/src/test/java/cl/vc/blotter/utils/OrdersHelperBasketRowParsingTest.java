package cl.vc.blotter.utils;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.blotter.Repository;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrdersHelperBasketRowParsingTest {

    @Test
    void parsesAllRowsFromTheEtfBasketWithChileanExcelNumbers() {
        Repository.setUsername("test-user");
        List<String> rows = List.of(
                "BUY\tCFMITNIPSA\tXSGO\t80.000\t47024924/0\tBEST\t0,00\t5001\tVC\t0\t10\t002\tT2\tETF",
                "SELL\tCFMITNIPSA\tXSGO\t80.000\t47024924/0\tBEST\t0,00\t5299\tVC\t0\t10\t002\tT2\tETF",
                "BUY\tCFMITNIPSA\tXSGO\t100.000\t47024924/0\tBEST\t0,00\t4901\tVC\t0\t10\t002\tT2\tETF",
                "SELL\tCFMITNIPSA\tXSGO\t100.000\t47024924/0\tBEST\t0,00\t5499\tVC\t0\t10\t002\tT2\tETF",
                "BUY\tCFIETFIPSA\tXSGO\t150.000\t47024924/0\tHOLGURA\t2,00\t0\tVC\t1301\t10\t002\tT2\tETF",
                "SELL\tCFIETFIPSA\tXSGO\t150.000\t47024924/0\tHOLGURA\t2,00\t0\tVC\t1448\t10\t002\tT2\tETF",
                "BUY\tCFIETFIPSA\tXSGO\t300.000\t47024924/0\tHOLGURA\t2,00\t0\tVC\t1251\t10\t002\tT2\tETF",
                "SELL\tCFIETFIPSA\tXSGO\t300.000\t47024924/0\tHOLGURA\t2,00\t0\tVC\t1499\t10\t002\tT2\tETF",
                "BUY\tCFIDHS2-A\tXSGO\t2.000\t47024924/0\tHOLGURA\t100,00\t0\tVC\t35005\t10\t002\tT2\tCFI",
                "SELL\tCFIDHS2-A\tXSGO\t3.000\t47024924/0\tHOLGURA\t100,00\t0\tVC\t38500\t10\t003\tT3\tCFI"
        );

        List<String> errors = new ArrayList<>();
        List<RoutingMessage.Order> orders = rows.stream()
                .map(row -> OrdersHelper.createOrderFromString(row, errors))
                .toList();

        assertEquals(10, orders.size());
        orders.forEach(order -> assertNotNull(order));
        assertEquals(8, orders.stream().filter(order -> order.getSecurityType() == RoutingMessage.SecurityType.ETF).count());
        assertEquals(2, orders.stream().filter(order -> order.getSecurityType() == RoutingMessage.SecurityType.CFI).count());
        assertEquals(80_000d, orders.get(0).getOrderQty());
        assertEquals(150_000d, orders.get(4).getOrderQty());
        assertEquals(2_000d, orders.get(8).getOrderQty());
        assertEquals(List.of(), errors);
    }
}
