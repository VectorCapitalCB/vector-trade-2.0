package cl.vc.blotter.utils;

import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.scene.input.Clipboard;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

@Slf4j
public class OrdersHelper {
    private static final String PRICED_BASKET_HEADER_ROW = "SIDE\tSYMBOL\tMARKET\tQTY\tACCOUNT\tBASKET\tSTRATEGY\tSPREAD\tLIMIT\tBROKER\tPX\n";

    private OrdersHelper() {
    }
    private static double parseNumber(String value) {
        if (value == null) return 0d;

        String cleaned = value.trim();
        if (cleaned.isEmpty()) return 0d;

        // Quitar espacios normales y no separables (Excel mete NBSP a veces)
        cleaned = cleaned.replace("\u00A0", "").replace(" ", "");

        // Quitar todo lo que NO sea dígito, coma, punto o signo menos
        cleaned = cleaned.replaceAll("[^0-9,.-]", "");

        if (cleaned.isEmpty()) {
            return 0d;
        }

        int commaCount = cleaned.length() - cleaned.replace(",", "").length();
        int dotCount   = cleaned.length() - cleaned.replace(".", "").length();

        boolean hasComma = commaCount > 0;
        boolean hasDot   = dotCount > 0;

        if (hasComma && hasDot) {
            // Tiene punto y coma: decidir por el último separador
            int lastDot = cleaned.lastIndexOf('.');
            int lastComma = cleaned.lastIndexOf(',');

            if (lastDot > lastComma) {
                // Ej: "1,234,567.89" -> estilo US
                //  - comas: miles (se eliminan)
                //  - punto: decimal (queda)
                cleaned = cleaned.replace(",", "");
            } else {
                // Ej: "1.234.567,89" -> estilo latam
                //  - puntos: miles (se eliminan)
                //  - coma: decimal (a punto)
                cleaned = cleaned.replace(".", "").replace(",", ".");
            }

        } else if (hasComma) {
            // Solo comas: puede ser miles o decimal
            if (commaCount > 1) {
                // Ej: "1,990,000" -> miles
                cleaned = cleaned.replace(",", "");
            } else {
                int idx = cleaned.indexOf(',');
                String before = cleaned.substring(0, idx);
                String after  = cleaned.substring(idx + 1);

                // Ej: "1,234" -> miles
                if (before.replace("-", "").matches("\\d+")
                        && after.matches("\\d{3}")
                        && before.length() <= 3) {
                    cleaned = before + after; // "1,234" -> "1234"
                } else {
                    // Ej: "123,45" -> decimal
                    cleaned = before + "." + after;
                }
            }

        } else if (hasDot) {
            // Solo puntos: puede ser miles o decimal
            if (dotCount > 1) {
                // Ej: "1.234.567" -> miles
                cleaned = cleaned.replace(".", "");
            } else {
                int idx = cleaned.indexOf('.');
                String before = cleaned.substring(0, idx);
                String after  = cleaned.substring(idx + 1);

                // Ej: "1.234" -> miles
                if (before.replace("-", "").matches("\\d+")
                        && after.matches("\\d{3}")
                        && before.length() <= 3) {
                    cleaned = before + after; // "1.234" -> "1234"
                }
                // Si no cumple, lo dejamos como decimal: "1234.56"
            }
        }

        return Double.parseDouble(cleaned);
    }



    public static BktStrategyProtos.Basket sendNewOrdersFromClipboard() {
        try {
            return createOrdersFromClipboard();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public static BktStrategyProtos.Basket sendNewBasketFromClipboard() {
        try {
            return createOrdersFromClipboard();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private static RoutingMessage.Order createOrderFromString(String row) {

        try {

            RoutingMessage.Order.Builder routing = RoutingMessage.Order.newBuilder();

            // Saltar posibles filas de encabezado
            if (row.contains("Side\tSymbol")) {
                return null;
            }

            String[] columns = row.split("\t");

            // Validación básica de columnas mínimas
            if (columns.length < 14) {
                Notifier.INSTANCE.notifyError(
                        "Error cargando basket",
                        "Fila inválida: columnas insuficientes -> " + row
                );
                return null;
            }

            String rawSide = columns[0].trim();
            String symbol = columns[1].trim();

            RoutingMessage.Side side;
            try {
                side = RoutingMessage.Side.valueOf(rawSide);
            } catch (IllegalArgumentException e) {
                Notifier.INSTANCE.notifyError(
                        "Error cargando basket",
                        "Side inválido '" + rawSide + "' para símbolo " + symbol
                );
                return null;
            }

            RoutingMessage.SecurityExchangeRouting exchange =
                    RoutingMessage.SecurityExchangeRouting.valueOf(columns[2]);

            double qty = 0d;

            try {
                qty = parseNumber(columns[3]);
            } catch (Exception e) {
                Notifier.INSTANCE.notifyError(
                        "Error loading basket",
                        symbol + ": invalid quantity (" + columns[3] + " - " + e.getMessage() + ")"
                );
                return null;
            }

            if (qty <= 0d) return null;

            String account = columns[4].trim();

            RoutingMessage.StrategyOrder strategy = RoutingMessage.StrategyOrder.NONE_STRATEGY;

            try {
                if (!columns[5].equals("")) {
                    strategy = RoutingMessage.StrategyOrder.valueOf(columns[5].trim());
                }
            } catch (IllegalArgumentException e) {
                Notifier.INSTANCE.notifyError("Error en el nombre de la estrategia", columns[5].trim());
                return null;
            }

            double spread  = columns[6].equals("") ? 0d : parseNumber(columns[6]);
            double limit   = columns[7].equals("") ? 0d : parseNumber(columns[7]);

            RoutingMessage.ExecBroker broker;
            try {
                broker = RoutingMessage.ExecBroker.valueOf(columns[8].trim());
            } catch (IllegalArgumentException e) {
                Notifier.INSTANCE.notifyError(
                        "Error cargando basket",
                        "Broker inválido '" + columns[8] + "' para símbolo " + symbol
                );
                return null;
            }

            double px      = columns[9].equals("") ? 0d : parseNumber(columns[9]);
            double iceberg = columns[10].equals("") ? 0d : parseNumber(columns[10]);

            String codeOperator = columns[11];

            RoutingMessage.SettlType settlType = RoutingMessage.SettlType.valueOf(columns[12]);
            RoutingMessage.SecurityType securityType =
                    RoutingMessage.SecurityType.valueOf(columns[13].replace("\r", ""));
            routing.setSecurityType(securityType);

            String basketID = columns.length > 14 ? columns[14].replace("\r", "") : "";

            RoutingMessage.ExStrategy exStrategy = RoutingMessage.ExStrategy.NO_STRATEGY;

            if (columns.length > 15 && !columns[15].equals("")) {
                exStrategy = RoutingMessage.ExStrategy.valueOf(columns[15].replace("\r", ""));
            }

            if (iceberg > 0d) {
                Double maxFloor = qty * iceberg / 100;
                routing.setMaxFloor(maxFloor.intValue());
                routing.setIcebergPercentage(columns[10]);
            }

            if (columns.length == 11 && !columns[10].equals("")) {
                routing.setMaxFloor((int) parseNumber(columns[10]));
            }

            return routing.setOrdType(RoutingMessage.OrdType.LIMIT)
                    .setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW)
                    .setHandlInst(RoutingMessage.HandlInst.PRIVATE_ORDER)
                    .setOperator(Repository.username)
                    .setId(IDGenerator.getID())
                    .setExStrategy(exStrategy)
                    .setSide(side)
                    .setSymbol(symbol)
                    .setSecurityExchange(exchange)
                    .setOrderQty(qty)
                    .setAccount(account)
                    .setTime(TimeGenerator.toProtoTimestampUTC(LocalDateTime.now()))
                    .setStrategyOrder(strategy)
                    .setSettlType(settlType)
                    .setSpread(spread)
                    .setCodeOperator(codeOperator)
                    .setLimit(limit)
                    .setBroker(broker)
                    .setPrice(px)
                    .setBasketID(basketID)
                    .build();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    private static BktStrategyProtos.Basket createOrdersFromClipboard() throws IOException {

        Clipboard clipboard = Clipboard.getSystemClipboard();
        String paste = clipboard.getString();

        if (paste == null || paste.trim().isEmpty()) {
            Notifier.INSTANCE.notifyError(
                    "Error cargando basket",
                    "El portapapeles está vacío o no contiene datos de órdenes."
            );
            return null;
        }

        paste = paste.replace(PRICED_BASKET_HEADER_ROW, "");

        StringTokenizer rows = new StringTokenizer(paste, "\n");

        List<RoutingMessage.Order> orders = new ArrayList<>();

        while (rows.hasMoreTokens()) {
            String row = rows.nextToken();

            if (row.contains("SIDE")) continue;

            RoutingMessage.Order order = null;

            try {
                order = createOrderFromString(row);
            } catch (Exception e) {
                log.error("Error parsing an order from a string.", e);
                Notifier.INSTANCE.notifyError("Error cargando basket", e.getMessage());
            }

            if (order != null) orders.add(order);
        }

        if (orders.isEmpty()) {
            Notifier.INSTANCE.notifyError(
                    "Error cargando basket",
                    "No se encontraron órdenes válidas en el portapapeles."
            );
            return null;
        }

        BktStrategyProtos.Basket.Builder basketBuilder = BktStrategyProtos.Basket.newBuilder();
        basketBuilder.setBasketID(orders.get(0).getBasketID());
        basketBuilder.setAccount(orders.get(0).getAccount());
        basketBuilder.addAllOrders(orders);

        return basketBuilder.build();
    }

}
