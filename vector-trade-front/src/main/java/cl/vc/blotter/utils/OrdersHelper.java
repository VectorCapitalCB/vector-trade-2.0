package cl.vc.blotter.utils;

import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.scene.input.Clipboard;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

@Slf4j
public class OrdersHelper {
    private static final String PRICED_BASKET_HEADER_ROW = "SIDE\tSYMBOL\tMARKET\tQTY\tACCOUNT\tBASKET\tSTRATEGY\tSPREAD\tLIMIT\tBROKER\tPX\n";

    /** Orden EXACTO de columnas que espera el parser (14 obligatorias; BASKET/ExStrategy opcionales 15/16). */
    private static final String[] EXPECTED_COLS = {
            "SIDE", "SYMBOL", "MARKET", "QTY", "ACCOUNT", "STRATEGY", "SPREAD",
            "LIMIT", "BROKER", "PX", "ICEBERG", "COD_OPERADOR", "SETTLTYPE", "SECURITYTYPE"
    };
    private static final String EXPECTED_FORMAT = String.join(" | ", EXPECTED_COLS);

    private OrdersHelper() {
    }

    private static boolean isValidSide(String s) {
        try {
            RoutingMessage.Side.valueOf(s == null ? "" : s.trim());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Fila legible y acotada para los mensajes de error. */
    private static String shorten(String row) {
        String r = row == null ? "" : row.replace("\t", " | ").trim();
        return r.length() > 90 ? r.substring(0, 90) + "…" : r;
    }

    /** Resume hasta 5 errores en un solo texto (evita un popup por fila). */
    private static String summarize(List<String> errors, String fallback) {
        if (errors == null || errors.isEmpty()) return fallback;
        int max = Math.min(5, errors.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < max; i++) sb.append("• ").append(errors.get(i)).append("\n");
        if (errors.size() > max) sb.append("…y ").append(errors.size() - max).append(" más.");
        return sb.toString().trim();
    }
    static double parseNumber(String value) {
        return FlexibleNumberParser.parse(value);
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

    static RoutingMessage.Order createOrderFromString(String row, List<String> errors) {

        try {

            RoutingMessage.Order.Builder routing = RoutingMessage.Order.newBuilder();

            // Saltar posibles filas de encabezado
            if (row.contains("Side\tSymbol")) {
                return null;
            }

            String[] columns = row.split("\t");

            // Validación básica de columnas mínimas
            if (columns.length < EXPECTED_COLS.length) {
                errors.add("Columnas insuficientes (" + columns.length + "/" + EXPECTED_COLS.length + "): " + shorten(row));
                return null;
            }

            String rawSide = columns[0].trim();
            String symbol = columns[1].trim();

            RoutingMessage.Side side;
            try {
                side = RoutingMessage.Side.valueOf(rawSide);
            } catch (IllegalArgumentException e) {
                errors.add("SIDE inválido '" + rawSide + "' (col 1) para " + symbol);
                return null;
            }

            RoutingMessage.SecurityExchangeRouting exchange;
            try {
                exchange = RoutingMessage.SecurityExchangeRouting.valueOf(columns[2].trim());
            } catch (IllegalArgumentException e) {
                errors.add("MERCADO inválido '" + columns[2].trim() + "' (col 3) para " + symbol);
                return null;
            }

            double qty = 0d;

            try {
                qty = parseNumber(columns[3]);
            } catch (Exception e) {
                errors.add(symbol + ": cantidad inválida (" + columns[3] + ")");
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
                errors.add("Estrategia inválida '" + columns[5].trim() + "' para " + symbol);
                return null;
            }

            double spread  = columns[6].equals("") ? 0d : parseNumber(columns[6]);
            double limit   = columns[7].equals("") ? 0d : parseNumber(columns[7]);

            RoutingMessage.ExecBroker broker;
            try {
                broker = RoutingMessage.ExecBroker.valueOf(columns[8].trim());
            } catch (IllegalArgumentException e) {
                errors.add("Broker inválido '" + columns[8].trim() + "' para " + symbol);
                return null;
            }

            double px      = columns[9].equals("") ? 0d : parseNumber(columns[9]);
            double iceberg = columns[10].equals("") ? 0d : parseNumber(columns[10]);

            String codeOperator = columns[11];

            RoutingMessage.SettlType settlType;
            RoutingMessage.SecurityType securityType;
            try {
                settlType = RoutingMessage.SettlType.valueOf(columns[12].trim());
                securityType = RoutingMessage.SecurityType.valueOf(columns[13].replace("\r", "").trim());
            } catch (IllegalArgumentException e) {
                errors.add(symbol + ": liquidación/tipo de instrumento inválido ('" + columns[12].trim() + "' / '" + columns[13].replace("\r", "").trim() + "')");
                return null;
            }
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
                    .setTime(TimeGenerator.getTimeProto())
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
            if (errors != null) errors.add("Fila no procesable: " + shorten(row));
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

        // Filas de datos (descartando encabezados y líneas vacías).
        List<String> dataRows = new ArrayList<>();
        for (StringTokenizer rows = new StringTokenizer(paste, "\n"); rows.hasMoreTokens(); ) {
            String row = rows.nextToken();
            if (row.trim().isEmpty()) continue;
            String upper = row.toUpperCase();
            if (upper.contains("SIDE") && upper.contains("SYMBOL")) continue; // encabezado
            dataRows.add(row);
        }

        if (dataRows.isEmpty()) {
            Notifier.INSTANCE.notifyError("Error cargando basket",
                    "No se encontraron filas de órdenes en el portapapeles.");
            return null;
        }

        // Pre-chequeo de forma con la 1ª fila: detecta el caso típico (faltan SIDE/SYMBOL)
        // y muestra UN SOLO mensaje claro en vez de un popup por cada fila.
        String[] first = dataRows.get(0).split("\t");
        if (first.length < EXPECTED_COLS.length) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cada fila tiene ").append(first.length)
              .append(" columnas y se requieren ").append(EXPECTED_COLS.length).append(".\n");
            if (!isValidSide(first[0])) {
                sb.append("La 1ª columna debe ser SIDE (BUY/SELL) y la 2ª SYMBOL — parecen faltar.\n");
            }
            sb.append("Formato esperado:\n").append(EXPECTED_FORMAT);
            Notifier.INSTANCE.notifyError("Falta(n) columna(s) en la canasta", sb.toString());
            return null;
        }

        List<String> errors = new ArrayList<>();
        List<RoutingMessage.Order> orders = new ArrayList<>();

        for (String row : dataRows) {
            RoutingMessage.Order order = null;
            try {
                order = createOrderFromString(row, errors);
            } catch (Exception e) {
                log.error("Error parsing an order from a string.", e);
                errors.add("Fila no procesable: " + shorten(row));
            }
            if (order != null) orders.add(order);
        }

        if (orders.isEmpty()) {
            Notifier.INSTANCE.notifyError("Error cargando basket",
                    summarize(errors, "No se encontraron órdenes válidas en el portapapeles."));
            return null;
        }

        if (!errors.isEmpty()) {
            // Se cargaron algunas órdenes; avisar UNA sola vez de las descartadas.
            Notifier.INSTANCE.notifyError("Basket con filas inválidas",
                    errors.size() + " fila(s) ignoradas:\n" + summarize(errors, ""));
        }

        // basketID compartido y NO vacío: sin él no se puede agrupar la canasta en su tab
        // (y dos canastas sin id colisionarían en la misma clave "").
        String basketID = orders.get(0).getBasketID();
        if (basketID == null || basketID.trim().isEmpty()) {
            basketID = "BKT-" + IDGenerator.getID();
        }
        final String bid = basketID;
        List<RoutingMessage.Order> stamped = new ArrayList<>(orders.size());
        for (RoutingMessage.Order o : orders) {
            stamped.add(bid.equals(o.getBasketID()) ? o : o.toBuilder().setBasketID(bid).build());
        }

        BktStrategyProtos.Basket.Builder basketBuilder = BktStrategyProtos.Basket.newBuilder();
        basketBuilder.setBasketID(bid);
        basketBuilder.setAccount(stamped.get(0).getAccount());
        basketBuilder.addAllOrders(stamped);

        return basketBuilder.build();
    }

}
