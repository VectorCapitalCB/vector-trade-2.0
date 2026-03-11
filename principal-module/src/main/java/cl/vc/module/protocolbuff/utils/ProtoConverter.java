package cl.vc.module.protocolbuff.utils;

import cl.vc.module.protocolbuff.keycloak.KeycloakService;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ProtoConverter {

    private static Map<String, String> routingDecryptMaps;
    private static Map<String, String> routingEncryptMaps;

    static {

        routingDecryptMaps = new HashMap<>();

        routingDecryptMaps.put("Nuevo", RoutingMessage.OrderStatus.NEW.name());
        routingDecryptMaps.put("Parcialmente completado", RoutingMessage.OrderStatus.PARTIALLY_FILLED.name());
        routingDecryptMaps.put("Ejecutado", RoutingMessage.OrderStatus.FILLED.name());
        routingDecryptMaps.put("Completado por el día", RoutingMessage.OrderStatus.DONE_FOR_DAY.name());
        routingDecryptMaps.put("Cancelado", RoutingMessage.OrderStatus.CANCELED.name());
        routingDecryptMaps.put("Reemplazado", RoutingMessage.OrderStatus.REPLACED.name());
        routingDecryptMaps.put("Pendiente de cancelación", RoutingMessage.OrderStatus.PENDING_CANCEL.name());
        routingDecryptMaps.put("Detenido", RoutingMessage.OrderStatus.STOPPED.name());
        routingDecryptMaps.put("Rechazado", RoutingMessage.OrderStatus.REJECTED.name());
        routingDecryptMaps.put("Suspendido", RoutingMessage.OrderStatus.SUSPENDED.name());
        routingDecryptMaps.put("Nuevo pendiente", RoutingMessage.OrderStatus.PENDING_NEW.name());
        routingDecryptMaps.put("Calculado", RoutingMessage.OrderStatus.CALCULATED.name());
        routingDecryptMaps.put("Expirado", RoutingMessage.OrderStatus.EXPIRED.name());
        routingDecryptMaps.put("Pendiente de reemplazo", RoutingMessage.OrderStatus.PENDING_REPLACE.name());
        routingDecryptMaps.put("Abortado", RoutingMessage.OrderStatus.ABORTED.name());
        routingDecryptMaps.put("Palo", RoutingMessage.OrderStatus.TRADE.name());
        routingDecryptMaps.put("Vivo/Ejecutada", RoutingMessage.OrderStatus.LIVE_TRADE.name());
        routingDecryptMaps.put("En vivo", RoutingMessage.OrderStatus.LIVE.name());
        routingDecryptMaps.put("Pendiente de activación", RoutingMessage.OrderStatus.PENDING_LIVE.name());
        routingDecryptMaps.put("Sólo pendiente", RoutingMessage.OrderStatus.PENDING_ONLY.name());
        routingDecryptMaps.put("Todos los estados", RoutingMessage.OrderStatus.ALL_STATUS.name());

        routingDecryptMaps.put("Nueva ejecución", RoutingMessage.ExecutionType.EXEC_NEW.name());
        routingDecryptMaps.put("Ejecución finalizada por el día", RoutingMessage.ExecutionType.EXEC_DONE_FOR_DAY.name());
        routingDecryptMaps.put("Ejecución cancelada", RoutingMessage.ExecutionType.EXEC_CANCELED.name());
        routingDecryptMaps.put("Ejecución reemplazada", RoutingMessage.ExecutionType.EXEC_REPLACED.name());
        routingDecryptMaps.put("Ejecución pendiente de cancelación", RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL.name());
        routingDecryptMaps.put("Ejecución pendiente de reemplazo", RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE.name());
        routingDecryptMaps.put("Ejecución rechazada", RoutingMessage.ExecutionType.EXEC_REJECTED.name());
        routingDecryptMaps.put("Ejecución de operación", RoutingMessage.ExecutionType.EXEC_TRADE.name());
        routingDecryptMaps.put("Ejecución reexpresada", RoutingMessage.ExecutionType.EXEC_RESTATED.name());
        routingDecryptMaps.put("Ejecución corregida", RoutingMessage.ExecutionType.EXEC_CORRECT.name());

        routingDecryptMaps.put("Sin lado", RoutingMessage.Side.NONE_SIDE.name());
        routingDecryptMaps.put("Compra", RoutingMessage.Side.BUY.name());
        routingDecryptMaps.put("Venta", RoutingMessage.Side.SELL.name());
        routingDecryptMaps.put("Venta corta", RoutingMessage.Side.SELL_SHORT.name());
        routingDecryptMaps.put("Todos los lados", RoutingMessage.Side.ALL_SIDE.name());

        routingDecryptMaps.put("PH", RoutingMessage.SettlType.CASH.name());
        routingDecryptMaps.put("PM", RoutingMessage.SettlType.NEXT_DAY.name());
        routingDecryptMaps.put("CN", RoutingMessage.SettlType.T2.name());

        routingEncryptMaps = new HashMap<>();

        routingEncryptMaps.put(RoutingMessage.OrderStatus.NEW.name(),"Nuevo" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.PARTIALLY_FILLED.name(),"Parcialmente completado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.FILLED.name(),"Ejecutado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.DONE_FOR_DAY.name(),"Completado por el día" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.CANCELED.name(),"Cancelado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.REPLACED.name(),"Reemplazado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.PENDING_CANCEL.name(),"Pendiente de cancelación" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.STOPPED.name(),"Detenido" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.REJECTED.name(),"Rechazado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.SUSPENDED.name(),"Suspendido" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.PENDING_NEW.name(),"Nuevo pendiente" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.CALCULATED.name(),"Calculado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.EXPIRED.name(),"Expirado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.PENDING_REPLACE.name(),"Pendiente de reemplazo" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.ABORTED.name(),"Abortado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.TRADE.name(),"Ejecutado" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.LIVE_TRADE.name(),"Operación en vivo" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.LIVE.name(),"En vivo" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.PENDING_LIVE.name(),"Pendiente de activación" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.PENDING_ONLY.name(),"Sólo pendiente" );
        routingEncryptMaps.put(RoutingMessage.OrderStatus.ALL_STATUS.name(),"Todos los estados" );

        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_NEW.name(),"ejecución" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_DONE_FOR_DAY.name(),"Ejecución finalizada por el día" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_CANCELED.name(),"Ejecución cancelada" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_REPLACED.name(),"Ejecución reemplazada" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL.name(),"Ejecución pendiente de cancelación" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE.name(),"Ejecución pendiente de reemplazo" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_REJECTED.name(),"Ejecución rechazada" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_TRADE.name(),"Ejecución de operación" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_RESTATED.name(),"Ejecución reexpresada" );
        routingEncryptMaps.put(RoutingMessage.ExecutionType.EXEC_CORRECT.name(),"Ejecución corregida" );

        routingEncryptMaps.put(RoutingMessage.Side.NONE_SIDE.name(),"Sin lado" );
        routingEncryptMaps.put(RoutingMessage.Side.BUY.name(),"Compra" );
        routingEncryptMaps.put(RoutingMessage.Side.SELL.name(),"Venta" );
        routingEncryptMaps.put(RoutingMessage.Side.SELL_SHORT.name(),"Venta corta" );
        routingEncryptMaps.put(RoutingMessage.Side.ALL_SIDE.name(),"Todos los lados" );

        routingEncryptMaps.put(RoutingMessage.SettlType.CASH.name(),"PH" );
        routingEncryptMaps.put(RoutingMessage.SettlType.NEXT_DAY.name(),"PM" );
        routingEncryptMaps.put(RoutingMessage.SettlType.T2.name(),"CN" );

    }

    public static String routingDecryptStatus(String enums){
        if(routingEncryptMaps.containsKey(enums)){
            return routingEncryptMaps.get(enums);
        }
        return enums;

    }


    public static String routingEncryptStatus(String enums){
        if(routingDecryptMaps.containsKey(enums)){
            return routingDecryptMaps.get(enums);
        }
        return enums;

    }




    public static void main(String[] arg){
        try {

            String descrypt = RoutingMessage.OrderStatus.FILLED.name();

            String status = routingDecryptStatus(descrypt);

            String statuss = routingEncryptStatus(status);

            System.out.println(statuss);


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

}
