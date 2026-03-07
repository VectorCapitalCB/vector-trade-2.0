package cl.vc.service.util;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class CalculoCreasys {

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static List<BlotterMessage.Simultaneas> getAllSimultaneas() {
        return SQLServerConnection.consultaSimultanea();
    }

    public static BlotterMessage.SnapshotPrestamos.Builder snapshotPrestamos(String numCuenta) {
        BlotterMessage.SnapshotPrestamos.Builder snapshot = BlotterMessage.SnapshotPrestamos.newBuilder();
        try {

            snapshot.setId("snapshot-prestamos-" + numCuenta);
            snapshot.setAccount(numCuenta);
            List<BlotterMessage.Prestamos.Builder> lista = SQLServerConnection.prestamos(numCuenta);
            for (BlotterMessage.Prestamos.Builder pb : lista) {
                snapshot.addPrestamos(pb.build());
            }
        } catch (Exception e) {
            log.error("Error construyendo SnapshotPrestamos para cuenta {}: {}", numCuenta, e.getMessage(), e);
        }
        return snapshot;
    }


    public static HashMap<RoutingMessage.Currency, BlotterMessage.Patrimonio.Builder> saldoCaja(String prefixAccount) {

        HashMap<RoutingMessage.Currency, BlotterMessage.Patrimonio.Builder> patrimonioMaps = new HashMap<>();
        try {

            List<BlotterMessage.SaldoCaja.Builder> saldoCaja = SQLServerConnection.saldoCaja(prefixAccount);

            if (saldoCaja != null) {
                saldoCaja.forEach(s -> {
                    RoutingMessage.Currency currency;

                    // Detectamos la moneda a partir de 'tipoCaja'
                    String tipoCaja = s.getTipoCaja();
                    if (tipoCaja != null) {
                        if (tipoCaja.contains("CLP") || tipoCaja.contains("PESO")) {
                            currency = RoutingMessage.Currency.CLP;
                        } else if (tipoCaja.contains("USD") || tipoCaja.contains("DOLAR")) {
                            currency = RoutingMessage.Currency.USD;
                        } else if (tipoCaja.contains("EUR")) {
                            currency = RoutingMessage.Currency.EUR;
                        } else {
                            currency = RoutingMessage.Currency.NO_CURRENCY;
                        }
                    } else {
                        currency = RoutingMessage.Currency.NO_CURRENCY;
                    }


                    double montoCaja = 0.0;
                    double montoTransito = 0.0;
                    double garantiaEfectivo = 0.0;
                    double prestamo = 0.0;

                    try {
                        montoCaja = Double.parseDouble(s.getMontoMonedaCaja());
                    } catch (NumberFormatException e) {
                        log.error("Error parseando montoMonedaCaja: {}", s.getMontoMonedaCaja());
                    }
                    try {
                        montoTransito = Double.parseDouble(s.getMontoTransito());
                    } catch (NumberFormatException e) {
                        log.error("Error parseando montoTransito: {}", s.getMontoTransito());
                    }
                    try {
                        String garantiaStr = s.getGarantiaEfectivo();
                        if (garantiaStr != null && !garantiaStr.trim().isEmpty()) {
                            garantiaEfectivo = Double.parseDouble(garantiaStr);
                        }
                    } catch (NumberFormatException e) {
                        log.error("Error parseando garantiaEfectivo: {}", s.getGarantiaEfectivo());
                    }

                    try {

                        prestamo = Double.parseDouble(s.getPrestamo());
                    } catch (NumberFormatException e) {
                        log.error("Error parseando prestamo: {}", s.getPrestamo());
                    }

                    // Creamos un Patrimonio con los valores leídos
                    BlotterMessage.Patrimonio.Builder nuevoPatrimonio = BlotterMessage.Patrimonio.newBuilder()
                            .setCaja(
                                    BlotterMessage.ValuesPatrimonio.newBuilder().setValues(montoCaja)
                            )
                            .setCuentaTransitoriasPorCobrarPagar(
                                    BlotterMessage.ValuesPatrimonio.newBuilder().setValues(montoTransito)
                            )
                            .setGarantiaEfectivo(
                                    BlotterMessage.ValuesPatrimonio.newBuilder().setValues(garantiaEfectivo)
                            )
                            .setPrestamos(
                                    BlotterMessage.ValuesPatrimonio.newBuilder().setValues(prestamo)
                            );

                    BlotterMessage.Patrimonio.Builder existingPatrimonio = patrimonioMaps.get(currency);

                    if (existingPatrimonio != null) {

                        // Caja existente + nueva caja
                        double oldCaja = existingPatrimonio.getCaja().getValues();
                        double newCaja = nuevoPatrimonio.getCaja().getValues();
                        double mergedCaja = oldCaja + newCaja;

                        // Tránsito existente + nuevo tránsito
                        double oldTransito = existingPatrimonio.getCuentaTransitoriasPorCobrarPagar().getValues();
                        double newTransito = nuevoPatrimonio.getCuentaTransitoriasPorCobrarPagar().getValues();
                        double mergedTransito = oldTransito + newTransito;

                        // Garantía en efectivo existente + nueva garantía
                        double oldGarantiaEfectivo = existingPatrimonio.getGarantiaEfectivo().getValues();
                        double newGarantiaEfectivo = nuevoPatrimonio.getGarantiaEfectivo().getValues();
                        double mergedGarantiaEfectivo = oldGarantiaEfectivo + newGarantiaEfectivo;

                        // Prestamo existente + nuevo préstamo
                        double oldPrestamo = existingPatrimonio.getPrestamos().getValues();
                        double newPrestamo = nuevoPatrimonio.getPrestamos().getValues();
                        double mergedPrestamo = oldPrestamo + newPrestamo;

                        // Actualizamos el existing con la suma
                        existingPatrimonio.setCaja(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(mergedCaja));
                        existingPatrimonio.setCuentaTransitoriasPorCobrarPagar(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(mergedTransito));
                        existingPatrimonio.setGarantiaEfectivo(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(mergedGarantiaEfectivo));
                        existingPatrimonio.setPrestamos(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(mergedPrestamo));

                        patrimonioMaps.put(currency, existingPatrimonio);

                    } else {
                        patrimonioMaps.put(currency, nuevoPatrimonio);
                    }

                });
            } else {
                log.warn("La lista de saldoCaja es null para la cuenta: {}", prefixAccount);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return patrimonioMaps;
    }

    public static BlotterMessage.SnapshotPositionHistory.Builder cierreCarteraResumida(
            String prefixAccount,
            HashMap<RoutingMessage.Currency, BlotterMessage.Patrimonio.Builder> patrimonioMaps,
            HashMap<String, BlotterMessage.Simultaneas> simultaneas) {

        BlotterMessage.SnapshotPositionHistory.Builder snapshotPositionHistory =
                BlotterMessage.SnapshotPositionHistory.newBuilder();

        try {
            List<BlotterMessage.CierreCarteraResumida.Builder> cierre =
                    SQLServerConnection.carteraResumida(prefixAccount);

            cierre.forEach(s -> {

                RoutingMessage.Currency currency;

                if (s.getCodigoMoneda().contains("CLP")) {
                    currency = RoutingMessage.Currency.CLP;
                } else if (s.getCodigoMoneda().contains("EUR")) {
                    currency = RoutingMessage.Currency.EUR;
                } else if (s.getCodigoMoneda().contains("DOLAR") || s.getCodigoMoneda().contains("USD")) {
                    currency = RoutingMessage.Currency.USD;
                } else {
                    currency = RoutingMessage.Currency.NO_CURRENCY;
                }

                BlotterMessage.Patrimonio.Builder patrimonio = patrimonioMaps.get(currency);

                if (patrimonio == null) {
                    log.warn("No existe patrimonio para moneda: {}. Se crea un Patrimonio.Builder vacío", currency);
                    patrimonio = BlotterMessage.Patrimonio.newBuilder();
                    patrimonioMaps.put(currency, patrimonio);
                }

                try {

                    double auxSimultana = simultaneas.values().stream()
                            .mapToDouble(sim -> Double.parseDouble(sim.getMontoPresente()) + Double.parseDouble(sim.getCostoDiario2()))
                            .sum();
                    patrimonio.setAuxSimultanea(-auxSimultana);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                // ####### Calculo Simultane #######################
                patrimonio.setAuxValorDeMercado(s.getValorMercadoClp() + patrimonio.getAuxValorDeMercado());

                BlotterMessage.PositionHistory.Builder positionHistory =
                        BlotterMessage.PositionHistory.newBuilder();
                ProtoDateProcessor.setDateProcesorIfMissing(positionHistory);
                positionHistory.setInstrument(s.getNemotecNico());
                positionHistory.setMarketPrice(s.getPrecioTasaMercado());
                positionHistory.setAccount(prefixAccount);

                positionHistory.setGuarantee(s.getValorMercadoClp());
                positionHistory.setSimultaneous(false);
                positionHistory.setGarantia(s.getGarantia());
                positionHistory.setCompraPlazo(s.getComprasPlazo());

                double qty = s.getLibre();
                positionHistory.setAvailableQuantity(qty + s.getGarantia());

                // Validación: solo calcular purchaseAmount si qty es mayor a 0
                if (qty > 0) {
                    double amount = s.getPrecioTasaMercado() * qty;
                    positionHistory.setPurchaseAmount(amount);
                } else {
                    positionHistory.setPurchaseAmount(0.0);
                }

                if (s.getSimVentaClp() > 0d || s.getSimCompraClp() > 0d) {
                    positionHistory.setSimultaneous(true);
                }

                try {
                    log.info(JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace().print(s));
                } catch (InvalidProtocolBufferException e) {
                    log.error(e.getMessage(), e);
                }

                // Actualiza simultaneas
                simultaneas.forEach((key, value) -> {
                    LocalDate inputDate = LocalDate.parse(value.getFechaOperacion(), formatter);
                    LocalDate currentDate = LocalDate.now();

                    if (value.getNemotecnico().equals(s.getNemotecNico()) && currentDate.isAfter(inputDate)) {
                        double qtys = positionHistory.getAvailableQuantity()
                                + Double.parseDouble(value.getCantidadOrig());
                        positionHistory.setAvailableQuantity(qtys);
                    }
                });

                snapshotPositionHistory.addPositionsHistory(positionHistory);
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return snapshotPositionHistory;
    }

}
