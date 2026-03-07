package cl.vc.service.akka.actors.routing;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.ActorPerSession;
import cl.vc.service.util.CalculatePosition;
import cl.vc.service.util.CalculoCreasys;
import cl.vc.service.util.LogicaPosition;
import cl.vc.service.util.ProtoDateProcessor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.EnumSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static cl.vc.service.MainApp.requiereCreasys;


@Slf4j
public class ActorGroupPerAccount extends AbstractActor {

    private final Object lock = new Object();
    private final Map<String, RoutingMessage.Order> exceIdProcess = new HashMap<>();
    private final HashMap<String, ActorRef> actorSession = new HashMap<>();
    private final HashMap<String, ActorRef> strategyActors = new HashMap<>();
    private final String account;
    private final HashMap<String, BlotterMessage.Simultaneas> simultaneasHashMap = new HashMap<>();
    private final BlotterMessage.Balance.Builder balance = BlotterMessage.Balance.newBuilder();
    private final HashMap<String, BlotterMessage.PositionHistory.Builder> snapshotPositionHistoryMaps = new HashMap<>();
    private LogicaPosition logicaPosition;
    private Map<String, BlotterMessage.Position> positionsMaps = Collections.synchronizedMap(new HashMap<>());
    private HashMap<String, RoutingMessage.Order> ordersMap = new HashMap<>();
    private HashMap<String, RoutingMessage.Order> tradesMap = new HashMap<>();
    private HashMap<RoutingMessage.Currency, BlotterMessage.Patrimonio.Builder> patrimonioMaps = new HashMap<>();
    private Double marginaccount;
    private Double marginLimit;
    private Double palanca;
    private CalculatePosition calculatePositions;
    private BlotterMessage.Patrimonio.Builder patrimonio = BlotterMessage.Patrimonio.newBuilder();
    private BlotterMessage.SnapshotPositionHistory.Builder snapshotPositionHistory = BlotterMessage.SnapshotPositionHistory.newBuilder();
    private BlotterMessage.SnapshotPrestamos.Builder snapshotPrestamos = BlotterMessage.SnapshotPrestamos.newBuilder();
    private boolean restoredFromRedisState = false;

    public ActorGroupPerAccount(String account, Double margen, Double palanca) {
        this.account = account;
        this.marginLimit = margen;
        this.marginaccount = margen;
        this.palanca = palanca != null ? palanca : 1.0;
    }

    public static Props props(String account, Double margen, Double palanca) {
        return Props.create(ActorGroupPerAccount.class, account, margen, palanca);
    }


    @Override
    public void preStart() {
        try {


            MainApp.getMessageEventBus().subscribe(getSelf(), account);
            calculatePositions = new CalculatePosition(account);
            MainApp.getAccountGroupUser().put(account, getSelf());
            restoreFromRedisIfEnabled();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    @Override
    public void postStop() {

    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Initialize.class, this::onInitialize)
                .match(UpdateMargin.class, this::onUpdateMargin)
                .match(UpdateLeverage.class, this::onUpdateLeverage)
                .match(RestorePositions.class, this::onRestorePositions)
                .match(BlotterMessage.PositionHistory.class, this::onPositionHistorySingle)
                .match(RestoreTrade.class, this::onRestoreTrade)
                .match(RestoreOrder.class, this::onRestoreOrder)
                .match(RestorePatrimonio.class, this::onRestorePatrimonio)
                .match(RestoreSnapshotPositionHistory.class, this::onRestoreSnapshotPositionHistory)
                .match(RestoreBalance.class, this::onRestoreBalance)
                .match(RestoreSnapshotSimultaneas.class, this::onRestoreSnapshotSimultaneas)
                .match(RestoreSnapshotPrestamos.class, this::onRestoreSnapshotPrestamos)
                .match(RemoveUser.class, this::onRemoveUser)
                .match(NewActorSession.class, this::onActorSession)
                .match(RoutingMessage.NewOrderRequest.class, this::onNewOrderRequest)
                .match(BlotterMessage.Balance.class, this::onBalances)
                .match(BlotterMessage.SnapshotPositionHistory.class, this::onPositionHistory)
                .match(RoutingMessage.OrderReplaceRequest.class, this::onReplaceRequest)
                .match(RoutingMessage.OrderCancelRequest.class, this::onCancelRequest)
                .match(RoutingMessage.Order.class, this::onOrders)
                .match(BlotterMessage.Position.class, this::onPositions)
                .match(RoutingMessage.OrderCancelReject.class, this::onRejected)
                .match(BlotterMessage.Simultaneas.class, this::onSimultaneas)
                .match(CalculatePatriminio.class, this::onCalculatePatriminio)

                .build();
    }

    public void onSimultaneas(BlotterMessage.Simultaneas simultaneas) {

        try {
            if (!isSimultaneaFromToday(simultaneas)) {
                return;
            }

            simultaneasHashMap.put(simultaneas.getId(), simultaneas);

            BlotterMessage.SnapshotSimultaneas snapshotSimultaneas = BlotterMessage.SnapshotSimultaneas.newBuilder()
                    .addAllSimultaneas(simultaneasHashMap.values())
                    .setAccount(account)
                    .build();

            persistSnapshotSimultaneas(snapshotSimultaneas);

            actorSession.forEach((key, value) -> value.tell(snapshotSimultaneas, ActorRef.noSender()));


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void calculatePrestamos() {
        try {

            if (!requiereCreasys) return;

            String prefixAccount = account.replace("-", "/");
            snapshotPrestamos = CalculoCreasys.snapshotPrestamos(prefixAccount);
            BlotterMessage.SnapshotPrestamos msg = snapshotPrestamos.build();
            persistSnapshotPrestamos(msg);
            actorSession.forEach((k, ses) -> ses.tell(msg, ActorRef.noSender()));

        } catch (Exception e) {
            log.error("Error calculando/enviando SnapshotPrestamos para {}: {}", account, e.getMessage(), e);
        }
    }

    public void calculatePatrimonio() {
        try {

            if (!requiereCreasys) {
                return;
            }

            log.info("⏳ [START] Cálculo patrimonio cuenta {}", account);
            long t0 = System.currentTimeMillis();

            String prefixAccount = account.replace("-", "/");

            simultaneasHashMap.clear();
            MainApp.getAllSimultaneas().forEach(s -> {
                String account = s.getNumCuenta();
                if (this.account.equals(account) && isSimultaneaFromToday(s)) {
                    simultaneasHashMap.put(s.getId(), s);
                }
            });

            patrimonioMaps = CalculoCreasys.saldoCaja(prefixAccount);

            if (!patrimonioMaps.containsKey(RoutingMessage.Currency.CLP)) {
                patrimonioMaps.put(RoutingMessage.Currency.CLP, BlotterMessage.Patrimonio.newBuilder()
                        .setCaja(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setCuentaTransitoriasPorCobrarPagar(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setGarantiaEfectivo(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setPrestamos(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                );
            }

            snapshotPositionHistory = CalculoCreasys.cierreCarteraResumida(prefixAccount, patrimonioMaps, simultaneasHashMap);
            snapshotPositionHistory.getPositionsHistoryBuilderList().forEach(s -> snapshotPositionHistoryMaps.put(s.getInstrument(), s));

            long durMs = System.currentTimeMillis() - t0;
            log.info("✅ [DONE] Patrimonio cuenta {} en {} ms", account, durMs);


            if (!patrimonioMaps.isEmpty()) {

                // Aseguramos que la moneda CLP esté en el mapa para evitar NPE
                BlotterMessage.Patrimonio.Builder patrimonioCLP = patrimonioMaps.get(RoutingMessage.Currency.CLP);
                if (patrimonioCLP == null) {
                    patrimonioCLP = BlotterMessage.Patrimonio.newBuilder()
                            .setCaja(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                            .setCuentaTransitoriasPorCobrarPagar(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                            .setGarantiaEfectivo(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                            .setPrestamos(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0));
                }

                /* ======================= LIQUIDEZ ======================= */
                BlotterMessage.ValuesPatrimonio.Builder caja = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("Cajas")
                        .setValues(patrimonioCLP.getCaja().getValues())
                        .setPorcentage(0d);

                BlotterMessage.ValuesPatrimonio.Builder cuentaTransitoriasPorCobrarPagar = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("Cuentas transitorias por cobrar/pagar")
                        .setValues(patrimonioCLP.getCuentaTransitoriasPorCobrarPagar().getValues())
                        .setPorcentage(0d);

                BlotterMessage.ValuesPatrimonio.Builder garantiaEfectivo = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("Garantías en efectivo")
                        .setValues(patrimonioCLP.getGarantiaEfectivo().getValues())
                        .setPorcentage(0d);

                double liquidezAux = caja.getValues() + cuentaTransitoriasPorCobrarPagar.getValues() + garantiaEfectivo.getValues();

                BlotterMessage.ValuesPatrimonio.Builder liquidez = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("LIQUIDEZ")
                        .setValues(liquidezAux)
                        .setPorcentage(0d);

                /* ======================= PRÉSTAMOS ======================= */
                BlotterMessage.ValuesPatrimonio.Builder prestamos = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("Préstamos")
                        .setValues(0d)
                        .setPorcentage(0d);

                if (patrimonioMaps.containsKey(RoutingMessage.Currency.CLP)) {
                    double p = patrimonioMaps.get(RoutingMessage.Currency.CLP).getPrestamos().getValues();
                    prestamos.setValues(-p);
                }

                /* Valores fijos con cero */
                BlotterMessage.ValuesPatrimonio.Builder rentaFija = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("RENTA FIJA").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder fondosMutuos = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Fondos Mutuos RV Extranjeros").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder fondoInversionRentaVariable = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Fondos Mutuos RV Nacional").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder activosInmobiliarios = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Activos Inmobiliarios").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder eftsRentaVariable = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Fondos Inversion Renta Variable").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder inversionesAlternativas = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("ETFs Renta Variable").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder derivados = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Derivados").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder rvNacional = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Renta Variable Nacional").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder rvExtranjeros = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Renta Variable Extraneros").setValues(0).setPorcentage(0);
                BlotterMessage.ValuesPatrimonio.Builder accionesExtranjeras = BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Acciones Extranjeras").setValues(0d).setPorcentage(0d);

                /* ======================= RENTA VARIABLE ======================= */
                AtomicReference<Double> aux = new AtomicReference<>(0d);
                snapshotPositionHistory.getPositionsHistoryList().forEach(s -> aux.set(aux.get() + s.getGuarantee()));

                BlotterMessage.ValuesPatrimonio.Builder simultaneas = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("Simultaneas")
                        .setValues(patrimonioCLP.getAuxSimultanea())
                        .setPorcentage(0d);

                BlotterMessage.ValuesPatrimonio.Builder accionesNacionales = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("Acciones nacionales")
                        .setValues(aux.get())
                        .setPorcentage(0d);

                BlotterMessage.ValuesPatrimonio.Builder rentaVariable = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("RENTA VARIABLE")
                        .setValues(simultaneas.getValues() + accionesNacionales.getValues() + prestamos.getValues())
                        .setPorcentage(0d);

                /* ======================= CABECERA TOTAL ======================= */
                double activosAux = rentaVariable.getValues() + liquidez.getValues();

                BlotterMessage.ValuesPatrimonio.Builder activos = BlotterMessage.ValuesPatrimonio.newBuilder()
                        .setDescription("Activos")
                        .setValues(activosAux)
                        .setPorcentage(100d);

                /*
                 * 🔄  **FIX**:  usamos "carteraActual" (aux.get()) en vez de balance.getCartera(),
                 */
                double carteraActual = aux.get();

                double leverage = (palanca != null && palanca > 0) ? palanca : 3.0;

                if (marginLimit != -1) {
                    marginaccount = activosAux * leverage - carteraActual;
                } else {
                    marginaccount = marginLimit;
                }

                /* ========== porcentajes individuales ========== */
                caja.setPorcentage(caja.getValues() / activosAux * 100);
                cuentaTransitoriasPorCobrarPagar.setPorcentage(cuentaTransitoriasPorCobrarPagar.getValues() / activosAux * 100);
                garantiaEfectivo.setPorcentage(garantiaEfectivo.getValues() / activosAux * 100);
                liquidez.setPorcentage(liquidez.getValues() / activosAux * 100);
                prestamos.setPorcentage(prestamos.getValues() / activosAux * 100);
                rentaFija.setPorcentage(rentaFija.getValues() / activosAux * 100);
                fondosMutuos.setPorcentage(fondosMutuos.getValues() / activosAux * 100);
                fondoInversionRentaVariable.setPorcentage(fondoInversionRentaVariable.getValues() / activosAux * 100);
                activosInmobiliarios.setPorcentage(activosInmobiliarios.getValues() / activosAux * 100);
                eftsRentaVariable.setPorcentage(eftsRentaVariable.getValues() / activosAux * 100);
                inversionesAlternativas.setPorcentage(inversionesAlternativas.getValues() / activosAux * 100);
                derivados.setPorcentage(derivados.getValues() / activosAux * 100);
                rvNacional.setPorcentage(rvNacional.getValues() / activosAux * 100);
                rvExtranjeros.setPorcentage(rvExtranjeros.getValues() / activosAux * 100);
                accionesExtranjeras.setPorcentage(accionesExtranjeras.getValues() / activosAux * 100);
                simultaneas.setPorcentage(simultaneas.getValues() / activosAux * 100);
                accionesNacionales.setPorcentage(accionesNacionales.getValues() / activosAux * 100);
                rentaVariable.setPorcentage(rentaVariable.getValues() / activosAux * 100);

                /* ======================= OBJETO PATRIMONIO ======================= */
                patrimonio = BlotterMessage.Patrimonio.newBuilder();
                patrimonio.setActivos(activos);
                patrimonio.setLiquidez(liquidez);
                patrimonio.setCaja(caja);
                patrimonio.setCuentaTransitoriasPorCobrarPagar(cuentaTransitoriasPorCobrarPagar);
                patrimonio.setGarantiaEfectivo(garantiaEfectivo);
                patrimonio.setRentaFija(rentaFija);
                patrimonio.setRentaVariable(rentaVariable);
                patrimonio.setAccionesNacionales(accionesNacionales);
                patrimonio.setSimultaneas(simultaneas);
                patrimonio.setPrestamos(prestamos);
                patrimonio.setAccionesextranjeras(accionesExtranjeras);
                patrimonio.setFondosMutos(fondosMutuos);
                patrimonio.setRvNacional(rvNacional);
                patrimonio.setRvExtranjeros(rvExtranjeros);
                patrimonio.setFondoInversionRentaVariable(fondoInversionRentaVariable);
                patrimonio.setActivosInmobiliarios(activosInmobiliarios);
                patrimonio.setEftsRentaVariable(eftsRentaVariable);
                patrimonio.setInversionesAlternativas(inversionesAlternativas);
                patrimonio.setDerivados(derivados);
                patrimonio.setCurrency(RoutingMessage.Currency.CLP);
                patrimonio.setCuenta(account);

                /* ======================= BALANCE ======================= */
                ProtoDateProcessor.setDateProcesorIfMissing(balance);
                balance.setCuenta(account);
                balance.setCartera(carteraActual);
                balance.setCupo(marginaccount);

                double saldoDisponible = marginaccount;

                if (saldoDisponible < 0) saldoDisponible = 0;
                balance.setSaldoDisponible(saldoDisponible);

            } else {
                /* cuenta sin posiciones ni caja */
                ProtoDateProcessor.setDateProcesorIfMissing(balance);
                balance.setCuenta(account).setCartera(0d).setCupo(marginaccount).setSaldoDisponible(marginaccount > 0 ? marginaccount : 0d);
            }

            /* ========== Actualizar lógica de posiciones y notificar sesiones ========== */
            logicaPosition = new LogicaPosition(marginaccount,
                    getSelf(),
                    balance,
                    snapshotPositionHistoryMaps,
                    simultaneasHashMap);

            actorSession.values().forEach(ses -> {
                ses.tell(patrimonio.build(), ActorRef.noSender());
                ses.tell(snapshotPositionHistory.build(), ActorRef.noSender());
                ses.tell(balance.build(), ActorRef.noSender());
            });

            persistPatrimonio(patrimonio.build());
            persistSnapshotPositionHistory(snapshotPositionHistory.build());
            persistBalance(balance.build());

            calculatePrestamos();



        } catch (Exception e) {
            log.error("❌ Error en calculatePatrimonio cuenta " + account + "{}", e);
        }
    }

    public void onPositionHistory(BlotterMessage.SnapshotPositionHistory msg) {

        snapshotPositionHistory = msg.toBuilder();
        persistSnapshotPositionHistory(msg);

        actorSession.forEach((key, value) -> value.tell(msg, ActorRef.noSender()));
    }

    public void onPositionHistorySingle(BlotterMessage.PositionHistory positionHistory) {
        try {

            snapshotPositionHistoryMaps.put(positionHistory.getInstrument(), positionHistory.toBuilder());

            BlotterMessage.SnapshotPositionHistory.Builder snapshotBuilder = BlotterMessage.SnapshotPositionHistory.newBuilder();
            snapshotBuilder.addAllPositionsHistory(snapshotPositionHistoryMaps.values().stream()
                    .map(BlotterMessage.PositionHistory.Builder::build)
                    .collect(Collectors.toList()));

            snapshotPositionHistory = snapshotBuilder;

            BlotterMessage.SnapshotPositionHistory snapshotPositionHistoryMsg = snapshotBuilder.build();
            persistSnapshotPositionHistory(snapshotPositionHistoryMsg);
            actorSession.values().forEach(sessionActor -> sessionActor.tell(snapshotPositionHistoryMsg, getSelf()));

            log.info("Posición actualizada para la cuenta: {} instrumento: {}", account, positionHistory.getInstrument());
        } catch (Exception e) {
            log.error("Error al actualizar la posición para la cuenta: {}", account, e);
        }
    }

    public void onBalances(BlotterMessage.Balance msg) {
        balance.clear();
        balance.mergeFrom(msg);
        persistBalance(msg);
        actorSession.forEach((key, value) -> value.tell(msg, ActorRef.noSender()));
    }

    public void onCancelRequest(RoutingMessage.OrderCancelRequest msg) {
        try {


            RoutingMessage.Order orderRequest = ordersMap.get(msg.getId());

            if (orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BEST) ||
                    orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.HOLGURA) ||
                    orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.TRAILING) ||
                    orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_AGGRESSIVE) ||
                    orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_LAST) ||
                    orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.VWAP) ||
                    orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_PASSIVE) ||
                    orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.OCO)) {

                strategyActors.get(msg.getId()).tell(msg, ActorRef.noSender());

            } else if (orderRequest.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET)) {
                MainApp.getConnections().get(RoutingMessage.SecurityExchangeRouting.BASKETS).sendMessage(msg);

            } else {
                MainApp.getConnections().get(orderRequest.getSecurityExchange()).sendMessage(msg);
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onReplaceRequest(RoutingMessage.OrderReplaceRequest msg) {

        try {

            RoutingMessage.Order order = ordersMap.get(msg.getId());

            if (logicaPosition != null && !logicaPosition.calculateBalanceReplace(msg, order)) {
                return;
            }

            if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BEST) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.HOLGURA) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.TRAILING) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_AGGRESSIVE) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_LAST) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_PASSIVE) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.OCO)) {

                strategyActors.get(msg.getId()).tell(msg, ActorRef.noSender());

            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET)) {
                MainApp.getConnections().get(RoutingMessage.SecurityExchangeRouting.BASKETS).sendMessage(msg);

            } else {
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(msg);
            }


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }




    private void onPositions(BlotterMessage.Position positions) {

        try {

            BlotterMessage.SnapshotPositions snapshotPositions =
                    BlotterMessage.SnapshotPositions.newBuilder().setId(account).addAllPositions(positionsMaps.values()).build();

            actorSession.forEach((key, value) -> value.tell(new ActorPerSession.SnapshotPositionsAccount(account, snapshotPositions), ActorRef.noSender()));


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onNewOrderRequest(RoutingMessage.NewOrderRequest orders) {

        if (logicaPosition != null && !logicaPosition.calculateBalanceReplace(orders)) {
            log.info("⚠️ Retornamos por orden sin custodia, ID: {} symbol: {} account: {}", orders.getOrder().getId(), orders.getOrder().getSymbol(), orders.getOrder().getAccount());
            return;
        }
        newOrder(orders);
    }

    private void newOrder(RoutingMessage.NewOrderRequest orders) {

        try {

            RoutingMessage.Order order = orders.getOrder();


            if (isStrategyManagedByActor(order.getStrategyOrder())) {

                ActorRef actorRef = MainApp.getSystem().actorOf(ActorStrategy.props(orders, getSelf(), strategyActors));
                strategyActors.put(order.getId(), actorRef);

            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET)) {
                MainApp.getMessageEventBus().subscribe(getSelf(), order.getId());
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                MainApp.getConnections().get(RoutingMessage.SecurityExchangeRouting.BASKETS).sendMessage(newOrderRequest);

            } else {
                MainApp.getMessageEventBus().subscribe(getSelf(), order.getId());
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(newOrderRequest);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("posiblemente destino no conectado");
            RoutingMessage.Order.Builder order1 = orders.getOrder().toBuilder();
            order1.setText("posiblemente destino no conectado");
            order1.setExecId(IDGenerator.getID());
            order1.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
            order1.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
            getSelf().tell(order1.build(), ActorRef.noSender());
        }


    }

    private void onRejected(RoutingMessage.OrderCancelReject rejected) {

        actorSession.forEach((key, value) -> value.tell(rejected, ActorRef.noSender()));

    }

    private void onOrders(RoutingMessage.Order order) {

        try {

            if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE)) {
                return;
            }

            if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET)) {



            } else {

                actorSession.forEach((key, value) -> value.tell(order, ActorRef.noSender()));

                RoutingMessage.Order orderold = ordersMap.get(order.getId());
                ordersMap.put(order.getId(), order);
                MainApp.getIdOrders().put(order.getId(), order);

                if (order.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)) {

                    if (strategyActors.containsKey(order.getId()) && !order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.VWAP)) {
                        strategyActors.get(order.getId()).tell(PoisonPill.getInstance(), ActorRef.noSender());
                        strategyActors.remove(order.getId());
                    }
                }

                if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE) && !exceIdProcess.containsKey(order.getExecId())) {

                    exceIdProcess.put(order.getExecId(), order);
                    tradesMap.put(order.getExecId(), order);
                    MainApp.getTradesMapAll().put(account, tradesMap);


                    ExecutorService executortrde = Executors.newSingleThreadExecutor();
                    CompletableFuture<Void> futuretrade = CompletableFuture.runAsync(() -> MainApp.getTradesMapReddis().put(account, tradesMap), executortrde);

                    try {
                        futuretrade.get(5, TimeUnit.SECONDS);
                    } catch (TimeoutException | InterruptedException | ExecutionException e) {
                        futuretrade.cancel(true);
                        log.error("Operation timed out");
                    } finally {
                        executortrde.shutdown();
                    }


                    synchronized (lock) {

                        BlotterMessage.Position positions = calculatePositions.onOrder(order, positionsMaps);
                        positionsMaps.put(positions.getId(), positions);

                        getSelf().tell(positions, ActorRef.noSender());

                        ExecutorService executor = Executors.newSingleThreadExecutor();

                        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> MainApp.getPositionsMapsRedis().put(account, positionsMaps), executor);

                        try {
                            future.get(5, TimeUnit.SECONDS);
                        } catch (TimeoutException | InterruptedException | ExecutionException e) {
                            future.cancel(true);
                            log.error("Operation timed out");
                        } finally {
                            executor.shutdown();
                        }

                    }
                }


                synchronized (lock) {
                    // calculo de posiciones historicas
                    if (logicaPosition != null) {
                        logicaPosition.orderUpdate(order, orderold);
                    } else {
                        createLogicalPosition();
                    }

                }
                ExecutorService executortrde = Executors.newSingleThreadExecutor();
                CompletableFuture<Void> futuretrade = CompletableFuture.runAsync(() -> MainApp.getOrdersMapRedis().put(account, ordersMap), executortrde);

                try {
                    futuretrade.get(5, TimeUnit.SECONDS);
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    futuretrade.cancel(true);
                    log.error("Operation timed out");
                } finally {
                    executortrde.shutdown();
                }
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void createLogicalPosition() {

    }

    private void onActorSession(NewActorSession newConnecition) {

        try {

            if (actorSession.containsKey(newConnecition.getIdActor())) {
                return;
            }

            actorSession.put(newConnecition.getIdActor(), newConnecition.getActorSession());

            BlotterMessage.SnapshotPositions snapshotPositions =
                    BlotterMessage.SnapshotPositions.newBuilder().setId(account).addAllPositions(positionsMaps.values()).build();

            newConnecition.getActorSession().tell(new ActorPerSession.SnapshotPositionsAccount(account, snapshotPositions), ActorRef.noSender());


            ordersMap.forEach((key, value) -> newConnecition.getActorSession().tell(value, ActorRef.noSender()));

            tradesMap.forEach((key, value) -> newConnecition.getActorSession().tell(value, ActorRef.noSender()));

            BlotterMessage.SnapshotSimultaneas snapshotSimultaneas = BlotterMessage.SnapshotSimultaneas.newBuilder()
                    .setAccount(account)
                    .addAllSimultaneas(simultaneasHashMap.values())
                    .build();

            newConnecition.getActorSession().tell(snapshotSimultaneas, ActorRef.noSender());
            newConnecition.getActorSession().tell(patrimonio.build(), ActorRef.noSender());
            newConnecition.getActorSession().tell(snapshotPositionHistory.build(), ActorRef.noSender());
            newConnecition.getActorSession().tell(balance.build(), ActorRef.noSender());

            if (!restoredFromRedisState && (snapshotPrestamos == null || snapshotPrestamos.getPrestamosCount() == 0)) {
                calculatePrestamos();
            } else {
                newConnecition.getActorSession().tell(snapshotPrestamos.build(), ActorRef.noSender());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private void onRemoveUser(RemoveUser newUser) {
        actorSession.remove(newUser.getActorId());
    }

    public static final class Initialize {
        public static final Initialize INSTANCE = new Initialize();
        private Initialize() {}
    }

    public static final class Initialized {
        public static final Initialized INSTANCE = new Initialized();
        private Initialized() {}
    }

    private void onInitialize(Initialize ignored) {
        try {

            if (requiereCreasys && !restoredFromRedisState) {
                calculatePatrimonio();
                calculatePrestamos();
            }
            getSender().tell(Initialized.INSTANCE, getSelf());

        } catch (Exception e) {
            log.error("Error inicializando actor de cuenta {}: {}", account, e.getMessage(), e);
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
        }
    }
    @Data
    @AllArgsConstructor
    public static final class UpdateMargin {
        private Double margin;
        private String username;
    }

    private void onUpdateMargin(UpdateMargin msg) {
        try {
            if (!Objects.equals(msg.getMargin(), marginLimit)) {
                log.info("se actualiza marginLimit de {} a {} para la cuenta {}",
                        BigDecimal.valueOf(marginLimit == null ? 0 : marginLimit).toPlainString(),
                        BigDecimal.valueOf(msg.getMargin()).toPlainString(),
                        msg.getUsername() + "->" + account);

                marginLimit = msg.getMargin();
                if (requiereCreasys) {
                    calculatePatrimonio();
                    calculatePrestamos();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Data
    @AllArgsConstructor
    public static final class UpdateLeverage {
        private Double leverage;
        private String username;
    }


    private void onUpdateLeverage(UpdateLeverage msg) {
        try {
            if (!Objects.equals(msg.getLeverage(), palanca)) {
                log.info("Se actualiza palanca de {} a {} para la cuenta {}",
                        palanca == null ? 0 : palanca,
                        msg.getLeverage(),
                        msg.getUsername() + "->" + account);

                palanca = msg.getLeverage();

                if (requiereCreasys) {
                    calculatePatrimonio();
                    calculatePrestamos();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    @Data
    @AllArgsConstructor
    public static final class CalculatePatriminio {

    }

    private void onCalculatePatriminio(CalculatePatriminio calculatePatriminio) {

        if (requiereCreasys) {
            calculatePatrimonio();
            calculatePrestamos();
        }

    }

    private void onRestoreOrder(RestoreOrder restoreOrder) {

        try {


            HashMap<String, RoutingMessage.Order> restoredToday = new HashMap<>();

            //ordenes con strategia

            restoreOrder.getMapOrders().forEach((key1, value1) -> {
                if (!MainApp.isOrderFromToday(value1)) {
                    return;
                }

                restoredToday.put(key1, value1);

                if (isStrategyManagedByActor(value1.getStrategyOrder())) {

                    if (!isFinalStatus(value1.getOrdStatus())) {

                        ActorRef actorRef = MainApp.getSystem().actorOf(ActorStrategy.props(value1, getSelf(), strategyActors));
                        strategyActors.put(value1.getId(), actorRef);
                    }
                }

                actorSession.forEach((key, value) -> value.tell(value1, ActorRef.noSender()));

                MainApp.getIdOrders().put(key1, value1);
                MainApp.getMessageEventBus().subscribe(getSelf(), value1.getId());

            });

            this.ordersMap = restoredToday;

            if (MainApp.getOrdersMapRedis() != null) {
                if (this.ordersMap.isEmpty()) {
                    MainApp.getOrdersMapRedis().fastRemove(account);
                } else {
                    MainApp.getOrdersMapRedis().put(account, this.ordersMap);
                }
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private void onRestoreTrade(RestoreTrade restoreTrade) {

        try {

            HashMap<String, RoutingMessage.Order> restoredToday = new HashMap<>();

            restoreTrade.getMapOrders().forEach((key1, value1) -> {
                if (!MainApp.isOrderFromToday(value1)) {
                    return;
                }
                restoredToday.put(key1, value1);
                actorSession.forEach((key2, value) -> value.tell(value1, ActorRef.noSender()));
            });

            this.tradesMap = restoredToday;

            if (MainApp.getTradesMapReddis() != null) {
                if (this.tradesMap.isEmpty()) {
                    MainApp.getTradesMapReddis().fastRemove(account);
                } else {
                    MainApp.getTradesMapReddis().put(account, this.tradesMap);
                }
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    private void onRestorePositions(RestorePositions restorePositions) {

        try {

            this.positionsMaps = restorePositions.getMapOrders();

            BlotterMessage.SnapshotPositions snapshotPositions =
                    BlotterMessage.SnapshotPositions.newBuilder().setId(account).addAllPositions(positionsMaps.values()).build();

            actorSession.forEach((key, value) -> value.tell(new ActorPerSession.SnapshotPositionsAccount(account, snapshotPositions), ActorRef.noSender()));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }


    @Data
    @AllArgsConstructor
    public static final class RemoveUser {
        private String actorId;
    }

    @Data
    @AllArgsConstructor
    public static final class NewActorSession {
        private ActorRef actorSession;
        private String idActor;
    }





    @Data
    @AllArgsConstructor
    public static final class RestoreOrder {
        private HashMap<String, RoutingMessage.Order> mapOrders;
    }

    @Data
    @AllArgsConstructor
    public static final class RestoreTrade {
        private HashMap<String, RoutingMessage.Order> mapOrders;
    }

    @Data
    @AllArgsConstructor
    public static final class RestorePositions {
        private Map<String, BlotterMessage.Position> mapOrders;
    }

    @Data
    @AllArgsConstructor
    public static final class RestorePatrimonio {
        private BlotterMessage.Patrimonio patrimonio;
    }

    @Data
    @AllArgsConstructor
    public static final class RestoreSnapshotPositionHistory {
        private BlotterMessage.SnapshotPositionHistory snapshotPositionHistory;
    }

    @Data
    @AllArgsConstructor
    public static final class RestoreBalance {
        private BlotterMessage.Balance balance;
    }

    @Data
    @AllArgsConstructor
    public static final class RestoreSnapshotSimultaneas {
        private BlotterMessage.SnapshotSimultaneas snapshotSimultaneas;
    }

    @Data
    @AllArgsConstructor
    public static final class RestoreSnapshotPrestamos {
        private BlotterMessage.SnapshotPrestamos snapshotPrestamos;
    }

    private boolean isStrategyManagedByActor(RoutingMessage.StrategyOrder strategyOrder) {
        return EnumSet.of(
                RoutingMessage.StrategyOrder.BEST,
                RoutingMessage.StrategyOrder.HOLGURA,
                RoutingMessage.StrategyOrder.TRAILING,
                RoutingMessage.StrategyOrder.BASKET_AGGRESSIVE,
                RoutingMessage.StrategyOrder.BASKET_PASSIVE,
                RoutingMessage.StrategyOrder.BASKET_LAST,
                RoutingMessage.StrategyOrder.VWAP,
                RoutingMessage.StrategyOrder.OCO
        ).contains(strategyOrder);
    }

    private boolean isFinalStatus(RoutingMessage.OrderStatus status) {
        return status == RoutingMessage.OrderStatus.CANCELED
                || status == RoutingMessage.OrderStatus.FILLED
                || status == RoutingMessage.OrderStatus.REJECTED
                || status == RoutingMessage.OrderStatus.STOPPED
                || status == RoutingMessage.OrderStatus.DONE_FOR_DAY;
    }

    private void restoreFromRedisIfEnabled() {
        try {
            if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia"))) {
                return;
            }
            String dailyKey = getDailyRedisKey();
            boolean restoredAny = false;

            if (MainApp.getOrdersMapRedis() != null) {
                HashMap<String, RoutingMessage.Order> redisOrders = MainApp.getOrdersMapRedis().get(account);
                if (redisOrders != null && !redisOrders.isEmpty()) {
                    onRestoreOrder(new RestoreOrder(new HashMap<>(redisOrders)));
                    restoredAny = true;
                }
            }

            if (MainApp.getTradesMapReddis() != null) {
                HashMap<String, RoutingMessage.Order> redisTrades = MainApp.getTradesMapReddis().get(account);
                if (redisTrades != null && !redisTrades.isEmpty()) {
                    onRestoreTrade(new RestoreTrade(new HashMap<>(redisTrades)));
                    restoredAny = true;
                }
            }

            if (MainApp.getPositionsMapsRedis() != null) {
                Map<String, BlotterMessage.Position> redisPositions = MainApp.getPositionsMapsRedis().get(account);
                if (redisPositions != null && !redisPositions.isEmpty()) {
                    onRestorePositions(new RestorePositions(new HashMap<>(redisPositions)));
                    restoredAny = true;
                }
            }

            if (MainApp.getPatrimonioMapsRedis() != null) {
                BlotterMessage.Patrimonio redisPatrimonio = MainApp.getPatrimonioMapsRedis().get(dailyKey);
                if (redisPatrimonio != null) {
                    onRestorePatrimonio(new RestorePatrimonio(redisPatrimonio));
                    restoredAny = true;
                }
            }

            if (MainApp.getSnapshotPositionHistoryRedis() != null) {
                BlotterMessage.SnapshotPositionHistory redisSnapshotPositionHistory = MainApp.getSnapshotPositionHistoryRedis().get(dailyKey);
                if (redisSnapshotPositionHistory != null) {
                    onRestoreSnapshotPositionHistory(new RestoreSnapshotPositionHistory(redisSnapshotPositionHistory));
                    restoredAny = true;
                }
            }

            if (MainApp.getBalanceRedis() != null) {
                BlotterMessage.Balance redisBalance = MainApp.getBalanceRedis().get(dailyKey);
                if (redisBalance != null) {
                    onRestoreBalance(new RestoreBalance(redisBalance));
                    restoredAny = true;
                }
            }

            if (MainApp.getSnapshotSimultaneasRedis() != null) {
                BlotterMessage.SnapshotSimultaneas redisSnapshotSimultaneas = MainApp.getSnapshotSimultaneasRedis().get(dailyKey);
                if (redisSnapshotSimultaneas != null) {
                    onRestoreSnapshotSimultaneas(new RestoreSnapshotSimultaneas(redisSnapshotSimultaneas));
                    restoredAny = true;
                }
            }

            if (MainApp.getSnapshotPrestamosRedis() != null) {
                BlotterMessage.SnapshotPrestamos redisSnapshotPrestamos = MainApp.getSnapshotPrestamosRedis().get(dailyKey);
                if (redisSnapshotPrestamos != null) {
                    onRestoreSnapshotPrestamos(new RestoreSnapshotPrestamos(redisSnapshotPrestamos));
                    restoredAny = true;
                }
            }

            restoredFromRedisState = restoredAny;
        } catch (Exception e) {
            log.error("Error restaurando estado desde redis para cuenta {}", account, e);
        }
    }

    private void onRestorePatrimonio(RestorePatrimonio restorePatrimonio) {
        try {
            if (restorePatrimonio.getPatrimonio() == null) {
                return;
            }
            patrimonio = restorePatrimonio.getPatrimonio().toBuilder();
        } catch (Exception e) {
            log.error("Error restaurando patrimonio para cuenta {}", account, e);
        }
    }

    private void onRestoreSnapshotPositionHistory(RestoreSnapshotPositionHistory restoreSnapshotPositionHistory) {
        try {
            if (restoreSnapshotPositionHistory.getSnapshotPositionHistory() == null) {
                return;
            }

            snapshotPositionHistory = restoreSnapshotPositionHistory.getSnapshotPositionHistory().toBuilder();
            snapshotPositionHistoryMaps.clear();
            restoreSnapshotPositionHistory.getSnapshotPositionHistory().getPositionsHistoryList()
                    .forEach(ph -> snapshotPositionHistoryMaps.put(ph.getInstrument(), ph.toBuilder()));
        } catch (Exception e) {
            log.error("Error restaurando snapshotPositionHistory para cuenta {}", account, e);
        }
    }

    private void onRestoreBalance(RestoreBalance restoreBalance) {
        try {
            if (restoreBalance.getBalance() == null) {
                return;
            }
            balance.clear();
            balance.mergeFrom(restoreBalance.getBalance());
        } catch (Exception e) {
            log.error("Error restaurando balance para cuenta {}", account, e);
        }
    }

    private void onRestoreSnapshotSimultaneas(RestoreSnapshotSimultaneas restoreSnapshotSimultaneas) {
        try {
            BlotterMessage.SnapshotSimultaneas snapshot = restoreSnapshotSimultaneas.getSnapshotSimultaneas();
            if (snapshot == null) {
                return;
            }

            simultaneasHashMap.clear();
            snapshot.getSimultaneasList().forEach(s -> {
                if (isSimultaneaFromToday(s)) {
                    simultaneasHashMap.put(s.getId(), s);
                }
            });
        } catch (Exception e) {
            log.error("Error restaurando snapshotSimultaneas para cuenta {}", account, e);
        }
    }

    private void onRestoreSnapshotPrestamos(RestoreSnapshotPrestamos restoreSnapshotPrestamos) {
        try {
            if (restoreSnapshotPrestamos.getSnapshotPrestamos() == null) {
                return;
            }
            snapshotPrestamos = restoreSnapshotPrestamos.getSnapshotPrestamos().toBuilder();
        } catch (Exception e) {
            log.error("Error restaurando snapshotPrestamos para cuenta {}", account, e);
        }
    }

    private void persistPatrimonio(BlotterMessage.Patrimonio patrimonioMsg) {
        try {
            if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia"))) {
                return;
            }
            if (MainApp.getPatrimonioMapsRedis() != null && patrimonioMsg != null) {
                MainApp.getPatrimonioMapsRedis().put(getDailyRedisKey(), patrimonioMsg);
            }
        } catch (Exception e) {
            log.error("Error persistiendo patrimonio para cuenta {}", account, e);
        }
    }

    private void persistSnapshotPositionHistory(BlotterMessage.SnapshotPositionHistory snapshotMsg) {
        try {
            if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia"))) {
                return;
            }
            if (MainApp.getSnapshotPositionHistoryRedis() != null && snapshotMsg != null) {
                MainApp.getSnapshotPositionHistoryRedis().put(getDailyRedisKey(), snapshotMsg);
            }
        } catch (Exception e) {
            log.error("Error persistiendo snapshotPositionHistory para cuenta {}", account, e);
        }
    }

    private void persistBalance(BlotterMessage.Balance balanceMsg) {
        try {
            if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia"))) {
                return;
            }
            if (MainApp.getBalanceRedis() != null && balanceMsg != null) {
                MainApp.getBalanceRedis().put(getDailyRedisKey(), balanceMsg);
            }
        } catch (Exception e) {
            log.error("Error persistiendo balance para cuenta {}", account, e);
        }
    }

    private void persistSnapshotSimultaneas(BlotterMessage.SnapshotSimultaneas snapshotMsg) {
        try {
            if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia"))) {
                return;
            }
            if (MainApp.getSnapshotSimultaneasRedis() != null && snapshotMsg != null) {
                MainApp.getSnapshotSimultaneasRedis().put(getDailyRedisKey(), snapshotMsg);
            }
        } catch (Exception e) {
            log.error("Error persistiendo snapshotSimultaneas para cuenta {}", account, e);
        }
    }

    private void persistSnapshotPrestamos(BlotterMessage.SnapshotPrestamos snapshotMsg) {
        try {
            if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia"))) {
                return;
            }
            if (MainApp.getSnapshotPrestamosRedis() != null && snapshotMsg != null) {
                MainApp.getSnapshotPrestamosRedis().put(getDailyRedisKey(), snapshotMsg);
            }
        } catch (Exception e) {
            log.error("Error persistiendo snapshotPrestamos para cuenta {}", account, e);
        }
    }

    private String getDailyRedisKey() {
        ZoneId zoneId = MainApp.getZoneId() != null ? MainApp.getZoneId() : ZoneId.of("America/Santiago");
        return account + "|" + LocalDate.now(zoneId);
    }

    private boolean isSimultaneaFromToday(BlotterMessage.Simultaneas s) {
        if (s == null || s.getFechaOperacion() == null || s.getFechaOperacion().isBlank()) {
            return false;
        }

        ZoneId zoneId = MainApp.getZoneId() != null ? MainApp.getZoneId() : ZoneId.of("America/Santiago");
        LocalDate today = LocalDate.now(zoneId);
        String raw = s.getFechaOperacion().trim();

        if (raw.length() >= 10) {
            String first10 = raw.substring(0, 10);
            try {
                LocalDate parsed = LocalDate.parse(first10, DateTimeFormatter.ISO_LOCAL_DATE);
                return today.equals(parsed);
            } catch (DateTimeParseException ignored) {
            }
        }

        DateTimeFormatter[] formats = new DateTimeFormatter[] {
                DateTimeFormatter.ISO_LOCAL_DATE,
                DateTimeFormatter.ofPattern("yyyyMMdd"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy")
        };

        for (DateTimeFormatter fmt : formats) {
            try {
                LocalDate parsed = LocalDate.parse(raw, fmt);
                return today.equals(parsed);
            } catch (DateTimeParseException ignored) {
            }
        }
        return false;
    }

}
