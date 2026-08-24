package cl.vc.service.akka.actors.routing;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.ActorPerSession;
import cl.vc.service.admin.ManualSaldoOverride;
import cl.vc.service.admin.OdAttempt;
import cl.vc.service.util.CalculatePosition;
import cl.vc.service.util.CalculoCreasys;
import cl.vc.service.util.LogicaPosition;
import cl.vc.service.util.MongoHistoryRepository;
import cl.vc.service.util.OrigClOrdIdRecoverySupport;
import cl.vc.service.util.OrderStateSupport;
import cl.vc.service.util.ProtoDateProcessor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static cl.vc.service.MainApp.requiereCreasys;
import scala.concurrent.duration.Duration;


@Slf4j
public class ActorGroupPerAccount extends AbstractActor {
    private static final long BOOK_REFRESH_DELAY_MS = 750L;

    private final Object lock = new Object();
    private final Map<String, RoutingMessage.Order> exceIdProcess = new HashMap<>();
    private final HashMap<String, ActorRef> actorSession = new HashMap<>();
    private final HashMap<String, ActorRef> strategyActors = new HashMap<>();
    private final Map<String, Integer> missingOrigClOrdIdRejects = new HashMap<>();
    private final Set<String> externallyUnavailableOrderIds = new HashSet<>();
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
                .match(UpdateRiskConfig.class, this::onUpdateRiskConfig)
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
                .match(ActorStrategy.ExternalOrderUnavailable.class, this::onExternalOrderUnavailable)
                .match(ActorStrategy.StrategyStatusReason.class, this::onStrategyStatusReason)
                .match(RoutingMessage.OrderCancelReject.class, this::onRejected)
                .match(BlotterMessage.Simultaneas.class, this::onSimultaneas)
                .match(RefreshManualSimultaneas.class, this::onRefreshManualSimultaneas)
                .match(RefreshManualPrestamos.class, this::onRefreshManualPrestamos)
                .match(RefreshManualSaldo.class, this::onRefreshManualSaldo)
                .match(CalculatePatriminio.class, this::onCalculatePatriminio)

                .build();
    }

    public void onSimultaneas(BlotterMessage.Simultaneas simultaneas) {

        try {
            if (MainApp.hasManualSimultaneasOverride(account)) {
                reloadSimultaneasFromSources();
                publishSimultaneasSnapshot();
                return;
            }

            if (!isSimultaneaVigente(simultaneas)) {
                return;
            }

            simultaneasHashMap.put(simultaneas.getId(), simultaneas);
            publishSimultaneasSnapshot();

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void onRefreshManualSimultaneas(RefreshManualSimultaneas msg) {
        try {
            if (msg.isRecalculate() && requiereCreasys) {
                calculatePatrimonio();
            } else {
                reloadSimultaneasFromSources();
                publishSimultaneasSnapshot();
            }
        } catch (Exception e) {
            log.error("Error refrescando simultáneas manuales para cuenta {}", account, e);
        }
    }

    private void calculatePrestamos() {
        try {

            if (!requiereCreasys) return;

            snapshotPrestamos = loadPrestamosFromSources();
            BlotterMessage.SnapshotPrestamos msg = snapshotPrestamos.build();
            persistSnapshotPrestamos(msg);
            actorSession.forEach((k, ses) -> ses.tell(msg, ActorRef.noSender()));

        } catch (Exception e) {
            log.error("Error calculando/enviando SnapshotPrestamos para {}: {}", account, e.getMessage(), e);
        }
    }

    private void onRefreshManualPrestamos(RefreshManualPrestamos msg) {
        try {
            calculatePrestamos();
            if (msg.isRecalculate() && requiereCreasys) {
                calculatePatrimonio();
            }
        } catch (Exception e) {
            log.error("Error refrescando préstamos manuales para cuenta {}", account, e);
        }
    }

    private void onRefreshManualSaldo(RefreshManualSaldo msg) {
        try {
            if (msg.isRecalculate() && requiereCreasys) {
                calculatePatrimonio();
            } else {
                publishManualSaldoWithoutSql();
            }
        } catch (Exception e) {
            log.error("Error refrescando saldo manual para cuenta {}", account, e);
        }
    }

    private void publishManualSaldoWithoutSql() {
        ManualSaldoOverride override = MainApp.getManualSaldoOverride(account);
        double cajaValue = override != null ? override.getCaja() : 0d;
        double transitoriasValue = override != null ? override.getCuentaTransitorias() : 0d;
        double garantiaValue = override != null ? override.getGarantiaEfectivo() : 0d;

        BlotterMessage.ValuesPatrimonio.Builder caja = patrimonio.hasCaja()
                ? patrimonio.getCaja().toBuilder()
                : BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Cajas");
        BlotterMessage.ValuesPatrimonio.Builder transitorias = patrimonio.hasCuentaTransitoriasPorCobrarPagar()
                ? patrimonio.getCuentaTransitoriasPorCobrarPagar().toBuilder()
                : BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Cuentas transitorias por cobrar/pagar");
        BlotterMessage.ValuesPatrimonio.Builder garantia = patrimonio.hasGarantiaEfectivo()
                ? patrimonio.getGarantiaEfectivo().toBuilder()
                : BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Garantias en efectivo");

        caja.setValues(cajaValue);
        transitorias.setValues(transitoriasValue);
        garantia.setValues(garantiaValue);

        double liquidezValue = cajaValue + transitoriasValue + garantiaValue;
        double rentaVariableValue = patrimonio.hasRentaVariable()
                ? patrimonio.getRentaVariable().getValues()
                : 0d;
        double activosValue = liquidezValue + rentaVariableValue;

        BlotterMessage.ValuesPatrimonio.Builder liquidez = patrimonio.hasLiquidez()
                ? patrimonio.getLiquidez().toBuilder()
                : BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("LIQUIDEZ");
        liquidez.setValues(liquidezValue);

        BlotterMessage.ValuesPatrimonio.Builder activos = patrimonio.hasActivos()
                ? patrimonio.getActivos().toBuilder()
                : BlotterMessage.ValuesPatrimonio.newBuilder().setDescription("Activos");
        activos.setValues(activosValue).setPorcentage(activosValue == 0d ? 0d : 100d);

        caja.setPorcentage(safePercentage(cajaValue, activosValue));
        transitorias.setPorcentage(safePercentage(transitoriasValue, activosValue));
        garantia.setPorcentage(safePercentage(garantiaValue, activosValue));
        liquidez.setPorcentage(safePercentage(liquidezValue, activosValue));

        patrimonio.setCuenta(account)
                .setCurrency(RoutingMessage.Currency.CLP)
                .setCaja(caja)
                .setCuentaTransitoriasPorCobrarPagar(transitorias)
                .setGarantiaEfectivo(garantia)
                .setLiquidez(liquidez)
                .setActivos(activos);

        ProtoDateProcessor.setDateProcesorIfMissing(balance);
        double carteraActual = balance.getCartera();
        double leverage = palanca != null && palanca > 0d ? palanca : 3d;
        marginaccount = marginLimit != null && marginLimit == -1d
                ? marginLimit
                : activosValue * leverage - carteraActual;
        balance.setCuenta(account)
                .setCupo(marginaccount)
                .setSaldoDisponible(Math.max(0d, marginaccount));

        if (override != null) {
            applyManualSaldoOverrideToBalance();
        } else {
            clearManualSaldoFieldsFromBalance();
        }

        rebuildLogicaPosition("MANUAL_SALDO");
        BlotterMessage.Patrimonio patrimonioMessage = patrimonio.build();
        BlotterMessage.Balance balanceMessage = balance.build();
        actorSession.values().forEach(session -> {
            session.tell(patrimonioMessage, ActorRef.noSender());
            session.tell(balanceMessage, ActorRef.noSender());
        });
        persistPatrimonio(patrimonioMessage);
        persistBalance(balanceMessage);
        log.info("[ManualSaldo] cuenta={} sql=false override={} caja={} activos={} saldoDisponible={} sesiones={}",
                account, override != null, cajaValue, activosValue, balanceMessage.getSaldoDisponible(), actorSession.size());
    }

    private void clearManualSaldoFieldsFromBalance() {
        balance.setGarantiasConstituidas(0d)
                .setGarantiasExigidas(0d)
                .setGarantiasReservadas(0d)
                .setLimiteFinanciero(0d)
                .setGarantiasDisponible(0d)
                .setOrdenesActivasCompras(0d)
                .setOrdenesActivasVentas(0d)
                .setOrdenesCalzadasCompras(0d)
                .setOrdenesCalzadasVentas(0d)
                .setOrdenesCestaCompras(0d)
                .setOrdenesCestaVentas(0d)
                .setRendimiento(0d)
                .setTotal(0d);
    }

    public void calculatePatrimonio() {
        try {

            if (!requiereCreasys) {
                return;
            }

            log.info("⏳ [START] Cálculo patrimonio cuenta {}", account);
            long t0 = System.currentTimeMillis();

            String prefixAccount = account.replace("-", "/");

            reloadSimultaneasFromSources();
            publishSimultaneasSnapshot();

            patrimonioMaps = CalculoCreasys.saldoCaja(prefixAccount);
            applyManualSaldoOverrideToPatrimonio();
            applyManualPrestamosOverrideToPatrimonio();

            if (!patrimonioMaps.containsKey(RoutingMessage.Currency.CLP)) {
                patrimonioMaps.put(RoutingMessage.Currency.CLP, BlotterMessage.Patrimonio.newBuilder()
                        .setCaja(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setCuentaTransitoriasPorCobrarPagar(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setGarantiaEfectivo(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setPrestamos(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                );
            }

            BlotterMessage.SnapshotPositionHistory.Builder sqlSnapshotPositionHistory =
                    CalculoCreasys.cierreCarteraResumida(prefixAccount, patrimonioMaps, simultaneasHashMap);
            snapshotPositionHistory = loadSqlSnapshotState(sqlSnapshotPositionHistory);

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
                        .setPorcentage(activosAux == 0d ? 0d : 100d);

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
                caja.setPorcentage(safePercentage(caja.getValues(), activosAux));
                cuentaTransitoriasPorCobrarPagar.setPorcentage(safePercentage(cuentaTransitoriasPorCobrarPagar.getValues(), activosAux));
                garantiaEfectivo.setPorcentage(safePercentage(garantiaEfectivo.getValues(), activosAux));
                liquidez.setPorcentage(safePercentage(liquidez.getValues(), activosAux));
                prestamos.setPorcentage(safePercentage(prestamos.getValues(), activosAux));
                rentaFija.setPorcentage(safePercentage(rentaFija.getValues(), activosAux));
                fondosMutuos.setPorcentage(safePercentage(fondosMutuos.getValues(), activosAux));
                fondoInversionRentaVariable.setPorcentage(safePercentage(fondoInversionRentaVariable.getValues(), activosAux));
                activosInmobiliarios.setPorcentage(safePercentage(activosInmobiliarios.getValues(), activosAux));
                eftsRentaVariable.setPorcentage(safePercentage(eftsRentaVariable.getValues(), activosAux));
                inversionesAlternativas.setPorcentage(safePercentage(inversionesAlternativas.getValues(), activosAux));
                derivados.setPorcentage(safePercentage(derivados.getValues(), activosAux));
                rvNacional.setPorcentage(safePercentage(rvNacional.getValues(), activosAux));
                rvExtranjeros.setPorcentage(safePercentage(rvExtranjeros.getValues(), activosAux));
                accionesExtranjeras.setPorcentage(safePercentage(accionesExtranjeras.getValues(), activosAux));
                simultaneas.setPorcentage(safePercentage(simultaneas.getValues(), activosAux));
                accionesNacionales.setPorcentage(safePercentage(accionesNacionales.getValues(), activosAux));
                rentaVariable.setPorcentage(safePercentage(rentaVariable.getValues(), activosAux));

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
                applyManualSaldoOverrideToBalance();

            } else {
                /* cuenta sin posiciones ni caja */
                ProtoDateProcessor.setDateProcesorIfMissing(balance);
                balance.setCuenta(account).setCartera(0d).setCupo(marginaccount).setSaldoDisponible(marginaccount > 0 ? marginaccount : 0d);
                applyManualSaldoOverrideToBalance();
            }

            reconcileIntradayStateFromOrdersAndTrades();

            /* ========== Actualizar lógica de posiciones y notificar sesiones ========== */
            rebuildLogicaPosition("PATRIMONIO");

            actorSession.values().forEach(ses -> {
                ses.tell(patrimonio.build(), ActorRef.noSender());
                ses.tell(snapshotPositionHistory.build(), ActorRef.noSender());
                ses.tell(balance.build(), ActorRef.noSender());
            });

            persistPatrimonio(patrimonio.build());
            persistSnapshotPositionHistory(snapshotPositionHistory.build());
            persistBalance(balance.build());
            logIntradayPublishSummary("POST_RECALC");

            calculatePrestamos();



        } catch (Exception e) {
            log.error("❌ Error en calculatePatrimonio cuenta {}", account, e);
        }
    }

    private BlotterMessage.SnapshotPositionHistory.Builder loadSqlSnapshotState(
            BlotterMessage.SnapshotPositionHistory.Builder sqlSnapshotPositionHistory) {

        snapshotPositionHistoryMaps.clear();
        sqlSnapshotPositionHistory.getPositionsHistoryBuilderList()
                .forEach(positionFromSql -> snapshotPositionHistoryMaps.put(positionFromSql.getInstrument(), positionFromSql.clone()));

        return rebuildSnapshotPositionHistory();
    }

    private void reconcileIntradayStateFromOrdersAndTrades() {
        try {
            double baseSaldoDisponible = balance.getSaldoDisponible();
            double saldoDelta = 0d;
            double ordenesActivasCompras = 0d;
            double ordenesActivasVentas = 0d;
            double ordenesCalzadasCompras = 0d;
            double ordenesCalzadasVentas = 0d;
            int tradesSeen = 0;
            int tradesApplied = 0;
            int tradesSkippedByDate = 0;
            int tradesSkippedByType = 0;
            int ordersSeen = 0;
            int activeOrdersApplied = 0;
            int ordersSkippedByDate = 0;
            int ordersSkippedInactive = 0;
            HashMap<String, Double> openSellLeavesBySymbol = new HashMap<>();
            HashMap<String, Double> baseQtyBySymbol = snapshotPositionHistoryMaps.values().stream()
                    .collect(Collectors.toMap(
                            BlotterMessage.PositionHistory.Builder::getInstrument,
                            BlotterMessage.PositionHistory.Builder::getAvailableQuantity,
                            (left, right) -> right,
                            HashMap::new));
            HashMap<String, Double> tradeQtyDeltaBySymbol = new HashMap<>();

            for (RoutingMessage.Order trade : tradesMap.values()) {
                tradesSeen++;

                if (!MainApp.isOrderFromToday(trade)) {
                    tradesSkippedByDate++;
                    continue;
                }

                if (!trade.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {
                    tradesSkippedByType++;
                    continue;
                }

                tradesApplied++;
                double notional = calculateNotional(trade.getSymbol(),
                        trade.getSecurityExchange(),
                        trade.getLastPx(),
                        trade.getLastQty());

                if (trade.getSide().equals(RoutingMessage.Side.BUY)) {
                    ordenesCalzadasCompras += notional;
                    saldoDelta -= notional;
                    tradeQtyDeltaBySymbol.merge(trade.getSymbol(), trade.getLastQty(), Double::sum);
                    applyTradeDeltaToPosition(trade, trade.getLastQty());
                } else if (trade.getSide().equals(RoutingMessage.Side.SELL)) {
                    ordenesCalzadasVentas += notional;
                    saldoDelta += notional;
                    tradeQtyDeltaBySymbol.merge(trade.getSymbol(), -trade.getLastQty(), Double::sum);
                    applyTradeDeltaToPosition(trade, -trade.getLastQty());
                }
            }

            for (RoutingMessage.Order order : ordersMap.values()) {
                ordersSeen++;

                if (!MainApp.isOrderFromToday(order)) {
                    ordersSkippedByDate++;
                    continue;
                }

                boolean activeOrder = order.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED)
                        || order.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED);

                if (!activeOrder || order.getLeaves() <= 0d) {
                    ordersSkippedInactive++;
                    continue;
                }

                activeOrdersApplied++;
                double reserveNotional = calculateNotional(order.getSymbol(),
                        order.getSecurityExchange(),
                        order.getPrice(),
                        order.getLeaves());

                if (order.getSide().equals(RoutingMessage.Side.BUY)) {
                    ordenesActivasCompras += reserveNotional;
                    saldoDelta -= reserveNotional;
                } else if (order.getSide().equals(RoutingMessage.Side.SELL)) {
                    ordenesActivasVentas += reserveNotional;
                    openSellLeavesBySymbol.merge(order.getSymbol(), order.getLeaves(), Double::sum);
                }
            }

            openSellLeavesBySymbol.forEach((symbol, reservedQty) -> {
                BlotterMessage.PositionHistory.Builder position = getOrCreatePositionHistory(symbol, 0d);
                position.setAvailableQuantity(position.getAvailableQuantity() - reservedQty);
                snapshotPositionHistoryMaps.put(symbol, position);
            });

            balance.setOrdenesActivasCompras(ordenesActivasCompras);
            balance.setOrdenesActivasVentas(ordenesActivasVentas);
            balance.setOrdenesCalzadasCompras(ordenesCalzadasCompras);
            balance.setOrdenesCalzadasVentas(ordenesCalzadasVentas);
            balance.setSaldoDisponible(Math.max(0d, baseSaldoDisponible + saldoDelta));

            snapshotPositionHistory = rebuildSnapshotPositionHistory();
            logIntradayPositionRebuild(baseQtyBySymbol, tradeQtyDeltaBySymbol, openSellLeavesBySymbol);

            if (tradesSeen > 0
                    || ordersSeen > 0
                    || Double.compare(ordenesCalzadasCompras, 0d) != 0
                    || Double.compare(ordenesCalzadasVentas, 0d) != 0
                    || Double.compare(ordenesActivasCompras, 0d) != 0
                    || Double.compare(ordenesActivasVentas, 0d) != 0) {
                log.info("[IntradayRebuild] cuenta {} key={} tradesSeen={} tradesApplied={} tradesSkipDate={} tradesSkipType={} ordersSeen={} activeOrdersApplied={} ordersSkipDate={} ordersSkipInactive={} saldoBase={} saldoFinal={} calzCompras={} calzVentas={} actCompras={} actVentas={} snapshotPositions={}",
                        account,
                        getDailyRedisKey(),
                        tradesSeen,
                        tradesApplied,
                        tradesSkippedByDate,
                        tradesSkippedByType,
                        ordersSeen,
                        activeOrdersApplied,
                        ordersSkippedByDate,
                        ordersSkippedInactive,
                        baseSaldoDisponible,
                        balance.getSaldoDisponible(),
                        ordenesCalzadasCompras,
                        ordenesCalzadasVentas,
                        ordenesActivasCompras,
                        ordenesActivasVentas,
                        snapshotPositionHistoryMaps.size());
            }

            if (balance.getSaldoDisponible() == 0d
                    && (Double.compare(ordenesCalzadasCompras, 0d) > 0
                    || Double.compare(ordenesActivasCompras, 0d) > 0)) {
                log.warn("[IntradayRebuild][SALDO_CERO] cuenta {} key={} saldoBase={} saldoDelta={} calzCompras={} actCompras={} cupo={}",
                        account,
                        getDailyRedisKey(),
                        baseSaldoDisponible,
                        saldoDelta,
                        ordenesCalzadasCompras,
                        ordenesActivasCompras,
                        balance.getCupo());
            }
        } catch (Exception e) {
            log.error("Error reconciliando estado intradía para cuenta {}", account, e);
        }
    }

    private void applyTradeDeltaToPosition(RoutingMessage.Order order, double qtyDelta) {
        boolean createdFromIntradayTrade = !snapshotPositionHistoryMaps.containsKey(order.getSymbol());
        BlotterMessage.PositionHistory.Builder position = getOrCreatePositionHistory(order.getSymbol(), order.getLastPx());
        double qtyBefore = position.getAvailableQuantity();
        position.setAvailableQuantity(position.getAvailableQuantity() + qtyDelta);

        if (position.getPurchaseAmount() == 0d && order.getLastPx() > 0d && position.getAvailableQuantity() > 0d) {
            position.setPurchaseAmount(order.getLastPx() * position.getAvailableQuantity());
        }

        snapshotPositionHistoryMaps.put(order.getSymbol(), position);

        if (createdFromIntradayTrade) {
            log.info("[IntradayRebuild][NEW_HISTORY] cuenta {} symbol={} side={} execId={} qtyDelta={} lastPx={} finalQty={}",
                    account,
                    order.getSymbol(),
                    order.getSide(),
                    order.getExecId(),
                    qtyDelta,
                    order.getLastPx(),
                    position.getAvailableQuantity());
        }

        if (position.getAvailableQuantity() < 0d) {
            log.warn("[IntradayRebuild][NEGATIVE_QTY] cuenta {} symbol={} side={} execId={} qtyBefore={} qtyDelta={} finalQty={}",
                    account,
                    order.getSymbol(),
                    order.getSide(),
                    order.getExecId(),
                    qtyBefore,
                    qtyDelta,
                    position.getAvailableQuantity());
        }
    }

    private BlotterMessage.PositionHistory.Builder getOrCreatePositionHistory(String symbol, double fallbackMarketPrice) {
        BlotterMessage.PositionHistory.Builder position = snapshotPositionHistoryMaps.get(symbol);
        if (position != null) {
            return position;
        }

        position = BlotterMessage.PositionHistory.newBuilder();
        ProtoDateProcessor.setDateProcesorIfMissing(position);
        position.setAccount(account);
        position.setInstrument(symbol);
        if (fallbackMarketPrice > 0d) {
            position.setMarketPrice(fallbackMarketPrice);
        }
        snapshotPositionHistoryMaps.put(symbol, position);
        return position;
    }

    private void logIntradayPositionRebuild(HashMap<String, Double> baseQtyBySymbol,
                                            HashMap<String, Double> tradeQtyDeltaBySymbol,
                                            HashMap<String, Double> openSellLeavesBySymbol) {
        HashSet<String> symbols = new HashSet<>();
        symbols.addAll(tradeQtyDeltaBySymbol.keySet());
        symbols.addAll(openSellLeavesBySymbol.keySet());

        symbols.forEach(symbol -> {
            BlotterMessage.PositionHistory.Builder finalPosition = snapshotPositionHistoryMaps.get(symbol);
            log.info("[IntradayRebuild] cuenta {} instrumento {} baseQty={} tradeDelta={} reservedSell={} finalQty={}",
                    account,
                    symbol,
                    baseQtyBySymbol.getOrDefault(symbol, 0d),
                    tradeQtyDeltaBySymbol.getOrDefault(symbol, 0d),
                    openSellLeavesBySymbol.getOrDefault(symbol, 0d),
                    finalPosition != null ? finalPosition.getAvailableQuantity() : 0d);

            if (finalPosition != null && finalPosition.getAvailableQuantity() < 0d) {
                log.warn("[IntradayRebuild][NEGATIVE_FINAL_QTY] cuenta {} instrumento {} baseQty={} tradeDelta={} reservedSell={} finalQty={}",
                        account,
                        symbol,
                        baseQtyBySymbol.getOrDefault(symbol, 0d),
                        tradeQtyDeltaBySymbol.getOrDefault(symbol, 0d),
                        openSellLeavesBySymbol.getOrDefault(symbol, 0d),
                        finalPosition.getAvailableQuantity());
            }
        });
    }

    private BlotterMessage.SnapshotPositionHistory.Builder rebuildSnapshotPositionHistory() {
        BlotterMessage.SnapshotPositionHistory.Builder rebuiltSnapshot = BlotterMessage.SnapshotPositionHistory.newBuilder();
        snapshotPositionHistoryMaps.values().forEach(rebuiltSnapshot::addPositionsHistory);
        return rebuiltSnapshot;
    }

    private double calculateNotional(String symbol,
                                     RoutingMessage.SecurityExchangeRouting exchange,
                                     double price,
                                     double quantity) {
        if (price <= 0d || quantity <= 0d) {
            return 0d;
        }

        String id = symbol + IDGenerator.conversorExdestination(exchange).name();

        if (MainApp.getSecurityExchangeSymbolsMaps().containsKey(id)
                && MainApp.getSecurityExchangeSymbolsMaps().get(id).getCurrency().equals(RoutingMessage.Currency.USD.name())) {
            String dolar = "USD/CLP" + "DATATEC_XBCL" + "T2" + "CS";
            cl.vc.service.util.BookSnapshot snapshot = MainApp.getSnapshotHashMap().get(dolar);
            if (snapshot != null && snapshot.getStatistic() != null && snapshot.getStatistic().getAskPx() > 0d) {
                return price * quantity * snapshot.getStatistic().getAskPx();
            }
        }

        return price * quantity;
    }

    private void logIntradayTradeIn(RoutingMessage.Order order,
                                    RoutingMessage.Order oldOrder,
                                    boolean duplicateExecId) {
        double notional = calculateNotional(order.getSymbol(),
                order.getSecurityExchange(),
                order.getLastPx(),
                order.getLastQty());

        log.info("[IntradayTrade][IN] cuenta {} key={} orderId={} execId={} symbol={} side={} status={} execType={} lastQty={} lastPx={} cumQty={} leaves={} notional={} duplicateExecId={} oldStatus={} oldExecType={} histQtyBefore={} saldoBefore={} tradesStored={} ordersStored={}",
                account,
                getDailyRedisKey(),
                order.getId(),
                order.getExecId(),
                order.getSymbol(),
                order.getSide(),
                order.getOrdStatus(),
                order.getExecType(),
                order.getLastQty(),
                order.getLastPx(),
                order.getCumQty(),
                order.getLeaves(),
                notional,
                duplicateExecId,
                oldOrder != null ? oldOrder.getOrdStatus() : "-",
                oldOrder != null ? oldOrder.getExecType() : "-",
                getPositionHistoryAvailableQuantity(order.getSymbol()),
                balance.getSaldoDisponible(),
                tradesMap.size(),
                ordersMap.size());
    }

    private void logIntradayTradeAfterLive(RoutingMessage.Order order, boolean duplicateExecId) {
        log.info("[IntradayTrade][LIVE_AFTER] cuenta {} key={} orderId={} execId={} symbol={} side={} histQtyAfter={} saldoAfter={} actCompras={} actVentas={} calzCompras={} calzVentas={} snapshotMapSize={} tradesStored={} ordersStored={} duplicateExecId={}",
                account,
                getDailyRedisKey(),
                order.getId(),
                order.getExecId(),
                order.getSymbol(),
                order.getSide(),
                getPositionHistoryAvailableQuantity(order.getSymbol()),
                balance.getSaldoDisponible(),
                balance.getOrdenesActivasCompras(),
                balance.getOrdenesActivasVentas(),
                balance.getOrdenesCalzadasCompras(),
                balance.getOrdenesCalzadasVentas(),
                snapshotPositionHistoryMaps.size(),
                tradesMap.size(),
                ordersMap.size(),
                duplicateExecId);
    }

    private double getPositionHistoryAvailableQuantity(String symbol) {
        BlotterMessage.PositionHistory.Builder position = snapshotPositionHistoryMaps.get(symbol);
        return position != null ? position.getAvailableQuantity() : 0d;
    }

    private boolean hasTodayIntradayActivity() {
        return tradesMap.values().stream()
                .anyMatch(order -> MainApp.isOrderFromToday(order)
                        && order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE))
                || ordersMap.values().stream().anyMatch(MainApp::isOrderFromToday);
    }

    private long countTodayTrades() {
        return tradesMap.values().stream()
                .filter(order -> MainApp.isOrderFromToday(order)
                        && order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE))
                .count();
    }

    private long countTodayOrders() {
        return ordersMap.values().stream()
                .filter(MainApp::isOrderFromToday)
                .count();
    }

    static String tradeExecutionKey(RoutingMessage.Order order) {
        String orderId = order.getId() == null ? "" : order.getId();
        String execId = order.getExecId() == null ? "" : order.getExecId();
        String cumulativeQuantity = BigDecimal.valueOf(order.getCumQty())
                .stripTrailingZeros()
                .toPlainString();
        return orderId + "|" + execId + "|" + cumulativeQuantity;
    }

    private long countNegativeHistoryPositions() {
        return snapshotPositionHistoryMaps.values().stream()
                .filter(position -> position.getAvailableQuantity() < 0d)
                .count();
    }

    private boolean isRedisPersistenceEnabled() {
        return Boolean.parseBoolean(MainApp.getProperties().getProperty("redis.enable.persistencia"));
    }

    private void logIntradayPublishSummary(String phase) {
        if (!hasTodayIntradayActivity()) {
            return;
        }

        log.info("[IntradayPublish] phase={} cuenta {} key={} sessions={} snapshotPositions={} negativePositions={} saldoDisponible={} cupo={} cartera={} actCompras={} actVentas={} calzCompras={} calzVentas={} tradesHoy={} ordersHoy={} redisPersistence={}",
                phase,
                account,
                getDailyRedisKey(),
                actorSession.size(),
                snapshotPositionHistoryMaps.size(),
                countNegativeHistoryPositions(),
                balance.getSaldoDisponible(),
                balance.getCupo(),
                balance.getCartera(),
                balance.getOrdenesActivasCompras(),
                balance.getOrdenesActivasVentas(),
                balance.getOrdenesCalzadasCompras(),
                balance.getOrdenesCalzadasVentas(),
                countTodayTrades(),
                countTodayOrders(),
                isRedisPersistenceEnabled());
    }

    public void onPositionHistory(BlotterMessage.SnapshotPositionHistory msg) {

        snapshotPositionHistory = msg.toBuilder();
        persistSnapshotPositionHistory(msg);

        if (hasTodayIntradayActivity()) {
            log.info("[IntradaySnapshot] cuenta {} key={} snapshotPositions={} mapPositions={} sessions={} negativePositions={}",
                    account,
                    getDailyRedisKey(),
                    msg.getPositionsHistoryCount(),
                    snapshotPositionHistoryMaps.size(),
                    actorSession.size(),
                    countNegativeHistoryPositions());
        }

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
        if (hasTodayIntradayActivity()) {
            log.info("[IntradayBalance] cuenta {} key={} saldoDisponible={} cupo={} cartera={} actCompras={} actVentas={} calzCompras={} calzVentas={} sessions={}",
                    account,
                    getDailyRedisKey(),
                    balance.getSaldoDisponible(),
                    balance.getCupo(),
                    balance.getCartera(),
                    balance.getOrdenesActivasCompras(),
                    balance.getOrdenesActivasVentas(),
                    balance.getOrdenesCalzadasCompras(),
                    balance.getOrdenesCalzadasVentas(),
                    actorSession.size());
        }
        actorSession.forEach((key, value) -> value.tell(msg, ActorRef.noSender()));
    }

    public void onCancelRequest(RoutingMessage.OrderCancelRequest msg) {
        try {
            RoutingMessage.Order orderRequest = ordersMap.get(msg.getId());
            if (isExternallyUnavailable(msg.getId(), orderRequest)) {
                publishRecoveryReject(msg.getId());
                return;
            }

            if (orderRequest == null) {
                log.warn("Cancel sin orden base en actor cuenta: {} id: {}", account, msg.getId());
                return;
            }

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
            if (order == null) {
                log.warn("Replace sin orden base en actor cuenta: {} id: {}", account, msg.getId());
                return;
            }
            if (isExternallyUnavailable(msg.getId(), order)) {
                publishRecoveryReject(msg.getId());
                return;
            }

            RoutingMessage.OrderReplaceRequest effectiveMsg = normalizeNoneStrategyReplace(order, msg);
            if (effectiveMsg.getMaxFloor() != msg.getMaxFloor()) {
                double targetQuantity = effectiveMsg.getQuantity() > 0d
                        ? effectiveMsg.getQuantity()
                        : order.getOrderQty();
                double leavesQuantity = Math.max(0d, targetQuantity - order.getCumQty());
                log.info("[Replace/NONE_STRATEGY] maxFloor ajustado {} -> {} cuenta={} id={} cumQty={} leaves={}",
                        msg.getMaxFloor(), effectiveMsg.getMaxFloor(), account, msg.getId(),
                        order.getCumQty(), leavesQuantity);
            }

            // Validar monto nominal del replace (nuevo precio × nueva cantidad)
            Optional<String> notionalRejection =
                    MainApp.checkNotionalLimit(order.getSecurityExchange(), effectiveMsg.getPrice(), effectiveMsg.getQuantity());
            if (notionalRejection.isPresent()) {
                log.warn("⛔ [Risk] {} — replace cuenta: {} id: {}", notionalRejection.get(), account, msg.getId());


                RoutingMessage.OrderCancelReject orderCancelReject = RoutingMessage.OrderCancelReject.newBuilder()
                        .setId(order.getId())
                        .setText(notionalRejection.get())
                        .build();

                getSelf().tell(orderCancelReject, ActorRef.noSender());
                return;
            }

            RoutingMessage.Order replaceCandidate = buildReplaceCandidate(order, effectiveMsg);
            Optional<RoutingMessage.Order> odConflict = MainApp.isOdProtectionEnabled()
                    ? findOdConflict(replaceCandidate)
                    : Optional.empty();
            if (odConflict.isPresent()) {
                RoutingMessage.Order conflictingOrder = odConflict.get();
                String reason = String.format(
                        "Replace rechazado OD: cuenta %s cruzaría contra punta %s propia en %s",
                        account, conflictingOrder.getSide().name(), replaceCandidate.getSymbol());
                MainApp.recordOdAttempt(buildOdAttempt(replaceCandidate, conflictingOrder, reason));
                log.warn("⛔ [OD] {} usuario={} replace={} {}@{} contra={} {}@{}",
                        reason,
                        replaceCandidate.getOperator(),
                        replaceCandidate.getSide(),
                        replaceCandidate.getOrderQty(),
                        replaceCandidate.getPrice(),
                        conflictingOrder.getId(),
                        conflictingOrder.getOrderQty(),
                        conflictingOrder.getPrice());
                RoutingMessage.OrderCancelReject orderCancelReject = RoutingMessage.OrderCancelReject.newBuilder()
                        .setId(order.getId())
                        .setExecId(IDGenerator.getID())
                        .setText(reason)
                        .build();
                getSelf().tell(orderCancelReject, ActorRef.noSender());
                return;
            }

            ensureLogicaPosition("REPLACE_REQUEST");
            if (!logicaPosition.calculateBalanceReplace(effectiveMsg, order)) {
                return;
            }

            if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BEST) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.HOLGURA) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.TRAILING) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_AGGRESSIVE) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_LAST) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET_PASSIVE) ||
                    order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.OCO)) {

                strategyActors.get(effectiveMsg.getId()).tell(effectiveMsg, ActorRef.noSender());

            } else if (order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET)) {
                MainApp.getConnections().get(RoutingMessage.SecurityExchangeRouting.BASKETS).sendMessage(effectiveMsg);

            } else {
                MainApp.getConnections().get(order.getSecurityExchange()).sendMessage(effectiveMsg);
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

        RoutingMessage.Order order = orders.getOrder();

        if (MainApp.getBlockedSymbols().contains(order.getSymbol())) {
            log.warn("⛔ [Risk] Orden rechazada por riesgo — símbolo bloqueado: {} cuenta: {} id: {}",
                    order.getSymbol(), account, order.getId());
            RoutingMessage.Order rejected = order.toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setExecId(cl.vc.module.protocolbuff.generator.IDGenerator.getID())
                    .setText("Orden rechazada por riesgo")
                    .build();
            getSelf().tell(rejected, ActorRef.noSender());
            return;
        }

        Optional<String> notionalRejection =
                MainApp.checkNotionalLimit(order.getSecurityExchange(), order.getPrice(), order.getOrderQty());
        if (notionalRejection.isPresent()) {
            log.warn("⛔ [Risk] {} — cuenta: {} id: {}", notionalRejection.get(), account, order.getId());
            getSelf().tell(order.toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setExecId(IDGenerator.getID())
                    .setText(notionalRejection.get())
                    .build(), ActorRef.noSender());
            return;
        }

        Optional<RoutingMessage.Order> odConflict = MainApp.isOdProtectionEnabled()
                ? findOdConflict(order)
                : Optional.empty();
        if (odConflict.isPresent()) {
            RoutingMessage.Order conflictingOrder = odConflict.get();
            String reason = String.format(
                    "Orden rechazada OD: cuenta %s ya tiene punta %s activa en %s",
                    account, conflictingOrder.getSide().name(), order.getSymbol());
            MainApp.recordOdAttempt(buildOdAttempt(order, conflictingOrder, reason));
            log.warn("⛔ [OD] {} usuario={} nueva={} {}@{} contra={} {}@{}",
                    reason,
                    order.getOperator(),
                    order.getSide(),
                    order.getOrderQty(),
                    order.getPrice(),
                    conflictingOrder.getId(),
                    conflictingOrder.getOrderQty(),
                    conflictingOrder.getPrice());
            getSelf().tell(order.toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED)
                    .setExecId(IDGenerator.getID())
                    .setText(reason)
                    .build(), ActorRef.noSender());
            return;
        }

        ensureLogicaPosition("NEW_ORDER_REQUEST");
        if (!logicaPosition.calculateBalanceReplace(orders)) {
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
        RoutingMessage.OrderCancelReject operatorReject = rejected;
        if (OrigClOrdIdRecoverySupport.isMissingFromSequence(rejected.getText())) {
            RoutingMessage.Order rejectedOrder = ordersMap.get(rejected.getId());
            if (rejectedOrder != null && isStrategyManagedByActor(rejectedOrder.getStrategyOrder())) {
                return;
            }

            int attempts = missingOrigClOrdIdRejects.merge(rejected.getId(), 1, Integer::sum);
            operatorReject = OrigClOrdIdRecoverySupport.withOperatorReason(rejected);
            log.warn("[OrderRecovery][ORIG_CL_ORD_ID_MISSING] account={} orderId={} attempt={}/{} exchangeReason={}",
                    account, rejected.getId(), attempts, OrigClOrdIdRecoverySupport.MAX_REJECTS,
                    rejected.getText());

            if (attempts >= OrigClOrdIdRecoverySupport.MAX_REJECTS) {
                markExternalOrderUnavailable(rejected.getId());
            }
        }

        RoutingMessage.OrderCancelReject finalReject = operatorReject;
        actorSession.forEach((key, value) -> value.tell(finalReject, ActorRef.noSender()));
    }

    private void onExternalOrderUnavailable(ActorStrategy.ExternalOrderUnavailable unavailable) {
        markExternalOrderUnavailable(unavailable.getOrderId());
        actorSession.forEach((key, value) -> value.tell(unavailable.getRejected(), ActorRef.noSender()));
    }

    private void onStrategyStatusReason(ActorStrategy.StrategyStatusReason status) {
        RoutingMessage.Order current = ordersMap.get(status.getOrderId());
        if (current == null || OrderStateSupport.isConclusiveStrategyTerminal(current)
                || isExternallyUnavailable(status.getOrderId(), current)) {
            return;
        }

        RoutingMessage.Order updated = current.toBuilder().setText(status.getReason()).build();
        ordersMap.put(updated.getId(), updated);
        MainApp.getIdOrders().put(updated.getId(), updated);
        actorSession.forEach((key, value) -> value.tell(updated, ActorRef.noSender()));
        persistOrdersAfterRecoveryMark();
        log.info("[StrategyRecovery][STATUS_REASON] account={} orderId={} strategy={} reason={}",
                account, updated.getId(), updated.getStrategyOrder(), status.getReason());
    }

    private boolean isExternallyUnavailable(String orderId, RoutingMessage.Order order) {
        return externallyUnavailableOrderIds.contains(orderId)
                || OrigClOrdIdRecoverySupport.isMarked(order);
    }

    private void publishRecoveryReject(String orderId) {
        RoutingMessage.OrderCancelReject rejected = RoutingMessage.OrderCancelReject.newBuilder()
                .setId(orderId)
                .setExecId(IDGenerator.getID())
                .setText(OrigClOrdIdRecoverySupport.POSSIBLE_FILLED_OR_CANCEL_REASON)
                .build();
        actorSession.forEach((key, value) -> value.tell(rejected, ActorRef.noSender()));
        log.warn("[OrderRecovery][REQUEST_BLOCKED] account={} orderId={} reason={}",
                account, orderId, rejected.getText());
    }

    private void markExternalOrderUnavailable(String orderId) {
        externallyUnavailableOrderIds.add(orderId);
        missingOrigClOrdIdRejects.remove(orderId);

        ActorRef strategyActor = strategyActors.remove(orderId);
        if (strategyActor != null) {
            getContext().stop(strategyActor);
        }

        RoutingMessage.Order current = ordersMap.get(orderId);
        if (current != null) {
            RoutingMessage.Order marked = current.toBuilder()
                    .setText(OrigClOrdIdRecoverySupport.POSSIBLE_FILLED_OR_CANCEL_REASON)
                    .build();
            ordersMap.put(orderId, marked);
            MainApp.getIdOrders().put(orderId, marked);
            persistOrdersAfterRecoveryMark();
        }

        log.error("[OrderRecovery][BLOCKED_AFTER_RETRIES] account={} orderId={} attempts={} reason={}",
                account, orderId, OrigClOrdIdRecoverySupport.MAX_REJECTS,
                OrigClOrdIdRecoverySupport.POSSIBLE_FILLED_OR_CANCEL_REASON);
    }

    private void persistOrdersAfterRecoveryMark() {
        HashMap<String, RoutingMessage.Order> ordersSnapshot = new HashMap<>(ordersMap);
        MainApp.submitRedisWrite(account, () -> {
            if (MainApp.getOrdersMapRedis() != null) {
                MainApp.getOrdersMapRedis().put(account, ordersSnapshot);
            }
        });
    }

    /**
     * Calcula el AvgPx (precio promedio ponderado, tag 6 FIX) cuando la bolsa deja de mandarlo.
     * Sólo actúa en fills (EXEC_TRADE) que llegan con avgPrice=0. Es el VWAP de TODOS los fills de
     * esa misma orden: Σ(lastPx·lastQty) / cumQty, tomando los fills previos de tradesMap + el actual.
     * No modifica la orden si la bolsa ya trae el avgPrice, ni si no es un trade.
     */
    private RoutingMessage.Order enrichAvgPx(RoutingMessage.Order order) {
        try {
            if (order == null
                    || !order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)
                    || order.getAvgPrice() > 0d) {
                return order;   // no es fill, o la bolsa ya lo mandó
            }

            double value = order.getLastPx() * order.getLastQty();   // fill actual
            double qty = order.getLastQty();

            // Sumar los fills PREVIOS de la misma orden (tradesMap aún no tiene el actual).
            for (RoutingMessage.Order f : tradesMap.values()) {
                if (f.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)
                        && f.getId().equals(order.getId())
                        && !tradeExecutionKey(f).equals(tradeExecutionKey(order))) {
                    value += f.getLastPx() * f.getLastQty();
                    qty += f.getLastQty();
                }
            }

            double denom = order.getCumQty() > 0d ? order.getCumQty() : qty;
            double avg = denom > 0d ? value / denom : order.getLastPx();

            if (avg > 0d) {
                return order.toBuilder().setAvgPrice(avg).build();
            }
        } catch (Exception e) {
            log.error("enrichAvgPx error id={} execId={}", order != null ? order.getId() : null,
                    order != null ? order.getExecId() : null, e);
        }
        return order;
    }

    private void onOrders(RoutingMessage.Order rawOrder) {

        try {

            // La bolsa dejó de mandar el AvgPx (tag 6 FIX). Si en un fill llega en 0, lo calculamos
            // como VWAP de los fills de esa orden. No pisa el valor si la bolsa sí lo manda.
            RoutingMessage.Order incomingOrder = normalizeInconsistentFilledState(enrichAvgPx(rawOrder));

            if (incomingOrder.getExecType().equals(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE)) {
                return;
            }

            if (incomingOrder.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BASKET)) {



            } else {

                RoutingMessage.Order orderold = ordersMap.get(incomingOrder.getId());
                incomingOrder = preserveKnownOrderQuantity(orderold, incomingOrder);
                boolean tradeExecution = incomingOrder.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE);
                String tradeExecutionKey = tradeExecution ? tradeExecutionKey(incomingOrder) : "";
                boolean duplicateExecId = tradeExecution && exceIdProcess.containsKey(tradeExecutionKey);

                if (duplicateExecId) {
                    log.warn("[OrderState][DUPLICATE_TRADE_IGNORED] cuenta={} orderId={} execId={} symbol={} side={} lastQty={} lastPx={} status={}",
                            account,
                            incomingOrder.getId(),
                            incomingOrder.getExecId(),
                            incomingOrder.getSymbol(),
                            incomingOrder.getSide(),
                            incomingOrder.getLastQty(),
                            incomingOrder.getLastPx(),
                            incomingOrder.getOrdStatus());
                    return;
                }

                if (shouldIgnoreStaleOrderUpdate(orderold, incomingOrder)) {
                    log.warn("[OrderState][STALE_UPDATE_IGNORED] cuenta={} orderId={} previousStatus={} previousCumQty={} incomingStatus={} incomingExecType={} incomingCumQty={} incomingExecId={}",
                            account,
                            incomingOrder.getId(),
                            orderold.getOrdStatus(),
                            orderold.getCumQty(),
                            incomingOrder.getOrdStatus(),
                            incomingOrder.getExecType(),
                            incomingOrder.getCumQty(),
                            incomingOrder.getExecId());
                    return;
                }

                RoutingMessage.Order order = preserveFinalStateForLateTrade(orderold, incomingOrder);
                boolean terminalOrder = OrderStateSupport.isConclusiveStrategyTerminal(order);
                if (terminalOrder) {
                    externallyUnavailableOrderIds.remove(order.getId());
                    missingOrigClOrdIdRejects.remove(order.getId());
                } else if (isExternallyUnavailable(order.getId(), orderold)) {
                    order = order.toBuilder()
                            .setText(OrigClOrdIdRecoverySupport.POSSIBLE_FILLED_OR_CANCEL_REASON)
                            .build();
                }

                for (ActorRef sessionActor : actorSession.values()) {
                    sessionActor.tell(order, ActorRef.noSender());
                }

                if (tradeExecution) {
                    logIntradayTradeIn(order, orderold, duplicateExecId);
                }

                ordersMap.put(order.getId(), order);
                MainApp.getIdOrders().put(order.getId(), order);

                if (shouldForceBookRefresh(orderold, order)) {
                    forceBookRefresh(order);
                }

                if (terminalOrder) {
                    if (strategyActors.containsKey(order.getId()) && !order.getStrategyOrder().equals(RoutingMessage.StrategyOrder.VWAP)) {
                        strategyActors.get(order.getId()).tell(PoisonPill.getInstance(), ActorRef.noSender());
                        strategyActors.remove(order.getId());
                    }
                }

                if (tradeExecution) {

                    exceIdProcess.put(tradeExecutionKey, order);
                    tradesMap.put(tradeExecutionKey, order);
                    MainApp.getTradesMapAll().put(account, tradesMap);

                    MongoHistoryRepository.recordFilledOrder(order);


                    // Copia defensiva: el writer serializa en otro hilo mientras el actor sigue mutando tradesMap.
                    HashMap<String, RoutingMessage.Order> tradesSnapshot = new HashMap<>(tradesMap);
                    MainApp.submitRedisWrite(account, () -> {
                        if (MainApp.getTradesMapReddis() != null) {
                            MainApp.getTradesMapReddis().put(account, tradesSnapshot);
                        }
                    });


                    synchronized (lock) {

                        BlotterMessage.Position positions = calculatePositions.onOrder(order, positionsMaps);
                        positionsMaps.put(positions.getId(), positions);

                        getSelf().tell(positions, ActorRef.noSender());

                        String positionsKey = getDailyRedisKey();
                        Map<String, BlotterMessage.Position> positionsSnapshot = new HashMap<>(positionsMaps);
                        MainApp.submitRedisWrite(account, () -> {
                            if (MainApp.getPositionsMapsRedis() != null) {
                                MainApp.getPositionsMapsRedis().put(positionsKey, positionsSnapshot);
                            }
                        });

                    }
                }


                boolean positionStateChanged;
                synchronized (lock) {
                    // calculo de posiciones historicas
                    ensureLogicaPosition("ORDER_UPDATE");
                    positionStateChanged = logicaPosition.orderUpdate(order, orderold);

                    if (tradeExecution) {
                        logIntradayTradeAfterLive(order, duplicateExecId);
                    }

                }
                if (positionStateChanged) {
                    publishIntradayPositionState();
                }
                HashMap<String, RoutingMessage.Order> ordersSnapshot = new HashMap<>(ordersMap);
                MainApp.submitRedisWrite(account, () -> {
                    if (MainApp.getOrdersMapRedis() != null) {
                        MainApp.getOrdersMapRedis().put(account, ordersSnapshot);
                    }
                });
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void createLogicalPosition() {
        rebuildLogicaPosition("LAZY_INIT");
    }

    private void ensureLogicaPosition(String reason) {
        if (logicaPosition == null) {
            rebuildLogicaPosition(reason);
        }
    }

    private void rebuildLogicaPosition(String reason) {
        logicaPosition = new LogicaPosition(resolveMarginForPositionLogic(),
                getSelf(),
                balance,
                snapshotPositionHistoryMaps,
                simultaneasHashMap);

        log.info("[IntradayRisk][LOGIC_READY] reason={} cuenta {} snapshotPositions={} saldoDisponible={} cupo={} ordersStored={} tradesStored={}",
                reason,
                account,
                snapshotPositionHistoryMaps.size(),
                balance.getSaldoDisponible(),
                balance.getCupo(),
                ordersMap.size(),
                tradesMap.size());

        // Publica el estado de custodia para la validación visual del admin (qué cuenta tiene/ no tiene data).
        MainApp.updateAccountCustody(account, snapshotPositionHistoryMaps.size(), balance.getSaldoDisponible());
    }

    private double resolveMarginForPositionLogic() {
        if (marginLimit != null && Double.compare(marginLimit, -1d) == 0) {
            return -1d;
        }
        if (marginaccount != null) {
            return marginaccount;
        }
        if (Double.compare(balance.getCupo(), 0d) != 0) {
            return balance.getCupo();
        }
        return marginLimit != null ? marginLimit : 0d;
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

    @Data
    @AllArgsConstructor
    public static final class RefreshManualSimultaneas {
        private boolean recalculate;
    }

    @Data
    @AllArgsConstructor
    public static final class RefreshManualPrestamos {
        private boolean recalculate;
    }

    @Data
    @AllArgsConstructor
    public static final class RefreshManualSaldo {
        private boolean recalculate;
    }

    private void onInitialize(Initialize ignored) {
        try {

            // ── Custodia: SQL es la fuente de verdad del cierre; Redis solo aporta el delta intradía ──
            // - Al INICIO DEL DÍA (sin órdenes/trades de hoy todavía) la custodia debe salir de SQL
            //   (el cierre del día anterior). Redis no aporta nada útil ahí, y si su snapshot vino
            //   vacío/stale (cacheado de madrugada antes de que el cierre estuviera en SQL) dejaba a
            //   la cuenta SIN custodia -> había que correr el admin web cada mañana (caso dbarrios).
            // - En un REINICIO INTRADÍA (ya hubo trades hoy) se confía en Redis: su snapshot ya incluye
            //   esos trades aplicados sobre el cierre, y evita re-consultar SQL.
            // - Una custodia VACÍA en Redis nunca se confía: se recalcula desde SQL.
            // Nota: calculatePatrimonio() re-aplica los trades intradía (tradesMap) sobre la base SQL
            // vía rebuildSnapshotPositionHistory(), así que no se pierden las posiciones del día.
            boolean restoredEmpty = restoredFromRedisState
                    && snapshotPositionHistory.getPositionsHistoryList().isEmpty();

            boolean usarSqlComoBase = !restoredFromRedisState
                    || !hasTodayIntradayActivity()
                    || restoredEmpty;

            if (requiereCreasys && usarSqlComoBase) {
                calculatePatrimonio();
                calculatePrestamos();
            } else if (restoredFromRedisState) {
                ensureLogicaPosition("INITIALIZE_RESTORE");
            }
            if (!requiereCreasys && MainApp.hasManualSaldoOverride(account)) {
                publishManualSaldoWithoutSql();
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
        private Long cycleId;
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
                    if (msg.getCycleId() != null && msg.getCycleId() > 0L) {
                        MainApp.getAccountLoadTracker().onBackgroundRefreshStart(msg.getCycleId(), account);
                    }

                    long startedAt = System.nanoTime();
                    try {
                        calculatePatrimonio();
                        calculatePrestamos();
                        if (msg.getCycleId() != null && msg.getCycleId() > 0L) {
                            MainApp.getAccountLoadTracker().onBackgroundRefreshFinished(msg.getCycleId(), true, null);
                        }
                    } catch (Exception e) {
                        if (msg.getCycleId() != null && msg.getCycleId() > 0L) {
                            MainApp.getAccountLoadTracker().onBackgroundRefreshFinished(msg.getCycleId(), false, e.getMessage());
                        }
                        throw e;
                    } finally {
                        long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
                        log.info("[Tracker] Refresh margin cuenta {} en {} ms", account, durationMs);
                    }
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
        private Long cycleId;
    }

    @Data
    @AllArgsConstructor
    public static final class UpdateRiskConfig {
        private Double margin;
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
                    if (msg.getCycleId() != null && msg.getCycleId() > 0L) {
                        MainApp.getAccountLoadTracker().onBackgroundRefreshStart(msg.getCycleId(), account);
                    }

                    long startedAt = System.nanoTime();
                    try {
                        calculatePatrimonio();
                        calculatePrestamos();
                        if (msg.getCycleId() != null && msg.getCycleId() > 0L) {
                            MainApp.getAccountLoadTracker().onBackgroundRefreshFinished(msg.getCycleId(), true, null);
                        }
                    } catch (Exception e) {
                        if (msg.getCycleId() != null && msg.getCycleId() > 0L) {
                            MainApp.getAccountLoadTracker().onBackgroundRefreshFinished(msg.getCycleId(), false, e.getMessage());
                        }
                        throw e;
                    } finally {
                        long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
                        log.info("[Tracker] Refresh palanca cuenta {} en {} ms", account, durationMs);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onUpdateRiskConfig(UpdateRiskConfig msg) {
        try {
            boolean changed = false;

            if (msg.getMargin() != null && !Objects.equals(msg.getMargin(), marginLimit)) {
                log.info("se actualiza marginLimit de {} a {} para la cuenta {}",
                        BigDecimal.valueOf(marginLimit == null ? 0 : marginLimit).toPlainString(),
                        BigDecimal.valueOf(msg.getMargin()).toPlainString(),
                        msg.getUsername() + "->" + account);
                marginLimit = msg.getMargin();
                changed = true;
            }

            if (msg.getLeverage() != null && !Objects.equals(msg.getLeverage(), palanca)) {
                log.info("Se actualiza palanca de {} a {} para la cuenta {}",
                        palanca == null ? 0 : palanca,
                        msg.getLeverage(),
                        msg.getUsername() + "->" + account);
                palanca = msg.getLeverage();
                changed = true;
            }

            if (changed && requiereCreasys) {
                long startedAt = System.nanoTime();
                try {
                    calculatePatrimonio();
                    calculatePrestamos();
                } finally {
                    long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
                    log.info("[Tracker] Refresh riesgo cuenta {} en {} ms", account, durationMs);
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

    @Data
    @AllArgsConstructor
    public static final class CalculatePatriminioResult {
        private boolean ok;
        private String account;
        private long durationMs;
        private String message;
    }

    private void onCalculatePatriminio(CalculatePatriminio calculatePatriminio) {
        long startedAt = System.nanoTime();
        try {
            if (requiereCreasys) {
                calculatePatrimonio();
                calculatePrestamos();
            }
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
            getSender().tell(new CalculatePatriminioResult(true, account, durationMs, "ok"), getSelf());
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;
            log.error("Error recalculando SQL para cuenta {}: {}", account, e.getMessage(), e);
            getSender().tell(new CalculatePatriminioResult(false, account, durationMs, e.getMessage()), getSelf());
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

                RoutingMessage.Order restoredOrderValue = normalizeInconsistentFilledState(value1);
                restoredToday.put(key1, restoredOrderValue);

                boolean recoveryBlocked = OrigClOrdIdRecoverySupport.isMarked(restoredOrderValue);
                if (recoveryBlocked) {
                    externallyUnavailableOrderIds.add(restoredOrderValue.getId());
                }

                if (isStrategyManagedByActor(restoredOrderValue.getStrategyOrder())) {

                    if (!isConclusiveFinalState(restoredOrderValue) && !recoveryBlocked) {

                        ActorRef actorRef = MainApp.getSystem().actorOf(ActorStrategy.props(restoredOrderValue, getSelf(), strategyActors));
                        strategyActors.put(restoredOrderValue.getId(), actorRef);
                    }
                }

                actorSession.forEach((key, value) -> value.tell(restoredOrderValue, ActorRef.noSender()));

                MainApp.getIdOrders().put(key1, restoredOrderValue);
                MainApp.getMessageEventBus().subscribe(getSelf(), restoredOrderValue.getId());

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
                boolean tradeExecution = value1.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE);
                String restoredKey = tradeExecution ? tradeExecutionKey(value1) : key1;
                restoredToday.put(restoredKey, value1);
                if (tradeExecution
                        && value1.getExecId() != null
                        && !value1.getExecId().isBlank()) {
                    exceIdProcess.put(restoredKey, value1);
                }
                if (tradeExecution) {
                    // Recupera en Mongo los fills intradia conservados en Redis. Los indices unicos
                    // hacen que sea seguro repetirlo en cada reinicio.
                    MongoHistoryRepository.recordFilledOrder(value1);
                }
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

    private RoutingMessage.Order normalizeInconsistentFilledState(RoutingMessage.Order order) {
        if (!OrderStateSupport.isInconsistentFilled(order)) {
            return order;
        }

        log.warn("[OrderState][INCONSISTENT_FILLED_NORMALIZED] cuenta={} orderId={} execId={} execType={} orderQty={} cumQty={} leaves={}",
                account,
                order.getId(),
                order.getExecId(),
                order.getExecType(),
                order.getOrderQty(),
                order.getCumQty(),
                order.getLeaves());

        return OrderStateSupport.normalizeInconsistentFilled(order);
    }

    private boolean isConclusiveFinalState(RoutingMessage.Order order) {
        return OrderStateSupport.isConclusiveFinalState(order);
    }

    private boolean isRealTradeEvent(RoutingMessage.Order order, RoutingMessage.Order previous) {
        return order != null
                && order.getExecType() == RoutingMessage.ExecutionType.EXEC_TRADE
                && (order.getLastQty() > 0d
                || previous == null
                || order.getCumQty() > previous.getCumQty());
    }

    private boolean shouldIgnoreStaleOrderUpdate(RoutingMessage.Order previous, RoutingMessage.Order incoming) {
        if (previous == null || incoming == null || !isConclusiveFinalState(previous)) {
            return false;
        }

        // La deduplicacion por ExecID ocurre antes de esta validacion. Un fill unico con
        // LastQty real debe contabilizarse aunque CumQty llegue fuera de orden.
        return !isRealTradeEvent(incoming, previous);
    }

    private RoutingMessage.Order preserveFinalStateForLateTrade(RoutingMessage.Order previous,
                                                                 RoutingMessage.Order incoming) {
        if (previous == null
                || !isRealTradeEvent(incoming, previous)) {
            return incoming;
        }

        boolean previousIsFinal = isConclusiveFinalState(previous);
        boolean cumulativeStateRegressed = incoming.getCumQty() < previous.getCumQty();
        if (!previousIsFinal && !cumulativeStateRegressed) {
            return incoming;
        }

        RoutingMessage.OrderStatus finalStatus = previous.getOrdStatus();
        if (incoming.getOrderQty() > 0d && incoming.getCumQty() >= incoming.getOrderQty()) {
            finalStatus = RoutingMessage.OrderStatus.FILLED;
        }

        RoutingMessage.Order.Builder merged = incoming.toBuilder()
                .setCumQty(Math.max(previous.getCumQty(), incoming.getCumQty()));

        if (previousIsFinal) {
            merged.setOrdStatus(finalStatus).setLeaves(0d);
        } else if (cumulativeStateRegressed) {
            // El fill se contabiliza por LastQty/ExecID, pero el estado visible no retrocede.
            merged.setLeaves(previous.getLeaves());
        }
        return merged.build();
    }

    private RoutingMessage.Order preserveKnownOrderQuantity(RoutingMessage.Order previous,
                                                              RoutingMessage.Order incoming) {
        if (previous == null || incoming == null
                || incoming.getOrderQty() > 0d || previous.getOrderQty() <= 0d) {
            return incoming;
        }
        return incoming.toBuilder().setOrderQty(previous.getOrderQty()).build();
    }

    private void publishIntradayPositionState() {
        BlotterMessage.SnapshotPositionHistory historyMsg = rebuildSnapshotPositionHistory().build();
        BlotterMessage.Balance balanceMsg = balance.build();

        onPositionHistory(historyMsg);
        onBalances(balanceMsg);
    }

    private boolean shouldForceBookRefresh(RoutingMessage.Order previous, RoutingMessage.Order current) {
        if (current == null || current.getOrdStatus() != RoutingMessage.OrderStatus.CANCELED) {
            return false;
        }
        return previous == null || previous.getOrdStatus() != RoutingMessage.OrderStatus.CANCELED;
    }

    private void forceBookRefresh(RoutingMessage.Order order) {
        MarketDataMessage.Subscribe subscribe = buildBookRefreshSubscribe(order);
        if (subscribe == null) {
            return;
        }

        String topicId = TopicGenerator.getTopicMKD(subscribe);
        MarketDataMessage.Subscribe refreshSubscribe = subscribe.toBuilder().setId(topicId).build();

        getContext().system().scheduler().scheduleOnce(
                Duration.create(BOOK_REFRESH_DELAY_MS, TimeUnit.MILLISECONDS),
                () -> {
                    try {
                        MainApp.getIdSymbolsSubscrib().remove(topicId);
                        MainApp.getSnapshotHashMap().remove(topicId);
                        MainApp.subscribeSymbol(refreshSubscribe, topicId);
                        log.info("[BookRefresh] Re-suscripción forzada por cancel id={} symbol={} mkd={} settlType={}",
                                order.getId(),
                                order.getSymbol(),
                                refreshSubscribe.getSecurityExchange(),
                                refreshSubscribe.getSettlType());
                    } catch (Exception e) {
                        log.error("[BookRefresh] Error refrescando libro para {}: {}", order.getId(), e.getMessage(), e);
                    }
                },
                getContext().dispatcher()
        );
    }

    private MarketDataMessage.Subscribe buildBookRefreshSubscribe(RoutingMessage.Order order) {
        if (order == null || order.getSymbol().isBlank()) {
            return null;
        }

        MarketDataMessage.SecurityExchangeMarketData mkdExchange = mapRoutingToMarketData(order.getSecurityExchange());
        if (mkdExchange == null) {
            return null;
        }

        return MarketDataMessage.Subscribe.newBuilder()
                .setSymbol(order.getSymbol())
                .setSecurityExchange(mkdExchange)
                .setSettlType(order.getSettlType())
                .setSecurityType(order.getSecurityType())
                .setDepth(MarketDataMessage.Depth.FULL_BOOK)
                .setBook(true)
                .setStatistic(true)
                .setTrade(true)
                .build();
    }

    private MarketDataMessage.SecurityExchangeMarketData mapRoutingToMarketData(RoutingMessage.SecurityExchangeRouting routingExchange) {
        return switch (routingExchange) {
            case XSGO, XSGO_OFS, XBCL, XSGO_1, XSGO_2, XSGO_3, XSGO_4 -> MarketDataMessage.SecurityExchangeMarketData.BCS;
            case NUAM -> MarketDataMessage.SecurityExchangeMarketData.NUAM_MKD;
            case IB_SMART -> MarketDataMessage.SecurityExchangeMarketData.FH_IBKR;
            case ALPACA -> MarketDataMessage.SecurityExchangeMarketData.ALPACA_MKD;
            default -> null;
        };
    }

    private Optional<RoutingMessage.Order> findOdConflict(RoutingMessage.Order incoming) {
        if (incoming == null || !isOdSide(incoming.getSide())) {
            return Optional.empty();
        }

        String incomingSymbol = normalizeOdValue(incoming.getSymbol());
        String incomingAccount = normalizeOdValue(incoming.getAccount());
        RoutingMessage.SettlType incomingSettlType = incoming.getSettlType();
        if (incomingSymbol.isEmpty() || incomingAccount.isEmpty()) {
            return Optional.empty();
        }

        return ordersMap.values().stream()
                .filter(Objects::nonNull)
                .filter(existing -> !Objects.equals(existing.getId(), incoming.getId()))
                .filter(existing -> !isConclusiveFinalState(existing))
                .filter(existing -> isOppositeOdSide(existing.getSide(), incoming.getSide()))
                .filter(existing -> wouldCrossOwnOrder(incoming, existing))
                .filter(existing -> incomingAccount.equals(normalizeOdValue(existing.getAccount())))
                .filter(existing -> incomingSymbol.equals(normalizeOdValue(existing.getSymbol())))
                .filter(existing -> existing.getSettlType() == incomingSettlType)
                .findFirst();
    }

    private boolean isOdSide(RoutingMessage.Side side) {
        return side == RoutingMessage.Side.BUY
                || side == RoutingMessage.Side.SELL
                || side == RoutingMessage.Side.SELL_SHORT;
    }

    private boolean isOppositeOdSide(RoutingMessage.Side existing, RoutingMessage.Side incoming) {
        return (existing == RoutingMessage.Side.BUY && isSellOdSide(incoming))
                || (incoming == RoutingMessage.Side.BUY && isSellOdSide(existing));
    }

    private boolean isSellOdSide(RoutingMessage.Side side) {
        return side == RoutingMessage.Side.SELL || side == RoutingMessage.Side.SELL_SHORT;
    }

    private String normalizeOdValue(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    private double odQuantity(RoutingMessage.Order order) {
        return order.getOrderQty() > 0 ? order.getOrderQty() : order.getLeaves();
    }

    private boolean wouldCrossOwnOrder(RoutingMessage.Order incoming, RoutingMessage.Order existing) {
        double incomingPrice = incoming.getPrice();
        double existingPrice = existing.getPrice();
        if (incomingPrice <= 0 || existingPrice <= 0) {
            return true;
        }
        if (incoming.getSide() == RoutingMessage.Side.BUY && isSellOdSide(existing.getSide())) {
            return incomingPrice >= existingPrice;
        }
        if (isSellOdSide(incoming.getSide()) && existing.getSide() == RoutingMessage.Side.BUY) {
            return incomingPrice <= existingPrice;
        }
        return false;
    }

    private RoutingMessage.Order buildReplaceCandidate(RoutingMessage.Order baseOrder, RoutingMessage.OrderReplaceRequest replace) {
        RoutingMessage.Order.Builder builder = baseOrder.toBuilder();
        if (replace.getPrice() > 0) {
            builder.setPrice(replace.getPrice());
        }
        if (replace.getQuantity() > 0) {
            builder.setOrderQty(replace.getQuantity());
        }
        if (replace.getAmount() > 0) {
            builder.setAmount(replace.getAmount());
        }
        return builder.build();
    }

    static RoutingMessage.OrderReplaceRequest normalizeNoneStrategyReplace(
            RoutingMessage.Order order,
            RoutingMessage.OrderReplaceRequest replace) {
        if (order.getStrategyOrder() != RoutingMessage.StrategyOrder.NONE_STRATEGY) {
            return replace;
        }

        double configuredMaxFloor = replace.getMaxFloor() > 0d
                ? replace.getMaxFloor()
                : order.getMaxFloor();
        if (configuredMaxFloor <= 0d) {
            return replace;
        }

        double targetQuantity = replace.getQuantity() > 0d
                ? replace.getQuantity()
                : order.getOrderQty();
        double leavesQuantity = Math.max(0d, targetQuantity - order.getCumQty());
        if (leavesQuantity <= 0d) {
            return replace;
        }

        double normalizedMaxFloor = Math.min(configuredMaxFloor, leavesQuantity);
        if (normalizedMaxFloor == replace.getMaxFloor()) {
            return replace;
        }
        return replace.toBuilder()
                .setMaxFloor(normalizedMaxFloor)
                .build();
    }

    private OdAttempt buildOdAttempt(RoutingMessage.Order incoming, RoutingMessage.Order conflicting, String reason) {
        return new OdAttempt(
                System.currentTimeMillis(),
                incoming.getOperator(),
                incoming.getAccount(),
                incoming.getSymbol(),
                incoming.getSide().name(),
                incoming.getPrice(),
                odQuantity(incoming),
                incoming.getId(),
                incoming.getSecurityExchange().name(),
                conflicting.getSide().name(),
                conflicting.getPrice(),
                odQuantity(conflicting),
                conflicting.getId(),
                conflicting.getOrdStatus().name(),
                reason
        );
    }

    private void restoreFromRedisIfEnabled() {
        try {
            if (!isRedisPersistenceEnabled()) {
                return;
            }
            String dailyKey = getDailyRedisKey();
            boolean restoredAny = false;
            int restoredOrders = 0;
            int restoredTrades = 0;
            int restoredPositions = 0;
            int restoredSnapshotPositions = 0;
            boolean restoredPatrimonio = false;
            boolean restoredBalance = false;
            boolean restoredSimultaneas = false;
            boolean restoredPrestamos = false;

            if (MainApp.getOrdersMapRedis() != null) {
                HashMap<String, RoutingMessage.Order> redisOrders = MainApp.getOrdersMapRedis().get(account);
                if (redisOrders != null && !redisOrders.isEmpty()) {
                    onRestoreOrder(new RestoreOrder(new HashMap<>(redisOrders)));
                    restoredOrders = ordersMap.size();
                    restoredAny = true;
                }
            }

            if (MainApp.getTradesMapReddis() != null) {
                HashMap<String, RoutingMessage.Order> redisTrades = MainApp.getTradesMapReddis().get(account);
                if (redisTrades != null && !redisTrades.isEmpty()) {
                    onRestoreTrade(new RestoreTrade(new HashMap<>(redisTrades)));
                    restoredTrades = tradesMap.size();
                    restoredAny = true;
                }
            }

            if (MainApp.getPositionsMapsRedis() != null) {
                Map<String, BlotterMessage.Position> redisPositions = MainApp.getPositionsMapsRedis().get(dailyKey);
                if (redisPositions != null && !redisPositions.isEmpty()) {
                    onRestorePositions(new RestorePositions(new HashMap<>(redisPositions)));
                    restoredPositions = positionsMaps.size();
                    restoredAny = true;
                }
            }

            if (MainApp.getPatrimonioMapsRedis() != null) {
                BlotterMessage.Patrimonio redisPatrimonio = MainApp.getPatrimonioMapsRedis().get(dailyKey);
                if (redisPatrimonio != null) {
                    onRestorePatrimonio(new RestorePatrimonio(redisPatrimonio));
                    restoredPatrimonio = true;
                    restoredAny = true;
                }
            }

            if (MainApp.getSnapshotPositionHistoryRedis() != null) {
                BlotterMessage.SnapshotPositionHistory redisSnapshotPositionHistory = MainApp.getSnapshotPositionHistoryRedis().get(dailyKey);
                if (redisSnapshotPositionHistory != null) {
                    onRestoreSnapshotPositionHistory(new RestoreSnapshotPositionHistory(redisSnapshotPositionHistory));
                    restoredSnapshotPositions = snapshotPositionHistoryMaps.size();
                    restoredAny = true;
                }
            }

            if (MainApp.getBalanceRedis() != null) {
                BlotterMessage.Balance redisBalance = MainApp.getBalanceRedis().get(dailyKey);
                if (redisBalance != null) {
                    onRestoreBalance(new RestoreBalance(redisBalance));
                    restoredBalance = true;
                    restoredAny = true;
                }
            }

            if (MainApp.getSnapshotSimultaneasRedis() != null) {
                BlotterMessage.SnapshotSimultaneas redisSnapshotSimultaneas = MainApp.getSnapshotSimultaneasRedis().get(dailyKey);
                if (redisSnapshotSimultaneas != null) {
                    onRestoreSnapshotSimultaneas(new RestoreSnapshotSimultaneas(redisSnapshotSimultaneas));
                    restoredSimultaneas = true;
                    restoredAny = true;
                }
            }

            if (MainApp.getSnapshotPrestamosRedis() != null) {
                BlotterMessage.SnapshotPrestamos redisSnapshotPrestamos = MainApp.getSnapshotPrestamosRedis().get(dailyKey);
                if (redisSnapshotPrestamos != null) {
                    onRestoreSnapshotPrestamos(new RestoreSnapshotPrestamos(redisSnapshotPrestamos));
                    restoredPrestamos = true;
                    restoredAny = true;
                }
            }

            if (restoredAny) {
                if (hasTodayIntradayActivity()) {
                    log.info("[RedisRestore][RECALC_REQUIRED] cuenta {} key={} ordersToday={} tradesToday={} snapshotPositions={}",
                            account,
                            dailyKey,
                            countTodayOrders(),
                            countTodayTrades(),
                            snapshotPositionHistoryMaps.size());
                } else {
                    rebuildLogicaPosition("RESTORE");
                }
                log.info("[RedisRestore] cuenta {} key={} ordersToday={} tradesToday={} positions={} snapshotPositions={} patrimonio={} balance={} simultaneas={} prestamos={}",
                        account,
                        dailyKey,
                        restoredOrders,
                        restoredTrades,
                        restoredPositions,
                        restoredSnapshotPositions,
                        restoredPatrimonio,
                        restoredBalance,
                        restoredSimultaneas,
                        restoredPrestamos);
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
            if (MainApp.hasManualSimultaneasOverride(account)) {
                reloadSimultaneasFromSources();
                return;
            }

            BlotterMessage.SnapshotSimultaneas snapshot = restoreSnapshotSimultaneas.getSnapshotSimultaneas();
            if (snapshot == null) {
                return;
            }

            simultaneasHashMap.clear();
            snapshot.getSimultaneasList().forEach(s -> {
                if (isSimultaneaVigente(s)) {
                    simultaneasHashMap.put(s.getId(), s);
                }
            });
        } catch (Exception e) {
            log.error("Error restaurando snapshotSimultaneas para cuenta {}", account, e);
        }
    }

    private void onRestoreSnapshotPrestamos(RestoreSnapshotPrestamos restoreSnapshotPrestamos) {
        try {
            if (MainApp.hasManualPrestamosOverride(account)) {
                snapshotPrestamos = loadPrestamosFromSources();
                return;
            }
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
            MainApp.reportRedisFailure("persist-patrimonio", e);
        }
    }

    private void persistSnapshotPositionHistory(BlotterMessage.SnapshotPositionHistory snapshotMsg) {
        try {
            if (!isRedisPersistenceEnabled()) {
                return;
            }
            if (MainApp.getSnapshotPositionHistoryRedis() != null && snapshotMsg != null) {
                MainApp.getSnapshotPositionHistoryRedis().put(getDailyRedisKey(), snapshotMsg);
                if (hasTodayIntradayActivity()) {
                    log.info("[IntradayPersist] type=SnapshotPositionHistory cuenta {} key={} positions={} negativePositions={}",
                            account,
                            getDailyRedisKey(),
                            snapshotMsg.getPositionsHistoryCount(),
                            countNegativeHistoryPositions());
                }
            }
        } catch (Exception e) {
            log.error("Error persistiendo snapshotPositionHistory para cuenta {}", account, e);
            MainApp.reportRedisFailure("persist-snapshot-position-history", e);
        }
    }

    private void persistBalance(BlotterMessage.Balance balanceMsg) {
        try {
            if (!isRedisPersistenceEnabled()) {
                return;
            }
            if (MainApp.getBalanceRedis() != null && balanceMsg != null) {
                MainApp.getBalanceRedis().put(getDailyRedisKey(), balanceMsg);
                if (hasTodayIntradayActivity()) {
                    log.info("[IntradayPersist] type=Balance cuenta {} key={} saldoDisponible={} cupo={} cartera={} actCompras={} actVentas={} calzCompras={} calzVentas={}",
                            account,
                            getDailyRedisKey(),
                            balanceMsg.getSaldoDisponible(),
                            balanceMsg.getCupo(),
                            balanceMsg.getCartera(),
                            balanceMsg.getOrdenesActivasCompras(),
                            balanceMsg.getOrdenesActivasVentas(),
                            balanceMsg.getOrdenesCalzadasCompras(),
                            balanceMsg.getOrdenesCalzadasVentas());
                }
            }
        } catch (Exception e) {
            log.error("Error persistiendo balance para cuenta {}", account, e);
            MainApp.reportRedisFailure("persist-balance", e);
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
            MainApp.reportRedisFailure("persist-snapshot-simultaneas", e);
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
            MainApp.reportRedisFailure("persist-snapshot-prestamos", e);
        }
    }

    private BlotterMessage.SnapshotPrestamos.Builder loadPrestamosFromSources() {
        if (MainApp.hasManualPrestamosOverride(account)) {
            BlotterMessage.SnapshotPrestamos.Builder manualSnapshot = BlotterMessage.SnapshotPrestamos.newBuilder();
            manualSnapshot.setId("snapshot-prestamos-" + account.replace("-", "/"));
            manualSnapshot.setAccount(account.replace("-", "/"));
            MainApp.getManualPrestamosOverride(account).forEach(manualSnapshot::addPrestamos);
            return manualSnapshot;
        }

        String prefixAccount = account.replace("-", "/");
        return CalculoCreasys.snapshotPrestamos(prefixAccount);
    }

    private void applyManualPrestamosOverrideToPatrimonio() {
        if (!MainApp.hasManualPrestamosOverride(account)) {
            return;
        }

        BlotterMessage.Patrimonio.Builder patrimonioClp = patrimonioMaps.computeIfAbsent(
                RoutingMessage.Currency.CLP,
                currency -> BlotterMessage.Patrimonio.newBuilder()
                        .setCaja(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setCuentaTransitoriasPorCobrarPagar(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setGarantiaEfectivo(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setPrestamos(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
        );

        double totalPrestamos = MainApp.getManualPrestamosOverride(account).stream()
                .mapToDouble(p -> p != null ? p.getMonto() : 0d)
                .sum();

        patrimonioClp.setPrestamos(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(totalPrestamos));
    }

    private void applyManualSaldoOverrideToPatrimonio() {
        if (!MainApp.hasManualSaldoOverride(account)) {
            return;
        }

        ManualSaldoOverride override = MainApp.getManualSaldoOverride(account);
        if (override == null) {
            return;
        }

        BlotterMessage.Patrimonio.Builder patrimonioClp = patrimonioMaps.computeIfAbsent(
                RoutingMessage.Currency.CLP,
                currency -> BlotterMessage.Patrimonio.newBuilder()
                        .setCaja(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setCuentaTransitoriasPorCobrarPagar(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setGarantiaEfectivo(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
                        .setPrestamos(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(0))
        );

        patrimonioClp.setCaja(BlotterMessage.ValuesPatrimonio.newBuilder().setValues(override.getCaja()));
        patrimonioClp.setCuentaTransitoriasPorCobrarPagar(
                BlotterMessage.ValuesPatrimonio.newBuilder().setValues(override.getCuentaTransitorias()));
        patrimonioClp.setGarantiaEfectivo(
                BlotterMessage.ValuesPatrimonio.newBuilder().setValues(override.getGarantiaEfectivo()));
    }

    private void applyManualSaldoOverrideToBalance() {
        if (!MainApp.hasManualSaldoOverride(account)) {
            return;
        }

        ManualSaldoOverride override = MainApp.getManualSaldoOverride(account);
        if (override == null) {
            return;
        }

        balance.setGarantiasConstituidas(override.getGarantiasConstituidas());
        balance.setGarantiasExigidas(override.getGarantiasExigidas());
        balance.setGarantiasReservadas(override.getGarantiasReservadas());
        balance.setLimiteFinanciero(override.getLimiteFinanciero());
        balance.setGarantiasDisponible(override.getGarantiasDisponible());
        balance.setOrdenesActivasCompras(override.getOrdenesActivasCompras());
        balance.setOrdenesActivasVentas(override.getOrdenesActivasVentas());
        balance.setOrdenesCalzadasCompras(override.getOrdenesCalzadasCompras());
        balance.setOrdenesCalzadasVentas(override.getOrdenesCalzadasVentas());
        balance.setOrdenesCestaCompras(override.getOrdenesCestaCompras());
        balance.setOrdenesCestaVentas(override.getOrdenesCestaVentas());
        balance.setRendimiento(override.getRendimiento());
        balance.setTotal(override.getTotal());
    }

    private void reloadSimultaneasFromSources() {
        simultaneasHashMap.clear();

        if (MainApp.hasManualSimultaneasOverride(account)) {
            MainApp.getManualSimultaneasOverride(account).forEach(simultanea -> {
                if (simultanea != null && simultanea.getId() != null && !simultanea.getId().isBlank() && isSimultaneaVigente(simultanea)) {
                    simultaneasHashMap.put(simultanea.getId(), simultanea);
                }
            });
            return;
        }

        List<BlotterMessage.Simultaneas> simultaneasSql = MainApp.getAllSimultaneas();
        if (simultaneasSql != null) {
            simultaneasSql.forEach(s -> {
                String sourceAccount = s.getNumCuenta();
                if (this.account.equals(sourceAccount) && isSimultaneaVigente(s)) {
                    simultaneasHashMap.put(s.getId(), s);
                }
            });
        }
    }

    private double safePercentage(double value, double total) {
        if (!Double.isFinite(value) || !Double.isFinite(total) || total == 0d) {
            return 0d;
        }
        double percentage = (value / total) * 100d;
        return Double.isFinite(percentage) ? percentage : 0d;
    }

    private void publishSimultaneasSnapshot() {
        BlotterMessage.SnapshotSimultaneas snapshotSimultaneas = BlotterMessage.SnapshotSimultaneas.newBuilder()
                .addAllSimultaneas(simultaneasHashMap.values())
                .setAccount(account)
                .build();

        persistSnapshotSimultaneas(snapshotSimultaneas);
        actorSession.forEach((key, value) -> value.tell(snapshotSimultaneas, ActorRef.noSender()));
    }

    private String getDailyRedisKey() {
        ZoneId zoneId = MainApp.getZoneId() != null ? MainApp.getZoneId() : ZoneId.of("America/Santiago");
        return account + "|" + LocalDate.now(zoneId);
    }

    private boolean isSimultaneaVigente(BlotterMessage.Simultaneas s) {
        if (s == null) {
            return false;
        }

        ZoneId zoneId = MainApp.getZoneId() != null ? MainApp.getZoneId() : ZoneId.of("America/Santiago");
        LocalDate today = LocalDate.now(zoneId);
        LocalDate fechaOperacion = parseSimultaneaDate(s.getFechaOperacion());
        LocalDate fechaVcto = parseSimultaneaDate(s.getFechaVcto());

        if (fechaOperacion != null && fechaOperacion.isAfter(today)) {
            return false;
        }

        if (fechaVcto != null) {
            return !fechaVcto.isBefore(today);
        }

        return fechaOperacion != null && !fechaOperacion.isAfter(today);
    }

    private LocalDate parseSimultaneaDate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }

        String trimmed = raw.trim();
        if (trimmed.length() >= 10) {
            String first10 = trimmed.substring(0, 10);
            try {
                return LocalDate.parse(first10, DateTimeFormatter.ISO_LOCAL_DATE);
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
                return LocalDate.parse(trimmed, fmt);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }

}
