package cl.vc.blotter.controller;

import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.blotter.Repository;
import cl.vc.blotter.model.BookVO;
import cl.vc.blotter.model.OrderBookEntry;
import cl.vc.blotter.utils.Notifier;
import cl.vc.blotter.utils.OrdersHelper;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import cl.vc.module.protocolbuff.utils.ProtoConverter;
import eu.hansolo.enzo.notification.Notification;
import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Bounds;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Popup;
import javafx.stage.Stage;
import javafx.stage.StageStyle;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static cl.vc.module.protocolbuff.generator.IDGenerator.conversorExdestination;

@Data
@Slf4j
public class LanzadorController {

    private DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm");

    @FXML
    public Label LblsecurityType;
    @FXML
    public Label currencylb;
    @FXML
    public Label strategylb;
    @FXML
    public Label spreadlb;
    @FXML
    public Label limitlb;

    public String text;
    @FXML
    private TitledPane titelpanelLauncher;
    @FXML
    private VBox myHBox;
    @FXML
    private Label lblSerach;
    @FXML
    private StackPane stackPane;
    @FXML
    private Label cOperadorlb;
    @FXML
    private VBox vboxNemo;
    @FXML
    private Label labelBroker;
    @FXML
    private TextField ticket;
    @FXML
    private Label searchIconLabel;
    @FXML
    private Button addBookButton;
    @FXML
    private Button addpreselect;
    @FXML
    private VBox VboxTmoneda;
    @FXML
    private AnchorPane anchopane;
    @FXML
    private Button paste;
    @FXML
    private VBox VboxLiquidacion;
    @FXML
    private VBox VboxSecurityType;
    @FXML
    private VBox VboxEstrategia;
    @FXML
    private VBox VboxCodOperador;
    @FXML
    private VBox VboxTipo;
    @FXML
    private VBox VboxSpread;
    @FXML
    private VBox VboxLimit;
    @FXML
    private VBox VboxSecExc;

    @FXML
    private VBox vboxTimeEffective;

    @FXML
    private VBox vboxTimeExpire;

    @FXML
    private VBox VboxBroker;
    @FXML
    private VBox VboxHandInst;
    @FXML
    private VBox VboxTIF;
    @FXML
    private HBox VboxOPCI;

    @FXML
    private VBox VboxCash;
    @FXML
    private Pane pane9;
    @FXML
    private Pane pane8;
    @FXML
    private ComboBox<RoutingMessage.ExecBroker> brokerOrder;
    @FXML
    private GridPane gpLauncher;

    @FXML
    private Label lblAgreement;

    // LINEA 1
    @FXML
    private ComboBox<String> sideOrder;
    @FXML
    private TextField quantity;
    @FXML
    private TextField priceOrder;
    @FXML
    private TextField cash;
    @FXML
    private ComboBox<RoutingMessage.Currency> currency;


    @FXML private Spinner<Integer> hourSpinnerEffective;
    @FXML private Spinner<Integer> minuteSpinnerEffective;
    @FXML private Spinner<Integer> hourSpinnerExpire;
    @FXML private Spinner<Integer> minuteSpinnerExpire;
    @FXML private Spinner<Integer> slice;


    // LINEA 2
    @FXML
    private ComboBox<RoutingMessage.SettlType> settltypeOrder;
    @FXML
    private TextField iceberg;
    @FXML
    private ComboBox<String> acAccount;
    @FXML
    private ComboBox<RoutingMessage.StrategyOrder> strategOrder;

    private boolean islibrazo = false;

    // LINEA 3
    @FXML
    private ComboBox<String> cOperador;
    @FXML
    private ComboBox<RoutingMessage.OrdType> typeOrder;
    @FXML
    private TextField spread;
    @FXML
    private TextField limit;
    @FXML
    private ComboBox<RoutingMessage.SecurityExchangeRouting> secExchOrder;

    // LINEA 4 - Ocultos por defecto
    @FXML
    private ComboBox<String> brokerComboBox;
    @FXML
    private ComboBox<RoutingMessage.HandlInst> handInstOrder;
    ;
    @FXML
    private ComboBox<RoutingMessage.Tif> tifOrder;
    @FXML
    private CheckBox chkIndivisible;
    @FXML
    private ComboBox<RoutingMessage.SecurityType> securityType;

    // Contenedor de botones en el footer
    @FXML
    private Button routeOrder;
    @FXML
    private Button bestR;
    @FXML
    private Button hitR;
    @FXML
    private Button clean;
    @FXML
    private Button unlock;
    @FXML
    private ComboBox<RoutingMessage.SecurityExchangeRouting> securityExchangeFilter;

    @FXML
    private ComboBox<String> sideFilter;
    @FXML
    private ListView<String> suggestionsList;

    private List<LibroEmergenteController> libroEmergenteControllerList = new ArrayList<>();

    private Popup suggestionsPopup;
    private boolean isUpdatingFromPortfolio = false;
    private ObservableList<String> allSymbols;
    private FilteredList<String> filteredList;
    private Map<String, List<String>> deaultoForm = new HashMap<>();
    private RoutingMessage.Order orderSelected;
    private boolean isProgrammaticChange = false;
    private BooleanProperty selectedInWorkingOrder = new SimpleBooleanProperty(false);
    private boolean isPopupVisible = false;
    private boolean isSettingValues = false;
    @Setter
    private Stage stage;
    @Setter
    private boolean skipConfirmationAlert = false;

    private String IdLibrazo;

    private ExecutionsController workingOrderController;

    private BooleanProperty isActiveLanzador = new SimpleBooleanProperty(false);

    private RoutingController routingController;

    private boolean isUpdatingFromOtherSource = false;

    public static String formatearNumeroBestHit(String numero) {

        if (numero.contains(",")) {
            numero = numero.replace(",", "");
        }

        DecimalFormat formato;

        if (numero.contains(".")) {
            formato = (DecimalFormat) NumberFormat.getInstance(Locale.US);
            formato.applyPattern("#,##0.################");
        } else {
            formato = (DecimalFormat) NumberFormat.getInstance(Locale.US);
            formato.applyPattern("#,##0");
        }

        try {
            if (numero.isEmpty()) {
                numero = "0";
            }

            return numero;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return numero;
        }
    }

    public static String formatearNumero(String numero) {

        if (numero.contains(",")) {
            numero = numero.replace(",", ".");
        }

        DecimalFormat formato;

        if (numero.contains(".")) {
            formato = (DecimalFormat) NumberFormat.getInstance(Locale.US);
            formato.applyPattern("#,##0.################");
        } else {
            formato = (DecimalFormat) NumberFormat.getInstance(Locale.US);
            formato.applyPattern("#,##0");
        }

        try {
            if (numero.isEmpty()) {
                numero = "0";
            }

            double num = formato.parse(numero).doubleValue();
            return formato.format(num);
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            return numero;
        }
    }

    private boolean isLightMode() {
        return Boolean.TRUE.equals(Repository.getIsLight());
    }

    private boolean isPremiumLiquidationUser() {
        return Repository.isPremiumLiquidationUser();
    }

    private boolean canAutoSetSettlType() {
        return !isLightMode() || isPremiumLiquidationUser();
    }

    private void selectSettlTypeIfAllowed(RoutingMessage.SettlType st) {
        selectSettlTypeIfAllowed(st, "unknown");
    }

    private void selectSettlTypeIfAllowed(RoutingMessage.SettlType st, String source) {
        if (st == null || st == RoutingMessage.SettlType.UNRECOGNIZED) return;
        if (settltypeOrder == null) return;
        RoutingMessage.SettlType before = settltypeOrder.getSelectionModel().getSelectedItem();

        if (!canAutoSetSettlType()) {
            log.info("[LIQUIDACION][BLOCK] source={} user={} light={} premium={} requested={} current={}",
                    source, Repository.getUsername(), isLightMode(), isPremiumLiquidationUser(), st, before);
            return;
        }

        settltypeOrder.getSelectionModel().select(st);
        RoutingMessage.SettlType after = settltypeOrder.getSelectionModel().getSelectedItem();
        log.info("[LIQUIDACION][APPLY] source={} user={} light={} premium={} requested={} before={} after={}",
                source, Repository.getUsername(), isLightMode(), isPremiumLiquidationUser(), st, before, after);
    }


    @FXML
    public void initialize() {

        setupSuggestionsPopup();

        hourSpinnerEffective.setValueFactory(
                new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 23, 9) // 9 horas
        );

        minuteSpinnerEffective.setValueFactory(
                new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 59, 30, 1) // 30 min
        );

        // ----- HORA TÉRMINO: 15:44 -----
        hourSpinnerExpire.setValueFactory(
                new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 23, 15) // 15 horas
        );

        minuteSpinnerExpire.setValueFactory(
                new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 59, 44, 1) // 44 min
        );

        slice.setValueFactory(
                new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 10) // 44 min
        );

        vboxTimeEffective.setVisible(false);
        vboxTimeEffective.setManaged(false);

        vboxTimeExpire.setVisible(false);
        vboxTimeExpire.setManaged(false);

        selectedInWorkingOrder.addListener((obs, wasSelected, isSelected) -> {
            if (isSelected && suggestionsPopup != null && suggestionsPopup.isShowing()) {
                suggestionsPopup.hide();
            }
        });




        quantity.textProperty().addListener((observable, oldValue, newValue) -> {
            try {
                String sanitizedValue = newValue.replace(",", "");
                quantity.setText(formatearNumero(sanitizedValue));
                if (!sanitizedValue.isEmpty() && !priceOrder.getText().isEmpty()) {
                    double parsedValue = Double.parseDouble(sanitizedValue);
                    Double casht = parsedValue * Double.parseDouble(priceOrder.getText().replace(",", ""));
                    cash.setText("$" + Repository.getFormatter4dec().format(casht));
                }
            } catch (NumberFormatException e) {
                log.error(e.getMessage(), e);
            }
        });

        iceberg.textProperty().addListener((observable, oldValue, newValue) -> {
            try {

            } catch (NumberFormatException e) {
                log.error(e.getMessage(), e);
            }
        });

        priceOrder.focusedProperty().addListener((obs, wasFocused, isNowFocused) -> {
            if (isNowFocused) {
                if (priceOrder.getText().contains(",")) {
                    isProgrammaticChange = true;
                    priceOrder.setText(priceOrder.getText().replace(",", ""));
                    isProgrammaticChange = false;
                }
                priceOrder.setTextFormatter(new TextFormatter<>(change -> {
                    if (isProgrammaticChange) {
                        return change;
                    }
                    String newText = change.getControlNewText();
                    if (newText.matches("\\d*\\.?\\d*")) {
                        return change;
                    } else {
                        if (change.getText().contains(",") || !change.getText().matches("[0-9.]*")) {
                            Notifier.INSTANCE.notify(new Notification(
                                    "Entrada no permitida",
                                    "No se permiten comas ni otros símbolos en el precio.",
                                    Notification.ERROR_ICON)
                            );
                        }
                        return null;
                    }
                }));
            } else if (wasFocused) {
                String newValue = priceOrder.getText().replace(",", "");

                isProgrammaticChange = true;
                priceOrder.setText(formatearNumero(newValue));
                isProgrammaticChange = false;

                if (!quantity.getText().isEmpty() && !newValue.isEmpty()) {
                    double parsedValue = Double.parseDouble(newValue);
                    Double casht = parsedValue * Double.parseDouble(quantity.getText().replace(",", ""));
                    cash.setText("$" + Repository.getFormatter4dec().format(casht));
                }
                priceOrder.setTextFormatter(null);
            }
        });


        strategOrder.setOnAction(event -> {
            RoutingMessage.StrategyOrder strategy = strategOrder.getSelectionModel().getSelectedItem();

            if (strategy == null) return;

            chkIndivisible.setDisable(true);
            vboxTimeEffective.setVisible(false);
            vboxTimeEffective.setManaged(false);
            vboxTimeExpire.setVisible(false);
            vboxTimeExpire.setManaged(false);

            Repository.getPrincipalController().getSplid().setDividerPosition(0,0.40);

            if (strategy.equals(RoutingMessage.StrategyOrder.BEST)) {
                spread.setDisable(true);
                spread.setText("");
                limit.setDisable(false);
                priceOrder.setDisable(true);
                limit.setText(priceOrder.getText());
                priceOrder.setText("");
            } else if (strategy.equals(RoutingMessage.StrategyOrder.BASKET_PASSIVE)) {
                spread.setDisable(true);
                spread.setText("");
                limit.setDisable(false);
                priceOrder.setDisable(true);
                limit.setText("");
                priceOrder.setText("");
            } else if (strategy.equals(RoutingMessage.StrategyOrder.BASKET_LAST)) {
                spread.setDisable(true);
                spread.setText("");
                limit.setDisable(false);
                priceOrder.setDisable(true);
                limit.setText("2");
                priceOrder.setText("");
            } else if (strategy.equals(RoutingMessage.StrategyOrder.BASKET_AGGRESSIVE)) {
                spread.setDisable(true);
                spread.setText("");
                limit.setDisable(false);
                priceOrder.setDisable(true);
                limit.setText("2");
                priceOrder.setText("");
            } else if (strategy.equals(RoutingMessage.StrategyOrder.HOLGURA)) {
                spread.setDisable(false);
                limit.setDisable(true);
                limit.setText("");
                priceOrder.setDisable(false);

            } else if (strategy.equals(RoutingMessage.StrategyOrder.VWAP)) {
                vboxTimeEffective.setVisible(true);
                vboxTimeEffective.setManaged(true);
                vboxTimeExpire.setVisible(true);
                vboxTimeExpire.setManaged(true);
                limit.setDisable(false);
                priceOrder.setDisable(true);

                Repository.getPrincipalController().getSplid().setDividerPosition(0,0.45);

            } else if (strategy.equals(RoutingMessage.StrategyOrder.NONE_STRATEGY)) {
                spread.setDisable(true);
                spread.setText("");
                limit.setDisable(true);
                limit.setText("");
                priceOrder.setDisable(false);

            } else {
                spread.setDisable(true);
                limit.setDisable(true);
                priceOrder.setDisable(false);
            }
        });

        secExchOrder.setOnAction(event -> {
            RoutingMessage.SecurityExchangeRouting selectedOption = secExchOrder.getSelectionModel().getSelectedItem();

            if (selectedOption == null) {
                return;
            }
            if (deaultoForm.containsKey(selectedOption.name())) {
                List<String> values = deaultoForm.get(selectedOption.name());

                acAccount.getSelectionModel().select(values.get(1));
                currency.getSelectionModel().select(RoutingMessage.Currency.valueOf(values.get(2)));
                cOperador.getSelectionModel().select(values.get(3));
                tifOrder.getSelectionModel().select(RoutingMessage.Tif.valueOf(values.get(4)));
                typeOrder.getSelectionModel().select(RoutingMessage.OrdType.valueOf(values.get(5)));
                selectSettlTypeIfAllowed(RoutingMessage.SettlType.valueOf(values.get(6)), "secExch.onAction.defaultRouting");
                brokerOrder.getSelectionModel().select(RoutingMessage.ExecBroker.valueOf(values.get(7)));
                handInstOrder.getSelectionModel().select(RoutingMessage.HandlInst.valueOf(values.get(8)));

            } else {
                if (selectedOption.equals(RoutingMessage.SecurityExchangeRouting.XSGO)) {
                    currency.getSelectionModel().select(RoutingMessage.Currency.CLP);

                    Repository.getPrincipalController().subscribeSymbol();

                } else if (selectedOption.equals(RoutingMessage.SecurityExchangeRouting.IB_SMART)) {
                    currency.getSelectionModel().select(RoutingMessage.Currency.USD);
                }
            }
        });

        paste.setOnAction(e -> {
            BktStrategyProtos.Basket snapshotBasket = OrdersHelper.sendNewBasketFromClipboard();


            if (snapshotBasket == null) {
                return;
            }

            if (snapshotBasket.getOrdersList().isEmpty()) {
                Notifier.INSTANCE.notifyError(
                        "Basket vacío",
                        "No hay órdenes válidas para enviar."
                );
                return;
            }

            String message = "¿Estás seguro de que quieres enviar " +
                    snapshotBasket.getOrdersList().size() + " órdenes con estrategia?";

            if (alertRoute(message)) {
                snapshotBasket.getOrdersList().forEach(s -> {
                    RoutingMessage.NewOrderRequest newOrderRequest =
                            RoutingMessage.NewOrderRequest.newBuilder().setOrder(s).build();
                    Repository.getClientService().sendMessage(newOrderRequest);
                });
            }
        });


        ticket.textProperty().addListener((obs, oldValue, newValue) -> {

            // Ignora cambios programáticos (setValues, sugerencias, etc.)
            if (isProgrammaticChange || isSettingValues || isUpdatingFromPortfolio) return;

            String nv = (newValue == null) ? "" : newValue.trim();

            // Si el usuario está escribiendo un símbolo nuevo, corta el “amarre” con la orden seleccionada
            if (orderSelected != null) {
                String oldSym = orderSelected.getSymbol() == null ? "" : orderSelected.getSymbol().trim();
                if (!oldSym.equalsIgnoreCase(nv)) {
                    clearSelectedOrderContext();
                }
            }

            // Uppercase sin recursion infinita
            String upper = nv.toUpperCase(Locale.ROOT);
            if (!upper.equals(nv)) {
                isProgrammaticChange = true;
                ticket.setText(upper);
                isProgrammaticChange = false;
                return; // salimos y dejamos que el próximo evento siga
            }

            if (upper.isEmpty()) {
                suggestionsPopup.hide();
                return;
            }

            // Sugerencias
            updateSuggestions(upper);

            // Actualiza security type según exchange seleccionado
            RoutingMessage.SecurityExchangeRouting selectedEx = secExchOrder.getSelectionModel().getSelectedItem();
            if (selectedEx != null) {
                MarketDataMessage.SecurityExchangeMarketData sec = conversorExdestination(selectedEx);
                updateSecurityTypeComboBox(upper, sec);
            }

            // Esto es CLAVE: si estás buscando nuevo símbolo, no uses datos viejos.
            routeOrder.setDisable(false);        // (o true si de verdad lo bloqueas hasta callback)
            Repository.getPrincipalController().subscribeSymbol();
        });



        typeOrder.setOnAction(event -> {
            RoutingMessage.OrdType selectedItem = typeOrder.getSelectionModel().getSelectedItem();
            chkIndivisible.setDisable(true);

            if (selectedItem.equals(RoutingMessage.OrdType.MARKET_CLOSE)) {
                chkIndivisible.setDisable(false);
            } else if (selectedItem.equals(RoutingMessage.OrdType.LIMIT)) {
                spread.setDisable(true);
                limit.setDisable(true);
                priceOrder.setDisable(false);
            } else {
                spread.setDisable(true);
                limit.setDisable(true);
            }
        });

        sideOrder.valueProperty().addListener((observable, oldValue, newValue) -> {
            try {
                String buttonStyleClass = newValue.equals(ProtoConverter.routingDecryptStatus(RoutingMessage.Side.BUY.name())) ? "buyButton" : "sellButton";
                updateButtonStyles(buttonStyleClass);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });

        clean.getStyleClass().clear();
        clean.getStyleClass().add("replaceButton");
        unlock.getStyleClass().clear();
        unlock.getStyleClass().add("replaceButton");

        currency.getItems().addAll(RoutingMessage.Currency.values());
        currency.getItems().remove(RoutingMessage.Currency.UNRECOGNIZED);

        sideOrder.getSelectionModel().select(ProtoConverter.routingDecryptStatus(RoutingMessage.Side.BUY.name()));
        sideOrder.getItems().remove(RoutingMessage.Side.ALL_SIDE.name());
        sideOrder.getItems().remove(RoutingMessage.Side.UNRECOGNIZED.name());
        sideOrder.getItems().remove(RoutingMessage.Side.NONE_SIDE.name());
        sideOrder.getItems().remove(RoutingMessage.Side.SELL_SHORT.name());
        typeOrder.getItems().addAll(RoutingMessage.OrdType.values());
        typeOrder.getItems().remove(RoutingMessage.OrdType.NONE);
        typeOrder.getItems().remove(RoutingMessage.OrdType.UNRECOGNIZED);
        typeOrder.getItems().remove(RoutingMessage.OrdType.STOP_LOSS);

        tifOrder.getItems().remove(RoutingMessage.OrdType.UNRECOGNIZED);
        tifOrder.getItems().remove(RoutingMessage.OrdType.NONE);

        chkIndivisible.setDisable(true);

        tifOrder.getItems().addAll(RoutingMessage.Tif.values());
        tifOrder.getItems().remove(RoutingMessage.Tif.UNRECOGNIZED);
        tifOrder.getSelectionModel().select(RoutingMessage.Tif.DAY);
        securityType.getItems().addAll(RoutingMessage.SecurityType.values());
        securityType.getSelectionModel().select(RoutingMessage.SecurityType.CS);
        securityType.getItems().remove(RoutingMessage.SecurityType.FUT);
        securityType.getItems().remove(RoutingMessage.SecurityType.OPT);
        securityType.getItems().remove(RoutingMessage.SecurityType.PAXOS);
        securityType.getItems().remove(RoutingMessage.SecurityType.UNRECOGNIZED);


        handInstOrder.getItems().addAll(RoutingMessage.HandlInst.values());
        handInstOrder.getSelectionModel().select(RoutingMessage.HandlInst.PRIVATE_ORDER);
        handInstOrder.getItems().remove(RoutingMessage.HandlInst.UNRECOGNIZED);
        handInstOrder.getItems().remove(RoutingMessage.HandlInst.NONE_HANDLINST);

        Arrays.asList(RoutingMessage.Side.values()).forEach(s -> {
            if (!s.name().equals("NONE_SIDE")
                    && !s.name().equals("UNRECOGNIZED")
                    && !s.name().equals("ALL_SIDE")
                    && !s.name().equals("SELL_SHORT")) {
                String value = ProtoConverter.routingDecryptStatus(s.name());
                sideOrder.getItems().add(value);
            }
        });


        settltypeOrder.getItems().addAll(RoutingMessage.SettlType.values());
        settltypeOrder.getSelectionModel().select(RoutingMessage.SettlType.T2);
        settltypeOrder.getItems().remove(RoutingMessage.SettlType.UNRECOGNIZED);
        settltypeOrder.getItems().remove(RoutingMessage.SettlType.REGULAR);
        settltypeOrder.getItems().remove(RoutingMessage.SettlType.T3);

        typeOrder.getSelectionModel().select(RoutingMessage.OrdType.LIMIT);
        brokerOrder.getSelectionModel().selectFirst();
        acAccount.getSelectionModel().selectFirst();


        acAccount.getItems().addListener((ListChangeListener<String>) change -> {
            Platform.runLater(() -> {
                acAccount.getItems().removeIf(acc -> {
                    if (acc == null) return false;
                    String t = acc.trim();
                    String allConst = Repository.getALL_ACCOUNT();
                    String lower = t.toLowerCase();

                    if (allConst != null && t.equalsIgnoreCase(allConst.trim())) return true;
                    if (t.equalsIgnoreCase("todas")) return true;
                    if (lower.startsWith("todas ")) return true;
                    if (lower.contains("todas las")) return true;
                    return false;
                });
            });
        });




    }

    public void clearButtonStyles() {
        Repository.getPrincipalController().getRoutingViewController().getHit().getStyleClass().removeAll("buybutton-hoverable", "sellbutton-hoverable", "button-hoverable");
        Repository.getPrincipalController().getRoutingViewController().getBest().getStyleClass().removeAll("buybutton-hoverable", "sellbutton-hoverable", "button-hoverable");
    }

    public void applyDefaultRoutingFromMapOrFirst() {
        try {
            RoutingMessage.SecurityExchangeRouting sel = getSecExchOrder().getSelectionModel().getSelectedItem();
            if (sel != null && deaultoForm.containsKey(sel.name())) {
                applyDefaultRoutingForKey(sel.name());
                return;
            }

            if (!deaultoForm.isEmpty()) {
                String firstKey = deaultoForm.keySet().iterator().next();
                try {
                    getSecExchOrder().getSelectionModel().select(RoutingMessage.SecurityExchangeRouting.valueOf(firstKey));
                    applyDefaultRoutingForKey(firstKey);
                } catch (IllegalArgumentException ex) {
                    log.warn("SecurityExchangeRouting inválido en default: {}", firstKey, ex);
                }
            }
        } catch (Exception e) {
            log.error("Error aplicando DefaultRouting", e);
        }
    }

    public void applyDefaultRoutingForKey(String secExchKey) {
        try {
            if (secExchKey == null) return;
            List<String> values = deaultoForm.get(secExchKey);
            if (values == null || values.size() < 9) {
                log.warn("DefaultRouting insuficiente para clave {}: {}", secExchKey, values);
                return;
            }

            String secExchStr   = values.get(0);
            String accountStr   = values.get(1);
            String currencyStr  = values.get(2);
            String codeOpStr    = values.get(3);
            String tifStr       = values.get(4);
            String ordTypeStr   = values.get(5);
            String settlStr     = values.get(6);
            String brokerStr    = values.get(7);
            String handlStr     = values.get(8);

            try {
                RoutingMessage.SecurityExchangeRouting ex = RoutingMessage.SecurityExchangeRouting.valueOf(secExchStr);
                if (getSecExchOrder().getSelectionModel().getSelectedItem() != ex) {
                    getSecExchOrder().getSelectionModel().select(ex);
                }
            } catch (IllegalArgumentException ex) {
                log.warn("SecExch inválido en default: {}", secExchStr);
            }

            if (accountStr != null && getAcAccount().getItems().contains(accountStr)) {
                getAcAccount().getSelectionModel().select(accountStr);
            }

            try { getCurrency().getSelectionModel().select(RoutingMessage.Currency.valueOf(currencyStr)); }
            catch (IllegalArgumentException ex) { log.warn("Currency inválida en default: {}", currencyStr); }

            if (codeOpStr != null && getCOperador().getItems().contains(codeOpStr)) {
                getCOperador().getSelectionModel().select(codeOpStr);
            }

            try { getTifOrder().getSelectionModel().select(RoutingMessage.Tif.valueOf(tifStr)); }
            catch (IllegalArgumentException ex) { log.warn("TIF inválido en default: {}", tifStr); }

            try { getTypeOrder().getSelectionModel().select(RoutingMessage.OrdType.valueOf(ordTypeStr)); }
            catch (IllegalArgumentException ex) { log.warn("OrdType inválido en default: {}", ordTypeStr); }

            try { selectSettlTypeIfAllowed(RoutingMessage.SettlType.valueOf(settlStr), "applyDefaultRoutingForKey"); }
            catch (IllegalArgumentException ex) { log.warn("SettlType inválido en default: {}", settlStr); }

            try { getBrokerOrder().getSelectionModel().select(RoutingMessage.ExecBroker.valueOf(brokerStr)); }
            catch (IllegalArgumentException ex) { log.warn("Broker inválido en default: {}", brokerStr); }

            try { getHandInstOrder().getSelectionModel().select(RoutingMessage.HandlInst.valueOf(handlStr)); }
            catch (IllegalArgumentException ex) { log.warn("HandlInst inválido en default: {}", handlStr); }

        } catch (Exception e) {
            log.error("Error aplicando DefaultRouting para {}", secExchKey, e);
        }
    }

    public void setValues(RoutingMessage.Order value) {

        isUpdatingFromPortfolio = true;
        getTicket().setText(value.getSymbol());
        isUpdatingFromPortfolio = false;
        orderSelected = value;


        Repository.getPrincipalController().getRoutingViewController().getEstrategiaLabel().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getEstrategia2().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getSpreadLabel().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getSpread2().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getLimitLabel().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getLimit2().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getBest().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getHit().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getTickMas().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getTickMenos().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getReplaceOrder().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getQuantity2().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getCancelOrder().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getLblquantity().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getVisible().setVisible(true);
        Repository.getPrincipalController().getRoutingViewController().getVisibleid().setVisible(true);

        // Habilitar los elementos relevantes de la UI
        Repository.getPrincipalController().getRoutingViewController().getEstrategiaLabel().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getEstrategia2().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getSpreadLabel().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getSpread2().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getLimitLabel().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getLimit2().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getBest().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getHit().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getTickMas().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getTickMenos().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getReplaceOrder().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getQuantity2().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getCancelOrder().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getLblquantity().setDisable(false);
        Repository.getPrincipalController().getRoutingViewController().getLblpriceOrder2().setDisable(false);

        switch (value.getStrategyOrder()) {
            case BEST:
                Repository.getPrincipalController().getRoutingViewController().getLimit2().setText(String.valueOf(value.getLimit()));
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setDisable(true);
                Repository.getPrincipalController().getRoutingViewController().getSpread2().setDisable(true);
                break;
            case HOLGURA:
                Repository.getPrincipalController().getRoutingViewController().getSpread2().setText(String.valueOf(value.getSpread()));
                Repository.getPrincipalController().getRoutingViewController().getLimit2().setDisable(true);
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setDisable(false);
                Repository.getPrincipalController().getRoutingViewController().getSpread2().setDisable(false);
                break;
            case OCO:
                Repository.getPrincipalController().getRoutingViewController().getSpread2().setText(String.valueOf(value.getSpread()));
                Repository.getPrincipalController().getRoutingViewController().getLimit2().setText(String.valueOf(value.getLimit()));
                Repository.getPrincipalController().getRoutingViewController().getSpread2().setDisable(false);
                Repository.getPrincipalController().getRoutingViewController().getLimit2().setDisable(false);
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setDisable(false);
                break;
            case TRAILING:
                Repository.getPrincipalController().getRoutingViewController().getLimit2().setText(String.valueOf(value.getLimit()));
                Repository.getPrincipalController().getRoutingViewController().getSpread2().setDisable(true);
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setDisable(false);
                break;
            case NONE_STRATEGY:
                Repository.getPrincipalController().getRoutingViewController().getEstrategiaLabel().setVisible(false);
                Repository.getPrincipalController().getRoutingViewController().getEstrategia2().setVisible(false);
                Repository.getPrincipalController().getRoutingViewController().getSpreadLabel().setVisible(false);
                Repository.getPrincipalController().getRoutingViewController().getSpread2().setVisible(false);
                Repository.getPrincipalController().getRoutingViewController().getLimitLabel().setVisible(false);
                Repository.getPrincipalController().getRoutingViewController().getLimit2().setVisible(false);
                getPriceOrder().setDisable(false);
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setDisable(false);
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setText("");
                break;
            default:
                getPriceOrder().setDisable(false);
                break;
        }

        getIceberg().setText("");
        Repository.getPrincipalController().getRoutingViewController().getVisibleid().setText("");
        if (!value.getIcebergPercentage().isEmpty()) {
            getIceberg().setText(value.getIcebergPercentage());
            Repository.getPrincipalController().getRoutingViewController().getVisibleid().setText(value.getIcebergPercentage());
        }

        if (value.getLimit() != 0d) {
            Repository.getPrincipalController().getRoutingViewController().getLimit2().setText(String.valueOf(value.getLimit()));
            Repository.getPrincipalController().getRoutingViewController().getLimit2().setDisable(false);
        }

        Repository.getPrincipalController().getRoutingViewController().getEstrategia2().getSelectionModel().select(value.getStrategyOrder());
        getSideOrder().getSelectionModel().select(ProtoConverter.routingDecryptStatus(value.getSide().name()));
        getBrokerOrder().getSelectionModel().select(value.getBroker());
        getQuantity().setText(formatearNumero(String.valueOf(value.getOrderQty())));
        getPriceOrder().setText(formatearNumero(String.valueOf(value.getPrice())));

        if (value.getCurrency().equals(RoutingMessage.Currency.NO_CURRENCY)) {
            getCurrency().getSelectionModel().select(RoutingMessage.Currency.CLP);
        } else {
            getCurrency().getSelectionModel().select(value.getCurrency());
        }

        Repository.getPrincipalController().getRoutingViewController().getQuantity2().setText(formatearNumero(String.valueOf(value.getOrderQty())));
        Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setText(formatearNumero(String.valueOf(value.getPrice())));

        double quantityValue = value.getOrderQty();
        double priceValue = value.getPrice();
        double cashValue = quantityValue * priceValue;
        getCash().setText(formatearNumero(String.valueOf(cashValue)));

        getTypeOrder().getSelectionModel().select(value.getOrdType());
        getSecurityType().getSelectionModel().select(value.getSecurityType());
        getBrokerOrder().getSelectionModel().select(value.getBroker());
        getHandInstOrder().getSelectionModel().select(value.getHandlInst());
        getAcAccount().getSelectionModel().select(value.getAccount());
        selectSettlTypeIfAllowed(value.getSettlType(), "setValues.orderSelected");
        getSecExchOrder().getSelectionModel().select(value.getSecurityExchange());
        getStrategOrder().getSelectionModel().select(value.getStrategyOrder());
        getCOperador().getSelectionModel().select(value.getCodeOperator());

        Repository.getPrincipalController().subscribeSymbol();
    }

    public void presele(RoutingMessage.Order value) {

        iceberg.setText("");

        if (!value.getIcebergPercentage().isEmpty()) {
            iceberg.setText(value.getIcebergPercentage());
        }

        sideOrder.getSelectionModel().select(ProtoConverter.routingDecryptStatus(value.getSide().name()));
        quantity.setText(String.valueOf(value.getOrderQty()));
        priceOrder.setText(String.valueOf(value.getPrice()));
        ticket.setText(value.getSymbol());
        typeOrder.getSelectionModel().select(value.getOrdType());
        securityType.getSelectionModel().select(value.getSecurityType());
        brokerOrder.getSelectionModel().select(value.getBroker());
        handInstOrder.getSelectionModel().select(value.getHandlInst());
        acAccount.getSelectionModel().select(value.getAccount());
        settltypeOrder.getSelectionModel().select(value.getSettlType());
        secExchOrder.getSelectionModel().select(value.getSecurityExchange());


    }

    private void updateButtonStyles(String buttonStyleClass) {
        routeOrder.getStyleClass().clear();
        routeOrder.getStyleClass().add(buttonStyleClass);

        bestR.getStyleClass().clear();
        bestR.getStyleClass().add(buttonStyleClass);

        hitR.getStyleClass().clear();
        hitR.getStyleClass().add(buttonStyleClass);
    }

    public void setFormByStatistic(MarketDataMessage.Statistic selectedItem) {
        try {

            RoutingMessage.SettlType prevSettl =
                    (settltypeOrder != null) ? settltypeOrder.getSelectionModel().getSelectedItem() : null;

            sideOrder.getSelectionModel().selectFirst();
            quantity.setText("");
            priceOrder.setText("");
            ticket.setText("");
            typeOrder.getSelectionModel().select(RoutingMessage.OrdType.LIMIT);
            securityType.getSelectionModel().select(RoutingMessage.SecurityType.CS);
            tifOrder.getSelectionModel().select(RoutingMessage.Tif.DAY);
            secExchOrder.getSelectionModel().select(RoutingMessage.SecurityExchangeRouting.XSGO);
            handInstOrder.getSelectionModel().select(RoutingMessage.HandlInst.PRIVATE_ORDER);
            //acAccount.getSelectionModel().selectFirst();


            if (canAutoSetSettlType()) {
                selectSettlTypeIfAllowed(RoutingMessage.SettlType.T2, "setFormByStatistic.resetT2");
            } else {
                if (settltypeOrder != null && prevSettl != null) {
                    settltypeOrder.getSelectionModel().select(prevSettl);
                    log.info("[LIQUIDACION][KEEP] source=setFormByStatistic.keepPrevious user={} light={} premium={} kept={}",
                            Repository.getUsername(), isLightMode(), isPremiumLiquidationUser(), prevSettl);
                }
            }

            cOperador.getSelectionModel().selectFirst();

            Repository.getRoutingController().getCancelOrder().setDisable(false);
            Repository.getRoutingController().getReplaceOrder().setDisable(false);
            Repository.getRoutingController().getBest().setDisable(false);
            Repository.getRoutingController().getHit().setDisable(false);
            Repository.getRoutingController().getTickMas().setDisable(false);
            Repository.getRoutingController().getTickMenos().setDisable(false);

            bestR.setDisable(false);
            hitR.setDisable(false);

            limit.setText("");
            spread.setText("");
            limit.setDisable(true);
            spread.setDisable(true);

            iceberg.setText("");

            sideOrder.getSelectionModel().select(ProtoConverter.routingDecryptStatus(RoutingMessage.Side.BUY.name()));
            quantity.setText(formatearNumero(String.valueOf(selectedItem.getAskQty())));
            priceOrder.setText(formatearNumero(String.valueOf(selectedItem.getAskPx())));

            isUpdatingFromPortfolio = true;
            ticket.setText(selectedItem.getSymbol());
            isUpdatingFromPortfolio = false;

            typeOrder.getSelectionModel().select(RoutingMessage.OrdType.LIMIT);
            securityType.getSelectionModel().select(RoutingMessage.SecurityType.CS);
            brokerOrder.getSelectionModel().select(RoutingMessage.ExecBroker.VC);
            handInstOrder.getSelectionModel().select(RoutingMessage.HandlInst.PRIVATE_ORDER);
            selectSettlTypeIfAllowed(selectedItem.getSettlType(), "setFormByStatistic.portfolioSymbol");

            RoutingMessage.SecurityExchangeRouting securityExchangeRouting =
                    conversorExdestination(selectedItem.getSecurityExchange());

            secExchOrder.getSelectionModel().select(securityExchangeRouting);
            strategOrder.getSelectionModel().select(RoutingMessage.StrategyOrder.NONE_STRATEGY);
            currency.getSelectionModel().select(RoutingMessage.Currency.CLP);

            DecimalFormat decimalFormat = NumberGenerator.getFormatNumberMilDec(selectedItem.getSecurityExchange());
            cash.setText("$" + decimalFormat.format(selectedItem.getAskQty() * selectedItem.getAskPx()));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public RoutingMessage.Order.Builder formToOrder() {

        RoutingMessage.Order.Builder orderBuilder = null;

        try {
            orderBuilder = RoutingMessage.Order.newBuilder();
            orderBuilder.setCodeOperator(cOperador.getSelectionModel().getSelectedItem());
            orderBuilder.setAccount(acAccount.getSelectionModel().getSelectedItem());
            orderBuilder.setTime(TimeGenerator.getTimeProto());
            orderBuilder.setId(IDGenerator.getID());
            orderBuilder.setSide(RoutingMessage.Side.valueOf(ProtoConverter.routingEncryptStatus(sideOrder.getSelectionModel().getSelectedItem())));
            orderBuilder.setTif(tifOrder.getSelectionModel().getSelectedItem());
            orderBuilder.setOrdType(typeOrder.getSelectionModel().getSelectedItem());
            orderBuilder.setSettlType(settltypeOrder.getSelectionModel().getSelectedItem());

            if (brokerOrder.getSelectionModel().getSelectedItem() != null) {
                orderBuilder.setBroker(brokerOrder.getSelectionModel().getSelectedItem());
            } else {
                orderBuilder.setBroker(RoutingMessage.ExecBroker.VC);
            }
            orderBuilder.setHandlInst(handInstOrder.getSelectionModel().getSelectedItem());
            orderBuilder.setSecurityExchange(secExchOrder.getSelectionModel().getSelectedItem());
            orderBuilder.setOrderID(IDGenerator.getID());
            orderBuilder.setExecId(IDGenerator.getID());
            orderBuilder.setOperator(Repository.username);

            if (!spread.getText().isEmpty()) {
                orderBuilder.setSpread(Double.parseDouble(spread.getText()));
            }

            if (!limit.getText().isEmpty()) {
                orderBuilder.setLimit(Double.parseDouble(limit.getText()));
            }

            if (!priceOrder.getText().isEmpty()) {
                orderBuilder.setPrice(Double.parseDouble(priceOrder.getText().replace(",", "")));
            }



            LocalDate today = LocalDate.now();

            Integer effHour  = (hourSpinnerEffective  != null) ? hourSpinnerEffective.getValue()  : null;
            Integer effMin   = (minuteSpinnerEffective != null) ? minuteSpinnerEffective.getValue() : null;
            Integer expHour  = (hourSpinnerExpire    != null) ? hourSpinnerExpire.getValue()    : null;
            Integer expMin   = (minuteSpinnerExpire   != null) ? minuteSpinnerExpire.getValue()   : null;


            if (effHour != null && effMin != null) {

                orderBuilder.setRiskRate(slice.getValue());

                LocalTime effTime = LocalTime.of(effHour, effMin);

                ZonedDateTime zdtEff = ZonedDateTime.of(today, effTime, Repository.getZoneID());
                com.google.protobuf.Timestamp effTs = com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(zdtEff.toEpochSecond())
                        .setNanos(zdtEff.getNano())
                        .build();

                orderBuilder.setEffectiveTime(effTs);   // ✅ listo, aquí ya van hora y min
            }

            if (expHour != null && expMin != null) {
                LocalTime expTime = LocalTime.of(expHour, expMin);

                ZonedDateTime zdtExp = ZonedDateTime.of(today, expTime, Repository.getZoneID());
                com.google.protobuf.Timestamp expTs = com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(zdtExp.toEpochSecond())
                        .setNanos(zdtExp.getNano())
                        .build();

                orderBuilder.setExpireTime(expTs);      // ✅ también con hora y min
            }


            orderBuilder.setLastPx(0d);
            orderBuilder.setAvgPrice(0d);
            orderBuilder.setIcebergPercentage("");
            orderBuilder.setCumQty(0d);
            orderBuilder.setMaxFloor(0d);
            orderBuilder.setLeaves(0d);
            orderBuilder.setOrderQty(Double.parseDouble(quantity.getText().replace(",", "")));

            if (!securityType.getSelectionModel().getSelectedItem().name().isEmpty()) {
                orderBuilder.setSecurityType(securityType.getSelectionModel().getSelectedItem());
            }

            if (currency.getSelectionModel().getSelectedItem() != null) {
                orderBuilder.setCurrency(currency.getSelectionModel().getSelectedItem());
            }

            if (strategOrder.getSelectionModel().getSelectedItem() != null) {
                orderBuilder.setStrategyOrder(strategOrder.getSelectionModel().getSelectedItem());
            }

            orderBuilder.setSymbol(ticket.getText().trim());

            if (!iceberg.getText().isEmpty()) {

                double quantityValue = Double.parseDouble(quantity.getText().replace(",", ""));

                double icebergValue = Double.parseDouble(iceberg.getText().replace("%", "")
                        .replace(".", "").replace(",", ""));

                double calculatedIceberg = (quantityValue * icebergValue) / 100;
                double minIceberg = (quantityValue * 0.1);

                if (calculatedIceberg < minIceberg) {
                    calculatedIceberg = minIceberg;
                }

                int maxFloor = (int) Math.ceil(calculatedIceberg);


                orderBuilder.setMaxFloor(maxFloor);
                orderBuilder.setIcebergPercentage(iceberg.getText());
            }


        } catch (Exception e) {
            Notifier.INSTANCE.notifyError("Error", "Faltan campos por completar");
            log.error(e.getMessage(), e);
            return null;
        }

        if (orderBuilder.getOrderQty() <= 0) {
            Notifier.INSTANCE.notifyError("Error", "La cantidad debe ser mayor que cero");
            return null;
        }

        if (!orderBuilder.getOrdType().equals(RoutingMessage.OrdType.MARKET)
                && !orderBuilder.getStrategyOrder().equals(RoutingMessage.StrategyOrder.BEST)
                && !orderBuilder.getStrategyOrder().equals(RoutingMessage.StrategyOrder.TRAILING)
                && !orderBuilder.getStrategyOrder().equals(RoutingMessage.StrategyOrder.OCO)
                && !orderBuilder.getStrategyOrder().equals(RoutingMessage.StrategyOrder.VWAP)
                && orderBuilder.getPrice() <= 0) {
            Notifier.INSTANCE.notifyError("Error", "El precio debe ser mayor que cero para una orden no de mercado.");
            return null;
        }

        return orderBuilder;
    }

    @FXML
    private void routeOrder() {

        RoutingMessage.Order.Builder order = formToOrder();

        if (order == null) {
            return;
        }

        RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(order).build();

        if (skipConfirmationAlert || alertRoute(order)) {
            Repository.getClientService().sendMessage(newOrderRequest);
            close();
        }

        libroEmergenteControllerList.forEach(s->{
            s.close();
        });
    }

    @FXML
    public void preSelectAction(ActionEvent actionEvent) {

        RoutingMessage.Order.Builder order = formToOrder();

        if (order == null) {
            return;
        }

        BlotterMessage.PreselectRequest reques = BlotterMessage.PreselectRequest.newBuilder()
                .setUsername(Repository.username)
                .setStatusPreselect(BlotterMessage.StatusPreselect.ADD_PRESELECT)
                .setOrders(order.build()).build();

        Repository.getClientService().sendMessage(reques);
    }

    private void updateSuggestions(String text) {
        if (text == null || text.isEmpty()) {
            suggestionsPopup.hide();
        } else {
            if (filteredList == null) {
                allSymbols = Repository.getAllSymbols();
                filteredList = new FilteredList<>(allSymbols);
            }

            filteredList.setPredicate(item -> item.toLowerCase().contains(text.toLowerCase()));
            suggestionsList.setItems(filteredList);

            if (!filteredList.isEmpty()) {
                showSuggestionsPopup();
            } else {
                suggestionsPopup.hide();
            }
        }
    }

    private synchronized void updateSecurityTypeComboBox(String ticket, MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData) {

        if (securityExchangeMarketData == null) {
            log.warn("securityExchangeMarketData es null para el ticket: {}", ticket);
            return;
        }

        if (!Repository.getSecurityListMaps().contains(ticket, securityExchangeMarketData.name())) {
            log.info("retorna {} {}", ticket, securityExchangeMarketData.name());
            return;
        }

        Platform.runLater(() -> {

            MarketDataMessage.Security security = Repository.getSecurityListMaps().get(ticket, securityExchangeMarketData.name());
            if (security != null) {
                String securityTypeString = security.getSecurityType();
                securityType.getSelectionModel().select(RoutingMessage.SecurityType.valueOf(securityTypeString));
            }



        });
    }

    public void setupTicketTextField() {
        try {
            getTicket().focusedProperty().addListener((obs, wasFocused, isNowFocused) -> {
                if (!isNowFocused) {
                    suggestionsPopup.hide();
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void showSuggestionsPopup() {
        Bounds bounds = ticket.localToScreen(ticket.getBoundsInLocal());

        if (bounds != null) {
            suggestionsPopup.show(ticket, bounds.getMinX(), bounds.getMaxY());
            isPopupVisible = true;
        }
    }

    public void setupSuggestionsPopup() {

        if (suggestionsPopup == null) {
            suggestionsList = new ListView<>();
            suggestionsPopup = new Popup();
            suggestionsPopup.getContent().add(suggestionsList);
            suggestionsPopup.setAutoHide(true);

            suggestionsList.setOnMouseClicked(event -> {
                String selectedItem = suggestionsList.getSelectionModel().getSelectedItem();
                if (selectedItem != null) {

                    clearSelectedOrderContext();

                    isUpdatingFromPortfolio = true;
                    ticket.setText(selectedItem.toUpperCase(Locale.ROOT));
                    quantity.clear();
                    priceOrder.clear();
                    isUpdatingFromPortfolio = false;

                    suggestionsPopup.hide();

                    Repository.getPrincipalController().subscribeSymbol();
                }
            });
        }
    }

    @FXML
    public void cleanform(ActionEvent actionEvent) {
        try {
            //cleanTableMKD();
            iceberg.setText("");
            sideOrder.getSelectionModel().selectFirst();
            quantity.setText("");
            priceOrder.setText("");
            ticket.setText("");
            typeOrder.getSelectionModel().select(RoutingMessage.OrdType.LIMIT);
            securityType.getSelectionModel().select(RoutingMessage.SecurityType.CS);
            tifOrder.getSelectionModel().select(RoutingMessage.Tif.DAY);
            secExchOrder.getSelectionModel().select(RoutingMessage.SecurityExchangeRouting.XSGO);
            handInstOrder.getSelectionModel().select(RoutingMessage.HandlInst.PRIVATE_ORDER);
            acAccount.getSelectionModel().selectFirst();
            settltypeOrder.getSelectionModel().select(RoutingMessage.SettlType.T2);
            cOperador.getSelectionModel().selectFirst();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public Boolean alertRoute(String message) {

        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirmación");
        alert.setHeaderText("Desea confirmar ingresar la Orden de compra ?");
        alert.setContentText(message);

        String cssPath = getClass().getResource(Repository.getSTYLE()).toExternalForm();
        alert.getDialogPane().getStylesheets().add(cssPath);
        alert.getDialogPane().getStyleClass().add("alert-dialog");

        Node cancelButton = alert.getDialogPane().lookupButton(ButtonType.CANCEL);
        Node acceptButton = alert.getDialogPane().lookupButton(ButtonType.OK);

        cancelButton.getStyleClass().add("button");
        acceptButton.getStyleClass().addAll("button", "cancel");

        alert.initStyle(StageStyle.UTILITY);
        alert.showAndWait();

        return alert.getResult() == ButtonType.OK;
    }

    public Boolean alertRoute(RoutingMessage.Order.Builder order) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setHeaderText("¿Desea enviar la nueva orden de " + getSideOrder().getSelectionModel().getSelectedItem() + " ?");
        String orderType = getSideOrder().getSelectionModel().getSelectedItem();
        String textColor = orderType.equalsIgnoreCase("Compra") ? "green" : "red";
        double total = order.getPrice() * order.getOrderQty();
        String formattedTotal = NumberGenerator.formatDouble(total);

        String text = (int) order.getOrderQty() + " " + order.getSymbol() + " a $" +
                NumberGenerator.formatDouble(order.getPrice()) + " , Total: $" + formattedTotal;

        Label contentLabel = new Label(text);
        contentLabel.setStyle("-fx-text-fill: " + textColor + ";");
        alert.getDialogPane().setContent(contentLabel);

        alert.setContentText(text);
        String cssPath = getClass().getResource(Repository.getSTYLE()).toExternalForm();
        alert.getDialogPane().getStylesheets().add(cssPath);
        alert.getDialogPane().getStyleClass().add("alert-dialog");

        Node cancelButton = alert.getDialogPane().lookupButton(ButtonType.CANCEL);
        Node acceptButton = alert.getDialogPane().lookupButton(ButtonType.OK);

        cancelButton.getStyleClass().add("button");
        acceptButton.getStyleClass().addAll("button", "cancel");

        alert.initStyle(StageStyle.UTILITY);
        alert.showAndWait();

        return alert.getResult() == ButtonType.OK;
    }

    public void handleAgreeAction() {
        Platform.runLater(() -> {
            if (chkIndivisible.isSelected()) {
                lblAgreement.setText("Agreement: OPCI");
            } else {
                lblAgreement.setText("Agreement: OPC");
            }
        });
    }

    @FXML
    private void addBook(ActionEvent event) {
        try {

            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/MultiLibroEmergente.fxml"));
            AnchorPane mainPane = loader.load();
            LibroEmergentePrincipalController controller = loader.getController();

            Repository.controllerMultibook.put(controller.id, controller);

            Stage stage = new Stage();
            Scene scene = new Scene(mainPane);

            scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("/blotter/css/style.css")).toExternalForm());

            if (Repository.isDayMode()) {
                scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("/blotter/css/daymode.css")).toExternalForm());
            }

            stage.setScene(scene);

            stage.setMaximized(true);

            stage.setOnCloseRequest(events -> {
                controller.unsubscribe();
                Repository.controllerMultibook.remove(controller.id);

                if (Repository.controllerMultibook.isEmpty()) {
                    Repository.countMultibook = -1;
                }
            });

            stage.show();

        } catch (IOException e) {
            log.error(e.getMessage(), e);
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Error");
            alert.setHeaderText("No se pudo abrir el nuevo libro de mercado");
            alert.setContentText("Ocurrió un error al intentar abrir el nuevo libro de mercado.");
            alert.showAndWait();
        }
    }

    public void bestActions(ActionEvent actionEvent) {

        try {

            Double px;

            String side = (orderSelected == null) ? getSideOrder().getSelectionModel().getSelectedItem() : orderSelected.getSide().name();
            RoutingMessage.SecurityExchangeRouting securityExchangeRouting = (orderSelected == null) ? getSecExchOrder().getSelectionModel().getSelectedItem() : orderSelected.getSecurityExchange();

            RoutingMessage.Side sidelauncher = RoutingMessage.Side.BUY;

            if (side.equals("SELL") || side.equals("Venta")) {
                sidelauncher = RoutingMessage.Side.SELL;
            }


            if (islibrazo) {

                String sides = sideOrder.getSelectionModel().getSelectedItem();
                BookVO bookVO = Repository.getBookPortMaps().get(IdLibrazo);

                if (sides.equals("BUY") || sides.equals("Compra")) {
                    px = Double.valueOf(formatearNumeroBestHit(bookVO.getBidBook().get(0).getPrice()));
                    BigDecimal pxTick = Ticks.getTick(securityExchangeRouting, BigDecimal.valueOf(px));
                    px = Ticks.roundToTick(BigDecimal.valueOf(px - pxTick.doubleValue()), pxTick).doubleValue();

                } else {
                    px = Double.valueOf(formatearNumeroBestHit(bookVO.getAskBook().get(0).getPrice()));
                    BigDecimal pxTick = Ticks.getTick(securityExchangeRouting, BigDecimal.valueOf(px));
                    px = Ticks.roundToTick(BigDecimal.valueOf(px - pxTick.doubleValue()), pxTick).doubleValue();
                }

            } else if (sidelauncher.equals(RoutingMessage.Side.BUY)) {

                if (Repository.getPrincipalController().getTableViewBookHController().getBidViewTable().getItems().isEmpty()) {
                    Notifier.INSTANCE.notifyError("Error", "No hay posturas para calcular precio");
                    return;
                }

                px = Double.valueOf(formatearNumeroBestHit(Repository.getPrincipalController().getTableViewBookHController().getBidViewTable().getItems().get(0).getPrice()));
                BigDecimal pxTick = Ticks.getTick(securityExchangeRouting, BigDecimal.valueOf(px));
                px = Ticks.roundToTick(BigDecimal.valueOf(px + pxTick.doubleValue()), pxTick).doubleValue();

            } else if (Repository.getPrincipalController().getRoutingViewController().getIsVertical()) {
                px = Double.valueOf(formatearNumeroBestHit(Repository.getPrincipalController().getTableViewBookVController().getOfferViewTable().getItems()
                        .get(0).getPrice()));
                BigDecimal pxTick = Ticks.getTick(securityExchangeRouting, BigDecimal.valueOf(px));
                px = Ticks.roundToTick(BigDecimal.valueOf(px - pxTick.doubleValue()), pxTick).doubleValue();

            } else {

                if (Repository.getMarketDataController().getOfferViewTable().getItems().isEmpty()) {
                    Notifier.INSTANCE.notifyError("Error", "No hay posturas para calcular precio");
                    return;
                }

                px = Double.valueOf(formatearNumeroBestHit(Repository.getMarketDataController().getOfferViewTable().getItems().get(0).getPrice()));
                BigDecimal pxTick = Ticks.getTick(securityExchangeRouting, BigDecimal.valueOf(px));
                px = Ticks.roundToTick(BigDecimal.valueOf(px - pxTick.doubleValue()), pxTick).doubleValue();

            }

            if (px <= 0d) {
                Notifier.INSTANCE.notifyError("Error", "El campo de precio está en cero.");
                return;
            }

            if (orderSelected == null) {
                getPriceOrder().setText(String.valueOf(px));
            } else {
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setText(px.toString());
            }

        } catch (NumberFormatException e) {
            Notifier.INSTANCE.notifyError("Error", "Formato de precio o cantidad no válido.");
        } catch (Exception e) {
            Notifier.INSTANCE.notifyError("Error", "Ocurrió un error al procesar la orden.");
            log.error(e.getMessage(), e);
        }

    }

    @FXML
    public void bestAction(ActionEvent actionEvent) {
        bestActions(actionEvent);
    }

    @FXML
    public void bestActionLight(ActionEvent actionEvent) {
        bestActions(actionEvent);
    }

    @FXML
    public void bestActionLight2(ActionEvent actionEvent) {
        bestActions(actionEvent);
    }

    public void hitActions(ActionEvent actionEvent) {

        try {

            String side = (orderSelected == null) ? getSideOrder().getSelectionModel().getSelectedItem() : orderSelected.getSide().name();
            RoutingMessage.Side sidelauncher = null;

            if (side.equals("BUY") || side.equals("Compra")) {
                sidelauncher = RoutingMessage.Side.BUY;
            } else if (side.equals("SELL") || side.equals("Venta")) {
                sidelauncher = RoutingMessage.Side.SELL;
            }


            Double px = 0d;


            if (islibrazo) {

                String sides = sideOrder.getSelectionModel().getSelectedItem();
                BookVO bookVO = Repository.getBookPortMaps().get(IdLibrazo);

                if(sides.equals("BUY") || sides.equals("Compra")) {
                    px = Double.valueOf(formatearNumeroBestHit(bookVO.getAskBook().get(0).getPrice()));
                    BigDecimal pxTick = Ticks.conversorExdestination(MarketDataMessage.SecurityExchangeMarketData.valueOf(bookVO.getSecurityExchange()), BigDecimal.valueOf(px));
                    px = Ticks.roundToTick(BigDecimal.valueOf(px), pxTick).doubleValue();

                } else {
                    px = Double.valueOf(formatearNumeroBestHit(bookVO.getBidBook().get(0).getPrice()));
                    BigDecimal pxTick = Ticks.conversorExdestination(MarketDataMessage.SecurityExchangeMarketData.valueOf(bookVO.getSecurityExchange()), BigDecimal.valueOf(px));
                    px = Ticks.roundToTick(BigDecimal.valueOf(px), pxTick).doubleValue();
                }

            } else if (Repository.getPrincipalController().getRoutingViewController().getIsVertical() && Objects.equals(sidelauncher, RoutingMessage.Side.BUY)) {
                px = Double.valueOf(formatearNumeroBestHit(Repository.getPrincipalController().getTableViewBookVController()
                        .getOfferViewTable().getItems().get(0).getPrice())) ;

            } else if (!Repository.getPrincipalController().getRoutingViewController().getIsVertical() && Objects.equals(sidelauncher, RoutingMessage.Side.BUY)) {
                px = Double.valueOf(formatearNumeroBestHit(Repository.getMarketDataController().getOfferViewTable().getItems().get(0).getPrice()));

            } else {
                assert sidelauncher != null;
                if (sidelauncher.equals(RoutingMessage.Side.SELL)) {
                    px = Double.valueOf(formatearNumeroBestHit(Repository.getPrincipalController().getTableViewBookVController().getBidViewTable().getItems().get(0).getPrice()));
                }
            }

            if (px <= 0d) {
                Notifier.INSTANCE.notifyError("Error", "El campo de precio está en cero.");
                return;
            }

            if (orderSelected == null) {
                getPriceOrder().setText(String.valueOf(px));
            } else {
                Repository.getPrincipalController().getRoutingViewController().getPriceOrder2().setText(px.toString());
            }


        } catch (NumberFormatException e) {
            Notifier.INSTANCE.notifyError("Error", "Formato de precio o cantidad no válido.");
        } catch (Exception e) {
            Notifier.INSTANCE.notifyError("Error", "Ocurrió un error al procesar la orden.");
            log.error(e.getMessage(), e);
        }
    }

    @FXML
    public void hitActionLight(ActionEvent actionEvent) {
        hitActions(actionEvent);
    }

    @FXML
    public void hitActionLight2(ActionEvent actionEvent) {
        hitActions(actionEvent);
    }

    @FXML
    public void hitAction(ActionEvent actionEvent) {
        hitActions(actionEvent);
    }

    public Boolean alertRoute2(String message) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirmación");
        alert.setHeaderText("Desea modificar la Orden ?");
        alert.setContentText(message);

        String cssPath = Objects.requireNonNull(getClass().getResource(Repository.getSTYLE())).toExternalForm();
        alert.getDialogPane().getStylesheets().add(cssPath);
        alert.getDialogPane().getStyleClass().add("alert-dialog");

        Node cancelButton = alert.getDialogPane().lookupButton(ButtonType.CANCEL);
        Node acceptButton = alert.getDialogPane().lookupButton(ButtonType.OK);

        cancelButton.getStyleClass().add("button");
        acceptButton.getStyleClass().addAll("button", "cancel");

        alert.initStyle(StageStyle.UTILITY);
        alert.showAndWait();

        return alert.getResult() == ButtonType.OK;
    }

    public void close() {
        if (stage != null) {
            stage.close();
        }
    }

    public void setValues(BlotterMessage.User user) {

        try {


            ObservableList<RoutingMessage.StrategyOrder> strtate = FXCollections.observableArrayList();
            strtate.addAll(user.getRoles().getStrategyList());
            strtate.add(RoutingMessage.StrategyOrder.NONE_STRATEGY);

            this.getStrategOrder().setItems(strtate);
            this.getStrategOrder().getSelectionModel().select(RoutingMessage.StrategyOrder.NONE_STRATEGY);


            ObservableList<RoutingMessage.ExecBroker> broker = FXCollections.observableArrayList();
            broker.addAll(user.getRoles().getBrokerList());
            broker.add(RoutingMessage.ExecBroker.VC);

            this.getCurrency().getItems().clear();
            this.getCurrency().getItems().add(RoutingMessage.Currency.CLP);
            this.getCurrency().getItems().add(RoutingMessage.Currency.USD);

            this.getCurrency().getSelectionModel().select(RoutingMessage.Currency.CLP);

            this.getBrokerOrder().setItems(broker);
            this.getBrokerOrder().getSelectionModel().select(RoutingMessage.ExecBroker.VC);

            // Security Exchange del usuario
            ObservableList<RoutingMessage.SecurityExchangeRouting> securityExchangeUser = FXCollections.observableArrayList();
            securityExchangeUser.addAll(user.getRoles().getDestinoRoutingList());

            this.getSecExchOrder().setItems(securityExchangeUser);
            this.getSecExchOrder().getSelectionModel().select(RoutingMessage.SecurityExchangeRouting.XSGO);

            // Código operador
            ObservableList<String> codeOperator = FXCollections.observableArrayList();
            codeOperator.addAll(user.getRoles().getCodeOperatorList());

            this.getCOperador().setItems(codeOperator);
            this.getCOperador().getSelectionModel().selectFirst();

            // Cuentas
            ObservableList<String> allAccountUsers = FXCollections.observableArrayList();
            ObservableList<String> accountUsers    = FXCollections.observableArrayList();

            // Todas las cuentas originales del usuario
            allAccountUsers.addAll(user.getAccountList());

            // Mantener la cuenta "Todas" en la lista general si la usas en otras partes
            if (!allAccountUsers.contains(Repository.getALL_ACCOUNT())) {
                allAccountUsers.add(Repository.getALL_ACCOUNT());
            }

            // Para el ComboBox acAccount:
            //  - Omitimos "Todas" / ALL_ACCOUNT
            //  - Ordenamos por el número principal de la cuenta
            for (String acc : user.getAccountList()) {
                if (acc == null) continue;

                String trimmed = acc.trim();

                // Omitir explícitamente "Todas" y la constante ALL_ACCOUNT
                if (trimmed.equalsIgnoreCase("todas")) continue;
                if (trimmed.equalsIgnoreCase(Repository.getALL_ACCOUNT())) continue;

                accountUsers.add(trimmed);
            }

            // Orden numérico por la parte antes del "/"
            Comparator<String> accountComparator = (a, b) -> {
                try {
                    String na = a.split("/")[0].replaceAll("\\D", "");
                    String nb = b.split("/")[0].replaceAll("\\D", "");

                    if (na.isEmpty() || nb.isEmpty()) {
                        return a.compareTo(b);
                    }

                    long la = Long.parseLong(na);
                    long lb = Long.parseLong(nb);
                    return Long.compare(la, lb);
                } catch (Exception e) {
                    // Si algo falla, orden alfabético normal
                    return a.compareTo(b);
                }
            };

            FXCollections.sort(accountUsers, accountComparator);

            this.getAcAccount().setItems(accountUsers);

            if (!accountUsers.isEmpty()) {
                // Tu lógica de "por defecto" sigue siendo la primera;
                // si después otra parte del código hace select(accountStr) la va a sobrescribir.
                this.getAcAccount().getSelectionModel().selectFirst();
            }

            this.getCurrency().getSelectionModel().select(RoutingMessage.Currency.CLP);

            Platform.runLater(() -> {
                this.getStrategOrder().getSelectionModel().select(RoutingMessage.StrategyOrder.HOLGURA);
            });

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }

    }

    private void clearSelectedOrderContext() {

        orderSelected = null;
        selectedInWorkingOrder.set(false);

        try {
            var pc = Repository.getPrincipalController();
            if (pc != null) pc.setOrderSelected(null);
        } catch (Exception ignore) {}

        try {
            var exec = Repository.getRoutingController().getExecutionsOrderController();
            if (exec != null && exec.getTableExecutionReports() != null) {
                exec.getTableExecutionReports().getSelectionModel().clearSelection();
            }
        } catch (Exception ignore) {}
    }


    public void setLibrazoController(LibroEmergenteController libroEmergenteController) {

        if(!libroEmergenteControllerList.contains(libroEmergenteController)){
            libroEmergenteControllerList.add(libroEmergenteController);
        }

    }

}

