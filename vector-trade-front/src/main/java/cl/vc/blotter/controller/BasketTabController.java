package cl.vc.blotter.controller;


import cl.vc.blotter.Repository;
import cl.vc.blotter.utils.Notifier;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import javafx.collections.ListChangeListener;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.stage.StageStyle;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;
import java.util.stream.IntStream;

/**
 * Controla UN tab de canasta: la lista de órdenes/ejecuciones en vivo (ExecutionsController
 * embebido) y el panel de estadísticas, que se calcula EN EL FRONT a partir de las órdenes
 * (el servidor no provee los agregados).
 */
@Slf4j
@Data
public class BasketTabController {

    // ── Panel de estadísticas (calculadas en el front) ──
    @FXML private Label lblBasket;
    @FXML private Label lblMontoTotal;
    @FXML private Label lblMontoDone;
    @FXML private Label lblMontoLeft;
    @FXML private Label lblPctDone;
    @FXML private Label lblPctLeft;
    @FXML private Label lblQtyTotal;
    @FXML private Label lblQtyDone;
    @FXML private Label lblQtyLeft;
    @FXML private Label lblNTotal;
    @FXML private Label lblNDone;
    @FXML private Label lblNPending;

    @FXML private Button execButton;
    @FXML private Button cancelOrder;
    @FXML private Button replaceOrder;
    @FXML private TextField quantityReplace;
    @FXML private TextField priceReplace;
    @FXML private TextField visibleReplace;
    @FXML private TextField spreadReplace;
    @FXML private TextField limitReplace;
    @FXML private ExecutionsController executionsOrderController;

    private Tab tab;
    private String basketId = "";

    @FXML
    private void initialize() {

        executionsOrderController.getHandlInst().setVisible(false);
        executionsOrderController.getSettlType().setVisible(false);
        executionsOrderController.getUsername().setVisible(true);
        executionsOrderController.getBasket().setVisible(true);
        executionsOrderController.getIceberg().setVisible(false);
        executionsOrderController.getExecType().setVisible(false);

        executionsOrderController.basketOrder();
        executionsOrderController.setOrderColumnConfigChangeHandler(this::applyOrderColumnConfig);
        executionsOrderController.applyOrderColumnConfig();
        setReplaceControlsDisabled(true);

        executionsOrderController.getTableExecutionReports().getSelectionModel().selectedItemProperty()
                .addListener((observable, oldValue, newValue) -> {
                    boolean live = isReplaceable(newValue);
                    cancelOrder.setDisable(!live);
                    replaceOrder.setDisable(!live);
                    setReplaceControlsDisabled(!live);
                    syncReplaceControls(newValue);
                });

        // Cualquier cambio en la lista de órdenes recalcula las estadísticas.
        executionsOrderController.getTableExecutionReports().getItems()
                .addListener((ListChangeListener<RoutingMessage.Order>) c -> recomputeStats());
    }

    /** Asocia el Tab y el basketID; fija el encabezado y el título inicial. */
    public void bindTab(Tab tab, String basketId) {
        this.tab = tab;
        this.basketId = basketId == null ? "" : basketId;
        if (lblBasket != null) lblBasket.setText("Canasta: " + this.basketId);
        updateTitle(0, 0);
    }

    /** Upsert por id de una orden en la grilla de la canasta. */
    public void upsertOrder(RoutingMessage.Order order) {
        var data = executionsOrderController.getData();
        if (data == null) return;
        int idx = IntStream.range(0, data.size())
                .filter(i -> data.get(i).getId().equals(order.getId()))
                .findFirst().orElse(-1);
        if (idx >= 0) {
            data.set(idx, order);
        } else {
            data.add(order);
        }
        executionsOrderController.getTableExecutionReports().refresh();
    }

    /**
     * Recalcula TODAS las estadísticas de la canasta desde las órdenes vivas:
     * monto total/ejecutado/pendiente, % ejecutado/pendiente (por cantidad),
     * qty total/ejecutada/pendiente y conteo de órdenes total/hechas/pendientes.
     *
     * Nota monto: para órdenes pasivas con PX=0 el monto se estima con el precio que haya
     * (price -> avgPrice -> lastPx); se vuelve exacto a medida que ejecutan.
     */
    public void recomputeStats() {
        try {
            double montoTotal = 0, montoDone = 0, qtyTotal = 0, qtyDone = 0;
            int nTotal = 0, nDone = 0, nPending = 0;

            if (executionsOrderController.getData() != null) {
                for (RoutingMessage.Order o : executionsOrderController.getData()) {
                    nTotal++;
                    double oq = o.getOrderQty();
                    double cum = o.getCumQty();
                    double avg = o.getAvgPrice();
                    double pxRef = o.getPrice() > 0 ? o.getPrice() : (avg > 0 ? avg : o.getLastPx());

                    qtyTotal += oq;
                    qtyDone += cum;
                    montoTotal += pxRef * oq;
                    montoDone += (avg > 0 ? avg : pxRef) * cum;

                    RoutingMessage.OrderStatus st = o.getOrdStatus();
                    if (st.equals(RoutingMessage.OrderStatus.FILLED)) {
                        nDone++;
                    } else if (st.equals(RoutingMessage.OrderStatus.PENDING_NEW)
                            || st.equals(RoutingMessage.OrderStatus.NEW)
                            || st.equals(RoutingMessage.OrderStatus.REPLACED)
                            || st.equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)
                            || st.equals(RoutingMessage.OrderStatus.PENDING_REPLACE)
                            || st.equals(RoutingMessage.OrderStatus.PENDING_CANCEL)) {
                        nPending++;
                    }
                }
            }

            double qtyLeft = Math.max(0, qtyTotal - qtyDone);
            double montoLeft = Math.max(0, montoTotal - montoDone);
            double pctDone = qtyTotal > 0 ? (qtyDone / qtyTotal) * 100d : 0d;
            double pctLeft = qtyTotal > 0 ? 100d - pctDone : 0d;

            DecimalFormat n0 = Repository.getFormatter0dec();
            DecimalFormat n2 = Repository.getFormatter2dec();

            setText(lblMontoTotal, "$" + n2.format(montoTotal));
            setText(lblMontoDone, "$" + n2.format(montoDone));
            setText(lblMontoLeft, "$" + n2.format(montoLeft));
            setText(lblPctDone, String.format("%.1f%%", pctDone));
            setText(lblPctLeft, String.format("%.1f%%", pctLeft));
            setText(lblQtyTotal, n0.format(qtyTotal));
            setText(lblQtyDone, n0.format(qtyDone));
            setText(lblQtyLeft, n0.format(qtyLeft));
            setText(lblNTotal, n0.format(nTotal));
            setText(lblNDone, n0.format(nDone));
            setText(lblNPending, n0.format(nPending));

            updateTitle(nDone, nTotal);

        } catch (Exception e) {
            log.error("Error recalculando estadísticas de la canasta", e);
        }
    }

    private void updateTitle(int done, int total) {
        if (tab != null) tab.setText(basketTabTitle(done, total));
    }

    static String basketTabTitle(int done, int total) {
        return "BKT (" + done + "/" + total + ")";
    }

    private void setText(Label l, String v) {
        if (l != null) l.setText(v);
    }

    private boolean isReplaceable(RoutingMessage.Order order) {
        if (order == null) {
            return false;
        }
        RoutingMessage.OrderStatus status = order.getOrdStatus();
        return status.equals(RoutingMessage.OrderStatus.NEW)
                || status.equals(RoutingMessage.OrderStatus.REPLACED)
                || status.equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED);
    }

    private void setReplaceControlsDisabled(boolean disabled) {
        quantityReplace.setDisable(disabled);
        priceReplace.setDisable(disabled);
        visibleReplace.setDisable(disabled);
        spreadReplace.setDisable(disabled);
        limitReplace.setDisable(disabled);
    }

    private void applyOrderColumnConfig() {
        if (Repository.getRoutingController() != null) {
            Repository.getRoutingController().applyOrderColumnConfigToAll();
            return;
        }
        executionsOrderController.applyOrderColumnConfig();
    }

    private void syncReplaceControls(RoutingMessage.Order order) {
        if (order == null) {
            quantityReplace.clear();
            priceReplace.clear();
            visibleReplace.clear();
            spreadReplace.clear();
            limitReplace.clear();
            return;
        }

        quantityReplace.setText(String.valueOf(order.getOrderQty()));
        priceReplace.setText(String.valueOf(order.getPrice()));
        visibleReplace.setText(order.getIcebergPercentage());
        spreadReplace.setText(String.valueOf(order.getSpread()));
        limitReplace.setText(String.valueOf(order.getLimit()));
    }

    private String valueOrDefault(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    @FXML
    public void execAll(ActionEvent actionEvent) {

        if (!alertRoute("Exec All")) return;

        executionsOrderController.getData().forEach(s -> {
            if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.PENDING_NEW)) {
                RoutingMessage.NewOrderRequest newOrderRequest = RoutingMessage.NewOrderRequest.newBuilder().setOrder(s).build();
                Repository.getClientService().sendMessage(newOrderRequest);
            }
        });

    }

    @FXML
    public void cancelOrder(ActionEvent actionEvent) {

        if (!alertRoute("Cancel Order")) return;

        RoutingMessage.Order order = executionsOrderController.getTableExecutionReports().getSelectionModel().getSelectedItem();
        if (order == null) return;
        RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(order.getId()).build();
        Repository.getClientService().sendMessage(orderCancelRequest);

    }

    @FXML
    public void replaceOrder(ActionEvent actionEvent) {
        RoutingMessage.Order order = executionsOrderController.getTableExecutionReports()
                .getSelectionModel()
                .getSelectedItem();

        if (!isReplaceable(order)) {
            Notifier.INSTANCE.notifyError("Canasta", "Selecciona una orden viva de la canasta para modificar.");
            return;
        }

        RoutingController routingController = Repository.getRoutingController();
        if (routingController == null) {
            Notifier.INSTANCE.notifyError("Canasta", "La vista de ruteo aún no está lista.");
            return;
        }

        Repository.getPrincipalController().setOrderSelected(order);
        routingController.getQuantity2().setText(valueOrDefault(quantityReplace.getText(), String.valueOf(order.getOrderQty())));
        routingController.getPriceOrder2().setText(valueOrDefault(priceReplace.getText(), String.valueOf(order.getPrice())));
        routingController.getVisibleid().setText(visibleReplace.getText() == null ? "" : visibleReplace.getText().trim());
        routingController.getSpread2().setText(valueOrDefault(spreadReplace.getText(), String.valueOf(order.getSpread())));
        routingController.getLimit2().setText(valueOrDefault(limitReplace.getText(), String.valueOf(order.getLimit())));

        log.info("[BASKET][REPLACE] basket={} orderId={} symbol={} qty={} price={} visible={} spread={} limit={}",
                basketId,
                order.getId(),
                order.getSymbol(),
                routingController.getQuantity2().getText(),
                routingController.getPriceOrder2().getText(),
                routingController.getVisibleid().getText(),
                routingController.getSpread2().getText(),
                routingController.getLimit2().getText());

        routingController.replaceOrderAction();
    }

    @FXML
    public void cancelAll(ActionEvent actionEvent) {

        if (!alertRoute("Cancel All")) return;

        executionsOrderController.getData().forEach(s -> {

            if (s.getOrdStatus().equals(RoutingMessage.OrderStatus.NEW) ||
                    s.getOrdStatus().equals(RoutingMessage.OrderStatus.REPLACED) ||
                    s.getOrdStatus().equals(RoutingMessage.OrderStatus.PARTIALLY_FILLED)) {

                RoutingMessage.OrderCancelRequest orderCancelRequest = RoutingMessage.OrderCancelRequest.newBuilder().setId(s.getId()).build();
                Repository.getClientService().sendMessage(orderCancelRequest);

            }

        });

    }


    public Boolean alertRoute(String text) {

        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirm Action");

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
}
