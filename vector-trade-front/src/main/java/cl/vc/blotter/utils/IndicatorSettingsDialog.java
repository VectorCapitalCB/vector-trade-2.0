package cl.vc.blotter.utils;

import cl.vc.blotter.Repository;
import javafx.geometry.Insets;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.layout.GridPane;
import javafx.stage.Window;

import java.util.Objects;
import java.util.Optional;

/**
 * Dialogo para editar los parametros de los indicadores.
 *
 * Edita una COPIA y solo la devuelve al aceptar: cancelar no debe dejar el grafico a medio cambiar.
 */
public final class IndicatorSettingsDialog {

    private IndicatorSettingsDialog() {
    }

    /**
     * @param actual  valores vigentes (no se modifican)
     * @param owner   ventana del grafico, para que el dialogo quede modal sobre ella
     * @return los valores nuevos ya normalizados, o vacio si el operador cancelo
     */
    public static Optional<IndicatorSettings> mostrar(IndicatorSettings actual, Window owner) {
        IndicatorSettings borrador = actual.copy();

        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Parámetros de indicadores");
        dialog.setHeaderText("Los cambios se aplican al gráfico y quedan guardados.");
        if (owner != null) dialog.initOwner(owner);

        ButtonType aplicar = new ButtonType("Aplicar", ButtonBar.ButtonData.OK_DONE);
        ButtonType restaurar = new ButtonType("Restaurar", ButtonBar.ButtonData.OTHER);
        dialog.getDialogPane().getButtonTypes().addAll(aplicar, restaurar, ButtonType.CANCEL);

        GridPane g = new GridPane();
        g.setHgap(12);
        g.setVgap(8);
        g.setPadding(new Insets(16));

        int[] fila = {0};

        titulo(g, fila, "Medias");
        Spinner<Integer> sma = periodo(g, fila, "SMA", borrador.smaPeriod);
        Spinner<Integer> ema = periodo(g, fila, "EMA", borrador.emaPeriod);

        titulo(g, fila, "Bollinger");
        Spinner<Integer> bbP = periodo(g, fila, "Período", borrador.bollingerPeriod);
        Spinner<Double> bbK = decimal(g, fila, "Desviaciones (k)", borrador.bollingerK);

        titulo(g, fila, "Osciladores");
        Spinner<Integer> rsi = periodo(g, fila, "RSI", borrador.rsiPeriod);
        Spinner<Integer> atr = periodo(g, fila, "ATR", borrador.atrPeriod);

        titulo(g, fila, "MACD");
        Spinner<Integer> mFast = periodo(g, fila, "Rápida", borrador.macdFast);
        Spinner<Integer> mSlow = periodo(g, fila, "Lenta", borrador.macdSlow);
        Spinner<Integer> mSig = periodo(g, fila, "Señal", borrador.macdSignal);
        aviso(g, fila, "La rápida debe ser menor que la lenta; si no, se ajusta sola.");

        titulo(g, fila, "Ichimoku");
        Spinner<Integer> iTen = periodo(g, fila, "Tenkan", borrador.ichimokuTenkan);
        Spinner<Integer> iKij = periodo(g, fila, "Kijun", borrador.ichimokuKijun);
        Spinner<Integer> iSen = periodo(g, fila, "Senkou B", borrador.ichimokuSenkouB);
        Spinner<Integer> iShift = periodo(g, fila, "Desplazamiento", borrador.ichimokuShift);

        dialog.getDialogPane().setContent(g);
        aplicarEstilo(dialog);

        // El boton Restaurar no debe cerrar el dialogo: repone los valores de fabrica en los spinners.
        dialog.getDialogPane().lookupButton(restaurar).addEventFilter(
                javafx.event.ActionEvent.ACTION, e -> {
                    IndicatorSettings def = new IndicatorSettings();
                    sma.getValueFactory().setValue(def.smaPeriod);
                    ema.getValueFactory().setValue(def.emaPeriod);
                    bbP.getValueFactory().setValue(def.bollingerPeriod);
                    bbK.getValueFactory().setValue(def.bollingerK);
                    rsi.getValueFactory().setValue(def.rsiPeriod);
                    atr.getValueFactory().setValue(def.atrPeriod);
                    mFast.getValueFactory().setValue(def.macdFast);
                    mSlow.getValueFactory().setValue(def.macdSlow);
                    mSig.getValueFactory().setValue(def.macdSignal);
                    iTen.getValueFactory().setValue(def.ichimokuTenkan);
                    iKij.getValueFactory().setValue(def.ichimokuKijun);
                    iSen.getValueFactory().setValue(def.ichimokuSenkouB);
                    iShift.getValueFactory().setValue(def.ichimokuShift);
                    e.consume();
                });

        if (dialog.showAndWait().orElse(ButtonType.CANCEL) != aplicar) {
            return Optional.empty();
        }

        borrador.smaPeriod = sma.getValue();
        borrador.emaPeriod = ema.getValue();
        borrador.bollingerPeriod = bbP.getValue();
        borrador.bollingerK = bbK.getValue();
        borrador.rsiPeriod = rsi.getValue();
        borrador.atrPeriod = atr.getValue();
        borrador.macdFast = mFast.getValue();
        borrador.macdSlow = mSlow.getValue();
        borrador.macdSignal = mSig.getValue();
        borrador.ichimokuTenkan = iTen.getValue();
        borrador.ichimokuKijun = iKij.getValue();
        borrador.ichimokuSenkouB = iSen.getValue();
        borrador.ichimokuShift = iShift.getValue();
        return Optional.of(borrador.normalizar());
    }

    private static void titulo(GridPane g, int[] fila, String texto) {
        if (fila[0] > 0) {
            g.add(new Separator(), 0, fila[0]++, 2, 1);
        }
        Label l = new Label(texto.toUpperCase());
        l.setStyle("-fx-font-weight: bold; -fx-opacity: 0.75;");
        g.add(l, 0, fila[0]++, 2, 1);
    }

    private static void aviso(GridPane g, int[] fila, String texto) {
        Label l = new Label(texto);
        l.setStyle("-fx-font-size: 10px; -fx-opacity: 0.6;");
        l.setWrapText(true);
        g.add(l, 0, fila[0]++, 2, 1);
    }

    private static Spinner<Integer> periodo(GridPane g, int[] fila, String etiqueta, int valor) {
        Spinner<Integer> sp = new Spinner<>(new SpinnerValueFactory.IntegerSpinnerValueFactory(
                IndicatorSettings.MIN_PERIODO, IndicatorSettings.MAX_PERIODO, valor));
        sp.setEditable(true);
        sp.setPrefWidth(110);
        blindarEdicion(sp, valor);
        g.add(new Label(etiqueta), 0, fila[0]);
        g.add(sp, 1, fila[0]++);
        return sp;
    }

    private static Spinner<Double> decimal(GridPane g, int[] fila, String etiqueta, double valor) {
        Spinner<Double> sp = new Spinner<>(new SpinnerValueFactory.DoubleSpinnerValueFactory(
                IndicatorSettings.MIN_K, IndicatorSettings.MAX_K, valor, 0.1d));
        sp.setEditable(true);
        sp.setPrefWidth(110);
        blindarEdicion(sp, valor);
        g.add(new Label(etiqueta), 0, fila[0]);
        g.add(sp, 1, fila[0]++);
        return sp;
    }

    /**
     * Con setEditable(true) el Spinner deja escribir cualquier cosa y su valor queda desincronizado
     * del texto (o tira excepcion al commitear). Esto lo repone al ultimo valor valido al perder
     * el foco, que es lo que evita un NumberFormatException al aceptar.
     */
    private static <T> void blindarEdicion(Spinner<T> sp, T fallback) {
        sp.focusedProperty().addListener((obs, tenia, tiene) -> {
            if (tiene) return;
            try {
                sp.getValueFactory().setValue(
                        sp.getValueFactory().getConverter().fromString(sp.getEditor().getText()));
            } catch (Exception ex) {
                sp.getValueFactory().setValue(sp.getValue() != null ? sp.getValue() : fallback);
            }
            sp.getEditor().setText(sp.getValueFactory().getConverter().toString(sp.getValue()));
        });
    }

    private static void aplicarEstilo(Dialog<?> dialog) {
        try {
            boolean dia = Repository.getPrincipalController() != null
                    && Repository.getPrincipalController().isDayMode();
            String css = dia ? "/blotter/css/daymode.css" : Repository.getSTYLE();
            dialog.getDialogPane().getStylesheets().add(Objects.requireNonNull(
                    IndicatorSettingsDialog.class.getResource(css)).toExternalForm());
        } catch (Exception ignore) {
            // Sin CSS el dialogo igual es usable; no vale abortar la edicion por el tema.
        }
    }
}
