package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.fx.ChartViewer;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.CandlestickRenderer;
import org.jfree.data.xy.DefaultOHLCDataset;
import org.jfree.data.xy.OHLCDataItem;
import org.jfree.data.xy.OHLCDataset;

import java.awt.Color;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CandleController implements Initializable {

    @FXML
    private ChartViewer chartViewer;
    @FXML
    private ComboBox<String> cmbSymbol;
    @FXML
    private ComboBox<String> cmbTimeframe;
    @FXML
    private Label lblDataState;
    @FXML
    private Label lblLastTradeAt;
    @FXML
    private Label lblSma20;
    @FXML
    private Label lblEma20;
    @FXML
    private Label lblRsi14;
    @FXML
    private Label lblMacd;
    @FXML
    private Label lblTradeRange;

    private final ListChangeListener<MarketDataMessage.TradeGeneral> tradeListener = change -> {
        refreshSymbolList();
        renderChart();
    };

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        Repository.getCandleTradeGenerales().addListener(tradeListener);
        if (cmbSymbol != null) {
            cmbSymbol.valueProperty().addListener((obs, oldV, newV) -> renderChart());
        }
        if (cmbTimeframe != null) {
            cmbTimeframe.setItems(FXCollections.observableArrayList("1m", "5m", "15m", "1h", "1d"));
            cmbTimeframe.getSelectionModel().select("1m");
            cmbTimeframe.valueProperty().addListener((obs, oldV, newV) -> renderChart());
        }
        refreshSymbolList();
        renderChart();
    }

    private void renderChart() {
        String symbol = cmbSymbol != null ? cmbSymbol.getValue() : null;
        List<MarketDataMessage.TradeGeneral> source = Repository.getCandleTradeGenerales();
        List<MarketDataMessage.TradeGeneral> filtered = source;
        if (symbol != null && !symbol.isBlank() && !"TODOS".equals(symbol)) {
            filtered = source.stream()
                    .filter(t -> symbol.equalsIgnoreCase(normalizeSymbol(t.getSymbol())))
                    .collect(Collectors.toList());
        }

        int timeframeMinutes = getTimeframeMinutes();
        DatasetBuildResult built = buildDatasetFromTrades(filtered, timeframeMinutes);
        OHLCDataset dataset = built.dataset;

        DateAxis timeAxis = new DateAxis("Tiempo");
        timeAxis.setLowerMargin(0.0);
        timeAxis.setUpperMargin(0.0);
        timeAxis.setLabelPaint(Color.WHITE);
        timeAxis.setTickLabelPaint(Color.WHITE);
        if (built.firstBucket != null && built.lastBucket != null) {
            Instant upper = built.lastBucket.plus(Duration.ofMinutes(Math.max(1, timeframeMinutes)));
            if (upper.isAfter(built.firstBucket)) {
                timeAxis.setAutoRange(false);
                timeAxis.setRange(Date.from(built.firstBucket), Date.from(upper));
            }
        }

        NumberAxis priceAxis = new NumberAxis("Precio");
        priceAxis.setAutoRangeIncludesZero(false);
        priceAxis.setLabelPaint(Color.WHITE);
        priceAxis.setTickLabelPaint(Color.WHITE);

        CandlestickRenderer candleRenderer = new CandlestickRenderer();
        candleRenderer.setAutoWidthMethod(CandlestickRenderer.WIDTHMETHOD_SMALLEST);
        candleRenderer.setDrawVolume(false);
        candleRenderer.setUseOutlinePaint(false);

        XYPlot plot = new XYPlot(dataset, timeAxis, priceAxis, candleRenderer);

        Color bg = new Color(0x1c, 0x1c, 0x1c);
        plot.setBackgroundPaint(bg);
        plot.setDomainGridlinesVisible(false);
        plot.setRangeGridlinesVisible(false);

        String tf = cmbTimeframe != null && cmbTimeframe.getValue() != null ? cmbTimeframe.getValue() : "1m";
        String title = (symbol == null || symbol.isBlank() || "TODOS".equals(symbol))
                ? "Velas (" + tf + ")"
                : "Velas (" + tf + ") - " + symbol;
        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, false);
        chart.setBackgroundPaint(bg);
        chartViewer.setChart(chart);
        chartViewer.setStyle("-fx-background-color: #1c1c1c;");

        updateDataState(filtered);
        updateIndicators(built.closes);
        updateTradeRange(built.firstTradeAt, built.lastTradeAt);
    }

    private DatasetBuildResult buildDatasetFromTrades(List<MarketDataMessage.TradeGeneral> trades, int timeframeMinutes) {
        if (trades == null || trades.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        List<MarketDataMessage.TradeGeneral> ordered = trades.stream()
                .filter(t -> t != null && t.hasT())
                .filter(t -> t.getPrice() > 0)
                .sorted(Comparator.comparingLong(t -> t.getT().getSeconds()))
                .collect(Collectors.toList());

        if (ordered.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        Map<Instant, Ohlc> rawBuckets = new TreeMap<>();
        Instant firstTradeAt = null;
        Instant lastTradeAt = null;
        for (MarketDataMessage.TradeGeneral trade : ordered) {
            Instant tradeTime = Instant.ofEpochSecond(trade.getT().getSeconds(), trade.getT().getNanos());
            if (firstTradeAt == null || tradeTime.isBefore(firstTradeAt)) {
                firstTradeAt = tradeTime;
            }
            if (lastTradeAt == null || tradeTime.isAfter(lastTradeAt)) {
                lastTradeAt = tradeTime;
            }
            Instant bucket = bucketize(tradeTime, Math.max(1, timeframeMinutes));
            rawBuckets.computeIfAbsent(bucket, k -> new Ohlc()).update(trade.getPrice(), trade.getQty());
        }

        if (rawBuckets.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        Instant first = rawBuckets.keySet().iterator().next();
        Instant last = rawBuckets.keySet().stream().max(Comparator.naturalOrder()).orElse(first);
        Duration step = Duration.ofMinutes(Math.max(1, timeframeMinutes));

        List<OHLCDataItem> items = new ArrayList<>();
        List<Double> closes = new ArrayList<>();
        Instant cursor = first;
        Double prevClose = null;
        while (!cursor.isAfter(last)) {
            Ohlc o = rawBuckets.get(cursor);
            if (o == null && prevClose != null) {
                o = Ohlc.synthetic(prevClose);
            }
            if (o != null && o.initialized) {
                items.add(new OHLCDataItem(Date.from(cursor), o.open, o.high, o.low, o.close, o.volume));
                closes.add(o.close);
                prevClose = o.close;
            }
            cursor = cursor.plus(step);
        }

        if (items.isEmpty()) {
            return DatasetBuildResult.empty();
        }

        OHLCDataset dataset = new DefaultOHLCDataset("OHLC", items.toArray(new OHLCDataItem[0]));
        return new DatasetBuildResult(dataset, closes, first, last, firstTradeAt, lastTradeAt);
    }

    private void refreshSymbolList() {
        if (cmbSymbol == null) {
            return;
        }
        String current = cmbSymbol.getValue();
        Set<String> symbols = Repository.getCandleTradeGenerales().stream()
                .map(MarketDataMessage.TradeGeneral::getSymbol)
                .map(this::normalizeSymbol)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toCollection(java.util.TreeSet::new));

        var items = FXCollections.<String>observableArrayList();
        items.add("TODOS");
        items.addAll(symbols);
        cmbSymbol.setItems(items);

        String preferred = Repository.getSelectedCandleSymbol();
        if (preferred != null && items.contains(preferred)) {
            cmbSymbol.getSelectionModel().select(preferred);
            Repository.setSelectedCandleSymbol(null);
            return;
        }
        if (current != null && items.contains(current)) {
            cmbSymbol.getSelectionModel().select(current);
        } else {
            cmbSymbol.getSelectionModel().selectFirst();
        }
    }

    private void updateDataState(List<MarketDataMessage.TradeGeneral> filtered) {
        if (lblDataState == null || lblLastTradeAt == null) {
            return;
        }

        if (filtered == null || filtered.isEmpty()) {
            lblDataState.setText("SIN DATOS REALES");
            lblDataState.setStyle("-fx-text-fill: #ff5f5f; -fx-font-weight: bold;");
            lblLastTradeAt.setText("-");
            return;
        }

        MarketDataMessage.TradeGeneral last = filtered.stream()
                .filter(t -> t.hasT())
                .max(Comparator.comparingLong(t -> t.getT().getSeconds()))
                .orElse(null);

        if (last == null || !last.hasT()) {
            lblDataState.setText("SIN TIMESTAMP");
            lblDataState.setStyle("-fx-text-fill: #ffb347; -fx-font-weight: bold;");
            lblLastTradeAt.setText("-");
            return;
        }

        Instant ts = Instant.ofEpochSecond(last.getT().getSeconds(), last.getT().getNanos());
        long ageMinutes = Duration.between(ts, Instant.now()).toMinutes();
        lblLastTradeAt.setText(ts.toString());
        if (ageMinutes <= 10) {
            lblDataState.setText("REAL-TIME");
            lblDataState.setStyle("-fx-text-fill: #39c16c; -fx-font-weight: bold;");
        } else {
            lblDataState.setText("ATRASADO");
            lblDataState.setStyle("-fx-text-fill: #ffb347; -fx-font-weight: bold;");
        }
    }

    private void updateIndicators(List<Double> closes) {
        if (lblSma20 == null || lblEma20 == null || lblRsi14 == null || lblMacd == null) {
            return;
        }
        if (closes == null || closes.isEmpty()) {
            lblSma20.setText("-");
            lblEma20.setText("-");
            lblRsi14.setText("-");
            lblMacd.setText("-");
            return;
        }

        Double sma20 = sma(closes, 20);
        Double ema20 = ema(closes, 20);
        Double rsi14 = rsi(closes, 14);
        MacdValue macd = macd(closes);

        lblSma20.setText(formatVal(sma20));
        lblEma20.setText(formatVal(ema20));
        lblRsi14.setText(formatVal(rsi14));
        lblMacd.setText(macd == null ? "-" : String.format(Locale.US, "M:%s S:%s H:%s",
                formatVal(macd.line), formatVal(macd.signal), formatVal(macd.histogram)));
    }

    private void updateTradeRange(Instant firstTradeAt, Instant lastTradeAt) {
        if (lblTradeRange == null) {
            return;
        }
        if (firstTradeAt == null || lastTradeAt == null) {
            lblTradeRange.setText("-");
            return;
        }
        lblTradeRange.setText(firstTradeAt + " -> " + lastTradeAt + " (UTC)");
    }

    private String formatVal(Double value) {
        if (value == null || value.isNaN() || value.isInfinite()) {
            return "-";
        }
        return String.format(Locale.US, "%.4f", value);
    }

    private Double sma(List<Double> values, int period) {
        if (values == null || values.size() < period || period <= 0) {
            return null;
        }
        double sum = 0d;
        for (int i = values.size() - period; i < values.size(); i++) {
            sum += values.get(i);
        }
        return sum / period;
    }

    private Double ema(List<Double> values, int period) {
        if (values == null || values.size() < period || period <= 0) {
            return null;
        }
        Double ema = sma(values.subList(0, period), period);
        double multiplier = 2.0d / (period + 1.0d);
        for (int i = period; i < values.size(); i++) {
            ema = ((values.get(i) - ema) * multiplier) + ema;
        }
        return ema;
    }

    private Double rsi(List<Double> values, int period) {
        if (values == null || values.size() <= period || period <= 0) {
            return null;
        }

        double gain = 0d;
        double loss = 0d;
        for (int i = 1; i <= period; i++) {
            double delta = values.get(i) - values.get(i - 1);
            if (delta >= 0) {
                gain += delta;
            } else {
                loss += -delta;
            }
        }

        double avgGain = gain / period;
        double avgLoss = loss / period;

        for (int i = period + 1; i < values.size(); i++) {
            double delta = values.get(i) - values.get(i - 1);
            double currentGain = Math.max(delta, 0);
            double currentLoss = Math.max(-delta, 0);
            avgGain = ((avgGain * (period - 1)) + currentGain) / period;
            avgLoss = ((avgLoss * (period - 1)) + currentLoss) / period;
        }

        if (avgLoss == 0d) {
            return 100d;
        }
        double rs = avgGain / avgLoss;
        return 100d - (100d / (1d + rs));
    }

    private MacdValue macd(List<Double> values) {
        List<Double> ema12 = emaSeries(values, 12);
        List<Double> ema26 = emaSeries(values, 26);
        if (ema12.isEmpty() || ema26.isEmpty()) {
            return null;
        }

        List<Double> macdSeries = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            Double a = ema12.get(i);
            Double b = ema26.get(i);
            if (a != null && b != null) {
                macdSeries.add(a - b);
            }
        }

        if (macdSeries.size() < 9) {
            return null;
        }

        Double signal = ema(macdSeries, 9);
        if (signal == null) {
            return null;
        }

        Double line = macdSeries.get(macdSeries.size() - 1);
        Double histogram = line - signal;
        return new MacdValue(line, signal, histogram);
    }

    private List<Double> emaSeries(List<Double> values, int period) {
        List<Double> series = new ArrayList<>();
        if (values == null || values.isEmpty() || period <= 0) {
            return series;
        }

        for (int i = 0; i < values.size(); i++) {
            series.add(null);
        }

        if (values.size() < period) {
            return series;
        }

        double first = 0d;
        for (int i = 0; i < period; i++) {
            first += values.get(i);
        }
        double ema = first / period;
        series.set(period - 1, ema);

        double multiplier = 2.0d / (period + 1.0d);
        for (int i = period; i < values.size(); i++) {
            ema = ((values.get(i) - ema) * multiplier) + ema;
            series.set(i, ema);
        }
        return series;
    }

    private int getTimeframeMinutes() {
        String tf = cmbTimeframe != null ? cmbTimeframe.getValue() : "1m";
        if (tf == null) return 1;
        return switch (tf) {
            case "5m" -> 5;
            case "15m" -> 15;
            case "1h" -> 60;
            case "1d" -> 1440;
            default -> 1;
        };
    }

    private Instant bucketize(Instant instant, int minutes) {
        ZonedDateTime z = instant.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
        if (minutes <= 1) {
            return z.toInstant();
        }
        if (minutes >= 1440) {
            return z.truncatedTo(ChronoUnit.DAYS).toInstant();
        }
        int minute = z.getMinute();
        int bucketMinute = (minute / minutes) * minutes;
        ZonedDateTime bucket = z.withMinute(bucketMinute).withSecond(0).withNano(0);
        return bucket.toInstant();
    }

    private String normalizeSymbol(String symbol) {
        if (symbol == null) {
            return "";
        }
        return symbol.trim().toUpperCase();
    }

    private static class Ohlc {
        double open;
        double high;
        double low;
        double close;
        double volume;
        boolean initialized;

        void update(double price, double qty) {
            if (!initialized) {
                open = high = low = close = price;
                volume = qty;
                initialized = true;
                return;
            }
            high = Math.max(high, price);
            low = Math.min(low, price);
            close = price;
            volume += qty;
        }

        static Ohlc synthetic(double closeValue) {
            Ohlc o = new Ohlc();
            o.open = closeValue;
            o.high = closeValue;
            o.low = closeValue;
            o.close = closeValue;
            o.volume = 0d;
            o.initialized = true;
            return o;
        }
    }

    private static class DatasetBuildResult {
        final OHLCDataset dataset;
        final List<Double> closes;
        final Instant firstBucket;
        final Instant lastBucket;
        final Instant firstTradeAt;
        final Instant lastTradeAt;

        DatasetBuildResult(
                OHLCDataset dataset,
                List<Double> closes,
                Instant firstBucket,
                Instant lastBucket,
                Instant firstTradeAt,
                Instant lastTradeAt
        ) {
            this.dataset = dataset;
            this.closes = closes;
            this.firstBucket = firstBucket;
            this.lastBucket = lastBucket;
            this.firstTradeAt = firstTradeAt;
            this.lastTradeAt = lastTradeAt;
        }

        static DatasetBuildResult empty() {
            return new DatasetBuildResult(
                    new DefaultOHLCDataset("OHLC", new OHLCDataItem[0]),
                    new ArrayList<>(),
                    null,
                    null,
                    null,
                    null
            );
        }
    }

    private static class MacdValue {
        final Double line;
        final Double signal;
        final Double histogram;

        MacdValue(Double line, Double signal, Double histogram) {
            this.line = line;
            this.signal = signal;
            this.histogram = histogram;
        }
    }
}
