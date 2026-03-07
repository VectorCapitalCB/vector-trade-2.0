package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import javafx.collections.ListChangeListener;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.CandlestickRenderer;
import org.jfree.chart.fx.ChartViewer;
import org.jfree.data.xy.DefaultOHLCDataset;
import org.jfree.data.xy.OHLCDataItem;
import org.jfree.data.xy.OHLCDataset;

import java.awt.Color;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.TreeMap;

public class CandleController implements Initializable {

    @FXML
    private ChartViewer chartViewer;

    private final ListChangeListener<MarketDataMessage.TradeGeneral> tradeListener = change -> renderChart();

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        Repository.getTradeGenerales().addListener(tradeListener);
        renderChart();
    }

    private void renderChart() {
        OHLCDataset dataset = buildDatasetFromTrades(Repository.getTradeGenerales());

        DateAxis timeAxis = new DateAxis("Tiempo");
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

        JFreeChart chart = new JFreeChart("Velas (1 minuto)", JFreeChart.DEFAULT_TITLE_FONT, plot, false);
        chart.setBackgroundPaint(bg);
        chartViewer.setChart(chart);
        chartViewer.setStyle("-fx-background-color: #1c1c1c;");
    }

    private OHLCDataset buildDatasetFromTrades(List<MarketDataMessage.TradeGeneral> trades) {
        if (trades == null || trades.isEmpty()) {
            return new DefaultOHLCDataset("OHLC", fallbackItems());
        }

        Map<Instant, Ohlc> buckets = new TreeMap<>();
        for (MarketDataMessage.TradeGeneral trade : trades) {
            if (trade == null || !trade.hasT()) {
                continue;
            }
            Instant t = Instant.ofEpochSecond(trade.getT().getSeconds(), trade.getT().getNanos())
                    .atZone(ZoneOffset.UTC)
                    .truncatedTo(ChronoUnit.MINUTES)
                    .toInstant();
            buckets.computeIfAbsent(t, k -> new Ohlc()).update(trade.getPrice(), trade.getQty());
        }

        List<OHLCDataItem> items = new ArrayList<>();
        buckets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .forEach(e -> {
                    Instant t = e.getKey();
                    Ohlc o = e.getValue();
                    items.add(new OHLCDataItem(Date.from(t), o.open, o.high, o.low, o.close, o.volume));
                });

        if (items.isEmpty()) {
            return new DefaultOHLCDataset("OHLC", fallbackItems());
        }

        return new DefaultOHLCDataset("OHLC", items.toArray(new OHLCDataItem[0]));
    }

    private OHLCDataItem[] fallbackItems() {
        OHLCDataItem[] items = new OHLCDataItem[30];
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
        double px = 100;
        for (int i = 0; i < items.length; i++) {
            ZonedDateTime ts = now.minusMinutes(items.length - i);
            items[i] = new OHLCDataItem(Date.from(ts.toInstant()), px, px + 0.3, px - 0.3, px + 0.1, 1000d);
            px += 0.05;
        }
        return items;
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
    }
}
