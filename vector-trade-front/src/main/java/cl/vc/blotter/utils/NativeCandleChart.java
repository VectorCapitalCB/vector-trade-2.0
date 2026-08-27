package cl.vc.blotter.utils;

import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.Region;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

/** JavaFX-only candle viewer used by the Gluon native executable, which has no AWT runtime. */
public final class NativeCandleChart extends Region {
    private static final ZoneId MARKET_ZONE = ZoneId.of("America/Santiago");
    private static final Color BACKGROUND = Color.web("#121820");
    private static final Color GRID = Color.web("#263340");
    private static final Color TEXT = Color.web("#d7dfe8");
    private static final Color MUTED = Color.web("#8795a5");
    private static final Color UP = Color.web("#22c55e");
    private static final Color DOWN = Color.web("#ef4444");
    private static final Color SMA = Color.web("#ffd166");
    private static final Color EMA = Color.web("#6bd4ff");
    private static final double LEFT = 72d;
    private static final double RIGHT = 18d;
    private static final double TOP = 42d;
    private static final double BOTTOM = 34d;

    private final Canvas canvas = new Canvas();
    private List<CandlePoint> points = List.of();
    private double[] sma;
    private double[] ema;
    private String title = "Velas";
    private int timeframeMinutes = 1440;
    private double viewStart;
    private double viewCount;
    private double dragStartX;
    private double dragStartView;

    public NativeCandleChart() {
        getChildren().add(canvas);
        setMinSize(240, 180);
        setStyle("-fx-background-color: #121820;");
        widthProperty().addListener((obs, oldV, newV) -> redraw());
        heightProperty().addListener((obs, oldV, newV) -> redraw());
        canvas.setOnScroll(event -> {
            if (points.size() < 2) return;
            double plotWidth = Math.max(1d, canvas.getWidth() - LEFT - RIGHT);
            double focus = clamp((event.getX() - LEFT) / plotWidth, 0d, 1d);
            double oldCount = viewCount;
            double factor = event.getDeltaY() > 0d ? 0.82d : 1.22d;
            viewCount = clamp(oldCount * factor, Math.min(points.size(), 8d), points.size());
            double focusIndex = viewStart + focus * oldCount;
            viewStart = clamp(focusIndex - focus * viewCount, 0d, points.size() - viewCount);
            redraw();
            event.consume();
        });
        canvas.setOnMousePressed(event -> {
            if (event.getButton() == MouseButton.PRIMARY) {
                dragStartX = event.getX();
                dragStartView = viewStart;
            }
        });
        canvas.setOnMouseDragged(event -> {
            if (!event.isPrimaryButtonDown() || points.isEmpty()) return;
            double plotWidth = Math.max(1d, canvas.getWidth() - LEFT - RIGHT);
            double shift = (dragStartX - event.getX()) / plotWidth * viewCount;
            viewStart = clamp(dragStartView + shift, 0d, points.size() - viewCount);
            redraw();
        });
        canvas.setOnMouseClicked(event -> {
            if (event.getButton() == MouseButton.PRIMARY && event.getClickCount() == 2) {
                resetViewport();
                redraw();
            }
        });
    }

    public void setData(String title, List<CandlePoint> points, int timeframeMinutes,
                        double[] sma, double[] ema) {
        boolean changed = !sameTimeline(this.points, points) || this.timeframeMinutes != timeframeMinutes;
        this.title = title == null ? "Velas" : title;
        this.points = points == null ? List.of() : List.copyOf(points);
        this.timeframeMinutes = timeframeMinutes;
        this.sma = sma == null ? null : sma.clone();
        this.ema = ema == null ? null : ema.clone();
        if (changed || viewCount <= 0d) resetViewport();
        redraw();
    }

    @Override
    protected void layoutChildren() {
        canvas.setWidth(getWidth());
        canvas.setHeight(getHeight());
        redraw();
    }

    private void resetViewport() {
        if (points.isEmpty()) {
            viewStart = viewCount = 0d;
            return;
        }
        int count = timeframeMinutes >= 1440 ? Math.min(90, points.size()) : lastSessionCount();
        viewCount = Math.max(1d, count);
        viewStart = Math.max(0d, points.size() - viewCount);
    }

    private int lastSessionCount() {
        var lastDay = points.get(points.size() - 1).time().atZone(MARKET_ZONE).toLocalDate();
        int first = points.size() - 1;
        while (first > 0 && points.get(first - 1).time().atZone(MARKET_ZONE).toLocalDate().equals(lastDay)) first--;
        return Math.max(1, points.size() - first);
    }

    private void redraw() {
        double width = canvas.getWidth();
        double height = canvas.getHeight();
        if (width <= 1d || height <= 1d) return;
        GraphicsContext g = canvas.getGraphicsContext2D();
        g.setFill(BACKGROUND);
        g.fillRect(0, 0, width, height);
        g.setFont(Font.font("System", 12));
        g.setFill(TEXT);
        g.fillText(title, Math.max(LEFT, (width - textWidth(title)) / 2d), 24d);
        if (points.isEmpty() || viewCount <= 0d) {
            g.setFill(MUTED);
            g.fillText("Sin datos para mostrar", Math.max(LEFT, width / 2d - 62d), height / 2d);
            return;
        }

        int from = Math.max(0, (int) Math.floor(viewStart));
        int to = Math.min(points.size(), (int) Math.ceil(viewStart + viewCount));
        if (to <= from) return;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (int i = from; i < to; i++) {
            min = Math.min(min, points.get(i).low());
            max = Math.max(max, points.get(i).high());
        }
        double span = Math.max(max - min, Math.max(Math.abs(max) * 0.002d, 0.0001d));
        min -= span * 0.08d;
        max += span * 0.08d;

        double plotWidth = Math.max(1d, width - LEFT - RIGHT);
        double plotHeight = Math.max(1d, height - TOP - BOTTOM);
        drawGrid(g, width, height, min, max);
        double slot = plotWidth / viewCount;
        double bodyWidth = clamp(slot * 0.68d, 1.2d, 18d);
        for (int i = from; i < to; i++) {
            CandlePoint p = points.get(i);
            double x = LEFT + ((i + 0.5d - viewStart) / viewCount) * plotWidth;
            Color color = p.close() >= p.open() ? UP : DOWN;
            double highY = y(p.high(), min, max, plotHeight);
            double lowY = y(p.low(), min, max, plotHeight);
            double openY = y(p.open(), min, max, plotHeight);
            double closeY = y(p.close(), min, max, plotHeight);
            g.setStroke(color);
            g.setLineWidth(1d);
            g.strokeLine(x, highY, x, lowY);
            g.setFill(color);
            g.fillRect(x - bodyWidth / 2d, Math.min(openY, closeY), bodyWidth,
                    Math.max(1d, Math.abs(closeY - openY)));
        }
        drawLine(g, sma, SMA, from, to, min, max, plotWidth, plotHeight);
        drawLine(g, ema, EMA, from, to, min, max, plotWidth, plotHeight);
        drawTimeLabels(g, from, to, width, height, plotWidth);
    }

    private void drawGrid(GraphicsContext g, double width, double height, double min, double max) {
        double plotHeight = Math.max(1d, height - TOP - BOTTOM);
        g.setStroke(GRID);
        g.setLineWidth(0.7d);
        g.setFill(MUTED);
        for (int line = 0; line <= 5; line++) {
            double y = TOP + plotHeight * line / 5d;
            g.strokeLine(LEFT, y, width - RIGHT, y);
            g.fillText(formatPrice(max - (max - min) * line / 5d), 6d, y + 4d);
        }
        for (int line = 0; line <= 6; line++) {
            double x = LEFT + (width - LEFT - RIGHT) * line / 6d;
            g.strokeLine(x, TOP, x, height - BOTTOM);
        }
    }

    private void drawLine(GraphicsContext g, double[] values, Color color, int from, int to,
                          double min, double max, double plotWidth, double plotHeight) {
        if (values == null || values.length < to) return;
        g.setStroke(color);
        g.setLineWidth(1.5d);
        boolean drawing = false;
        g.beginPath();
        for (int i = from; i < to; i++) {
            double value = values[i];
            if (!Double.isFinite(value)) {
                drawing = false;
                continue;
            }
            double x = LEFT + ((i + 0.5d - viewStart) / viewCount) * plotWidth;
            double y = y(value, min, max, plotHeight);
            if (drawing) g.lineTo(x, y); else g.moveTo(x, y);
            drawing = true;
        }
        g.stroke();
    }

    private void drawTimeLabels(GraphicsContext g, int from, int to, double width, double height, double plotWidth) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                timeframeMinutes >= 1440 ? "dd/MM/yy" : "dd/MM HH:mm", Locale.forLanguageTag("es-CL"));
        g.setFill(MUTED);
        int labels = Math.min(6, Math.max(1, to - from));
        for (int n = 0; n < labels; n++) {
            int index = labels == 1 ? from : from + (to - from - 1) * n / (labels - 1);
            double x = LEFT + ((index + 0.5d - viewStart) / viewCount) * plotWidth;
            String label = formatter.format(points.get(index).time().atZone(MARKET_ZONE));
            g.fillText(label, clamp(x - 34d, LEFT, width - RIGHT - 68d), height - 10d);
        }
    }

    private double y(double value, double min, double max, double plotHeight) {
        return TOP + (max - value) / (max - min) * plotHeight;
    }

    private static boolean sameTimeline(List<CandlePoint> current, List<CandlePoint> next) {
        if (current == null || next == null || current.size() != next.size()) return false;
        if (current.isEmpty()) return true;
        return current.get(0).time().equals(next.get(0).time())
                && current.get(current.size() - 1).time().equals(next.get(next.size() - 1).time());
    }

    private static String formatPrice(double value) {
        if (Math.abs(value) >= 1000d) return String.format(Locale.US, "%,.0f", value);
        if (Math.abs(value) >= 10d) return String.format(Locale.US, "%,.2f", value);
        return String.format(Locale.US, "%,.4f", value);
    }

    private static double textWidth(String value) {
        return value == null ? 0d : value.length() * 7d;
    }

    private static double clamp(double value, double minimum, double maximum) {
        return Math.max(minimum, Math.min(maximum, value));
    }

    public record CandlePoint(Instant time, double open, double high, double low, double close) { }
}
