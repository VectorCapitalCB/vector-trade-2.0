package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.collections.ListChangeListener;
import javafx.fxml.FXML;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ListCell;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.awt.Desktop;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import javafx.collections.FXCollections;
import java.util.Comparator;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NewsController {
    private static final Pattern URL_PATTERN = Pattern.compile("(?i)\\b((?:https?://|www\\.)[^\\s<>\"']+)");
    private static final Pattern HREF_PATTERN = Pattern.compile("(?i)href\\s*=\\s*['\"]?([^'\"\\s>]+)['\"]?");
    private static final Pattern TAG_PATTERN = Pattern.compile("(?i)</?[^>]+>");

    @FXML
    private ListView<Repository.NewsItem> newsListView;
    @FXML
    private Label lblNewsState;

    private final ListChangeListener<Repository.NewsItem> newsListener = change -> {
        FXCollections.sort(Repository.getNewsMessages(),
                Comparator.comparingLong(Repository.NewsItem::getPublishedAt).reversed());
        while (change.next()) {
            if (!change.wasAdded()) {
                continue;
            }
            newsListView.scrollTo(0);
        }
    };
    private Timeline statusTimeline;

    @FXML
    public void initialize() {
        newsListView.setItems(Repository.getNewsMessages());
        FXCollections.sort(Repository.getNewsMessages(),
                Comparator.comparingLong(Repository.NewsItem::getPublishedAt).reversed());
        Repository.getNewsMessages().addListener(newsListener);
        newsListView.setCellFactory(lv -> new NewsCell());

        Repository.newsConnectedProperty().addListener((obs, oldV, newV) ->
                updateStatus(Boolean.TRUE.equals(newV)));
        updateStatus(Repository.newsConnectedProperty().get());

        statusTimeline = new Timeline(new KeyFrame(Duration.seconds(1), e ->
                updateStatus(Repository.newsConnectedProperty().get())));
        statusTimeline.setCycleCount(Timeline.INDEFINITE);
        statusTimeline.play();
    }

    private void updateStatus(boolean connected) {
        if (lblNewsState == null) {
            return;
        }
        lblNewsState.setText("NEWS: " + (connected ? "ON" : "OFF"));
        lblNewsState.setStyle("-fx-font-weight: bold; -fx-text-fill: " + (connected ? "#39c16c" : "#ff5f5f") + ";");
    }

    private static class NewsCell extends ListCell<Repository.NewsItem> {
        private static final int MAX_TITLE_CHARS = 220;
        private static final int MAX_SUMMARY_CHARS = 260;
        private final Label title = new Label();
        private final Label summary = new Label();
        private final Hyperlink link = new Hyperlink("Abrir fuente");
        private final VBox box = new VBox(6);

        private NewsCell() {
            title.setWrapText(true);
            summary.setWrapText(true);
            box.getStyleClass().add("news-card");
            title.getStyleClass().add("news-title");
            summary.getStyleClass().add("news-summary");
            link.getStyleClass().add("news-link");
            // Link arriba para que siempre se vea, incluso con textos largos
            box.getChildren().addAll(link, title, summary);
            setPrefWidth(0);
        }

        @Override
        protected void updateItem(Repository.NewsItem item, boolean empty) {
            super.updateItem(item, empty);
            if (empty || item == null) {
                setGraphic(null);
                setText(null);
                return;
            }

            String raw = item.getMessage() == null ? "" : item.getMessage();
            ParsedMessage parsed = parseMessage(raw);
            title.setText(truncate(parsed.title, MAX_TITLE_CHARS));
            summary.setText(truncate(parsed.summary, MAX_SUMMARY_CHARS));
            summary.setVisible(parsed.summary != null && !parsed.summary.isBlank());
            summary.setManaged(summary.isVisible());

            double maxWidth = Math.max(600, getListView() == null ? 900 : getListView().getWidth() - 30);
            title.setMaxWidth(maxWidth);
            summary.setMaxWidth(maxWidth);

            String url = resolveNewsUrl(item);
            boolean hasUrl = url != null && !url.isBlank();
            link.setVisible(hasUrl);
            link.setManaged(hasUrl);
            link.setText(hasUrl ? formatLinkLabel(url) : "Abrir fuente");
            link.setOnAction(e -> openLink(url));

            setGraphic(box);
            setText(null);
        }

        private void openLink(String url) {
            if (url == null || url.isBlank()) {
                return;
            }
            try {
                if (Desktop.isDesktopSupported()) {
                    Desktop.getDesktop().browse(new URI(url));
                }
            } catch (Exception ignored) {
            }
        }

        private String resolveNewsUrl(Repository.NewsItem item) {
            String normalizedDirectUrl = normalizeUrl(item == null ? null : item.getUrl());
            if (normalizedDirectUrl != null) {
                return normalizedDirectUrl;
            }
            if (item == null) {
                return null;
            }
            String message = item.getMessage() == null ? "" : item.getMessage();
            Matcher hrefMatcher = HREF_PATTERN.matcher(message);
            if (hrefMatcher.find()) {
                String hrefUrl = normalizeUrl(hrefMatcher.group(1));
                if (hrefUrl != null) {
                    return hrefUrl;
                }
            }
            Matcher urlMatcher = URL_PATTERN.matcher(message);
            if (urlMatcher.find()) {
                return normalizeUrl(urlMatcher.group(1));
            }
            return null;
        }

        private String normalizeUrl(String rawUrl) {
            if (rawUrl == null) {
                return null;
            }

            String cleaned = rawUrl.trim()
                    .replace("\\/", "/")
                    .replace("&amp;", "&")
                    .replace("%3A", ":")
                    .replace("%2F", "/")
                    .replaceAll("[\\u0000-\\u001F]+", "")
                    .replaceAll("^[\\(\"'\\[]+", "")
                    .replaceAll("[)\\],.;:]+$", "");

            if (cleaned.isBlank()) {
                return null;
            }

            Matcher embeddedMatcher = URL_PATTERN.matcher(cleaned);
            if (embeddedMatcher.find()) {
                cleaned = embeddedMatcher.group(1);
            }

            if (cleaned.startsWith("//")) {
                cleaned = "https:" + cleaned;
            } else if (cleaned.regionMatches(true, 0, "www.", 0, 4)) {
                cleaned = "https://" + cleaned;
            } else if (!cleaned.toLowerCase(Locale.ROOT).startsWith("http://")
                    && !cleaned.toLowerCase(Locale.ROOT).startsWith("https://")) {
                cleaned = "https://" + cleaned;
            }

            cleaned = cleaned.replace(" ", "%20");

            try {
                URI uri = new URI(cleaned).normalize();
                String scheme = uri.getScheme();
                String host = uri.getHost();
                if (scheme == null || host == null || host.isBlank()) {
                    return null;
                }
                if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
                    return null;
                }
                return uri.toString();
            } catch (URISyntaxException e) {
                try {
                    URL fallbackUrl = new URL(cleaned);
                    String protocol = fallbackUrl.getProtocol();
                    if (!"http".equalsIgnoreCase(protocol) && !"https".equalsIgnoreCase(protocol)) {
                        return null;
                    }
                    return fallbackUrl.toString();
                } catch (Exception ignored) {
                    return null;
                }
            }
        }

        private String formatLinkLabel(String url) {
            try {
                URI uri = new URI(url);
                String host = uri.getHost();
                if (host == null || host.isBlank()) {
                    return "Abrir fuente";
                }
                return host.startsWith("www.") ? host.substring(4) : host;
            } catch (Exception e) {
                return "Abrir fuente";
            }
        }

        private ParsedMessage parseMessage(String raw) {
            if (raw == null || raw.isBlank()) {
                return new ParsedMessage("", "");
            }
            String sanitized = sanitizeNewsText(raw);
            int first = sanitized.indexOf(" | ");
            int last = sanitized.lastIndexOf(" | ");
            if (first >= 0 && last > first) {
                String titlePart = sanitizeNewsText(sanitized.substring(0, last));
                String summaryPart = sanitizeNewsText(sanitized.substring(last + 3));
                return new ParsedMessage(titlePart, summaryPart);
            }
            String[] lines = sanitized.split("\\s+[\\-|:]\\s+", 2);
            if (lines.length == 2 && lines[0].length() <= 220) {
                return new ParsedMessage(lines[0], lines[1]);
            }
            return new ParsedMessage(sanitized, "");
        }

        private String truncate(String value, int max) {
            if (value == null) {
                return "";
            }
            String clean = sanitizeNewsText(value);
            if (clean.length() <= Math.max(40, max)) {
                return clean;
            }
            return clean.substring(0, Math.max(40, max)) + "...";
        }

        private String sanitizeNewsText(String value) {
            if (value == null) {
                return "";
            }
            String clean = value
                    .replace("\\/", "/")
                    .replace("&amp;", "&")
                    .replace("&nbsp;", " ")
                    .replace("&#39;", "'")
                    .replace("&quot;", "\"")
                    .replaceAll("(?i)\\[([^\\]]+)]\\((https?://[^)]+)\\)", "$1")
                    .replaceAll("(?i)<a[^>]+href=['\"]?([^'\"> ]+)['\"]?[^>]*>(.*?)</a>", "$2")
                    .replaceAll("(?i)</?a[^>]*>", " ")
                    .replaceAll("(?i)href\\s*=\\s*['\"]?[^\\s'\"]+['\"]?", " ")
                    .replaceAll("(?i)target\\s*=\\s*['\"]?[^\\s'\"]+['\"]?", " ")
                    .replaceAll("(?i)rel\\s*=\\s*['\"]?[^\\s'\"]+['\"]?", " ")
                    .replaceAll("(?i)class\\s*=\\s*['\"]?[^\\s'\"]+['\"]?", " ")
                    .replaceAll("(?i)style\\s*=\\s*['\"]?[^'\"]*['\"]?", " ")
                    .replaceAll("(?i)<[a-zA-Z][^>]*", " ")
                    .replace("<", " ")
                    .replace(">", " ");
            clean = TAG_PATTERN.matcher(clean).replaceAll(" ");
            clean = URL_PATTERN.matcher(clean).replaceAll(" ");
            clean = clean.replaceAll("(?i)\\ba\\b\\s+href\\b", " ");
            clean = clean.replaceAll("(?i)\\bhttps?\\s*:\\s*/\\s*/\\S+", " ");
            clean = clean.replaceAll("\\s+", " ").trim();
            return clean;
        }

        private static class ParsedMessage {
            final String title;
            final String summary;

            ParsedMessage(String title, String summary) {
                this.title = title == null ? "" : title;
                this.summary = summary == null ? "" : summary;
            }
        }
    }
}
