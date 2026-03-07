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
import javafx.collections.FXCollections;
import java.util.Comparator;

public class NewsController {

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

            String url = item.getUrl();
            boolean hasUrl = url != null && !url.isBlank();
            link.setVisible(hasUrl);
            link.setManaged(hasUrl);
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

        private ParsedMessage parseMessage(String raw) {
            if (raw == null || raw.isBlank()) {
                return new ParsedMessage("", "");
            }
            int first = raw.indexOf(" | ");
            int last = raw.lastIndexOf(" | ");
            if (first >= 0 && last > first) {
                String titlePart = raw.substring(0, last).trim();
                String summaryPart = raw.substring(last + 3).trim();
                return new ParsedMessage(titlePart, summaryPart);
            }
            return new ParsedMessage(raw.trim(), "");
        }

        private String truncate(String value, int max) {
            if (value == null) {
                return "";
            }
            String clean = value.replaceAll("\\s+", " ").trim();
            if (clean.length() <= Math.max(40, max)) {
                return clean;
            }
            return clean.substring(0, Math.max(40, max)) + "...";
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
