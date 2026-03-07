package cl.vc.blotter.controller;

import javafx.fxml.FXML;
import javafx.scene.layout.Pane;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraficoController {


    public WebView webView;

    @FXML
    private Pane webViewContainer;

    @FXML
    private void initialize() {

        webView = new WebView();
        webViewContainer.getChildren().add(webView);
        loadInitialContent();

    }

    private void loadInitialContent() {
        try {


            log.info("carga de grafico ");
            //webView.getEngine().load(GraficoController.class.getResource("/graph/index.html").toString());


            loadHtmlContent("AMZN");


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public void loadHtmlContent(String symbol) {
        try {

            WebEngine webEngine = webView.getEngine();



            String html = "<!DOCTYPE html>" +
                    "<html>" +
                    "<head>" +
                    "  <script type=\"text/javascript\" src=\"file:///C:/Users/vnazar/vc/vt/tv.js\"></script>" +
                    "<script type=\"text/javascript\">" +
                    "function loadChart() {" +
                    "  new TradingView.widget({" +
                    "    \"width\": 980," +
                    "    \"height\": 610," +
                    "    \"symbol\": \"NASDAQ:AAPL\"," +
                    "    \"interval\": \"D\"," +
                    "    \"timezone\": \"Etc/UTC\"," +
                    "    \"theme\": \"light\"," +
                    "    \"style\": \"1\"," +
                    "    \"locale\": \"en\"," +
                    "    \"toolbar_bg\": \"#f1f3f6\"," +
                    "    \"enable_publishing\": false," +
                    "    \"hide_top_toolbar\": true," +
                    "    \"save_image\": false," +
                    "    \"container_id\": \"tradingview_widget\"" +
                    "  });" +
                    "}" +
                    "</script>" +
                    "</head>" +
                    "<body onload=\"loadChart()\">" +
                    "<div id=\"tradingview_widget\"></div>" +
                    "</body>" +
                    "</html>";

            // Escuchar errores de carga
            webEngine.setOnError(event -> System.out.println("Error: " + event.getMessage()));
            webEngine.setOnAlert(event -> System.out.println("Alert: " + event.getData()));


            webEngine.loadContent(html);

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }
}

