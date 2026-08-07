package cl.vc.service.admin.servlet;

import akka.actor.ActorRef;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.service.MainApp;
import cl.vc.service.akka.actors.BuySideConnect;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * POST /api/news/inject
 *   body: { "texto": "...", "lineoftext": "...", "securityExchange": "BCS" }
 *   → Inyecta una MarketDataMessage.News a todos los clientes conectados y la guarda en listNews.
 *
 * GET /api/news
 *   → Lista las últimas noticias en memoria (listNews).
 */
@Slf4j
public class NewsServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            JSONArray arr = new JSONArray();
            for (MarketDataMessage.News n : MainApp.getListNews()) {
                arr.put(new JSONObject()
                        .put("texto",            n.getTexto())
                        .put("lineoftext",       n.getLineoftext())
                        .put("securityExchange", n.getSecurityExchange().name()));
            }
            res.getWriter().write(arr.toString());
        } catch (Exception e) {
            log.error("[Admin/News] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            String body = req.getReader().lines().collect(Collectors.joining());
            JSONObject json = new JSONObject(body);

            String texto      = json.optString("texto", "").trim();
            String lineoftext = json.optString("lineoftext", "").trim();
            String exchange   = json.optString("securityExchange", "NONE_MKD").trim();

            if (texto.isEmpty()) {
                res.setStatus(400);
                res.getWriter().write(new JSONObject().put("error", "El campo 'texto' es requerido").toString());
                return;
            }

            MarketDataMessage.SecurityExchangeMarketData mkdExchange;
            try {
                mkdExchange = MarketDataMessage.SecurityExchangeMarketData.valueOf(exchange);
            } catch (IllegalArgumentException ex) {
                mkdExchange = MarketDataMessage.SecurityExchangeMarketData.NONE_MKD;
            }

            MarketDataMessage.News news = MarketDataMessage.News.newBuilder()
                    .setTexto(texto)
                    .setLineoftext(lineoftext)
                    .setSecurityExchange(mkdExchange)
                    .setT(TimeGenerator.getTimeProto())
                    .build();

            // Guarda en historial para que nuevos clientes la reciban al conectarse
            MainApp.getListNews().add(news);

            // Tell directo a cada ActorPerSession conectado actualmente
            int count = 0;
            for (ActorRef actor : BuySideConnect.getActorPerSessionMaps().values()) {
                actor.tell(news, ActorRef.noSender());
                count++;
            }

            log.info("[Admin/News] Noticia inyectada → exchange={} texto={} | actores conectados={}",
                    mkdExchange.name(), texto, count);

            res.getWriter().write(new JSONObject()
                    .put("ok",       true)
                    .put("message",  "Noticia inyectada a " + count + " usuario(s) conectado(s)")
                    .put("exchange", mkdExchange.name())
                    .put("texto",    texto)
                    .put("count",    count)
                    .toString());

        } catch (Exception e) {
            log.error("[Admin/News] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage() != null
                    ? e.getMessage() : e.getClass().getSimpleName()).toString());
        }
    }
}

