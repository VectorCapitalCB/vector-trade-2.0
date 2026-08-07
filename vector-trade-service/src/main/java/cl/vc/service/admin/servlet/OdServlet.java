package cl.vc.service.admin.servlet;

import cl.vc.service.MainApp;
import cl.vc.service.admin.OdAttempt;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@Slf4j
public class OdServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            List<OdAttempt> attempts = MainApp.getOdAttemptsSnapshot();
            JSONArray arr = new JSONArray();
            for (OdAttempt attempt : attempts) {
                arr.put(toJson(attempt));
            }
            res.getWriter().write(new JSONObject()
                    .put("ok", true)
                    .put("enabled", MainApp.isOdProtectionEnabled())
                    .put("count", arr.length())
                    .put("attempts", arr)
                    .toString());
        } catch (Exception e) {
            log.error("[Admin/OD] GET error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        try {
            if ("/clear".equals(req.getPathInfo())) {
                MainApp.clearOdAttempts();
                res.getWriter().write(new JSONObject().put("ok", true).toString());
                return;
            }
            if ("/enable".equals(req.getPathInfo())) {
                MainApp.setOdProtectionEnabled(true);
                res.getWriter().write(new JSONObject()
                        .put("ok", true)
                        .put("enabled", MainApp.isOdProtectionEnabled())
                        .toString());
                return;
            }
            if ("/disable".equals(req.getPathInfo())) {
                MainApp.setOdProtectionEnabled(false);
                res.getWriter().write(new JSONObject()
                        .put("ok", true)
                        .put("enabled", MainApp.isOdProtectionEnabled())
                        .toString());
                return;
            }
            res.setStatus(404);
            res.getWriter().write(new JSONObject().put("error", "Path no reconocido").toString());
        } catch (Exception e) {
            log.error("[Admin/OD] POST error: {}", e.getMessage(), e);
            res.setStatus(500);
            res.getWriter().write(new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private JSONObject toJson(OdAttempt attempt) {
        return new JSONObject()
                .put("timestamp", attempt.getTimestamp())
                .put("username", attempt.getUsername())
                .put("account", attempt.getAccount())
                .put("symbol", attempt.getSymbol())
                .put("attemptedSide", attempt.getAttemptedSide())
                .put("attemptedPrice", attempt.getAttemptedPrice())
                .put("attemptedQuantity", attempt.getAttemptedQuantity())
                .put("attemptedOrderId", attempt.getAttemptedOrderId())
                .put("attemptedExchange", attempt.getAttemptedExchange())
                .put("conflictingSide", attempt.getConflictingSide())
                .put("conflictingPrice", attempt.getConflictingPrice())
                .put("conflictingQuantity", attempt.getConflictingQuantity())
                .put("conflictingOrderId", attempt.getConflictingOrderId())
                .put("conflictingStatus", attempt.getConflictingStatus())
                .put("reason", attempt.getReason());
    }
}
