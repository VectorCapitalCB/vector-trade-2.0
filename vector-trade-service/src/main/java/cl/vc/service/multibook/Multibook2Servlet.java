package cl.vc.service.multibook;

import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Collectors;

/**
 * API del multibook para el blotter. Va montada en el mismo contexto que el websocket, o sea detras
 * del {@code AuthenticationFilter} que ya valida el header Basic contra Keycloak: no hay auth nueva
 * y el username sale de esa credencial, no de la ruta, asi que nadie lee el multibook de otro.
 *
 * <pre>
 * GET    /api/multibook2                    -> documento del usuario
 * PUT    /api/multibook2                    -> guarda el documento completo
 * POST   /api/multibook2/rename?from=&to=   -> renombra una configuracion
 * POST   /api/multibook2/active?name=       -> marca la configuracion activa
 * DELETE /api/multibook2/{name}             -> elimina una configuracion
 * </pre>
 */
@Slf4j
public class Multibook2Servlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String username = resolveUsername(req, res);
        if (username == null) {
            return;
        }
        try {
            write(res, Multibook2Repository.load(username));
        } catch (Exception e) {
            fail(res, "GET", e);
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String username = resolveUsername(req, res);
        if (username == null) {
            return;
        }
        try {
            String body = req.getReader().lines().collect(Collectors.joining());
            if (body == null || body.isBlank()) {
                error(res, 400, "Body vacio");
                return;
            }
            Multibook2Repository.save(username, new JSONObject(body));
            write(res, Multibook2Repository.load(username));
        } catch (Exception e) {
            fail(res, "PUT", e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String username = resolveUsername(req, res);
        if (username == null) {
            return;
        }
        try {
            String action = path(req);

            if ("rename".equals(action)) {
                if (!Multibook2Repository.renameLayout(username, req.getParameter("from"), req.getParameter("to"))) {
                    error(res, 400, "No se pudo renombrar: no existe o el nombre ya esta en uso");
                    return;
                }
            } else if ("active".equals(action)) {
                if (!Multibook2Repository.setActive(username, req.getParameter("name"))) {
                    error(res, 404, "La configuracion no existe");
                    return;
                }
            } else {
                error(res, 404, "Accion desconocida");
                return;
            }

            write(res, Multibook2Repository.load(username));

        } catch (Exception e) {
            fail(res, "POST", e);
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String username = resolveUsername(req, res);
        if (username == null) {
            return;
        }
        try {
            String name = path(req);
            if (name.isEmpty()) {
                error(res, 400, "Debe indicar la configuracion en la ruta");
                return;
            }
            if (!Multibook2Repository.deleteLayout(username, name)) {
                error(res, 400, "No se pudo eliminar: no existe o es la unica configuracion");
                return;
            }
            write(res, Multibook2Repository.load(username));
        } catch (Exception e) {
            fail(res, "DELETE", e);
        }
    }

    private String path(HttpServletRequest req) {
        String pathInfo = req.getPathInfo();
        if (pathInfo == null || pathInfo.isBlank() || "/".equals(pathInfo)) {
            return "";
        }
        return URLDecoder.decode(pathInfo.substring(1), StandardCharsets.UTF_8);
    }

    /**
     * Username del header Basic (viene cifrado, igual que en el upgrade del websocket). Solo si el
     * servicio corre con {@code passwordrequiere=false} -- donde el filtro deja pasar sin header --
     * se acepta el parametro {@code user}.
     */
    private String resolveUsername(HttpServletRequest req, HttpServletResponse res) throws IOException {

        res.setContentType("application/json;charset=UTF-8");

        String header = req.getHeader("Authorization");

        if (header != null && header.startsWith("Basic")) {
            try {
                String credentials = new String(Base64.getDecoder()
                        .decode(header.substring("Basic".length()).trim()), StandardCharsets.UTF_8);
                String[] values = credentials.split(":", 2);
                return AESEncryption.decrypt(values[0]);
            } catch (Exception e) {
                error(res, 401, "Credencial ilegible");
                return null;
            }
        }

        if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("passwordrequiere"))) {
            String user = req.getParameter("user");
            if (user != null && !user.isBlank()) {
                return user.trim();
            }
        }

        error(res, 401, "Falta la cabecera Authorization");
        return null;
    }

    private void write(HttpServletResponse res, JSONObject body) throws IOException {
        res.setContentType("application/json;charset=UTF-8");
        res.getWriter().write(body.toString());
    }

    private void error(HttpServletResponse res, int status, String message) throws IOException {
        res.setStatus(status);
        res.setContentType("application/json;charset=UTF-8");
        res.getWriter().write(new JSONObject().put("error", message).toString());
    }

    private void fail(HttpServletResponse res, String verb, Exception e) throws IOException {
        log.error("[Multibook2] {} error: {}", verb, e.getMessage(), e);
        error(res, 500, String.valueOf(e.getMessage()));
    }
}
