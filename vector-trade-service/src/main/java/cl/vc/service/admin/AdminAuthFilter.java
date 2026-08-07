package cl.vc.service.admin;

import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Filtro de seguridad para el panel de administración.
 *
 * Autenticación: Bearer token configurado en application.properties (admin.token).
 * FAIL-CLOSED: si admin.token está vacío, se RECHAZA todo acceso (503). Hay que configurarlo.
 * CORS restringido al origen configurado en admin.cors.origin (nunca '*'; vacío = mismo origen).
 */
@Slf4j
public class AdminAuthFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {}

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest  req = (HttpServletRequest)  request;
        HttpServletResponse res = (HttpServletResponse) response;

        // CORS — solo el origen configurado (nunca '*'). Vacío = mismo origen.
        String corsOrigin = MainApp.getProperties().getProperty("admin.cors.origin", "").trim();
        if (!corsOrigin.isEmpty()) {
            res.setHeader("Access-Control-Allow-Origin", corsOrigin);
            res.setHeader("Vary", "Origin");
        }
        res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");

        if ("OPTIONS".equalsIgnoreCase(req.getMethod())) {
            res.setStatus(HttpServletResponse.SC_OK);
            return;
        }

        String adminToken = MainApp.getProperties().getProperty("admin.token", "").trim();

        // FAIL-CLOSED: sin admin.token configurado NO se permite acceso (antes era acceso libre).
        if (adminToken.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            res.setContentType("application/json;charset=UTF-8");
            res.getWriter().write("{\"error\":\"admin.token no configurado\"}");
            log.error("[Admin] Rechazado: admin.token vacio (fail-closed). Configura admin.token en el properties.");
            return;
        }

        String authHeader = req.getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7).trim();
            if (adminToken.equals(token)) {
                chain.doFilter(request, response);
                return;
            }
        }

        res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        res.setContentType("application/json;charset=UTF-8");
        res.getWriter().write("{\"error\":\"Unauthorized\"}");
        log.warn("[Admin] Acceso rechazado desde {}", req.getRemoteAddr());
    }

    @Override
    public void destroy() {}
}
