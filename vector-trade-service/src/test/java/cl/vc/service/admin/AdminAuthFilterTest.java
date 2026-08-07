package cl.vc.service.admin;

import cl.vc.service.MainApp;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Valida el hardening del filtro del panel admin:
 *  - FAIL-CLOSED: admin.token vacio -> 503 y NO pasa la cadena (antes era acceso libre).
 *  - CORS: nunca '*'; solo el origen configurado en admin.cors.origin.
 */
class AdminAuthFilterTest {

    private HttpServletRequest req(String method, String authHeader) {
        HttpServletRequest r = mock(HttpServletRequest.class);
        when(r.getMethod()).thenReturn(method);
        when(r.getHeader("Authorization")).thenReturn(authHeader);
        return r;
    }

    private HttpServletResponse res() throws Exception {
        HttpServletResponse r = mock(HttpServletResponse.class);
        when(r.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
        return r;
    }

    private Properties props(String adminToken, String corsOrigin) {
        Properties p = new Properties();
        if (adminToken != null) p.setProperty("admin.token", adminToken);
        if (corsOrigin != null) p.setProperty("admin.cors.origin", corsOrigin);
        return p;
    }

    @Test
    void tokenVacio_failClosed_rechaza503_yNoPasaLaCadena() throws Exception {
        HttpServletRequest req = req("GET", null);
        HttpServletResponse res = res();
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props("", null));
            new AdminAuthFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void tokenValido_conBearerCorrecto_pasa() throws Exception {
        HttpServletRequest req = req("GET", "Bearer secreto123");
        HttpServletResponse res = res();
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props("secreto123", null));
            new AdminAuthFilter().doFilter(req, res, chain);
        }

        verify(chain).doFilter(req, res);
        verify(res, never()).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    void tokenSeteado_sinBearer_rechaza401() throws Exception {
        HttpServletRequest req = req("GET", null);
        HttpServletResponse res = res();
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props("secreto123", null));
            new AdminAuthFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void options_devuelve200_preflight_sinPasarLaCadena() throws Exception {
        HttpServletRequest req = req("OPTIONS", null);
        HttpServletResponse res = res();
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props("secreto123", null));
            new AdminAuthFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_OK);
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void cors_nuncaWildcard_usaSoloElOrigenConfigurado() throws Exception {
        HttpServletRequest req = req("GET", "Bearer secreto123");
        HttpServletResponse res = res();
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props("secreto123", "https://admin.vectortrade.cl"));
            new AdminAuthFilter().doFilter(req, res, chain);
        }

        verify(res).setHeader("Access-Control-Allow-Origin", "https://admin.vectortrade.cl");
        verify(res, never()).setHeader("Access-Control-Allow-Origin", "*");
    }
}
