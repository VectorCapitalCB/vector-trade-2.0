package cl.vc.service.akka.actors.websocket;

import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.module.protocolbuff.keycloak.KeycloakService;
import cl.vc.service.MainApp;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test de regresión del filtro de autenticación del WebSocket
 * (cl.vc.service.akka.actors.websocket.AuthenticationFilter).
 *
 * FIJA EL COMPORTAMIENTO ACTUAL (no el ideal). Documenta explícitamente:
 *  - El flag 'passwordrequiere': si es false, el filtro NO autentica (bypass total).
 *  - Las credenciales Basic llegan como base64("encUser:encPass") donde cada parte
 *    va cifrada con AES (AESEncryption); el filtro descifra ambas.
 *  - El antiguo password UUID especial ya no omite Keycloak.
 *  - El flujo normal SÍ invoca KeycloakService.getTokenKeycloak.
 *  - Rate-limit por IP: a los 20 fallos la IP queda bloqueada (403) antes de mirar auth.
 *
 * Keycloak, Properties y KeycloakService se mockean (Mockito / mockStatic sobre MainApp).
 * No se tocan recursos externos reales (SQL/Redis/Keycloak/red).
 */
class AuthenticationFilterTest {

    private static final String MAGIC_PASSWORD = "9f3c2a1b-7e44-4d8a-9c21-5b7f3a9e8d12";

    /** Genera una IP distinta por test para no cruzar el estado estático (failCount/blockedUntil). */
    private static final AtomicInteger IP_SEQ = new AtomicInteger(1);

    private String uniqueIp() {
        return "10.0.0." + IP_SEQ.getAndIncrement();
    }

    /** Cabecera Basic tal como la espera el filtro: base64( AES(user) + ":" + AES(pass) ). */
    private String basicHeader(String plainUser, String plainPass) {
        String enc = AESEncryption.encrypt(plainUser) + ":" + AESEncryption.encrypt(plainPass);
        return "Basic " + Base64.getEncoder().encodeToString(enc.getBytes());
    }

    private HttpServletRequest requestWith(String ip, String authHeader) {
        HttpServletRequest req = mock(HttpServletRequest.class);
        when(req.getHeader("X-Forwarded-For")).thenReturn(ip);
        when(req.getHeader("Authorization")).thenReturn(authHeader);
        return req;
    }

    private Properties props(boolean passwordRequiere) {
        Properties p = new Properties();
        p.setProperty("passwordrequiere", String.valueOf(passwordRequiere));
        return p;
    }

    // ---------------------------------------------------------------------

    @Test
    void passwordrequiereFalse_bypasseaTodaAutenticacion() throws Exception {
        HttpServletRequest req = requestWith(uniqueIp(), null);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(false));

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        // Bypass total: pasa la cadena sin mirar credenciales ni status de error.
        verify(chain).doFilter(req, res);
        verify(res, never()).setStatus(anyInt());
    }

    @Test
    void passwordMagicoUUID_yaNoBypassea_seValidaContraKeycloak() throws Exception {
        HttpServletRequest req = requestWith(uniqueIp(), basicHeader("operador1", MAGIC_PASSWORD));
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        KeycloakService keycloak = mock(KeycloakService.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));
            main.when(() -> MainApp.isUserProcessed("operador1")).thenReturn(true);
            main.when(MainApp::getKeycloakService).thenReturn(keycloak);

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        // BACKDOOR REMOVIDO: el UUID mágico ya NO es especial; se valida contra Keycloak como cualquier password.
        verify(keycloak).getTokenKeycloak("operador1", MAGIC_PASSWORD);
        verify(chain).doFilter(req, res);
    }

    @Test
    void passwordNormal_validaContraKeycloak_yPasa() throws Exception {
        HttpServletRequest req = requestWith(uniqueIp(), basicHeader("operador2", "claveReal"));
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        KeycloakService keycloak = mock(KeycloakService.class);
        when(keycloak.getTokenKeycloak("operador2", "claveReal")).thenReturn("token-ok");

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));
            main.when(() -> MainApp.isUserProcessed("operador2")).thenReturn(true);
            main.when(MainApp::getKeycloakService).thenReturn(keycloak);

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        verify(keycloak).getTokenKeycloak("operador2", "claveReal");
        verify(chain).doFilter(req, res);
        verify(res, never()).setStatus(anyInt());
    }

    @Test
    void keycloakLanzaExcepcion_devuelve401_yNoPasa() throws Exception {
        HttpServletRequest req = requestWith(uniqueIp(), basicHeader("operador3", "claveMala"));
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        KeycloakService keycloak = mock(KeycloakService.class);
        when(keycloak.getTokenKeycloak("operador3", "claveMala"))
                .thenThrow(new RuntimeException("credenciales inválidas en Keycloak"));

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));
            main.when(() -> MainApp.isUserProcessed("operador3")).thenReturn(true);
            main.when(MainApp::getKeycloakService).thenReturn(keycloak);

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void usuarioNoProcesado_devuelve403_yNoPasa() throws Exception {
        HttpServletRequest req = requestWith(uniqueIp(), basicHeader("desconocido", "cualquiera"));
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        KeycloakService keycloak = mock(KeycloakService.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));
            main.when(() -> MainApp.isUserProcessed("desconocido")).thenReturn(false);
            main.when(MainApp::getKeycloakService).thenReturn(keycloak);

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_FORBIDDEN);
        verify(keycloak, never()).getTokenKeycloak(anyString(), anyString());
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void sinCabeceraAuthorization_devuelve401_yNoPasa() throws Exception {
        HttpServletRequest req = requestWith(uniqueIp(), null);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void cabeceraBasicMalFormada_sinDosPuntos_devuelve401() throws Exception {
        // base64 de un texto SIN ':' -> split(":",2) da length 1 -> 401
        String header = "Basic " + Base64.getEncoder().encodeToString("soloUnValor".getBytes());
        HttpServletRequest req = requestWith(uniqueIp(), header);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void credencialAESArbitraria_descifraSinLanzar_yCaeEnUsuarioNoProcesado_403() throws Exception {
        // Comportamiento ACTUAL documentado: AESEncryption.decrypt es tolerante y NO lanza
        // ante estas partes arbitrarias; devuelve una cadena, que al no ser un usuario
        // procesado termina en 403 (rama isUserProcessed=false), no en 401.
        String header = "Basic " + Base64.getEncoder().encodeToString("noAes:tampocoAes".getBytes());
        HttpServletRequest req = requestWith(uniqueIp(), header);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));
            // isUserProcessed sin stub -> default false para cualquier usuario.

            new AuthenticationFilter().doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_FORBIDDEN);
        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    void veinteFallos_bloqueanLaIP_conForbidden() throws Exception {
        String ip = uniqueIp();
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            main.when(MainApp::getProperties).thenReturn(props(true));

            AuthenticationFilter filter = new AuthenticationFilter();

            // 20 fallos (sin Authorization) desde la misma IP -> MAX_FAILS alcanzado.
            for (int i = 0; i < 20; i++) {
                filter.doFilter(requestWith(ip, null), mock(HttpServletResponse.class), mock(FilterChain.class));
            }

            // La 21ª petición se corta por bloqueo ANTES de mirar credenciales.
            HttpServletRequest req = requestWith(ip, basicHeader("operador2", "claveReal"));
            filter.doFilter(req, res, chain);
        }

        verify(res).setStatus(HttpServletResponse.SC_FORBIDDEN);
        verify(chain, never()).doFilter(any(), any());
    }
}
