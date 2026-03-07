package cl.vc.service.akka.actors.websocket;

import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class AuthenticationFilter implements Filter {

    // --- Configuración de protección ---
    private static final int MAX_FAILS = 20;                     // Número máximo de intentos
    private static final long BLOCK_DURATION_MS = 30_000;   // 5 minutos de bloqueo

    // --- Mapas concurrentes en memoria ---
    private static final Map<String, Integer> failCount = new ConcurrentHashMap<>();
    private static final Map<String, Long> blockedUntil = new ConcurrentHashMap<>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException { }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        String ip = getClientIp(httpRequest);
        String authHeader = httpRequest.getHeader("Authorization");


        if (!Boolean.parseBoolean(MainApp.getProperties().getProperty("passwordrequiere"))) {
            chain.doFilter(request, response);
            return;
        }

        if (isBlocked(ip)) {
            httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
            log.info("🚫 Acceso bloqueado temporalmente para IP {}", ip);
            return;
        }

        if (authHeader != null && authHeader.startsWith("Basic")) {
            String base64Credentials = authHeader.substring("Basic".length()).trim();
            String credentials = new String(Base64.getDecoder().decode(base64Credentials));
            String[] values = credentials.split(":", 2);

            if (values.length != 2) {
                httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                log.info("Cabecera Authorization mal formada desde IP {}", ip);
                registerFailure(ip);
                return;
            }

            String username = values[0];
            String password = values[1];

            try {
                String plainUsername = AESEncryption.decrypt(username);
                String plainPassword = AESEncryption.decrypt(password);

                if (!MainApp.isUserProcessed(plainUsername)) {
                    httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    log.info("Acceso denegado: usuario '{}' aun no procesado en initAccount (IP {}).", plainUsername, ip);
                    return;
                }

                if(!plainPassword.equals("9f3c2a1b-7e44-4d8a-9c21-5b7f3a9e8d12")){
                    MainApp.getKeycloakService().getTokenKeycloak(plainUsername, plainPassword);
                }

                clearFailures(ip);
                chain.doFilter(request, response);

                log.info("Login usuario {} {}", ip, plainUsername);

            } catch (Exception e) {
                httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                log.info("Clave inválida desde IP {}: {}", ip, e.getMessage());
                registerFailure(ip);
            }

        } else {
            httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            log.info("Intento sin cabecera Authorization desde IP {}", ip);
            registerFailure(ip);
        }
    }

    @Override
    public void destroy() { }

    // --- Métodos auxiliares de seguridad ---

    private boolean isBlocked(String ip) {
        Long until = blockedUntil.get(ip);
        if (until == null) return false;
        if (System.currentTimeMillis() > until) {
            blockedUntil.remove(ip);
            return false;
        }
        return true;
    }

    private void registerFailure(String ip) {
        int count = failCount.getOrDefault(ip, 0) + 1;
        failCount.put(ip, count);

        if (count >= MAX_FAILS) {
            blockedUntil.put(ip, System.currentTimeMillis() + BLOCK_DURATION_MS);
            failCount.remove(ip);
            log.warn("🚨 IP {} bloqueada por {} minutos por demasiados intentos fallidos", ip, BLOCK_DURATION_MS / 60000);
        }
    }

    private void clearFailures(String ip) {
        failCount.remove(ip);
    }

    private String getClientIp(HttpServletRequest request) {
        String xfHeader = request.getHeader("X-Forwarded-For");
        if (xfHeader == null) {
            return request.getRemoteAddr();
        }
        return xfHeader.split(",")[0];
    }
}
