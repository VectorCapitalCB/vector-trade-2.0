package cl.vc.service.admin;

import cl.vc.service.admin.servlet.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import javax.servlet.*;
import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Properties;

@Slf4j
public class AdminServer extends Thread {

    private final Properties properties;

    public AdminServer(Properties properties) {
        this.properties = properties;
        setName("admin-http-server");
        setDaemon(true);
    }

    @Override
    public void run() {
        try {
            int port = Integer.parseInt(properties.getProperty("admin.port", "8087"));
            Server server = new Server(port);

            ServletContextHandler ctx = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            ctx.setContextPath("/");

            // Filtro de autenticación solo sobre rutas /api/*
            FilterHolder authHolder = new FilterHolder(new AdminAuthFilter());
            ctx.addFilter(authHolder, "/api/*", EnumSet.of(DispatcherType.REQUEST));

            // ── API Servlets ──────────────────────────────────────────────────
            ctx.addServlet(new ServletHolder(new AdminAuthServlet()),   "/api/auth");
            ctx.addServlet(new ServletHolder(new SymbolServlet()),      "/api/symbols/*");
            ctx.addServlet(new ServletHolder(new AccountServlet()),     "/api/accounts/*");
            ctx.addServlet(new ServletHolder(new ConnectionServlet()),  "/api/connections/*");
            ctx.addServlet(new ServletHolder(new OrderServlet()),       "/api/orders/*");
            ctx.addServlet(new ServletHolder(new InjectServlet()),      "/api/inject");
            ctx.addServlet(new ServletHolder(new NewsServlet()),        "/api/news/*");
            ctx.addServlet(new ServletHolder(new PropertiesServlet()),  "/api/properties/*");
            ctx.addServlet(new ServletHolder(new RiskServlet()),        "/api/risk/*");
            ctx.addServlet(new ServletHolder(new SessionServlet()),     "/api/sessions/*");
            ctx.addServlet(new ServletHolder(new IpSecurityServlet()),  "/api/security/*");
            ctx.addServlet(new ServletHolder(new SimultaneasServlet()), "/api/simultaneas/*");
            ctx.addServlet(new ServletHolder(new PrestamosServlet()),   "/api/prestamos/*");
            ctx.addServlet(new ServletHolder(new SaldoServlet()),       "/api/saldo/*");
            ctx.addServlet(new ServletHolder(new SqlRecoveryServlet()), "/api/sql-recovery/*");
            ctx.addServlet(new ServletHolder(new OdServlet()),          "/api/od/*");
            ctx.addServlet(new ServletHolder(new MultibookServlet()),   "/api/multibook/*");
            ctx.addServlet(new ServletHolder(new RedisServlet()),       "/api/redis/*");
            ctx.addServlet(new ServletHolder(new MongoServlet()),       "/api/mongo/*");
            ctx.addServlet(new ServletHolder(new HistoryServlet()),     "/api/history/*");

            // ── No-cache para index.html (evita que el browser cachee el HTML y rompa los assets) ──
            FilterHolder noCacheFilter = new FilterHolder(new Filter() {
                @Override public void init(FilterConfig fc) {}
                @Override public void destroy() {}
                @Override
                public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                        throws IOException, ServletException {
                    HttpServletRequest  req = (HttpServletRequest)  request;
                    HttpServletResponse res = (HttpServletResponse) response;
                    String path = req.getRequestURI();
                    if (path.endsWith(".html") || path.equals("/") || path.isEmpty()) {
                        res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
                        res.setHeader("Pragma",         "no-cache");
                        res.setHeader("Expires",        "0");
                    }
                    chain.doFilter(request, response);
                }
            });
            ctx.addFilter(noCacheFilter, "/*", EnumSet.of(DispatcherType.REQUEST));

            // ── Archivos estáticos React (bundleados en el JAR) ───────────────
            Resource staticRes = Resource.newClassPathResource("/admin");
            if (staticRes != null && staticRes.exists()) {
                ctx.setBaseResource(staticRes);
                ctx.setWelcomeFiles(new String[]{"index.html"});
                log.info("[Admin] Sirviendo UI desde classpath:/admin");
            } else {
                log.warn("[Admin] No se encontró classpath:/admin — UI no disponible. Ejecuta 'npm run build' en admin-web/");
            }

            ServletHolder defHolder = new ServletHolder("default", DefaultServlet.class);
            defHolder.setInitParameter("dirAllowed",    "false");
            defHolder.setInitParameter("etags",         "true");
            defHolder.setInitParameter("cacheControl",  "max-age=31536000, public, immutable");
            ctx.addServlet(defHolder, "/");

            server.setHandler(ctx);
            log.info("[Admin] Arrancando servidor HTTP en puerto {}...", port);
            server.start();
            log.info("##############################################################");
            log.info("##  ADMIN WEB OK  -->  http://localhost:{}  ##", port);
            log.info("##############################################################");
            server.join();

        } catch (Exception e) {
            log.error("##############################################################");
            log.error("##  ADMIN WEB ERROR: {}  ##", e.getMessage());
            log.error("##############################################################", e);
        }
    }
}
