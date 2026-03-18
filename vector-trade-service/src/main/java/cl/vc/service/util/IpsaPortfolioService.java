package cl.vc.service.util;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Slf4j
public final class IpsaPortfolioService {

    public static final String DEFAULT_PORTFOLIO_NAME = "IPSA";
    public static final String DEFAULT_PRIMARY_PORTFOLIO_NAME = "Principal";
    private static final String PROP_ENABLED = "ipsa.portfolio.enabled";
    private static final String PROP_NAME = "ipsa.portfolio.name";
    private static final String PROP_URL = "ipsa.portfolio.url";
    private static final String PROP_REFRESH_MINUTES = "ipsa.portfolio.refresh.minutes";
    private static final String PROP_TIMEOUT_SECONDS = "ipsa.portfolio.timeout.seconds";
    private static final String PROP_USER_AGENT = "ipsa.portfolio.user.agent";
    private static final String PROP_FALLBACK_SYMBOLS = "ipsa.portfolio.fallback.symbols";
    private static final String DEFAULT_URL = "https://es.investing.com/indices/ipsa-components";
    private static final String DEFAULT_USER_AGENT =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";
    private static final List<String> DEFAULT_SYMBOLS = List.of(
            "AGUAS-A", "BCI", "IAM", "CCU", "SONDA", "VAPORES", "COPEC",
            "ANDINA-B", "RIPLEY", "CMPC", "CAP", "SQM-B", "CONCHATORO", "ILC",
            "MALLPLAZA", "ENELCHILE", "FALABELLA", "LTM", "CENCOMALLS", "SALFACORP", "CHILE",
            "ENTEL", "CENCOSUD", "ECL", "ENELAM", "BSANTANDER", "PARAUCO", "COLBUN"
    );
    private static final Map<String, List<String>> IPSA_ALIASES = createAliases();
    private static volatile CacheEntry cache;

    private IpsaPortfolioService() {
    }

    public static boolean isEnabled(Properties properties) {
        return Boolean.parseBoolean(properties.getProperty(PROP_ENABLED, "true"));
    }

    public static String getPortfolioName(Properties properties) {
        return properties.getProperty(PROP_NAME, DEFAULT_PORTFOLIO_NAME).trim();
    }

    public static BlotterMessage.Portfolio buildPortfolio(String username, Properties properties) {
        return buildPortfolio(username, properties, getPortfolioName(properties));
    }

    public static BlotterMessage.Portfolio buildPortfolio(String username, Properties properties, String portfolioName) {
        String resolvedPortfolioName = portfolioName == null || portfolioName.isBlank()
                ? getPortfolioName(properties)
                : portfolioName.trim();
        BlotterMessage.Portfolio.Builder builder = BlotterMessage.Portfolio.newBuilder()
                .setId(resolvedPortfolioName)
                .setNamePortfolio(resolvedPortfolioName)
                .setUsername(username == null ? "" : username);

        for (String symbol : resolveIpsaSymbols(properties)) {
            builder.addAsset(buildAsset(symbol));
        }

        return builder.build();
    }

    public static List<String> resolveIpsaSymbols(Properties properties) {
        if (!isEnabled(properties)) {
            return List.of();
        }

        CacheEntry current = cache;
        Instant now = Instant.now();
        if (current != null && current.expiresAt.isAfter(now)) {
            return current.symbols;
        }

        synchronized (IpsaPortfolioService.class) {
            current = cache;
            now = Instant.now();
            if (current != null && current.expiresAt.isAfter(now)) {
                return current.symbols;
            }

            List<String> resolved = fetchSymbols(properties);
            long refreshMinutes = parseLong(properties.getProperty(PROP_REFRESH_MINUTES), 360L);
            cache = new CacheEntry(List.copyOf(resolved), now.plus(Duration.ofMinutes(Math.max(5L, refreshMinutes))));
            return cache.symbols;
        }
    }

    private static List<String> fetchSymbols(Properties properties) {
        List<String> fallback = fallbackSymbols(properties);
        String url = properties.getProperty(PROP_URL, DEFAULT_URL).trim();

        try {
            String html = download(url, properties);
            List<String> fromHtml = extractSymbolsFromHtml(html, fallback);
            if (fromHtml.size() >= 20) {
                log.info("IPSA portfolio actualizado desde {} con {} simbolos", url, fromHtml.size());
                return fromHtml;
            }
            log.warn("IPSA source {} devolvio {} simbolos detectados; usando fallback", url, fromHtml.size());
        } catch (Exception e) {
            log.warn("No fue posible actualizar portfolio IPSA desde {}: {}", url, e.getMessage());
        }

        return fallback;
    }

    private static String download(String url, Properties properties) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(Math.max(5L, parseLong(properties.getProperty(PROP_TIMEOUT_SECONDS), 15L))))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(Math.max(5L, parseLong(properties.getProperty(PROP_TIMEOUT_SECONDS), 15L))))
                .header("User-Agent", properties.getProperty(PROP_USER_AGENT, DEFAULT_USER_AGENT))
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "es-CL,es;q=0.9,en;q=0.8")
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() >= 400) {
            throw new IOException("HTTP " + response.statusCode());
        }
        return response.body();
    }

    private static List<String> extractSymbolsFromHtml(String html, List<String> fallbackOrder) {
        String normalizedHtml = normalize(html);
        Set<String> resolved = new LinkedHashSet<>();

        for (String symbol : fallbackOrder) {
            for (String alias : IPSA_ALIASES.getOrDefault(symbol, List.of(symbol))) {
                if (normalizedHtml.contains(normalize(alias))) {
                    resolved.add(symbol);
                    break;
                }
            }
        }

        return new ArrayList<>(resolved);
    }

    private static List<String> fallbackSymbols(Properties properties) {
        String raw = properties.getProperty(PROP_FALLBACK_SYMBOLS, "");
        if (raw == null || raw.isBlank()) {
            return DEFAULT_SYMBOLS;
        }

        List<String> symbols = new ArrayList<>();
        for (String token : raw.split(",")) {
            String value = canonicalSymbol(token == null ? "" : token.trim().toUpperCase(Locale.ROOT));
            if (!value.isBlank()) {
                symbols.add(value);
            }
        }
        return symbols.isEmpty() ? DEFAULT_SYMBOLS : List.copyOf(symbols);
    }

    private static BlotterMessage.Asset buildAsset(String symbol) {
        MarketDataMessage.Statistic statistic = MarketDataMessage.Statistic.newBuilder()
                .setId(IDGenerator.getID())
                .setSymbol(symbol)
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .setSettlType(RoutingMessage.SettlType.T2)
                .setSecurityType(RoutingMessage.SecurityType.CS)
                .build();

        return BlotterMessage.Asset.newBuilder()
                .setId(symbol)
                .setSymbol(symbol)
                .setStatistic(statistic)
                .setSecurityexchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .build();
    }

    private static String normalize(String value) {
        String normalized = Normalizer.normalize(value == null ? "" : value, Normalizer.Form.NFD);
        return normalized.replaceAll("\\p{M}+", "")
                .replace('\u00A0', ' ')
                .toLowerCase(Locale.ROOT);
    }

    private static long parseLong(String raw, long fallback) {
        try {
            return Long.parseLong(raw);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static String canonicalSymbol(String symbol) {
        if ("CENCOSHOPP".equalsIgnoreCase(symbol) || "CENCOSUDSHOPP".equalsIgnoreCase(symbol)) {
            return "CENCOMALLS";
        }
        return symbol;
    }

    private static Map<String, List<String>> createAliases() {
        Map<String, List<String>> aliases = new LinkedHashMap<>();
        aliases.put("AGUAS-A", List.of("Aguas Andinas SA", "Aguas Andinas"));
        aliases.put("ANDINA-B", List.of("Andina-B", "Embotelladora Andina SA", "Embotelladora Andina B"));
        aliases.put("BCI", List.of("Banco de Credito e Inversiones", "Banco de Credito e Inversion", "BCI"));
        aliases.put("CAP", List.of("CAP SA", "CAP"));
        aliases.put("CCU", List.of("CCU", "Compania Cervecerias Unidas"));
        aliases.put("CENCOMALLS", List.of(
                "Cenco Malls SA",
                "Cenco Malls",
                "Cencosud Shopping SA",
                "Cencosud Shopping",
                "CENCOSHOPP"
        ));
        aliases.put("CENCOSUD", List.of("Cencosud SA", "Cencosud"));
        aliases.put("CHILE", List.of("Banco de Chile"));
        aliases.put("CMPC", List.of("CMPC"));
        aliases.put("COLBUN", List.of("Colbun SA", "Colbun"));
        aliases.put("CONCHATORO", List.of("Vina Concha y Toro SA", "Concha y Toro"));
        aliases.put("COPEC", List.of("Copec SA", "Copec"));
        aliases.put("ECL", List.of("Engie Energia Chile SA", "Engie Energia Chile", "ECL"));
        aliases.put("ENELAM", List.of("Enel Americas SA", "Enel Americas"));
        aliases.put("ENELCHILE", List.of("Enel Chile SA", "Enel Chile"));
        aliases.put("ENTEL", List.of("Entel SA", "Entel"));
        aliases.put("FALABELLA", List.of("Falabella SA", "Falabella"));
        aliases.put("IAM", List.of("Inversiones Aguas Metropolitanas SA", "Inversiones Aguas Metropolitanas"));
        aliases.put("ILC", List.of("ILC", "Inversiones La Construccion", "Inv La Construccion"));
        aliases.put("LTM", List.of("Latam Airlines", "LATAM Airlines Group", "LATAM"));
        aliases.put("MALLPLAZA", List.of("Mallplaza", "Plaza SA"));
        aliases.put("PARAUCO", List.of("Parque Arauco SA", "Parque Arauco"));
        aliases.put("RIPLEY", List.of("Ripley Corp", "Ripley"));
        aliases.put("SALFACORP", List.of("Salfacorp", "SalfaCorp"));
        aliases.put("SMU", List.of("SMU SA", "SMU"));
        aliases.put("SONDA", List.of("Sonda SA", "Sonda"));
        aliases.put("SQM-B", List.of("SQM-B", "Sociedad Quimica y Minera de Chile SA B"));
        aliases.put("BSANTANDER", List.of("Santander Chile", "Banco Santander-Chile", "Banco Santander Chile"));
        aliases.put("VAPORES", List.of("Vapores", "Compania Sud Americana de Vapores SA", "Sud Americana de Vapores"));
        return aliases;
    }

    private static final class CacheEntry {
        private final List<String> symbols;
        private final Instant expiresAt;

        private CacheEntry(List<String> symbols, Instant expiresAt) {
            this.symbols = symbols;
            this.expiresAt = expiresAt;
        }
    }
}
