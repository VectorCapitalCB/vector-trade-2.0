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
public final class IgpaPortfolioService {

    public static final String DEFAULT_PORTFOLIO_NAME = "IGPA";
    private static final String PROP_ENABLED = "igpa.portfolio.enabled";
    private static final String PROP_NAME = "igpa.portfolio.name";
    private static final String PROP_URL = "igpa.portfolio.url";
    private static final String PROP_REFRESH_MINUTES = "igpa.portfolio.refresh.minutes";
    private static final String PROP_TIMEOUT_SECONDS = "igpa.portfolio.timeout.seconds";
    private static final String PROP_USER_AGENT = "igpa.portfolio.user.agent";
    private static final String PROP_FALLBACK_SYMBOLS = "igpa.portfolio.fallback.symbols";
    private static final String DEFAULT_URL = "https://es.investing.com/indices/igpa-components";
    private static final String DEFAULT_USER_AGENT =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";
    private static final List<String> DEFAULT_SYMBOLS = List.of(
            "AGUAS-A", "ANDINA-A", "ANDINA-B", "ANTARCHILE", "BCI", "BESALCO", "CAP", "CMPC",
            "CCU", "CENCOSUD", "CHILE", "ALMENDRAL", "CONCHATORO", "CAMANCHACA", "ITAUCL",
            "COLBUN", "COPEC", "CRISTALES", "ECL", "ECH", "EMBONOR-B", "ENELGXCH", "ENELAM",
            "ENTEL", "ENAEX", "FALABELLA", "FORUS", "HITES", "IAM", "INGEVEC", "ILC",
            "INVERCAP", "ABC", "MASISA", "MULTI X", "NORTEGRAN", "ORO BLANCO", "PARAUCO",
            "PAZ", "QUINENCO", "RIPLEY", "SMSAAM", "SALFACORP", "SOCOVESA", "SK", "SONDA",
            "SQM-B", "BSANTANDER", "VAPORES", "WATTS", "BLUMAR", "ENELCHILE", "TRICOT", "SMU",
            "CENCOMALLS",
            "SALMOCAM", "MALLPLAZA", "MANQUEHUE"
    );
    private static final Map<String, List<String>> IGPA_ALIASES = createAliases();
    private static volatile CacheEntry cache;

    private IgpaPortfolioService() {
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

        for (String symbol : resolveIgpaSymbols(properties)) {
            builder.addAsset(buildAsset(symbol));
        }

        return builder.build();
    }

    public static List<String> resolveIgpaSymbols(Properties properties) {
        if (!isEnabled(properties)) {
            return List.of();
        }

        CacheEntry current = cache;
        Instant now = Instant.now();
        if (current != null && current.expiresAt.isAfter(now)) {
            return current.symbols;
        }

        synchronized (IgpaPortfolioService.class) {
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
            if (fromHtml.size() >= 30) {
                log.info("IGPA portfolio actualizado desde {} con {} simbolos", url, fromHtml.size());
                return fromHtml;
            }
            log.warn("IGPA source {} devolvio {} simbolos detectados; usando fallback", url, fromHtml.size());
        } catch (Exception e) {
            log.warn("No fue posible actualizar portfolio IGPA desde {}: {}", url, e.getMessage());
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
            for (String alias : IGPA_ALIASES.getOrDefault(symbol, List.of(symbol))) {
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
        if ("MULTI-X".equalsIgnoreCase(symbol)) {
            return "MULTI X";
        }
        if ("NORTEGRANDE".equalsIgnoreCase(symbol)) {
            return "NORTEGRAN";
        }
        if ("OROBLANCO".equalsIgnoreCase(symbol)) {
            return "ORO BLANCO";
        }
        return symbol;
    }

    private static Map<String, List<String>> createAliases() {
        Map<String, List<String>> aliases = new LinkedHashMap<>();
        aliases.put("AGUAS-A", List.of("Aguas Andinas"));
        aliases.put("ANDINA-A", List.of("Embotelladora Andina"));
        aliases.put("ANDINA-B", List.of("Embotelladora Andina B"));
        aliases.put("ANTARCHILE", List.of("Antar Chile"));
        aliases.put("BCI", List.of("Banco de Credito e Inversiones", "BCI"));
        aliases.put("BESALCO", List.of("Besalco"));
        aliases.put("CAP", List.of("Cap", "CAP"));
        aliases.put("CMPC", List.of("Empresas CMPC", "CMPC"));
        aliases.put("CCU", List.of("Cervecerias", "Compania Cervecerias Unidas", "CCU"));
        aliases.put("CENCOMALLS", List.of(
                "Cenco Malls SA",
                "Cenco Malls",
                "Cencosud Shopping SA",
                "Cencosud Shopping",
                "CENCOSHOPP"
        ));
        aliases.put("CENCOSUD", List.of("Cencosud"));
        aliases.put("CHILE", List.of("Banco De Chile", "Banco de Chile"));
        aliases.put("ALMENDRAL", List.of("Almendral"));
        aliases.put("CONCHATORO", List.of("Vina Concha To", "Concha y Toro"));
        aliases.put("CAMANCHACA", List.of("Pes Camanchaca"));
        aliases.put("ITAUCL", List.of("Itau CorpBanca"));
        aliases.put("COLBUN", List.of("Colbun"));
        aliases.put("COPEC", List.of("Empresas Copec", "Copec"));
        aliases.put("CRISTALES", List.of("Cristales"));
        aliases.put("ECL", List.of("Engie Energia Chile"));
        aliases.put("ECH", List.of("Eche Izquierdo"));
        aliases.put("EMBONOR-B", List.of("Embonor B"));
        aliases.put("ENELGXCH", List.of("Enel Generacion Chile"));
        aliases.put("ENELAM", List.of("ENEL Americas", "Enel Americas"));
        aliases.put("ENTEL", List.of("Empresa Nacional de Telecomunicaciones", "Entel"));
        aliases.put("ENAEX", List.of("Enaex"));
        aliases.put("FALABELLA", List.of("Falabella"));
        aliases.put("FORUS", List.of("Forus"));
        aliases.put("HITES", List.of("Hites"));
        aliases.put("IAM", List.of("Inversiones Aguas Metropolitanas"));
        aliases.put("INGEVEC", List.of("Ingevec"));
        aliases.put("ILC", List.of("Inv La Constru"));
        aliases.put("INVERCAP", List.of("Invercap"));
        aliases.put("ABC", List.of("ABC SA"));
        aliases.put("MASISA", List.of("Masisa"));
        aliases.put("MULTI X", List.of("MULTI X", "MULTI-X", "Multiexport Fo"));
        aliases.put("NORTEGRAN", List.of("NORTEGRAN", "NORTEGRANDE", "Norte Grande"));
        aliases.put("ORO BLANCO", List.of("ORO BLANCO", "OROBLANCO", "Oro Blanco"));
        aliases.put("PARAUCO", List.of("Parq Arauco", "Parque Arauco"));
        aliases.put("PAZ", List.of("Paz Corp"));
        aliases.put("QUINENCO", List.of("Quinenco"));
        aliases.put("RIPLEY", List.of("Ripley Corp"));
        aliases.put("SMSAAM", List.of("Sociedad Matriz"));
        aliases.put("SALFACORP", List.of("Salfacorp"));
        aliases.put("SOCOVESA", List.of("Socovesa"));
        aliases.put("SK", List.of("Sigdo Koppers"));
        aliases.put("SONDA", List.of("Sonda"));
        aliases.put("SQM-B", List.of("Soquimich B"));
        aliases.put("BSANTANDER", List.of("Santander Chile"));
        aliases.put("VAPORES", List.of("Vapores"));
        aliases.put("WATTS", List.of("Watts SA", "Watts"));
        aliases.put("BLUMAR", List.of("Blumar"));
        aliases.put("ENELCHILE", List.of("Enel Chile"));
        aliases.put("TRICOT", List.of("Empresas Tricot", "Tricot"));
        aliases.put("SMU", List.of("SMU"));
        aliases.put("SALMOCAM", List.of("Salmones Camanchaca"));
        aliases.put("MALLPLAZA", List.of("Plaza", "Mallplaza"));
        aliases.put("MANQUEHUE", List.of("Inmobiliaria Manquehue", "Manquehue"));
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
