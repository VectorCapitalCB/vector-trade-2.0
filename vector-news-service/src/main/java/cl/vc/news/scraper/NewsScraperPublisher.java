package cl.vc.news.scraper;

import cl.vc.news.websocket.NewsSessionRegistry;
import org.eclipse.jetty.websocket.api.Session;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.URLEncoder;
import java.net.SocketException;
import java.net.SocketTimeoutException;

public class NewsScraperPublisher extends Thread {
    private static final Logger log = LoggerFactory.getLogger(NewsScraperPublisher.class);
    private static final String DEFAULT_SOURCES = String.join(",",
            "https://www.sec.gov/news/pressreleases.rss",
            "https://www.sec.gov/rss/litigation/litreleases.xml",
            "https://www.federalreserve.gov/feeds/press_all.xml",
            "https://www.bcentral.cl/o/web/banco-central/rss-noticias");
    private static final String DEFAULT_KEYWORDS = String.join(",",
            "chile", "chileno", "chilean", "eeuu", "usa", "united states",
            "fed", "federal reserve", "sec", "dolar", "dólar", "usd", "clp", "usdclp",
            "peso chileno", "treasury", "wall street", "nasdaq", "dow", "s&p",
            "inflation", "rate", "interest", "copper", "cobre");
    private static final List<DateTimeFormatter> ISO_DATE_PARSERS = Arrays.asList(
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            DateTimeFormatter.ISO_DATE_TIME
    );

    private final Properties properties;
    private final ConcurrentHashMap<String, Boolean> recentlySeen = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> errorCountsBySource = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> translationCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastSecFallbackLogMs = new ConcurrentHashMap<>();
    private final Deque<NewsMongoRepository.NewsRow> recentPublished = new LinkedList<>();
    private volatile boolean requirePublishedDate = true;
    private volatile int summaryMaxChars = 240;
    private volatile boolean alertsEnabled = true;
    private volatile boolean summaryEnabled = true;
    private volatile int summaryIntervalMinutes = 30;
    private volatile int summaryMaxItems = 8;
    private volatile long lastSummaryAtMs = 0L;

    public NewsScraperPublisher(Properties properties) {
        this.properties = properties;
        setName("news-scraper-publisher");
        setDaemon(true);
    }

    @Override
    public void run() {
        int pollMs = parseInt(properties.getProperty("news.scraper.poll.ms"), 15000);
        int timeoutMs = parseInt(properties.getProperty("news.scraper.timeout.ms"), 12000);
        int maxPerSource = parseInt(properties.getProperty("news.scraper.max.items.per.source"), 8);
        int maxGlobalPerCycle = parseInt(properties.getProperty("news.scraper.max.global.per.cycle"), 15);
        int maxAgeHours = parseInt(properties.getProperty("news.scraper.max.age.hours"), 72);
        requirePublishedDate = Boolean.parseBoolean(
                properties.getProperty("news.scraper.require.published.date", "true"));
        int retryCount = parseInt(properties.getProperty("news.scraper.retry.count"), 2);
        boolean onlyRelevant = Boolean.parseBoolean(properties.getProperty("news.scraper.only.relevant", "true"));
        boolean requirePriorityKeyword = Boolean.parseBoolean(
                properties.getProperty("news.scraper.require.priority.keyword", "true"));
        int minRelevanceScore = parseInt(properties.getProperty("news.scraper.min.relevance.score"), 3);
        alertsEnabled = Boolean.parseBoolean(properties.getProperty("news.alerts.enabled", "true"));
        summaryEnabled = Boolean.parseBoolean(properties.getProperty("news.summary.enabled", "true"));
        summaryIntervalMinutes = parseInt(properties.getProperty("news.summary.interval.minutes"), 30);
        summaryMaxItems = parseInt(properties.getProperty("news.summary.max.items"), 8);
        String userAgent = properties.getProperty("news.scraper.user.agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
        boolean translateEnabled = Boolean.parseBoolean(properties.getProperty("news.translate.enabled", "true"));
        String translateTargetLang = properties.getProperty("news.translate.target.lang", "es");
        int translateTimeoutMs = parseInt(properties.getProperty("news.translate.timeout.ms"), 7000);
        int translateMaxChars = parseInt(properties.getProperty("news.translate.max.chars"), 450);
        summaryMaxChars = parseInt(properties.getProperty("news.message.summary.max.chars"), 240);
        List<String> sources = csv(properties.getProperty("news.scraper.sources", DEFAULT_SOURCES));
        List<String> keywords = csv(properties.getProperty("news.scraper.keywords", DEFAULT_KEYWORDS));
        List<String> priorityKeywords = csv(properties.getProperty("news.scraper.priority.keywords", ""));

        log.info("News scraper iniciado pollMs={} sources={} onlyRelevant={}", pollMs, sources.size(), onlyRelevant);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                AtomicInteger publishedCount = new AtomicInteger(0);
                for (String source : sources) {
                    if (publishedCount.get() >= Math.max(1, maxGlobalPerCycle)) {
                        break;
                    }
                    scrapeSource(source, keywords, onlyRelevant, timeoutMs, maxPerSource, userAgent,
                            translateEnabled, translateTargetLang, translateTimeoutMs, translateMaxChars, retryCount,
                            priorityKeywords, minRelevanceScore, maxAgeHours, publishedCount, maxGlobalPerCycle,
                            requirePriorityKeyword);
                }
                maybePublishSummary();
                cleanupRecentlySeen();
                Thread.sleep(Math.max(3000, pollMs));
            } catch (InterruptedException e) {
                log.warn("News scraper interrumpido");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error general en news scraper", e);
            }
        }
    }

    private void scrapeSource(String sourceUrl,
                              List<String> keywords,
                              boolean onlyRelevant,
                              int timeoutMs,
                              int maxPerSource,
                              String userAgent,
                              boolean translateEnabled,
                              String translateTargetLang,
                              int translateTimeoutMs,
                              int translateMaxChars,
                              int retryCount,
                              List<String> priorityKeywords,
                              int minRelevanceScore,
                              int maxAgeHours,
                              AtomicInteger publishedCount,
                              int maxGlobalPerCycle,
                              boolean requirePriorityKeyword) {
        if (isBlank(sourceUrl)) {
            return;
        }
        List<String> alternatives = splitAlternatives(sourceUrl);
        Exception lastError = null;
        for (String candidateUrl : alternatives) {
            for (int attempt = 1; attempt <= Math.max(1, retryCount); attempt++) {
                try {
                    scrapeSingleUrl(candidateUrl, keywords, onlyRelevant, timeoutMs, maxPerSource, userAgent,
                            translateEnabled, translateTargetLang, translateTimeoutMs, translateMaxChars,
                            priorityKeywords, minRelevanceScore, maxAgeHours, publishedCount, maxGlobalPerCycle,
                            requirePriorityKeyword);
                    return;
                } catch (Exception e) {
                    lastError = e;
                    boolean transientError = isTransientNetworkError(e);
                    if (!transientError || attempt >= Math.max(1, retryCount)) {
                        break;
                    }
                    try {
                        Thread.sleep(500L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
        if (lastError != null) {
            logScrapeError(sourceUrl, lastError);
        }
    }

    private void scrapeSingleUrl(String sourceUrl,
                                 List<String> keywords,
                                 boolean onlyRelevant,
                                 int timeoutMs,
                                 int maxPerSource,
                                 String userAgent,
                                 boolean translateEnabled,
                                 String translateTargetLang,
                                  int translateTimeoutMs,
                                 int translateMaxChars,
                                 List<String> priorityKeywords,
                                 int minRelevanceScore,
                                 int maxAgeHours,
                                 AtomicInteger publishedCount,
                                 int maxGlobalPerCycle,
                                 boolean requirePriorityKeyword) throws Exception {
        try {
            String raw = Jsoup.connect(sourceUrl)
                    .ignoreContentType(true)
                    .timeout(Math.max(3000, timeoutMs))
                    .userAgent(userAgent)
                    .referrer("https://www.sec.gov/")
                    .header("Accept", "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9,es;q=0.8")
                    .execute()
                    .body();
            Document doc = Jsoup.parse(raw, "", Parser.xmlParser());
            Elements entries = doc.select("item, entry");
            int count = 0;
            for (Element item : entries) {
                if (count >= Math.max(1, maxPerSource)) {
                    break;
                }
                Candidate candidate = candidateFromItem(sourceUrl, item);
                if (candidate == null || isBlank(candidate.title)) {
                    continue;
                }
                if (publishedCount.get() >= Math.max(1, maxGlobalPerCycle)) {
                    break;
                }
                if (!publishCandidate(candidate, keywords, onlyRelevant, translateEnabled,
                        translateTargetLang, translateTimeoutMs, translateMaxChars, userAgent,
                        priorityKeywords, minRelevanceScore, maxAgeHours, publishedCount, maxGlobalPerCycle,
                        requirePriorityKeyword)) {
                    continue;
                }
                count++;
            }
            errorCountsBySource.remove(sourceUrl);
        } catch (HttpStatusException e) {
            int status = e.getStatusCode();
            if ((status == 403 || status == 404) && isSecLitigationFeed(sourceUrl, e.getUrl())) {
                logSecFallback(sourceUrl, status);
                scrapeSecLitigationHtml(timeoutMs, maxPerSource, userAgent, keywords, onlyRelevant,
                        translateEnabled, translateTargetLang, translateTimeoutMs, translateMaxChars,
                        priorityKeywords, minRelevanceScore, maxAgeHours, publishedCount, maxGlobalPerCycle,
                        requirePriorityKeyword);
                return;
            }
            throw e;
        }
    }

    private boolean publishCandidate(Candidate candidate,
                                     List<String> keywords,
                                     boolean onlyRelevant,
                                     boolean translateEnabled,
                                     String translateTargetLang,
                                     int translateTimeoutMs,
                                     int translateMaxChars,
                                     String userAgent,
                                     List<String> priorityKeywords,
                                     int minRelevanceScore,
                                     int maxAgeHours,
                                     AtomicInteger publishedCount,
                                     int maxGlobalPerCycle,
                                     boolean requirePriorityKeyword) {
        if (publishedCount.get() >= Math.max(1, maxGlobalPerCycle)) {
            return false;
        }
        long now = System.currentTimeMillis();
        long maxAgeMillis = Math.max(1, maxAgeHours) * 3600_000L;
        if (requirePublishedDate && candidate.publishedAt <= 0) {
            return false;
        }
        if (candidate.publishedAt > 0 && (now - candidate.publishedAt) > maxAgeMillis) {
            return false;
        }

        String relevanceText = (candidate.title + " " + candidate.summary).toLowerCase(Locale.ROOT);
        boolean hasPrioritySignal = containsKeyword(relevanceText, priorityKeywords);
        if (requirePriorityKeyword && !hasPrioritySignal) {
            return false;
        }
        int score = relevanceScore(candidate.title, relevanceText, keywords, priorityKeywords);
        if (onlyRelevant && score < Math.max(1, minRelevanceScore)) {
            return false;
        }

        String hash = sha256(normalize(candidate.title) + "|" + normalize(candidate.url));
        if (hash.isEmpty() || recentlySeen.putIfAbsent(hash, Boolean.TRUE) != null) {
            return false;
        }

        NewsMongoRepository.NewsRow row = new NewsMongoRepository.NewsRow();
        row.source = candidate.source;
        row.title = candidate.title;
        row.url = candidate.url;
        row.hash = hash;
        row.publishedAt = candidate.publishedAt;
        row.scrapedAt = System.currentTimeMillis();
        String impact = alertsEnabled ? classifyImpact(score, relevanceText) : "NORMAL";
        row.impact = impact;
        row.type = "news";
        String displayTitle = candidate.title;
        String displaySummary = candidate.summary;
        if (translateEnabled) {
            String translated = translateTo(displayTitle, translateTargetLang, translateTimeoutMs, translateMaxChars, userAgent);
            if (!isBlank(translated)) {
                displayTitle = translated;
            }
            if (!isBlank(displaySummary)) {
                String translatedSummary = translateTo(displaySummary, translateTargetLang, translateTimeoutMs, translateMaxChars, userAgent);
                if (!isBlank(translatedSummary)) {
                    displaySummary = translatedSummary;
                }
            }
        }
        row.message = composeMessageWithSummary(row.source, displayTitle, displaySummary, row.publishedAt, summaryMaxChars, impact);

        boolean inserted = NewsMongoRepository.saveIfNew(row);
        if (!inserted) {
            return false;
        }
        broadcastNews(row, impact, "news");
        keepRecent(row);
        publishedCount.incrementAndGet();
        return true;
    }

    private void scrapeSecLitigationHtml(int timeoutMs,
                                         int maxPerSource,
                                         String userAgent,
                                         List<String> keywords,
                                         boolean onlyRelevant,
                                         boolean translateEnabled,
                                         String translateTargetLang,
                                         int translateTimeoutMs,
                                         int translateMaxChars,
                                         List<String> priorityKeywords,
                                         int minRelevanceScore,
                                         int maxAgeHours,
                                         AtomicInteger publishedCount,
                                         int maxGlobalPerCycle,
                                         boolean requirePriorityKeyword) {
        String url = "https://www.sec.gov/enforcement-litigation/litigation-releases";
        try {
            Document doc = Jsoup.connect(url)
                    .timeout(Math.max(3000, timeoutMs))
                    .userAgent(userAgent)
                    .referrer("https://www.sec.gov/")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .get();

            Elements rows = doc.select("table tbody tr");
            int count = 0;
            for (Element row : rows) {
                if (count >= Math.max(1, maxPerSource)) {
                    break;
                }
                Elements tds = row.select("td");
                if (tds.isEmpty()) {
                    continue;
                }

                String dateRaw = tds.get(0).text();
                String title = tds.size() > 1 ? tds.get(1).text() : "";
                Element link = row.selectFirst("a[href]");
                String href = link == null ? "" : link.attr("abs:href");
                if (isBlank(href) && link != null) {
                    String rel = link.attr("href");
                    if (!isBlank(rel)) {
                        href = rel.startsWith("http") ? rel : "https://www.sec.gov" + rel;
                    }
                }

                if (isBlank(title)) {
                    continue;
                }

                Candidate c = new Candidate();
                c.source = "SEC-LITIGATION";
                c.title = title;
                c.url = href;
                c.summary = title;
                c.publishedAt = parseDateMillis(dateRaw);

                if (publishedCount.get() >= Math.max(1, maxGlobalPerCycle)) {
                    break;
                }
                if (publishCandidate(c, keywords, onlyRelevant, translateEnabled, translateTargetLang,
                        translateTimeoutMs, translateMaxChars, userAgent,
                        priorityKeywords, minRelevanceScore, maxAgeHours, publishedCount, maxGlobalPerCycle,
                        requirePriorityKeyword)) {
                    count++;
                }
            }
            errorCountsBySource.remove(url);
        } catch (Exception e) {
            logScrapeError(url, e);
        }
    }

    private void broadcastNews(NewsMongoRepository.NewsRow row, String impact, String type) {
        JSONObject payload = new JSONObject()
                .put("type", type == null ? "news" : type)
                .put("message", row.message)
                .put("source", safe(row.source))
                .put("title", safe(row.title))
                .put("url", safe(row.url))
                .put("impact", impact == null ? "NORMAL" : impact)
                .put("publishedAt", row.publishedAt);

        String text = payload.toString();
        for (Session session : NewsSessionRegistry.sessions()) {
            try {
                if (session != null && session.isOpen()) {
                    session.getRemote().sendString(text);
                }
            } catch (Exception e) {
                log.warn("No se pudo enviar news a session={}", session, e);
            }
        }
        log.info("News publicada {}", row.message);
    }

    private Candidate candidateFromItem(String sourceUrl, Element item) {
        String title = firstNonBlank(
                item.selectFirst("title") != null ? item.selectFirst("title").text() : null);
        String summary = firstNonBlank(
                item.selectFirst("description") != null ? item.selectFirst("description").text() : null,
                item.selectFirst("summary") != null ? item.selectFirst("summary").text() : null,
                item.selectFirst("content") != null ? item.selectFirst("content").text() : null
        );

        String url = "";
        Element linkEl = item.selectFirst("link");
        if (linkEl != null) {
            String href = linkEl.attr("href");
            url = !isBlank(href) ? href : linkEl.text();
        }
        long publishedAt = parseDateMillis(firstNonBlank(
                item.selectFirst("pubDate") != null ? item.selectFirst("pubDate").text() : null,
                item.selectFirst("published") != null ? item.selectFirst("published").text() : null,
                item.selectFirst("updated") != null ? item.selectFirst("updated").text() : null,
                item.selectFirst("dc|date") != null ? item.selectFirst("dc|date").text() : null
        ));

        Candidate c = new Candidate();
        c.source = hostLabel(sourceUrl);
        c.title = title == null ? "" : title.trim();
        c.summary = summary == null ? "" : summary.trim();
        c.url = url == null ? "" : url.trim();
        c.publishedAt = publishedAt;
        return c;
    }

    private String composeMessageWithSummary(String source,
                                             String title,
                                             String summary,
                                             long publishedAtMillis,
                                             int maxSummaryChars,
                                             String impact) {
        String base = NewsMongoRepository.composeMessage(source, title, publishedAtMillis > 0
                ? publishedAtMillis : System.currentTimeMillis());
        String tag = isBlank(impact) ? "" : "[" + impact.toUpperCase(Locale.ROOT) + "] ";
        base = tag + base;
        if (isBlank(summary)) {
            return base;
        }
        String cleanSummary = summary.replaceAll("\\s+", " ").trim();
        if (cleanSummary.equalsIgnoreCase(title == null ? "" : title.trim())) {
            return base;
        }
        int limit = Math.max(80, maxSummaryChars);
        if (cleanSummary.length() > limit) {
            cleanSummary = cleanSummary.substring(0, limit) + "...";
        }
        return base + " | " + cleanSummary;
    }

    private String classifyImpact(int score, String relevanceText) {
        String text = relevanceText == null ? "" : relevanceText;
        if (text.contains("cpi") || text.contains("nfp") || text.contains("fomc")
                || text.contains("fed") || text.contains("tpm")
                || text.contains("banco central") || text.contains("usdclp")
                || text.contains("caída de acciones") || text.contains("caida de acciones")) {
            return "ALTA";
        }
        if (score >= 12) {
            return "ALTA";
        }
        if (score >= 6) {
            return "MEDIA";
        }
        return "BAJA";
    }

    private void keepRecent(NewsMongoRepository.NewsRow row) {
        synchronized (recentPublished) {
            recentPublished.addFirst(row);
            while (recentPublished.size() > 300) {
                recentPublished.removeLast();
            }
        }
    }

    private void maybePublishSummary() {
        if (!summaryEnabled) {
            return;
        }
        long now = System.currentTimeMillis();
        long intervalMs = Math.max(1, summaryIntervalMinutes) * 60_000L;
        if (lastSummaryAtMs > 0 && (now - lastSummaryAtMs) < intervalMs) {
            return;
        }
        List<NewsMongoRepository.NewsRow> rows = new ArrayList<>();
        synchronized (recentPublished) {
            for (NewsMongoRepository.NewsRow row : recentPublished) {
                rows.add(row);
                if (rows.size() >= Math.max(1, summaryMaxItems)) {
                    break;
                }
            }
        }
        if (rows.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[RESUMEN] Top noticias recientes: ");
        int i = 0;
        for (NewsMongoRepository.NewsRow r : rows) {
            if (i > 0) {
                sb.append(" || ");
            }
            String title = safe(r.title);
            if (title.length() > 70) {
                title = title.substring(0, 70) + "...";
            }
            sb.append(title);
            i++;
        }

        NewsMongoRepository.NewsRow summary = new NewsMongoRepository.NewsRow();
        summary.source = "SYSTEM";
        summary.title = "Resumen de mercado";
        summary.url = "";
        summary.publishedAt = now;
        summary.scrapedAt = now;
        summary.hash = sha256("summary|" + (now / intervalMs) + "|" + sb);
        summary.message = sb.toString();
        summary.impact = "MEDIA";
        summary.type = "news_summary";
        if (NewsMongoRepository.saveIfNew(summary)) {
            broadcastNews(summary, "MEDIA", "news_summary");
            keepRecent(summary);
            lastSummaryAtMs = now;
        }
    }

    private long parseDateMillis(String raw) {
        if (isBlank(raw)) {
            return 0L;
        }
        String value = raw.trim();

        for (DateTimeFormatter formatter : ISO_DATE_PARSERS) {
            try {
                return OffsetDateTime.parse(value, formatter).toInstant().toEpochMilli();
            } catch (Exception ignored) {
            }
        }

        List<String> patterns = Arrays.asList(
                "MMMM d, yyyy",
                "MMM d, yyyy",
                "MMM. d, yyyy",
                "EEE, dd MMM yyyy HH:mm:ss zzz",
                "dd MMM yyyy HH:mm:ss zzz",
                "yyyy-MM-dd'T'HH:mm:ssXXX",
                "yyyy-MM-dd'T'HH:mm:ss'Z'",
                "yyyy-MM-dd HH:mm:ss"
        );
        for (String p : patterns) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(p, Locale.ENGLISH);
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date date = sdf.parse(value);
                if (date != null) {
                    return date.getTime();
                }
            } catch (Exception ignored) {
            }
        }
        return 0L;
    }

    private List<String> csv(String raw) {
        if (isBlank(raw)) {
            return Collections.emptyList();
        }
        List<String> rows = new ArrayList<>();
        for (String part : raw.split(",")) {
            if (!isBlank(part)) {
                rows.add(part.trim());
            }
        }
        return rows;
    }

    private boolean containsKeyword(String text, List<String> keywords) {
        if (isBlank(text) || keywords == null || keywords.isEmpty()) {
            return false;
        }
        String value = text.toLowerCase(Locale.ROOT);
        for (String keyword : keywords) {
            if (!isBlank(keyword) && value.contains(keyword.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private int relevanceScore(String title,
                               String fullTextLower,
                               List<String> keywords,
                               List<String> priorityKeywords) {
        String ttl = title == null ? "" : title.toLowerCase(Locale.ROOT);
        String text = fullTextLower == null ? "" : fullTextLower;
        int score = 0;

        if (priorityKeywords != null) {
            for (String k : priorityKeywords) {
                if (isBlank(k)) {
                    continue;
                }
                String kw = k.toLowerCase(Locale.ROOT);
                if (ttl.contains(kw)) {
                    score += 4;
                } else if (text.contains(kw)) {
                    score += 2;
                }
            }
        }
        if (keywords != null) {
            for (String k : keywords) {
                if (isBlank(k)) {
                    continue;
                }
                String kw = k.toLowerCase(Locale.ROOT);
                if (ttl.contains(kw)) {
                    score += 2;
                } else if (text.contains(kw)) {
                    score += 1;
                }
            }
        }
        return score;
    }

    private int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return fallback;
        }
    }

    private String hostLabel(String url) {
        if (isBlank(url)) {
            return "source";
        }
        String value = url.trim();
        int i = value.indexOf("://");
        if (i >= 0) {
            value = value.substring(i + 3);
        }
        int slash = value.indexOf('/');
        if (slash > 0) {
            value = value.substring(0, slash);
        }
        return value.toUpperCase(Locale.ROOT);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (!isBlank(value)) {
                return value.trim();
            }
        }
        return "";
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private String sha256(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private List<String> splitAlternatives(String raw) {
        if (isBlank(raw)) {
            return Collections.emptyList();
        }
        if (!raw.contains("|")) {
            return List.of(raw.trim());
        }
        List<String> out = new ArrayList<>();
        for (String part : raw.split("\\|")) {
            if (!isBlank(part)) {
                out.add(part.trim());
            }
        }
        return out.isEmpty() ? List.of(raw.trim()) : out;
    }

    private void cleanupRecentlySeen() {
        if (recentlySeen.size() <= 5000) {
            return;
        }
        recentlySeen.clear();
    }

    private boolean isTransientNetworkError(Throwable e) {
        Throwable cur = e;
        while (cur != null) {
            if (cur instanceof SocketException || cur instanceof SocketTimeoutException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private String translateTo(String text, String targetLang, int timeoutMs, int maxChars, String userAgent) {
        if (isBlank(text)) {
            return text;
        }
        String trimmed = text.trim();
        if (trimmed.length() > Math.max(50, maxChars)) {
            trimmed = trimmed.substring(0, Math.max(50, maxChars));
        }
        String key = targetLang + "|" + trimmed;
        String cached = translationCache.get(key);
        if (cached != null) {
            return cached;
        }
        try {
            String q = URLEncoder.encode(trimmed, StandardCharsets.UTF_8);
            String url = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl="
                    + URLEncoder.encode(targetLang, StandardCharsets.UTF_8)
                    + "&dt=t&q=" + q;
            String raw = Jsoup.connect(url)
                    .ignoreContentType(true)
                    .timeout(Math.max(2000, timeoutMs))
                    .userAgent(userAgent)
                    .execute()
                    .body();
            JSONArray root = new JSONArray(raw);
            JSONArray parts = root.optJSONArray(0);
            if (parts == null) {
                return trimmed;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length(); i++) {
                JSONArray p = parts.optJSONArray(i);
                if (p != null) {
                    String piece = p.optString(0, "");
                    if (!piece.isEmpty()) {
                        sb.append(piece);
                    }
                }
            }
            String translated = sb.length() == 0 ? trimmed : sb.toString();
            translationCache.put(key, translated);
            if (translationCache.size() > 10000) {
                translationCache.clear();
            }
            return translated;
        } catch (Exception e) {
            return trimmed;
        }
    }

    private boolean isSecLitigationFeed(String sourceUrl, String effectiveUrl) {
        return isSecLitigationFeedUrl(sourceUrl) || isSecLitigationFeedUrl(effectiveUrl);
    }

    private boolean isSecLitigationFeedUrl(String url) {
        String value = url == null ? "" : url.toLowerCase(Locale.ROOT);
        return value.contains("sec.gov/rss/litigation/litreleases.xml")
                || value.contains("sec.gov/litigation/litreleases/rss");
    }

    private void logScrapeError(String sourceUrl, Exception e) {
        AtomicInteger count = errorCountsBySource.computeIfAbsent(sourceUrl, k -> new AtomicInteger(0));
        int current = count.incrementAndGet();
        boolean transientError = isTransientNetworkError(e);
        if (transientError && current < 5) {
            log.debug("Error transitorio leyendo fuente {} ({} intentos): {}", sourceUrl, current, e.getMessage());
            return;
        }
        if (current == 1 || current % 20 == 0) {
            log.warn("Error leyendo fuente {} ({} intentos)", sourceUrl, current, e);
        } else {
            log.debug("Error leyendo fuente {} ({} intentos): {}", sourceUrl, current, e.getMessage());
        }
    }

    private void logSecFallback(String sourceUrl, int status) {
        long now = System.currentTimeMillis();
        long cooldownMs = 30L * 60L * 1000L; // 30 minutos
        Long last = lastSecFallbackLogMs.get(sourceUrl);
        if (last == null || (now - last) >= cooldownMs) {
            lastSecFallbackLogMs.put(sourceUrl, now);
            log.warn("SEC RSS no disponible (status={}) para {}, usando fallback HTML", status, sourceUrl);
        } else {
            log.debug("SEC RSS sigue no disponible (status={}) para {}, fallback activo", status, sourceUrl);
        }
    }

    private static class Candidate {
        String source;
        String title;
        String summary;
        String url;
        long publishedAt;
    }
}
