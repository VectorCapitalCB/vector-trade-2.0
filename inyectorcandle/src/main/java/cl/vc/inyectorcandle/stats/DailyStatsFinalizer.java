package cl.vc.inyectorcandle.stats;

import cl.vc.inyectorcandle.model.InstrumentDailyStats;
import cl.vc.inyectorcandle.model.MarketDaySummary;
import cl.vc.inyectorcandle.model.MarketSession;
import cl.vc.inyectorcandle.mongo.MongoMarketRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;

/**
 * Cierre del proceso de inyeccion: encadena los cierres entre dias y reconstruye el agregado de
 * mercado sobre {@code instrument_daily} ya consolidado.
 * <p>
 * Corre una sola vez y al final a proposito. Cada replay por separado ve solo su fuente, y la
 * variacion del dia necesita el cierre del dia habil anterior, que puede haber llegado por la otra.
 */
public final class DailyStatsFinalizer {
    private static final Logger LOG = LoggerFactory.getLogger(DailyStatsFinalizer.class);
    private static final int RANKING_SIZE = 20;

    private final MongoMarketRepository repository;
    private final String market;
    private final MarketSession session;

    public DailyStatsFinalizer(MongoMarketRepository repository, String market, MarketSession session) {
        this.repository = repository;
        this.market = market;
        this.session = session;
    }

    public void run() {
        repository.fillMissingPreviousClose(market);

        List<LocalDate> days = repository.distinctTradingDays(market);
        int written = 0;
        for (LocalDate day : days) {
            if (runForDay(day)) {
                written++;
            }
        }
        LOG.info("Agregados diarios reconstruidos={} de {} dias", written, days.size());
    }

    /** @return true si el dia tenia instrumentos transados y se escribio su agregado */
    public boolean runForDay(LocalDate day) {
        List<InstrumentDailyStats> rows = repository.loadInstrumentDaily(market, day);
        MarketDaySummary summary = MarketDaySummary.of(market, day, rows, RANKING_SIZE, session);
        if (summary == null) {
            LOG.info("[MERCADO][{}] sin instrumentos transados, no se escribe agregado", day);
            return false;
        }
        repository.upsertMarketDayHistory(summary);
        LOG.info("[MERCADO][{}] instrumentos={} volumen={} monto={} indice={} trades={} sentPos={}% tendencia={}",
                day, rows.size(), fmt(summary.totalVolumen()), fmt(summary.montoTotal()),
                fmt(summary.indicePromedio()), summary.numeroTotalTrades(),
                fmt(summary.sentimientoPositivo()), summary.tendenciaGeneral());
        return true;
    }

    private static String fmt(double value) {
        return String.format(java.util.Locale.US, "%,.2f", value);
    }
}
