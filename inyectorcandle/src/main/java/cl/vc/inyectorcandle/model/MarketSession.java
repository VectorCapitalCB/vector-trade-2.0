package cl.vc.inyectorcandle.model;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

/** Horario de rueda de una plaza. Configurable: BCS no es la unica bolsa que se va a inyectar. */
public record MarketSession(ZoneId zone, LocalTime open, LocalTime close) {

    public static MarketSession of(String zoneId, String open, String close) {
        return new MarketSession(ZoneId.of(zoneId), LocalTime.parse(open), LocalTime.parse(close));
    }

    public Instant start(LocalDate day) {
        return day.atTime(open).atZone(zone).toInstant();
    }

    public Instant end(LocalDate day) {
        return day.atTime(close).atZone(zone).toInstant();
    }
}
