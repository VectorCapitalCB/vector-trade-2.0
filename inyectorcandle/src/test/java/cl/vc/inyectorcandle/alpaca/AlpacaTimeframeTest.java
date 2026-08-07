package cl.vc.inyectorcandle.alpaca;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AlpacaTimeframeTest {

    @Test
    void mapsEveryConfiguredTimeframeToAnAlpacaValue() {
        assertEquals("1Min", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("PT1M")));
        assertEquals("5Min", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("PT5M")));
        assertEquals("15Min", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("PT15M")));
        assertEquals("30Min", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("PT30M")));
        assertEquals("1Hour", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("PT1H")));
        assertEquals("4Hour", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("PT4H")));
        assertEquals("1Day", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("P1D")));
        assertEquals("1Week", AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("P7D")));
    }

    @Test
    void rejectsTimeframesOutsideTheAlpacaGrid() {
        // Alpaca acepta [1-59]Min, [1-23]Hour, 1Day, 1Week y [1,2,3,4,6,12]Month.
        assertThrows(IllegalArgumentException.class, () -> AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("PT90S")));
        assertThrows(IllegalArgumentException.class, () -> AlpacaCandleSource.toAlpacaTimeframe(Duration.parse("P2D")));
    }
}
