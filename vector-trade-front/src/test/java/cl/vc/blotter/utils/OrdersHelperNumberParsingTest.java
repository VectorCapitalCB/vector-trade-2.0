package cl.vc.blotter.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrdersHelperNumberParsingTest {

    @Test
    void parsesChileanAndUsThousands() {
        assertEquals(80_000d, OrdersHelper.parseNumber("80.000"));
        assertEquals(80_000d, OrdersHelper.parseNumber("80,000"));
        assertEquals(1_234_567d, OrdersHelper.parseNumber("1.234.567"));
        assertEquals(1_234_567d, OrdersHelper.parseNumber("1,234,567"));
    }

    @Test
    void parsesChileanAndUsDecimals() {
        assertEquals(1_301.50d, OrdersHelper.parseNumber("1.301,50"));
        assertEquals(1_301.50d, OrdersHelper.parseNumber("1,301.50"));
        assertEquals(2d, OrdersHelper.parseNumber("2,00"));
        assertEquals(2d, OrdersHelper.parseNumber("2.00"));
    }

    @Test
    void ignoresExcelSpacesAndCurrencyCharacters() {
        assertEquals(3_000d, OrdersHelper.parseNumber(" 3\u00A0000 "));
        assertEquals(25.20d, OrdersHelper.parseNumber("$25,20"));
    }
}
