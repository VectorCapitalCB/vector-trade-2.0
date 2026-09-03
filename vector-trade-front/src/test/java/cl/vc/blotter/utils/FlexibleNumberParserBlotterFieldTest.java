package cl.vc.blotter.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Fija la semantica de los campos numericos del blotter (precio, limite, spread, cantidad),
 * que debe ser identica a la de produccion: el PUNTO es siempre decimal y la coma es
 * separador de miles.
 *
 * <p>Regresion que cubre: el 2.0 leia esos campos con {@link FlexibleNumberParser#parse(String)},
 * cuya heuristica de formato chileno trata "1.234" como mil doscientos treinta y cuatro. En un
 * instrumento de 3 decimales eso multiplica el precio por 1000 sin aviso y la orden se ejecuta.
 */
class FlexibleNumberParserBlotterFieldTest {

    @Test
    void puntoEsSiempreDecimalEnCamposDelBlotter() {
        assertEquals(0.001d, FlexibleNumberParser.parseBlotterField("0.001"));
        assertEquals(1.234d, FlexibleNumberParser.parseBlotterField("1.234"));
        assertEquals(12.345d, FlexibleNumberParser.parseBlotterField("12.345"));
        assertEquals(123.456d, FlexibleNumberParser.parseBlotterField("123.456"));
        assertEquals(0.5d, FlexibleNumberParser.parseBlotterField("0.5"));
        assertEquals(1250d, FlexibleNumberParser.parseBlotterField("1250"));
    }

    @Test
    void comaEsSeparadorDeMiles() {
        assertEquals(83000d, FlexibleNumberParser.parseBlotterField("83,000"));
        assertEquals(1234567d, FlexibleNumberParser.parseBlotterField("1,234,567"));
        assertEquals(1000.75d, FlexibleNumberParser.parseBlotterField("1,000.75"));
    }

    @Test
    void toleraEspaciosSimbolosYValoresVacios() {
        assertEquals(20d, FlexibleNumberParser.parseBlotterField(" 20% "));
        assertEquals(1500.25d, FlexibleNumberParser.parseBlotterField("$1,500.25"));
        assertEquals(0d, FlexibleNumberParser.parseBlotterField(""));
        assertEquals(0d, FlexibleNumberParser.parseBlotterField(null));
        assertEquals(0d, FlexibleNumberParser.parseBlotterField("   "));
        assertEquals(-12.5d, FlexibleNumberParser.parseBlotterField("-12.5"));
    }

    @Test
    void laVarianteBigDecimalConservaLaEscalaDelCampo() {
        assertEquals(new BigDecimal("0.001"), FlexibleNumberParser.parseBlotterFieldBigDecimal("0.001"));
        assertEquals(new BigDecimal("1.234"), FlexibleNumberParser.parseBlotterFieldBigDecimal("1.234"));
        assertEquals(new BigDecimal("83000"), FlexibleNumberParser.parseBlotterFieldBigDecimal("83,000"));
        assertEquals(BigDecimal.ZERO, FlexibleNumberParser.parseBlotterFieldBigDecimal(""));
    }

    @Test
    void elParserDePlanillasNoSeAltera() {
        // parse() conserva su heuristica chilena: la usa el importador de baskets (OrdersHelper)
        // y esta congelada por OrdersHelperNumberParsingTest. No debe cambiar.
        assertEquals(80000d, FlexibleNumberParser.parse("80.000"));
        assertEquals(1234d, FlexibleNumberParser.parse("1.234"));
        // ...y por eso mismo NO sirve para un campo de precio:
        assertEquals(1.234d, FlexibleNumberParser.parseBlotterField("1.234"));
    }
}
