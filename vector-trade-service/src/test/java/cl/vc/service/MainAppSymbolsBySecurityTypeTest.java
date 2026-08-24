package cl.vc.service;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clasificacion de simbolos BCS por SecurityType, que es lo que usa la recarga de cierres del Admin
 * para ir en el orden CS -> CFI -> ETF por lotes.
 *
 * Importa porque close_prices NO trae el tipo de instrumento: la unica fuente es la SecurityList del
 * sellside. Si esta clasificacion falla, la recarga lee los papeles equivocados o ninguno.
 *
 * securityExchangeMaps es privado y estatico, asi que se inyecta por reflexion (mismo estilo que
 * OrderServletTest).
 */
public class MainAppSymbolsBySecurityTypeTest {

    private static final String FIELD = "securityExchangeMaps";

    private static void setMap(
            Map<MarketDataMessage.SecurityExchangeMarketData, MarketDataMessage.SecurityList> value)
            throws Exception {
        Field f = MainApp.class.getDeclaredField(FIELD);
        f.setAccessible(true);
        f.set(null, value);
    }

    private static MarketDataMessage.Security sec(String symbol, String securityType) {
        MarketDataMessage.Security.Builder b = MarketDataMessage.Security.newBuilder().setSymbol(symbol);
        if (securityType != null) b.setSecurityType(securityType);
        return b.build();
    }

    private void givenBcsSecurities(MarketDataMessage.Security... securities) throws Exception {
        MarketDataMessage.SecurityList list = MarketDataMessage.SecurityList.newBuilder()
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                .addAllListSecurities(List.of(securities))
                .build();
        Map<MarketDataMessage.SecurityExchangeMarketData, MarketDataMessage.SecurityList> m = new HashMap<>();
        m.put(MarketDataMessage.SecurityExchangeMarketData.BCS, list);
        setMap(m);
    }

    @AfterEach
    public void limpiarEstadoEstatico() throws Exception {
        setMap(new HashMap<>());
    }

    @Test
    public void agrupaPorSecurityTypeDeLaSecurityList() throws Exception {
        givenBcsSecurities(
                sec("SQM-B", "CS"),
                sec("FALABELLA", "CS"),
                sec("CFIETFIPSA", "CFI"),
                sec("ITAUCL", "ETF"));

        Map<String, List<String>> byType = MainApp.snapshotBcsSymbolsBySecurityType();

        assertEquals(List.of("SQM-B", "FALABELLA"), byType.get("CS"));
        assertEquals(List.of("CFIETFIPSA"), byType.get("CFI"));
        assertEquals(List.of("ITAUCL"), byType.get("ETF"));
    }

    @Test
    public void sinSecurityTypeCaeACfiPorPrefijoDelNombre() throws Exception {
        // La SecurityList a veces llega sin securityType; el core ya clasifica CFI por convencion
        // de nombre (isCfiSymbol). Sin este fallback esos papeles no se recargarian nunca.
        givenBcsSecurities(sec("CFMDIVO", ""), sec("CFIALGO", null));

        Map<String, List<String>> byType = MainApp.snapshotBcsSymbolsBySecurityType();

        assertEquals(List.of("CFIALGO"), byType.get("CFI"));
        // CFMDIVO no empieza con "CFI" -> cae a CS, que es el default del resto del sistema
        assertEquals(List.of("CFMDIVO"), byType.get("CS"));
    }

    @Test
    public void normalizaElTipoAMayusculasYSinEspacios() throws Exception {
        givenBcsSecurities(sec("AAA", " etf "), sec("BBB", "Cs"));

        Map<String, List<String>> byType = MainApp.snapshotBcsSymbolsBySecurityType();

        assertEquals(List.of("AAA"), byType.get("ETF"));
        assertEquals(List.of("BBB"), byType.get("CS"));
    }

    @Test
    public void ignoraSimbolosVacios() throws Exception {
        givenBcsSecurities(sec("", "CS"), sec("   ", "CS"), sec("SQM-B", "CS"));

        Map<String, List<String>> byType = MainApp.snapshotBcsSymbolsBySecurityType();

        assertEquals(List.of("SQM-B"), byType.get("CS"));
    }

    @Test
    public void sinSecurityListDevuelveVacioYNoRevienta() throws Exception {
        setMap(new HashMap<>());

        Map<String, List<String>> byType = MainApp.snapshotBcsSymbolsBySecurityType();

        assertTrue(byType.isEmpty(), "sin SecurityList no hay nada que recargar");
        assertNull(byType.get("CS"));
    }

    @Test
    public void soloConsideraBcsYNoOtrosDestinos() throws Exception {
        MarketDataMessage.SecurityList nasdaq = MarketDataMessage.SecurityList.newBuilder()
                .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.NASDAQ)
                .addListSecurities(sec("AAPL", "CS"))
                .build();
        Map<MarketDataMessage.SecurityExchangeMarketData, MarketDataMessage.SecurityList> m = new HashMap<>();
        m.put(MarketDataMessage.SecurityExchangeMarketData.NASDAQ, nasdaq);
        setMap(m);

        Map<String, List<String>> byType = MainApp.snapshotBcsSymbolsBySecurityType();

        assertTrue(byType.isEmpty(), "la var% de Mongo es solo para BCS");
    }
}
