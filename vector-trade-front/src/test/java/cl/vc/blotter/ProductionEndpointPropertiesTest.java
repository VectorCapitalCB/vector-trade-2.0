package cl.vc.blotter;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ProductionEndpointPropertiesTest {

    @Test
    void productionServiceEndpointTargetsProductionCore() throws Exception {
        Properties properties = loadProperties("/blotter/enviroment/application.production.properties");

        assertEquals("ws://172.16.0.6:8096/websocket/", properties.getProperty("production"));
        assertEquals("ws://172.16.0.6:8096/websocket/", properties.getProperty("production.service"));
        assertEquals("ws://172.16.0.6:8098/ws/", properties.getProperty("production.candle"));
        assertEquals("ws://172.16.0.6:8097/ws/", properties.getProperty("production.chat"));
        assertEquals("ws://172.16.0.6:8100/ws/", properties.getProperty("production.news"));
    }

    @Test
    void qaServiceEndpointTargetsSameProductionCore() throws Exception {
        Properties properties = loadProperties("/blotter/enviroment/application.qa.properties");

        assertEquals("ws://172.16.0.6:8096/websocket/", properties.getProperty("qa.service"));
        assertEquals("ws://172.16.0.6:8098/ws/", properties.getProperty("qa.candle"));
        assertEquals("ws://172.16.0.6:8097/ws/", properties.getProperty("qa.chat"));
        assertEquals("ws://172.16.0.6:8100/ws/", properties.getProperty("qa.news"));
    }

    private Properties loadProperties(String resource) throws Exception {
        Properties properties = new Properties();
        try (InputStream input = getClass().getResourceAsStream(resource)) {
            assertNotNull(input, "No se encontro " + resource);
            properties.load(input);
        }
        return properties;
    }
}
