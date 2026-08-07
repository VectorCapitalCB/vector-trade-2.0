package cl.vc.service.util;

import cl.vc.service.MainApp;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

/**
 * Regresión del incidente 05-06: con la conexión a SQL Server en null (no disponible),
 * las consultas lanzaban NullPointerException ("Cannot invoke ...prepareStatement because
 * connection is null") por horas → la custodia no se cargaba → ventas rechazadas "sin custodia".
 * Ahora deben devolver VACÍO de forma segura (sin NPE) y sin colgar reintentando contra la BD.
 */
class SQLServerConnectionTest {

    @Test
    void sinConexionSql_consultasDevuelvenVacio_noNPE() {
        try (MockedStatic<MainApp> main = mockStatic(MainApp.class)) {
            // properties == null ⇒ ensureConnection() no intenta DriverManager y devuelve null
            main.when(MainApp::getProperties).thenReturn(null);
            SQLServerConnection.connection = null;

            assertNull(SQLServerConnection.ensureConnection(),
                    "sin properties no debe poder (re)conectar");

            // Ninguna de estas debe lanzar NPE; todas devuelven vacío.
            assertTrue(SQLServerConnection.prestamos("16936145/9").isEmpty(), "prestamos vacío");
            assertTrue(SQLServerConnection.carteraResumida("16936145/9").isEmpty(), "carteraResumida (custodia) vacía");
            assertTrue(SQLServerConnection.saldoCaja("16936145/9").isEmpty(), "saldoCaja vacío");
            assertTrue(SQLServerConnection.getAccountByrut("16936145").isEmpty(), "getAccountByrut vacío");
            assertTrue(SQLServerConnection.consultaSimultanea().isEmpty(), "consultaSimultanea vacía");
        }
    }
}
