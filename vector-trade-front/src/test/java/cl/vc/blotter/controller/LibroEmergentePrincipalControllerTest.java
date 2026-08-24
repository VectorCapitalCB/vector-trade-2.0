package cl.vc.blotter.controller;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LibroEmergentePrincipalControllerTest {

    @Test
    void acceptsOnlySupportedPageSizes() {
        assertEquals(10, LibroEmergentePrincipalController.normalizeBookCount(7));
        assertEquals(10, LibroEmergentePrincipalController.normalizeBookCount(10));
        assertEquals(20, LibroEmergentePrincipalController.normalizeBookCount(20));
        assertEquals(30, LibroEmergentePrincipalController.normalizeBookCount(30));
        assertEquals(40, LibroEmergentePrincipalController.normalizeBookCount(40));
        assertEquals(50, LibroEmergentePrincipalController.normalizeBookCount(50));
    }
}
