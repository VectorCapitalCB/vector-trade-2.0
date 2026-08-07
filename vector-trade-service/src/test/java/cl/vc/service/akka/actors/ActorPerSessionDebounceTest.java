package cl.vc.service.akka.actors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cubre la decisión del debounce de onConnect (ActorPerSession): un ActorPerSession = una
 * sesión WebSocket; si el front reenvía Connect sobre el mismo socket (loop de reconexión),
 * el trabajo caro (Keycloak + addAccount + snapshots) NO debe repetirse.
 */
class ActorPerSessionDebounceTest {

    @Test
    void primerConnect_noEsDuplicado() {
        // aún no inicializado -> debe procesar (no es duplicado)
        assertFalse(ActorPerSession.isDuplicateConnect(false, null, "fricci"));
    }

    @Test
    void connectRepetido_mismoUsuario_esDuplicado() {
        // ya inicializado para fricci y vuelve a llegar fricci -> debounce
        assertTrue(ActorPerSession.isDuplicateConnect(true, "fricci", "fricci"));
    }

    @Test
    void cambioDeUsuario_mismoSocket_noEsDuplicado() {
        // re-login como otro usuario sobre el mismo socket -> re-inicializa
        assertFalse(ActorPerSession.isDuplicateConnect(true, "fricci", "otro"));
    }

    @Test
    void usuarioNull_noRompe_yNoEsDuplicado() {
        assertFalse(ActorPerSession.isDuplicateConnect(true, "fricci", null));
    }
}
