package cl.vc.blotter.controller;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RoutingControllerTest {

    @Test
    void calculatesMaxFloorFromVisiblePercentage() {
        assertEquals(30_000L, RoutingController.calculateMaxFloor(300_000d, 10d));
        assertEquals(75_000L, RoutingController.calculateMaxFloor(300_000d, 25d));
    }

    @Test
    void enforcesTenPercentMinimumAndRoundsUp() {
        assertEquals(8_000L, RoutingController.calculateMaxFloor(80_000d, 2d));
        assertEquals(2L, RoutingController.calculateMaxFloor(11d, 10d));
    }
}
