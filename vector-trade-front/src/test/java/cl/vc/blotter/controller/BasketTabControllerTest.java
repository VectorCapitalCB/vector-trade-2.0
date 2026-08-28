package cl.vc.blotter.controller;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BasketTabControllerTest {

    @Test
    void basketTabTitleKeepsCounterAndOmitsBasketId() {
        String title = BasketTabController.basketTabTitle(0, 35);

        assertEquals("BKT (0/35)", title);
        assertFalse(title.contains("13p2cxpejgti1"));
    }
}
