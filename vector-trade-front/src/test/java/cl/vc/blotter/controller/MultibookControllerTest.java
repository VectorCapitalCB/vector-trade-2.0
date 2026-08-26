package cl.vc.blotter.controller;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultibookControllerTest {

    @Test
    void redistributesFiftyBooksIntoFivePagesOfTen() {
        JSONArray source = new JSONArray();
        for (int page = 0; page < 2; page++) {
            JSONArray books = new JSONArray();
            for (int slot = 0; slot < 25; slot++) {
                books.put(new JSONObject()
                        .put("symbol", "BOOK-" + (page * 25 + slot))
                        .put("slot", slot));
            }
            source.put(new JSONObject().put("name", String.valueOf(page + 1)).put("books", books));
        }

        JSONArray result = MultibookController.repaginatePages(source, 10, 15);

        assertEquals(5, result.length());
        for (int page = 0; page < result.length(); page++) {
            JSONObject pageJson = result.getJSONObject(page);
            assertEquals(10, pageJson.getInt("bookCount"));
            assertEquals(15, pageJson.getInt("depth"));
            assertEquals(10, pageJson.getJSONArray("books").length());
            for (int slot = 0; slot < 10; slot++) {
                JSONObject book = pageJson.getJSONArray("books").getJSONObject(slot);
                assertEquals("BOOK-" + (page * 10 + slot), book.getString("symbol"));
                assertEquals(slot, book.getInt("slot"));
            }
        }
    }

    @Test
    void keepsLocalDocumentWhenItHasAnUnsynchronizedNewerChange() {
        JSONObject local = new JSONObject().put("clientUpdatedAt", 200L);
        JSONObject remote = new JSONObject().put("clientUpdatedAt", 100L);

        assertTrue(MultibookController.preferLocalDocument(local, remote));
    }

    @Test
    void keepsRemoteDocumentWhenItIsAtLeastAsRecent() {
        JSONObject local = new JSONObject().put("clientUpdatedAt", 100L);
        JSONObject remote = new JSONObject().put("clientUpdatedAt", 200L);

        assertFalse(MultibookController.preferLocalDocument(local, remote));
        assertFalse(MultibookController.preferLocalDocument(new JSONObject(), remote));
    }
}
