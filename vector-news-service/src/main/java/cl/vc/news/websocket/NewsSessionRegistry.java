package cl.vc.news.websocket;

import org.eclipse.jetty.websocket.api.Session;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NewsSessionRegistry {
    private static final Set<Session> sessions = ConcurrentHashMap.newKeySet();

    public static void register(Session session) {
        if (session != null) {
            sessions.add(session);
        }
    }

    public static void unregister(Session session) {
        if (session != null) {
            sessions.remove(session);
        }
    }

    public static Set<Session> sessions() {
        return new HashSet<>(sessions);
    }
}
