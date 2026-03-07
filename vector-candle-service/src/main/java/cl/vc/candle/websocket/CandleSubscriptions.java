package cl.vc.candle.websocket;

import org.eclipse.jetty.websocket.api.Session;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CandleSubscriptions {

    private static final ConcurrentHashMap<CandleSubscriptionKey, Set<Session>> sessionsByKey = new ConcurrentHashMap<>();

    public static void subscribe(Session session, CandleSubscriptionKey key) {
        sessionsByKey.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(session);
    }

    public static void unsubscribe(Session session, CandleSubscriptionKey key) {
        Set<Session> sessions = sessionsByKey.get(key);
        if (sessions != null) {
            sessions.remove(session);
            if (sessions.isEmpty()) {
                sessionsByKey.remove(key);
            }
        }
    }

    public static void removeSession(Session session) {
        for (CandleSubscriptionKey key : new HashSet<>(sessionsByKey.keySet())) {
            unsubscribe(session, key);
        }
    }

    public static Set<CandleSubscriptionKey> activeKeys() {
        return new HashSet<>(sessionsByKey.keySet());
    }

    public static Set<Session> sessions(CandleSubscriptionKey key) {
        Set<Session> sessions = sessionsByKey.get(key);
        return sessions == null ? Collections.emptySet() : new HashSet<>(sessions);
    }
}
