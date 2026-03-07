package cl.vc.chat.websocket;

import org.eclipse.jetty.websocket.api.Session;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ChatSessionRegistry {

    private static final ConcurrentHashMap<String, Set<Session>> sessionsByUser = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Session, String> userBySession = new ConcurrentHashMap<>();

    public static void bind(Session session, String username) {
        if (session == null || username == null || username.trim().isEmpty()) {
            return;
        }

        unbind(session);
        String normalized = username.trim().toLowerCase();
        sessionsByUser.computeIfAbsent(normalized, k -> ConcurrentHashMap.newKeySet()).add(session);
        userBySession.put(session, normalized);
    }

    public static void unbind(Session session) {
        if (session == null) {
            return;
        }
        String username = userBySession.remove(session);
        if (username == null) {
            return;
        }
        Set<Session> sessions = sessionsByUser.get(username);
        if (sessions != null) {
            sessions.remove(session);
            if (sessions.isEmpty()) {
                sessionsByUser.remove(username);
            }
        }
    }

    public static Set<Session> sessionsOf(String username) {
        if (username == null) {
            return Collections.emptySet();
        }
        Set<Session> sessions = sessionsByUser.get(username.trim().toLowerCase());
        return sessions == null ? Collections.emptySet() : new HashSet<>(sessions);
    }
}
