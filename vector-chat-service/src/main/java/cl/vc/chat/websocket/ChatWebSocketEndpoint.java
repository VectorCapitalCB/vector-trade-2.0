package cl.vc.chat.websocket;

import org.bson.Document;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Comparator;

@WebSocket
public class ChatWebSocketEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChatWebSocketEndpoint.class);
    private static final Logger USER_CONNECTION_LOGGER = LoggerFactory.getLogger("chat.user.connection");

    @OnWebSocketConnect
    public void onConnect(Session session) {
        USER_CONNECTION_LOGGER.info("websocket_open sessionId={} remote={}", sessionId(session), session.getRemoteAddress());
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        ChatSessionRegistry.unbind(session);
        USER_CONNECTION_LOGGER.info("websocket_close sessionId={} statusCode={} reason={}", sessionId(session), statusCode, reason);
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String raw) throws Exception {
        try {
            JSONObject request = new JSONObject(raw);
            String action = resolveAction(request);

            if ("chat_register".equals(action)) {
                String username = firstNonBlank(
                        request.optString("username", ""),
                        request.optString("user", "")
                ).trim();
                if (username.isEmpty()) {
                    sendError(session, "chat_register requiere username");
                    return;
                }
                ChatSessionRegistry.bind(session, username);
                ChatMongoRepository.upsertUser(username);
                USER_CONNECTION_LOGGER.info("user_connected username={} sessionId={}", username, sessionId(session));
                session.getRemote().sendString(new JSONObject().put("type", "chat_registered").put("username", username).toString());
                return;
            }

            if ("chat_users".equals(action)) {
                int limit = request.optInt("limit", 200);
                List<String> users = ChatMongoRepository.listKnownUsers(limit);
                JSONArray rows = new JSONArray();
                for (String user : users) {
                    rows.put(user);
                }
                session.getRemote().sendString(new JSONObject().put("type", "chat_users").put("users", rows).toString());
                return;
            }

            if ("chat_snapshot".equals(action)) {
                String username = firstNonBlank(
                        request.optString("username", ""),
                        request.optString("user", "")
                ).trim();
                if (username.isEmpty()) {
                    sendError(session, "snapshot_request requiere user/username");
                    return;
                }

                ChatSessionRegistry.bind(session, username);
                ChatMongoRepository.upsertUser(username);

                List<String> knownUsers = ChatMongoRepository.listKnownUsers(300);
                JSONArray users = new JSONArray();
                for (String user : knownUsers) {
                    if (user != null && !user.trim().isEmpty() && !username.equalsIgnoreCase(user.trim())) {
                        users.put(user.trim());
                    }
                }

                List<Document> conversations = ChatMongoRepository.conversations(username, 200);
                List<Document> mergedMessages = new ArrayList<>();
                for (Document c : conversations) {
                    String withUsername = c.getString("withUsername");
                    if (withUsername == null || withUsername.trim().isEmpty()) {
                        continue;
                    }
                    mergedMessages.addAll(ChatMongoRepository.history(username, withUsername, 300));
                }
                mergedMessages.sort(Comparator.comparingLong(d -> d.getLong("timestamp")));

                int maxMessages = 2000;
                int fromIndex = Math.max(0, mergedMessages.size() - maxMessages);
                JSONArray messages = new JSONArray();
                for (int i = fromIndex; i < mergedMessages.size(); i++) {
                    Document d = mergedMessages.get(i);
                    String from = d.getString("fromUsername");
                    String to = d.getString("toUsername");
                    String message = d.getString("message");
                    messages.put(new JSONObject()
                            .put("id", ChatMongoRepository.idHex(d))
                            .put("from", from)
                            .put("to", to)
                            .put("msg", message)
                            .put("message", message)
                            .put("timestamp", d.getLong("timestamp")));
                }

                session.getRemote().sendString(new JSONObject()
                        .put("type", "snapshot")
                        .put("user", username)
                        .put("users", users)
                        .put("messages", messages)
                        .toString());
                return;
            }

            if ("chat_conversations".equals(action)) {
                String username = firstNonBlank(
                        request.optString("username", ""),
                        request.optString("user", "")
                ).trim();
                int limit = request.optInt("limit", 100);
                if (username.isEmpty()) {
                    sendError(session, "chat_conversations requiere username");
                    return;
                }

                ChatSessionRegistry.bind(session, username);
                ChatMongoRepository.upsertUser(username);

                List<Document> rows = ChatMongoRepository.conversations(username, limit);
                JSONArray conversations = new JSONArray();
                for (Document d : rows) {
                    conversations.put(new JSONObject()
                            .put("withUsername", d.getString("withUsername"))
                            .put("conversationId", d.getString("conversationId"))
                            .put("lastMessage", d.getString("lastMessage"))
                            .put("timestamp", d.getLong("timestamp"))
                            .put("fromUsername", d.getString("fromUsername"))
                            .put("toUsername", d.getString("toUsername")));
                }

                session.getRemote().sendString(new JSONObject()
                        .put("type", "chat_conversations")
                        .put("username", username)
                        .put("conversations", conversations)
                        .toString());
                return;
            }

            if ("chat_history".equals(action)) {
                String username = firstNonBlank(
                        request.optString("username", ""),
                        request.optString("user", "")
                ).trim();
                String withUsername = firstNonBlank(
                        request.optString("withUsername", ""),
                        request.optString("with", "")
                ).trim();
                int limit = request.optInt("limit", 500);
                if (username.isEmpty() || withUsername.isEmpty()) {
                    sendError(session, "chat_history requiere username y withUsername");
                    return;
                }

                ChatSessionRegistry.bind(session, username);
                ChatMongoRepository.upsertUser(username);
                List<Document> rows = ChatMongoRepository.history(username, withUsername, limit);
                JSONArray messages = new JSONArray();
                for (Document d : rows) {
                    messages.put(new JSONObject()
                            .put("id", ChatMongoRepository.idHex(d))
                            .put("conversationId", d.getString("conversationId"))
                            .put("fromUsername", d.getString("fromUsername"))
                            .put("toUsername", d.getString("toUsername"))
                            .put("message", d.getString("message"))
                            .put("timestamp", d.getLong("timestamp")));
                }

                session.getRemote().sendString(new JSONObject()
                        .put("type", "chat_history")
                        .put("username", username)
                        .put("withUsername", withUsername)
                        .put("messages", messages)
                        .toString());
                return;
            }

            if ("chat_send".equals(action)) {
                String fromUsername = firstNonBlank(
                        request.optString("fromUsername", ""),
                        request.optString("from", "")
                ).trim();
                String toUsername = firstNonBlank(
                        request.optString("toUsername", ""),
                        request.optString("to", "")
                ).trim();
                String message = firstNonBlank(
                        request.optString("message", ""),
                        request.optString("msg", "")
                ).trim();
                if (fromUsername.isEmpty() || toUsername.isEmpty() || message.isEmpty()) {
                    sendError(session, "chat_send requiere fromUsername, toUsername y message");
                    return;
                }

                ChatSessionRegistry.bind(session, fromUsername);
                ChatMongoRepository.upsertUser(fromUsername);
                ChatMongoRepository.upsertUser(toUsername);

                Document saved = ChatMongoRepository.save(fromUsername, toUsername, message);
                JSONObject payload = new JSONObject()
                        .put("type", "chat_message")
                        .put("id", ChatMongoRepository.idHex(saved))
                        .put("conversationId", saved.getString("conversationId"))
                        .put("fromUsername", fromUsername)
                        .put("toUsername", toUsername)
                        .put("from", fromUsername)
                        .put("to", toUsername)
                        .put("msg", message)
                        .put("message", message)
                        .put("timestamp", saved.getLong("timestamp"));

                Set<Session> receivers = new HashSet<>();
                receivers.addAll(ChatSessionRegistry.sessionsOf(fromUsername));
                receivers.addAll(ChatSessionRegistry.sessionsOf(toUsername));
                if (receivers.isEmpty()) {
                    receivers.add(session);
                }

                for (Session receiver : receivers) {
                    if (receiver != null && receiver.isOpen()) {
                        receiver.getRemote().sendString(payload.toString());
                    }
                }
                return;
            }

            sendError(session, "action no soportada");
        } catch (Exception e) {
            LOGGER.error("Error processing websocket message", e);
            sendError(session, "error procesando mensaje: " + e.getMessage());
        }
    }

    private void sendError(Session session, String message) throws Exception {
        session.getRemote().sendString(new JSONObject().put("type", "error").put("message", message).toString());
    }

    private String resolveAction(JSONObject request) {
        String action = request.optString("action", "").trim().toLowerCase();
        if (!action.isEmpty()) {
            return action;
        }

        String type = request.optString("type", "").trim().toLowerCase();
        if ("chat_connect".equals(type) || "chat_register".equals(type)) {
            return "chat_register";
        }
        if ("snapshot_request".equals(type) || "chat_snapshot".equals(type)) {
            return "chat_snapshot";
        }
        if ("chat_message".equals(type)) {
            return "chat_send";
        }
        if ("chat_users".equals(type)) {
            return "chat_users";
        }
        if ("chat_history".equals(type)) {
            return "chat_history";
        }
        if ("chat_conversations".equals(type)) {
            return "chat_conversations";
        }
        return "";
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.trim().isEmpty()) {
            return first;
        }
        return second == null ? "" : second;
    }

    private String sessionId(Session session) {
        return session == null ? "null" : Integer.toHexString(System.identityHashCode(session));
    }
}
