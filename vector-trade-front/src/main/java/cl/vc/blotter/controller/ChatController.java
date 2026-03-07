package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;
import javafx.util.Duration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.awt.Toolkit;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ChatController {
    private static final Logger log = LoggerFactory.getLogger(ChatController.class);
    private static final Gson GSON = new GsonBuilder().create();
    private static final DateTimeFormatter CHAT_TIMESTAMP_TODAY_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter CHAT_TIMESTAMP_FULL_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Map<String, String> EMOJI_IMAGE_PATHS = new LinkedHashMap<>();
    private static final Map<String, Image> EMOJI_IMAGES = new HashMap<>();
    private static final List<String> EMOJI_TOKENS = new ArrayList<>();

    static {
        EMOJI_IMAGE_PATHS.put("😀", "/blotter/img/emoji/grin.png");
        EMOJI_IMAGE_PATHS.put("😃", "/blotter/img/emoji/smile.png");
        EMOJI_IMAGE_PATHS.put("😊", "/blotter/img/emoji/blush.png");
        EMOJI_IMAGE_PATHS.put("😉", "/blotter/img/emoji/wink.png");
        EMOJI_IMAGE_PATHS.put("😜", "/blotter/img/emoji/tongue.png");
        EMOJI_IMAGE_PATHS.put("👍", "/blotter/img/emoji/thumbsup.png");
        EMOJI_IMAGE_PATHS.put("👎", "/blotter/img/emoji/thumbsdown.png");
        EMOJI_IMAGE_PATHS.put("👏", "/blotter/img/emoji/clap.png");
        EMOJI_IMAGE_PATHS.put("🔥", "/blotter/img/emoji/fire.png");
        EMOJI_IMAGE_PATHS.put("❤️", "/blotter/img/emoji/heart.png");
        EMOJI_IMAGE_PATHS.put("❤", "/blotter/img/emoji/heart.png");
        EMOJI_TOKENS.addAll(EMOJI_IMAGE_PATHS.keySet());
        EMOJI_TOKENS.sort(Comparator.comparingInt(String::length).reversed());
    }

    @FXML
    private ListView<String> chatListView;
    @FXML
    private ListView<String> usersListView;
    @FXML
    private TextField txtMessage;
    @FXML
    private TextField txtNewUser;
    @FXML
    private Label lblChatState;
    @FXML
    private Label lblActiveUser;
    @FXML
    private Button btnEmojiGrin;
    @FXML
    private Button btnEmojiSmile;
    @FXML
    private Button btnEmojiBlush;
    @FXML
    private Button btnEmojiWink;
    @FXML
    private Button btnEmojiTongue;
    @FXML
    private Button btnEmojiThumbsup;
    @FXML
    private Button btnEmojiThumbsdown;
    @FXML
    private Button btnEmojiClap;
    @FXML
    private Button btnEmojiFire;
    @FXML
    private Button btnEmojiHeart;

    private final ObservableList<String> users = FXCollections.observableArrayList();
    private final Map<String, ObservableList<String>> conversations = new HashMap<>();
    private final Set<String> unreadUsers = new LinkedHashSet<>();
    private String activeUser;
    private final Set<String> processedMessages = new LinkedHashSet<>();
    private String me = "yo";
    private boolean loadingState = false;
    private boolean suppressChatAlerts = false;

    private Timeline statusTimeline;

    @FXML
    private void initialize() {
        final String emojiTextStyle = "-fx-font-family: 'Segoe UI Emoji'; -fx-font-size: 13px;";
        usersListView.setItems(users);
        usersListView.setCellFactory(lv -> new ListCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setStyle("");
                    return;
                }
                boolean unread = unreadUsers.contains(item);
                setText((unread ? "● " : "") + item);
                setStyle(unread ? "-fx-font-weight: bold;" : "-fx-font-weight: normal;");
            }
        });
        setupEmojiButtons();
        chatListView.setStyle(emojiTextStyle);
        txtMessage.setStyle("-fx-font-family: 'Segoe UI Emoji'; -fx-font-size: 14px;");
        chatListView.setCellFactory(lv -> new ListCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setGraphic(null);
                } else {
                    setText(null);
                    setGraphic(buildMessageGraphic(item));
                }
            }
        });
        txtMessage.setOnAction(e -> sendMessage());
        txtNewUser.setOnAction(e -> addUser());

        me = currentUsername();
        loadingState = true;
        loadLocalState();
        removeSelfUser();
        if (users.isEmpty()) {
            ensureUser("General");
        }
        if (activeUser == null || activeUser.isBlank() || !users.contains(activeUser)) {
            activeUser = users.get(0);
        }
        loadingState = false;
        usersListView.getSelectionModel().select(activeUser);

        usersListView.getSelectionModel().selectedItemProperty().addListener((obs, oldV, newV) -> {
            if (newV != null) {
                activeUser = newV;
                chatListView.setItems(conversations.get(newV));
                lblActiveUser.setText(newV);
                unreadUsers.remove(newV);
                usersListView.refresh();
                scrollToBottom();
                saveLocalState();
            }
        });

        // Enruta mensajes entrantes al historial correcto
        Repository.getChatMessages().addListener((ListChangeListener<String>) change -> {
            while (change.next()) {
                if (!change.wasAdded()) {
                    continue;
                }
                for (String raw : change.getAddedSubList()) {
                    routeIncoming(raw);
                }
            }
        });
        // Si el snapshot llegó antes de abrir la vista, lo aplicamos igual.
        for (String raw : Repository.getChatMessages()) {
            suppressChatAlerts = true;
            routeIncoming(raw);
            suppressChatAlerts = false;
        }
        requestSnapshot();

        statusTimeline = new Timeline(new KeyFrame(Duration.seconds(1), e -> updateStatus()));
        statusTimeline.setCycleCount(Timeline.INDEFINITE);
        statusTimeline.play();
        Repository.chatConnectedProperty().addListener((obs, oldV, newV) ->
                updateStatus(Boolean.TRUE.equals(newV)));
        updateStatus();
    }

    @FXML
    private void addUser() {
        String user = txtNewUser.getText();
        if (user == null || user.isBlank()) {
            return;
        }
        user = user.trim();
        if (user.equalsIgnoreCase(me)) {
            txtNewUser.clear();
            return;
        }
        ensureUser(user);
        usersListView.getSelectionModel().select(user);
        txtNewUser.clear();
        saveLocalState();
    }

    @FXML
    private void sendMessage() {
        String msg = txtMessage.getText();
        if (msg == null || msg.isBlank()) {
            return;
        }
        if (activeUser == null || activeUser.isBlank()) {
            ensureUser("General");
            activeUser = "General";
            usersListView.getSelectionModel().select(activeUser);
        }

        String line = formatMessageLine(me, msg);
        conversations.get(activeUser).add(line);
        scrollIfActive(activeUser);

        String payload = buildChatJson(activeUser, me, msg);
        if (Repository.getChatClientService() != null) {
            Repository.getChatClientService().sendMessage(payload);
        } else {
            conversations.get(activeUser).add("SYSTEM: canal chat no conectado");
        }

        txtMessage.clear();
        saveLocalState();
    }

    @FXML
    private void insertEmoji(ActionEvent event) {
        if (!(event.getSource() instanceof Button button)) {
            return;
        }
        String emoji = button.getUserData() instanceof String ? (String) button.getUserData() : button.getText();
        if (emoji == null || emoji.isBlank()) {
            return;
        }
        String current = txtMessage.getText();
        if (current == null) {
            current = "";
        }
        if (!current.isEmpty() && !current.endsWith(" ")) {
            current += " ";
        }
        txtMessage.setText(current + emoji + " ");
        txtMessage.positionCaret(txtMessage.getText().length());
        txtMessage.requestFocus();
    }

    private void setupEmojiButtons() {
        setupEmojiButton(btnEmojiGrin, "😀");
        setupEmojiButton(btnEmojiSmile, "😃");
        setupEmojiButton(btnEmojiBlush, "😊");
        setupEmojiButton(btnEmojiWink, "😉");
        setupEmojiButton(btnEmojiTongue, "😜");
        setupEmojiButton(btnEmojiThumbsup, "👍");
        setupEmojiButton(btnEmojiThumbsdown, "👎");
        setupEmojiButton(btnEmojiClap, "👏");
        setupEmojiButton(btnEmojiFire, "🔥");
        setupEmojiButton(btnEmojiHeart, "❤️");
    }

    private void setupEmojiButton(Button button, String emoji) {
        if (button == null || emoji == null) {
            return;
        }
        button.setUserData(emoji);
        Image image = getEmojiImage(emoji);
        if (image != null) {
            ImageView imageView = new ImageView(image);
            imageView.setFitWidth(18);
            imageView.setFitHeight(18);
            imageView.setPreserveRatio(true);
            button.setText("");
            button.setGraphic(imageView);
        }
    }

    private TextFlow buildMessageGraphic(String message) {
        TextFlow flow = new TextFlow();
        if (message == null || message.isEmpty()) {
            return flow;
        }
        int i = 0;
        while (i < message.length()) {
            String matched = matchEmojiAt(message, i);
            if (matched != null) {
                Image img = getEmojiImage(matched);
                if (img != null) {
                    ImageView imageView = new ImageView(img);
                    imageView.setFitWidth(16);
                    imageView.setFitHeight(16);
                    imageView.setPreserveRatio(true);
                    flow.getChildren().add(imageView);
                } else {
                    flow.getChildren().add(new Text(matched));
                }
                i += matched.length();
                continue;
            }
            int next = findNextEmojiIndex(message, i);
            String chunk = next < 0 ? message.substring(i) : message.substring(i, next);
            flow.getChildren().add(new Text(chunk));
            i = next < 0 ? message.length() : next;
        }
        return flow;
    }

    private String matchEmojiAt(String text, int index) {
        for (String token : EMOJI_TOKENS) {
            if (text.startsWith(token, index)) {
                return token;
            }
        }
        return null;
    }

    private int findNextEmojiIndex(String text, int from) {
        int next = -1;
        for (String token : EMOJI_TOKENS) {
            int idx = text.indexOf(token, from);
            if (idx >= 0 && (next < 0 || idx < next)) {
                next = idx;
            }
        }
        return next;
    }

    private Image getEmojiImage(String emoji) {
        String path = EMOJI_IMAGE_PATHS.get(emoji);
        if (path == null) {
            return null;
        }
        if (EMOJI_IMAGES.containsKey(emoji)) {
            return EMOJI_IMAGES.get(emoji);
        }
        try {
            Image image = new Image(getClass().getResourceAsStream(path));
            EMOJI_IMAGES.put(emoji, image);
            return image;
        } catch (Exception e) {
            log.warn("No se pudo cargar emoji image {}: {}", path, e.getMessage());
            EMOJI_IMAGES.put(emoji, null);
            return null;
        }
    }

    private void ensureUser(String user) {
        if (user == null || user.isBlank()) {
            return;
        }
        if (user.equalsIgnoreCase(me)) {
            return;
        }
        if (!conversations.containsKey(user)) {
            conversations.put(user, FXCollections.observableArrayList());
        }
        if (!users.contains(user)) {
            users.add(user);
            if (!loadingState) {
                saveLocalState();
            }
        }
    }

    private void routeIncoming(String raw) {
        if (raw == null || raw.isBlank()) {
            return;
        }
        if (!processedMessages.add(raw)) {
            return;
        }

        long messageTimestamp = System.currentTimeMillis();

        // Esperado: SERVER: TO:user|FROM:otro|MSG:hola
        String msg = raw;
        if (raw.startsWith("SERVER: ")) {
            msg = raw.substring("SERVER: ".length());
        }
        if (msg.startsWith("{")) {
            try {
                JSONObject obj = new JSONObject(msg);
                String type = obj.optString("type", "");
                if ("snapshot".equalsIgnoreCase(type)) {
                    suppressChatAlerts = true;
                    try {
                        JSONArray arrUsers = obj.optJSONArray("users");
                        if (arrUsers != null) {
                            for (int i = 0; i < arrUsers.length(); i++) {
                                String user = arrUsers.optString(i, "");
                                if (!user.isBlank()) {
                                    ensureUser(user);
                                }
                            }
                        }
                        JSONArray arrMessages = obj.optJSONArray("messages");
                        if (arrMessages != null) {
                            for (int i = 0; i < arrMessages.length(); i++) {
                                JSONObject m = arrMessages.optJSONObject(i);
                                if (m == null) {
                                    continue;
                                }
                                String jTo = m.optString("to", "");
                                String jFrom = m.optString("from", "");
                                String jMsg = m.optString("msg", m.optString("message", ""));
                                long jTimestamp = m.optLong("timestamp", System.currentTimeMillis());
                                if (!jFrom.isBlank() || !jTo.isBlank() || !jMsg.isBlank()) {
                                    routeIncoming("SERVER: TO:" + jTo + "|FROM:" + jFrom + "|MSG:" + jMsg + "|TS:" + jTimestamp);
                                }
                            }
                        }
                        saveLocalState();
                    } finally {
                        suppressChatAlerts = false;
                    }
                    return;
                }
                if (obj.has("users")) {
                    JSONArray arr = obj.optJSONArray("users");
                    if (arr != null) {
                        for (int i = 0; i < arr.length(); i++) {
                            String user = arr.optString(i, "");
                            if (!user.isBlank()) {
                                ensureUser(user);
                            }
                        }
                    }
                    return;
                }
                if (obj.has("user")) {
                    String user = obj.optString("user", "");
                    if (!user.isBlank()) {
                        ensureUser(user);
                    }
                    return;
                }
                String jTo = obj.optString("to", "");
                String jFrom = obj.optString("from", "");
                String jMsg = obj.optString("msg", obj.optString("message", ""));
                messageTimestamp = obj.optLong("timestamp", System.currentTimeMillis());
                if (!jFrom.isBlank() || !jTo.isBlank() || !jMsg.isBlank()) {
                    msg = "TO:" + jTo + "|FROM:" + jFrom + "|MSG:" + jMsg + "|TS:" + messageTimestamp;
                }
            } catch (Exception ignored) {
                // Si no es JSON válido de chat, sigue parsing legacy.
            }
        }
        if (msg.startsWith("SNAPSHOT_REQUESTED")) {
            return;
        }
        if (msg.startsWith("SNAPSHOT_USER:")) {
            String user = msg.substring("SNAPSHOT_USER:".length()).trim();
            if (!user.isBlank()) {
                ensureUser(user);
            }
            return;
        }
        if (msg.startsWith("SNAPSHOT_USERS:")) {
            String usersRaw = msg.substring("SNAPSHOT_USERS:".length()).trim();
            if (!usersRaw.isBlank()) {
                for (String user : usersRaw.split(",")) {
                    String trimmed = user == null ? "" : user.trim();
                    if (!trimmed.isBlank()) {
                        ensureUser(trimmed);
                    }
                }
            }
            return;
        }

        String to = extract(msg, "TO:", "|");
        String from = extract(msg, "FROM:", "|");
        String body = extract(msg, "MSG:", "|TS:");
        if (body == null) {
            body = extract(msg, "MSG:", null);
        }
        String ts = extract(msg, "TS:", "|");
        if (ts != null && !ts.isBlank()) {
            try {
                messageTimestamp = Long.parseLong(ts.trim());
            } catch (Exception ignored) {
            }
        }

        String counterpart;
        if (from == null || from.isBlank()) {
            counterpart = "General";
        } else if (from.equalsIgnoreCase(me) && to != null && !to.isBlank()) {
            counterpart = to.trim();
        } else {
            counterpart = from.trim();
        }
        if (counterpart.equalsIgnoreCase(me)) {
            counterpart = (to != null && !to.isBlank() && !to.equalsIgnoreCase(me)) ? to.trim() : "General";
        }
        if (body == null || body.isBlank()) {
            body = msg;
        }

        ensureUser(counterpart);
        String sender = (from == null || from.isBlank()) ? counterpart : from.trim();
        if (sender.equalsIgnoreCase(me)) {
            sender = me;
        }

        // If server echoes my own message, avoid duplicating it in the sender chat window.
        ObservableList<String> conversation = conversations.get(counterpart);
        String formatted = formatMessageLine(sender, body, messageTimestamp);
        if (sender.equalsIgnoreCase(me) && !conversation.isEmpty()) {
            String last = conversation.get(conversation.size() - 1);
            if (stripTimestamp(last).equals(sender + ": " + body)) {
                return;
            }
        }

        conversation.add(formatted);
        scrollIfActive(counterpart);

        boolean incomingFromOtherUser = !sender.equalsIgnoreCase(me);
        if (incomingFromOtherUser && (activeUser == null || !activeUser.equalsIgnoreCase(counterpart))) {
            unreadUsers.add(counterpart);
            usersListView.refresh();
        }
        if (incomingFromOtherUser && !suppressChatAlerts) {
            try {
                Toolkit.getDefaultToolkit().beep();
            } catch (Exception ignored) {
                log.debug("No se pudo reproducir beep de chat");
            }
        }
        saveLocalState();

        if (activeUser == null) {
            activeUser = counterpart;
            usersListView.getSelectionModel().select(counterpart);
        }
    }

    private String extract(String source, String key, String endToken) {
        int start = source.indexOf(key);
        if (start < 0) {
            return null;
        }
        start += key.length();
        if (endToken == null) {
            return source.substring(start).trim();
        }
        int end = source.indexOf(endToken, start);
        if (end < 0) {
            return source.substring(start).trim();
        }
        return source.substring(start, end).trim();
    }

    private String formatMessageLine(String sender, String body) {
        return formatMessageLine(sender, body, System.currentTimeMillis());
    }

    private String formatMessageLine(String sender, String body, long epochMillis) {
        java.time.ZoneId zone = java.time.ZoneId.systemDefault();
        LocalDateTime messageDateTime = java.time.Instant.ofEpochMilli(epochMillis)
                .atZone(zone)
                .toLocalDateTime();
        String timestamp = messageDateTime.toLocalDate().equals(LocalDate.now(zone))
                ? messageDateTime.format(CHAT_TIMESTAMP_TODAY_FORMAT)
                : messageDateTime.format(CHAT_TIMESTAMP_FULL_FORMAT);
        return "[" + timestamp + "] " + sender + ": " + body;
    }

    private String stripTimestamp(String line) {
        if (line == null) {
            return "";
        }
        if (line.matches("^\\[(?:[0-9]{2}:[0-9]{2}:[0-9]{2}|[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})]\\s+.*$")) {
            int idx = line.indexOf("] ");
            if (idx >= 0 && idx + 2 < line.length()) {
                return line.substring(idx + 2);
            }
        }
        return line;
    }

    private void scrollIfActive(String conversationUser) {
        if (conversationUser == null || activeUser == null) {
            return;
        }
        if (activeUser.equalsIgnoreCase(conversationUser)) {
            scrollToBottom();
        }
    }

    private void scrollToBottom() {
        Platform.runLater(() -> {
            if (chatListView == null || chatListView.getItems() == null || chatListView.getItems().isEmpty()) {
                return;
            }
            chatListView.scrollTo(chatListView.getItems().size() - 1);
        });
    }

    private void updateStatus() {
        updateStatus(Repository.chatConnectedProperty().get());
    }

    private void updateStatus(boolean connected) {
        lblChatState.setText("CHAT: " + (connected ? "ON" : "OFF"));
        lblChatState.setStyle("-fx-font-weight: bold; -fx-text-fill: " + (connected ? "#39c16c" : "#ff5f5f") + ";");
    }

    private void requestSnapshot() {
        String me = Repository.getUsername() == null ? "" : Repository.getUsername().trim();
        if (Repository.getChatClientService() == null || me.isBlank()) {
            return;
        }
        Repository.getChatClientService().sendMessage(buildSnapshotRequestJson(me));
        Repository.appendChatMessage("SNAPSHOT_REQUESTED");
    }

    private String buildChatJson(String to, String from, String message) {
        JSONObject json = new JSONObject();
        json.put("type", "chat_message");
        json.put("to", to == null ? "" : to);
        json.put("from", from == null ? "" : from);
        json.put("msg", message == null ? "" : message);
        return json.toString();
    }

    private String buildSnapshotRequestJson(String user) {
        JSONObject json = new JSONObject();
        json.put("type", "snapshot_request");
        json.put("user", user == null ? "" : user);
        return json.toString();
    }

    private String currentUsername() {
        String username = Repository.getUsername();
        if (username == null || username.isBlank()) {
            return "yo";
        }
        return username.trim();
    }

    private Path getStoragePath() {
        String home = System.getProperty("user.home");
        String company = Repository.getProperties().getProperty("company");
        if (company == null || company.isBlank()) {
            company = "vc";
        }
        String application = Repository.getProperties().getProperty("application");
        if (application == null || application.isBlank()) {
            application = "VectorTrade";
        }
        String userSafe = currentUsername().replaceAll("[\\\\/:*?\"<>|\\s]+", "_");
        return Paths.get(home, company, application, "chat", userSafe + ".json");
    }

    private void loadLocalState() {
        Path path = getStoragePath();
        if (!Files.exists(path)) {
            return;
        }
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            if (Files.size(path) == 0L) {
                return;
            }
            ChatState state = GSON.fromJson(reader, ChatState.class);
            if (state == null) {
                return;
            }
            if (state.users != null) {
                for (String user : state.users) {
                    if (user != null && !user.isBlank() && !user.equalsIgnoreCase(me)) {
                        if (!conversations.containsKey(user)) {
                            conversations.put(user, FXCollections.observableArrayList());
                        }
                        if (!users.contains(user)) {
                            users.add(user);
                        }
                    }
                }
            }
            if (state.conversations != null) {
                for (Map.Entry<String, List<String>> entry : state.conversations.entrySet()) {
                    String user = entry.getKey();
                    if (user == null || user.isBlank()) {
                        continue;
                    }
                    if (user.equalsIgnoreCase(me)) {
                        continue;
                    }
                    ensureUser(user);
                    ObservableList<String> conv = conversations.get(user);
                    conv.clear();
                    if (entry.getValue() != null) {
                        conv.addAll(entry.getValue());
                    }
                }
            }
            if (state.activeUser != null && !state.activeUser.isBlank()) {
                activeUser = state.activeUser;
            }
            if (activeUser != null && activeUser.equalsIgnoreCase(me)) {
                activeUser = null;
            }
        } catch (Exception ignored) {
            log.warn("No se pudo cargar estado local chat en {}", path, ignored);
        }
    }

    private void saveLocalState() {
        if (loadingState) {
            return;
        }
        try {
            Path path = getStoragePath();
            Files.createDirectories(path.getParent());
            Path tmpPath = path.resolveSibling(path.getFileName().toString() + ".tmp");

            ChatState state = new ChatState();
            state.users = new ArrayList<>();
            for (String u : users) {
                if (!u.equalsIgnoreCase(me)) {
                    state.users.add(u);
                }
            }
            state.activeUser = (activeUser != null && !activeUser.equalsIgnoreCase(me)) ? activeUser : null;
            state.conversations = new HashMap<>();
            for (Map.Entry<String, ObservableList<String>> e : conversations.entrySet()) {
                if (!e.getKey().equalsIgnoreCase(me)) {
                    state.conversations.put(e.getKey(), new ArrayList<>(e.getValue()));
                }
            }

            try (Writer writer = Files.newBufferedWriter(tmpPath, StandardCharsets.UTF_8)) {
                GSON.toJson(state, writer);
            }
            try {
                Files.move(tmpPath, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception atomicEx) {
                Files.move(tmpPath, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception ignored) {
            log.warn("No se pudo guardar estado local chat para usuario {}: {}", me, ignored.getMessage());
        }
    }

    private static class ChatState {
        List<String> users;
        String activeUser;
        Map<String, List<String>> conversations;
    }

    private void removeSelfUser() {
        users.removeIf(u -> u != null && u.equalsIgnoreCase(me));
        conversations.remove(me);
        if (activeUser != null && activeUser.equalsIgnoreCase(me)) {
            activeUser = null;
        }
    }
}
