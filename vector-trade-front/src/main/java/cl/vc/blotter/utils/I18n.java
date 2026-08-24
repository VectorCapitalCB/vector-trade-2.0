package cl.vc.blotter.utils;

import javafx.application.Platform;
import javafx.beans.property.StringProperty;
import javafx.collections.ListChangeListener;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import javafx.stage.Window;
import javafx.util.StringConverter;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.prefs.Preferences;

/** Visual-only localization. Domain values and messages sent to the core remain untouched. */
public final class I18n {

    private static final String LANGUAGE_KEY = "ui.language";
    private static final Preferences PREFS = Preferences.userNodeForPackage(I18n.class);
    private static final Map<String, Translation> TRANSLATIONS = new HashMap<>();
    private static final Map<StringProperty, String> SOURCES = new WeakHashMap<>();
    private static final Set<StringProperty> BOUND_PROPERTIES = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Set<StringProperty> UPDATING_PROPERTIES = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Set<Window> BOUND_WINDOWS = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Set<Scene> BOUND_SCENES = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Set<Parent> BOUND_PARENTS = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Set<TabPane> BOUND_TAB_PANES = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Set<TableView<?>> BOUND_TABLES = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Set<TreeTableView<?>> BOUND_TREE_TABLES = Collections.newSetFromMap(new WeakHashMap<>());
    private static final Map<ComboBox<?>, StringConverter<?>> COMBO_CONVERTERS = new WeakHashMap<>();
    private static final Map<ChoiceBox<?>, StringConverter<?>> CHOICE_CONVERTERS = new WeakHashMap<>();
    private static final List<Runnable> LISTENERS = new CopyOnWriteArrayList<>();
    private static volatile Language language = Language.fromCode(PREFS.get(LANGUAGE_KEY, "es"));
    private static volatile boolean installed;

    static {
        registerCoreTerms();
        registerTradingTerms();
        registerSettingsAndMessages();
    }

    private I18n() {}

    public static Language getLanguage() {
        return language;
    }

    public static void setLanguage(Language newLanguage) {
        Language target = newLanguage == null ? Language.SPANISH : newLanguage;
        if (language == target) return;
        language = target;
        PREFS.put(LANGUAGE_KEY, target.code());
        runOnFxThread(() -> {
            Window.getWindows().forEach(I18n::applyToWindow);
            LISTENERS.forEach(Runnable::run);
        });
    }

    public static void addLanguageListener(Runnable listener) {
        if (listener != null) LISTENERS.add(listener);
    }

    public static void removeLanguageListener(Runnable listener) {
        LISTENERS.remove(listener);
    }

    public static void install() {
        if (installed) return;
        installed = true;
        runOnFxThread(() -> {
            Window.getWindows().forEach(I18n::bindWindow);
            Window.getWindows().addListener((ListChangeListener<Window>) change -> {
                while (change.next()) {
                    if (change.wasAdded()) change.getAddedSubList().forEach(I18n::bindWindow);
                }
            });
        });
    }

    public static String tr(String source) {
        return translate(language, source);
    }

    static String translate(Language targetLanguage, String source) {
        if (source == null || source.isBlank()) return source;
        String leading = source.substring(0, source.indexOf(source.trim()));
        String trailing = source.substring(source.lastIndexOf(source.trim()) + source.trim().length());
        String value = source.trim();
        Translation translation = TRANSLATIONS.get(normalize(value));
        if (translation != null) return leading + translation.forLanguage(targetLanguage) + trailing;

        String prefixed = translatePrefix(targetLanguage, value);
        return leading + prefixed + trailing;
    }

    public static String display(Object value) {
        return value == null ? "" : tr(String.valueOf(value));
    }

    public static void apply(Scene scene) {
        if (scene == null) return;
        bindScene(scene);
        translateParent(scene.getRoot());
    }

    private static void bindWindow(Window window) {
        if (window == null) return;
        if (BOUND_WINDOWS.add(window)) {
            window.sceneProperty().addListener((observable, oldScene, newScene) -> apply(newScene));
            if (window instanceof Stage stage) bind(stage.titleProperty());
        }
        applyToWindow(window);
    }

    private static void applyToWindow(Window window) {
        if (window != null) apply(window.getScene());
    }

    private static void bindScene(Scene scene) {
        if (scene == null || !BOUND_SCENES.add(scene)) return;
        scene.rootProperty().addListener((observable, oldRoot, newRoot) -> translateParent(newRoot));
    }

    private static void translateParent(Parent parent) {
        if (parent == null) return;
        if (BOUND_PARENTS.add(parent)) {
            parent.getChildrenUnmodifiable().addListener((ListChangeListener<Node>) change -> {
                while (change.next()) {
                    if (!change.wasAdded()) continue;
                    for (Node child : change.getAddedSubList()) {
                        translateNode(child);
                        if (child instanceof Parent childParent) translateParent(childParent);
                    }
                }
            });
        }
        translateNode(parent);
        if (parent instanceof Cell<?>) return;
        for (Node child : parent.getChildrenUnmodifiable()) {
            translateNode(child);
            if (child instanceof Parent childParent) translateParent(childParent);
        }
    }

    private static void translateNode(Node node) {
        // Live market-data cells update very frequently. Translating their text property
        // on every tick would add avoidable work to the JavaFX thread.
        if (node instanceof Labeled labeled
                && !(node instanceof TableCell<?, ?>)
                && !(node instanceof TreeTableCell<?, ?>)
                && !(node instanceof ListCell<?>)) {
            bind(labeled.textProperty());
        }
        if (node instanceof Text text) bind(text.textProperty());
        if (node instanceof TextInputControl input) bind(input.promptTextProperty());
        if (node instanceof Control control && control.getTooltip() != null) bind(control.getTooltip().textProperty());
        if (node instanceof TabPane tabPane) translateTabPane(tabPane);
        if (node instanceof TableView<?> table) translateTable(table);
        if (node instanceof TreeTableView<?> table) translateTreeTable(table);
        if (node instanceof ComboBox<?> comboBox) installComboConverter(comboBox);
        if (node instanceof ChoiceBox<?> choiceBox) installChoiceConverter(choiceBox);
        if (node instanceof MenuButton menuButton) menuButton.getItems().forEach(I18n::translateMenuItem);
        if (node instanceof Control control && control.getContextMenu() != null) {
            control.getContextMenu().getItems().forEach(I18n::translateMenuItem);
        }
    }

    private static void translateTabPane(TabPane tabPane) {
        if (BOUND_TAB_PANES.add(tabPane)) {
            tabPane.getTabs().addListener((ListChangeListener<Tab>) change -> {
                while (change.next()) {
                    if (change.wasAdded()) change.getAddedSubList().forEach(I18n::translateTab);
                }
            });
        }
        tabPane.getTabs().forEach(I18n::translateTab);
    }

    private static void translateTable(TableView<?> table) {
        if (BOUND_TABLES.add(table)) {
            table.getColumns().addListener((ListChangeListener<TableColumn<?, ?>>) change -> {
                while (change.next()) {
                    if (change.wasAdded()) change.getAddedSubList().forEach(I18n::translateColumn);
                }
            });
        }
        table.getColumns().forEach(I18n::translateColumn);
        table.refresh();
    }

    private static void translateTreeTable(TreeTableView<?> table) {
        if (BOUND_TREE_TABLES.add(table)) {
            table.getColumns().addListener((ListChangeListener<TreeTableColumn<?, ?>>) change -> {
                while (change.next()) {
                    if (change.wasAdded()) change.getAddedSubList().forEach(I18n::translateTreeColumn);
                }
            });
        }
        table.getColumns().forEach(I18n::translateTreeColumn);
        table.refresh();
    }

    private static void translateTab(Tab tab) {
        bind(tab.textProperty());
        if (tab.getTooltip() != null) bind(tab.getTooltip().textProperty());
        if (tab.getContent() instanceof Parent parent) translateParent(parent);
    }

    private static void translateColumn(TableColumn<?, ?> column) {
        bind(column.textProperty());
        column.getColumns().forEach(I18n::translateColumn);
    }

    private static void translateTreeColumn(TreeTableColumn<?, ?> column) {
        bind(column.textProperty());
        column.getColumns().forEach(I18n::translateTreeColumn);
    }

    private static void translateMenuItem(MenuItem item) {
        bind(item.textProperty());
        if (item instanceof Menu menu) menu.getItems().forEach(I18n::translateMenuItem);
    }

    private static void bind(StringProperty property) {
        if (property == null || property.isBound()) return;
        if (BOUND_PROPERTIES.add(property)) {
            SOURCES.put(property, property.get());
            property.addListener((observable, oldValue, newValue) -> {
                if (UPDATING_PROPERTIES.contains(property)) return;
                SOURCES.put(property, newValue);
                update(property);
            });
        }
        update(property);
    }

    private static void update(StringProperty property) {
        if (property.isBound()) return;
        String source = SOURCES.getOrDefault(property, property.get());
        String translated = tr(source);
        if (Objects.equals(property.get(), translated)) return;
        UPDATING_PROPERTIES.add(property);
        try {
            property.set(translated);
        } finally {
            UPDATING_PROPERTIES.remove(property);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void installComboConverter(ComboBox comboBox) {
        if (COMBO_CONVERTERS.containsKey(comboBox)) {
            comboBox.requestLayout();
            return;
        }
        StringConverter original = comboBox.getConverter();
        COMBO_CONVERTERS.put(comboBox, original);
        comboBox.setConverter(new StringConverter<>() {
            @Override public String toString(Object value) {
                String raw = original == null ? String.valueOf(value == null ? "" : value) : original.toString(value);
                return tr(raw);
            }

            @Override public Object fromString(String value) {
                if (original != null) return original.fromString(value);
                for (Object item : comboBox.getItems()) {
                    if (Objects.equals(tr(String.valueOf(item)), value) || Objects.equals(String.valueOf(item), value)) return item;
                }
                return value;
            }
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void installChoiceConverter(ChoiceBox choiceBox) {
        if (CHOICE_CONVERTERS.containsKey(choiceBox)) return;
        StringConverter original = choiceBox.getConverter();
        CHOICE_CONVERTERS.put(choiceBox, original);
        choiceBox.setConverter(new StringConverter<>() {
            @Override public String toString(Object value) {
                String raw = original == null ? String.valueOf(value == null ? "" : value) : original.toString(value);
                return tr(raw);
            }

            @Override public Object fromString(String value) {
                return original == null ? value : original.fromString(value);
            }
        });
    }

    private static String translatePrefix(Language targetLanguage, String value) {
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.startsWith("user: ") || lower.startsWith("usuario: ")) {
            return (targetLanguage == Language.SPANISH ? "Usuario: " : "User: ") + value.substring(value.indexOf(':') + 1).trim();
        }
        if (lower.startsWith("entorno: ") || lower.startsWith("environment: ")) {
            return (targetLanguage == Language.SPANISH ? "Entorno: " : "Environment: ") + value.substring(value.indexOf(':') + 1).trim();
        }
        if (lower.startsWith("canasta: ") || lower.startsWith("basket: ")) {
            return (targetLanguage == Language.SPANISH ? "Canasta: " : "Basket: ") + value.substring(value.indexOf(':') + 1).trim();
        }
        String translated = translateLeading(targetLanguage, value, "Últimas Operaciones Nemo", "Latest Instrument Trades");
        if (translated != null) return translated;
        translated = translateLeading(targetLanguage, value, "Últimas Operaciones", "Latest Trades");
        if (translated != null) return translated;
        translated = translateLeading(targetLanguage, value, "Trades Generales", "General Trades");
        if (translated != null) return translated;
        translated = translateLeading(targetLanguage, value, "Trabajando", "Working");
        if (translated != null) return translated;
        translated = translateLeading(targetLanguage, value, "Datos de Mercado", "Market Data");
        if (translated != null) return translated;
        translated = translateLeading(targetLanguage, value, "Velas", "Candles");
        if (translated != null) return translated;
        translated = translateLeading(targetLanguage, value, "Multi Libro", "Multi Book");
        if (translated != null) return translated;
        return value;
    }

    private static String translateLeading(Language targetLanguage, String value, String spanish, String english) {
        String spanishLower = spanish.toLowerCase(Locale.ROOT);
        String englishLower = english.toLowerCase(Locale.ROOT);
        String lower = value.toLowerCase(Locale.ROOT);
        String matched = null;
        if (lower.startsWith(spanishLower)) matched = value.substring(0, spanish.length());
        else if (lower.startsWith(englishLower)) matched = value.substring(0, english.length());
        if (matched == null || value.length() == matched.length()) return null;
        char separator = value.charAt(matched.length());
        if (separator != ' ' && separator != '(' && separator != ':' && separator != '-') return null;
        return (targetLanguage == Language.SPANISH ? spanish : english) + value.substring(matched.length());
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    private static void add(String es, String en, String... aliases) {
        Translation translation = new Translation(es, en);
        TRANSLATIONS.put(normalize(es), translation);
        TRANSLATIONS.put(normalize(en), translation);
        for (String alias : aliases) TRANSLATIONS.put(normalize(alias), translation);
    }

    private static void registerCoreTerms() {
        add("Aceptar", "Accept"); add("Cancelar", "Cancel"); add("Cerrar", "Close");
        add("Agregar", "Add"); add("Remover", "Remove"); add("Eliminar", "Delete");
        add("Actualizar", "Refresh"); add("Guardar", "Save"); add("Limpiar", "Clear");
        add("Importar", "Import"); add("Exportar", "Export"); add("Duplicar", "Duplicate");
        add("Renombrar", "Rename"); add("Nueva", "New"); add("Modificar", "Modify");
        add("Enviar", "Send"); add("Suscribir", "Subscribe"); add("Reconectar", "Reconnect");
        add("Sí", "Yes"); add("No", "No"); add("Todas", "All"); add("Todos", "All");
        add("Seleccione", "Select"); add("Sin datos", "No data", "SIN DATOS");
        add("Usuario", "User"); add("Usuarios", "Users"); add("Nombre de usuario", "Username", "User name");
        add("Contraseña", "Password", "Contrasena"); add("Confirmar contraseña", "Confirm password", "Confirm Password");
        add("Correo", "Email"); add("Nombre", "First name", "First Name"); add("Apellido", "Last name", "Last Name");
        add("Teléfono", "Phone"); add("Cuenta", "Account"); add("Cuentas", "Accounts");
        add("Estado", "Status", "State"); add("Tipo", "Type"); add("Acción", "Action");
        add("Comentarios", "Comments"); add("Componente", "Component"); add("Mensaje", "Message");
        add("Hora", "Time", "Hour"); add("Desde", "From"); add("Hasta", "To");
        add("Fecha", "Date"); add("Descripción", "Description"); add("Porcentaje", "Percentage");
        add("Configuraciones", "Settings"); add("Configuración", "Configuration");
        add("Notificaciones", "Notifications"); add("Sonido", "Sound"); add("Noticias", "News");
        add("Estadísticas", "Statistics"); add("Consola", "Console"); add("Chat", "Chat");
        add("Buscar instrumento", "Search instrument"); add("Filtra cuenta…", "Filter account…", "Filter account...");
        add("Instrumento", "Instrument"); add("Símbolo", "Symbol"); add("Mercado", "Market");
        add("Cantidad", "Quantity", "Qty"); add("Precio", "Price"); add("Monto", "Amount");
        add("Volumen", "Volume"); add("Mayor", "High"); add("Menor", "Low"); add("Último", "Last", "Ultimo");
        add("Apertura", "Open"); add("Cierre anterior", "Previous close", "Cierre Ant.");
        add("Código operador", "Operator code", "Code Operator"); add("Bolsa", "Security Exchange", "Security Exchange");
        add("Tipo", "Type", "Type"); add("Operación", "Trade", "Trade");
        add("Operaciones generales", "General Trades", "Trade General"); add("Margen", "Margin", "Margin");
        add("Sin seleccionar", "Not selected", "(sin seleccionar)");
    }

    private static void registerTradingTerms() {
        add("Compra", "Buy", "BUY"); add("Venta", "Sell", "SELL");
        add("Compras", "Buys"); add("Ventas", "Sells"); add("Comprador", "Buyer"); add("Vendedor", "Seller");
        add("Trabajando", "Working"); add("Ejecutadas", "Executed"); add("Enviadas", "Sent");
        add("Activas", "Active"); add("Calzadas", "Filled orders");
        add("Calzada", "Filled", "FILLED", "CALZADA");
        add("Nueva", "New", "NEW", "NUEVA"); add("Parcial", "Partially filled", "PARTIALLY_FILLED", "PARTIAL");
        add("Cancelada", "Canceled", "CANCELED", "CANCELLED", "CANCELADA");
        add("Rechazada", "Rejected", "REJECTED", "RECHAZADA");
        add("Reemplazada", "Replaced", "REPLACED", "REMPLAZADA");
        add("Pendiente nueva", "Pending new", "PENDING_NEW");
        add("Pendiente cancelar", "Pending cancel", "PENDING_CANCEL");
        add("Pendiente reemplazar", "Pending replace", "PENDING_REPLACE");
        add("Suspendida", "Suspended", "SUSPENDED"); add("Abortada", "Aborted", "ABORTED");
        add("Órdenes pendientes", "Pending orders", "PENDING_ONLY");
        add("Órdenes vigentes", "Live orders", "LIVE"); add("Vigentes y ejecuciones", "Live and filled", "LIVE_TRADE");
        add("Límite", "Limit", "LIMIT"); add("Orden a mercado", "Market order");
        add("Estrategia", "Strategy"); add("Sin estrategia", "No strategy", "NONE_STRATEGY", "NONE_ST...");
        add("Todos los estados", "All statuses", "ALL_STATUS");
        add("Todos los destinos", "All destinations", "ALL_SECURITY_EXCHANGE", "ALL_SECURITY...");
        add("Sin ruteo", "No routing", "NONE_ROUTING");
        add("Ruteo", "Routing"); add("Posiciones", "Positions"); add("Datos del Mercado", "Market Data");
        add("Nacional", "National"); add("Administración de usuarios", "User Administration", "Adm. Usuarios");
        add("Lanzador de Órdenes", "Order Launcher", "Lanzador de Ordenes"); add("Libros", "Books");
        add("Órdenes Históricas", "Historical Orders"); add("Pre-Digitadas", "Staged Orders", "Pre-Digitados");
        add("Mejor", "Best"); add("Agredir", "Cross"); add("Pánico", "Panic");
        add("Cantidad ejecutada", "Executed quantity"); add("Cantidad pendiente", "Pending quantity");
        add("Cantidad orden", "Order quantity"); add("Precio promedio", "Average price", "Precio Promedio");
        add("Cantidad original", "Original quantity", "Qty Ori"); add("Cantidad total", "Total quantity", "Qty total");
        add("Cantidad garantía", "Collateral quantity", "Qty Garantia"); add("Cantidad plazo", "Term quantity", "Qty Plazo");
        add("Última cantidad", "Last quantity", "Ult. Cantidad"); add("Último precio", "Last price", "Ult. Precio");
        add("Monto ejecutado", "Executed amount"); add("Monto pendiente", "Pending amount");
        add("Órdenes", "Orders"); add("Palos", "Executions"); add("Motivo", "Reason");
        add("Visible %", "Visible %"); add("Liquidación", "Settlement"); add("Clase", "Class");
        add("Tipo de Orden", "Order type"); add("Tipo Moneda", "Currency"); add("Código operador", "Operator code", "Cod. Operador");
        add("Precio Compra", "Bid price", "Px Compra"); add("Precio Venta", "Ask price", "Px Venta");
        add("Cantidad Compra", "Bid quantity", "Qty Compra"); add("Cantidad Venta", "Ask quantity", "Qty Venta");
        add("Tendencia", "Trend"); add("Variación %", "Change %", "Var %", "V.%");
        add("Apertura", "Open"); add("Máximo", "High"); add("Mínimo", "Low");
        add("Monto teórico", "Theoretical amount"); add("Precio teórico", "Theoretical price");
        add("Desbalance teórico", "Theoretical imbalance"); add("Eliminar portafolio", "Delete portfolio");
        add("Agregar libro", "Add book", "Agregar Libro"); add("Pegar orden", "Paste Order");
        add("Límite (PX)", "Limit (PX)");
        add("Indivisible", "All or none"); add("Ocultar IDs", "Hide IDs");
        add("Bróker", "Broker", "Broker"); add("Condición", "Condition");
        add("Divisa", "Currency"); add("Destino", "Destination"); add("Lado", "Side");
        add("Cantidad disponible", "Available quantity"); add("Cantidad vigente", "Working quantity");
        add("Última ejecución", "Last execution"); add("Últimas Operaciones", "Latest Trades");
        add("Últimas Operaciones Nemo", "Latest Instrument Trades");
        add("Tipo de Ejecución", "Execution type"); add("Fecha Ingreso", "Entry time");
        add("Total compra", "Total Buy", "Total Buy"); add("Total venta", "Total Sell", "Total Sell");
        add("Total neto", "Total Net", "Total Net"); add("Valor mercado", "Market value");
        add("Precio mercado", "Market price"); add("Precio ayer", "Previous price");
        add("Garantía", "Collateral"); add("Garantías", "Collateral");
        add("Garantías constituidas", "Posted collateral"); add("Garantías exigidas", "Required collateral");
        add("Garantías reservadas", "Reserved collateral"); add("Límite financiero", "Financial limit");
        add("Rendimiento", "Yield"); add("Costo diario", "Daily cost");
        add("Fecha vencimiento", "Maturity date"); add("Plazo", "Term");
        add("Nemotécnico", "Ticker"); add("Número de cliente", "Client number", "N Cli");
        add("Código instrumento", "Instrument code", "Cod Ins"); add("ID cliente", "Client ID", "Id CLien.");
        add("Fecha operación", "Trade date", "F. Ope"); add("Fecha venta", "Sell date", "F. Ven");
        add("Precio contado", "Cash price", "Monto Contado"); add("Precio mercado", "Market price", "PX Mercado");
        add("Total disponible:", "Total available:"); add("Total garantías:", "Total collateral:");
        add("Total operaciones:", "Total trades:"); add("Total simultáneas:", "Total simultaneous:");
        add("Diarias", "Daily"); add("Históricas", "Historical"); add("Saldo", "Balance");
        add("Préstamos", "Loans"); add("Simultánea", "Simultaneous", "Simultanea");
        add("Simultáneas", "Simultaneous trades", "Simultaneas");
        add("Saldo de cartera", "Portfolio balance"); add("Patrimonio", "Equity");
        add("Disponible", "Available"); add("Cartera", "Portfolio"); add("Cupo", "Credit line");
        add("Órdenes activas", "Active orders"); add("Órdenes calzadas", "Filled orders");
        add("Resumen de Mercado", "Market Summary"); add("Ranking de Mercado", "Market Ranking");
        add("Evolución Mercado", "Market Evolution"); add("Top Volumen", "Top Volume");
        add("Volumen Total", "Total Volume"); add("Monto Total", "Total Amount");
        add("Capitalización Total", "Total Market Cap"); add("Tendencia General", "Overall Trend");
        add("Volatilidad Promedio", "Average Volatility"); add("Sentimiento Positivo", "Positive Sentiment");
        add("Sentimiento Negativo", "Negative Sentiment"); add("Rango Promedio", "Average Range");
        add("Índice Promedio", "Average Index"); add("Índice Máximo", "Maximum Index"); add("Índice Mínimo", "Minimum Index");
        add("Nº Total Trades", "Total Trades"); add("Cap. Promedio", "Average Market Cap");
        add("Precio Prom. Acum.", "Accum. Average Price"); add("Precio Máx. Acum.", "Accum. Maximum Price");
        add("Tendencia Promedio", "Average Trend"); add("Ver Velas", "View Candles");
        add("Copiar Datos", "Copy Data"); add("Aplicar Fecha", "Apply Date"); add("Limpiar Fecha", "Clear Date");
        add("Temporalidad", "Timeframe"); add("Indicadores", "Indicators"); add("Papel (Velas)", "Instrument (Candles)");
        add("Fecha Stats", "Statistics date"); add("Temporalidad Stats", "Statistics timeframe");
        add("Rango Hist.", "Historical range"); add("TF Hist.", "Historical timeframe");
        add("Más tranzado", "Most traded"); add("Período:", "Period:"); add("Último cierre:", "Last close:");
        add("Canasta", "Basket"); add("Ejecutar todo", "Execute all"); add("Cancelar todo", "Cancel all");
        add("Cancelar orden", "Cancel order"); add("Modificar orden", "Modify order");
        add("Monto total", "Total amount"); add("Ejecutado %", "Executed %"); add("Pendiente %", "Pending %");
        add("Órdenes hechas", "Completed orders"); add("Órdenes pendientes", "Pending orders");
        add("P&L realizado (rango)", "Realized P&L (range)");
        add("Precio prom. compra", "Average buy price"); add("Precio prom. venta", "Average sell price");
        add("Monto compras", "Buy amount"); add("Monto ventas", "Sell amount");
        add("Monto operado · Top 5 + Otros", "Traded amount · Top 5 + Others");
        add("Exportar Excel", "Export Excel");
        add("Exportar resumen, órdenes y todos sus palos según los filtros actuales",
                "Export summary, orders and every execution using the current filters");
        add("Promedio ponderado por cantidad de los palos comprados en el filtro actual",
                "Quantity-weighted average of buy executions in the current filter");
        add("Promedio ponderado por cantidad de los palos vendidos en el filtro actual",
                "Quantity-weighted average of sell executions in the current filter");
        add("Configuración del Multibook", "MultiBook settings"); add("DISEÑO", "LAYOUT");
        add("LIBROS/PÁG.", "BOOKS/PAGE"); add("PÁGINAS", "PAGES"); add("PROF.", "DEPTH");
        add("Renombrar página", "Rename page"); add("VISTA", "VIEW"); add("Gráficos", "Charts", "Graficos");
        add("Actividad de órdenes", "Order activity"); add("Selecciona la pestaña para consultar", "Select a tab to view");
        add("Selecciona mercado", "Select market"); add("Selecciona papel", "Select instrument");
        add("Selecciona rango", "Select range"); add("Selecciona temporalidad", "Select timeframe");
        add("Selecciona un filtro", "Select a filter"); add("Hora término", "End time");
        add("Minutos / Hora Inicio", "Minutes / Start Time"); add("Precio promedio compra", "Average buy price");
        add("Total", "Total"); add("Resumen", "Summary"); add("Desbloquear", "Unlock");
    }

    private static void registerSettingsAndMessages() {
        add("Idioma", "Language"); add("Idioma de la interfaz", "Interface language");
        add("Español", "Spanish"); add("Inglés", "English");
        add("Personaliza la interfaz sin modificar la mensajería de trading.",
                "Customize the interface without changing trading messages.");
        add("El cambio es inmediato y se conserva para el próximo inicio.",
                "The change is immediate and saved for the next launch.");
        add("Actualiza la contraseña de tu usuario conectado.",
                "Update the password for your signed-in user.");
        add("Nueva contraseña", "New password");
        add("Seguridad", "Security"); add("Restablecer contraseña", "Reset password");
        add("Repetir contraseña", "Repeat password"); add("Cambiar contraseña", "Change password");
        add("Los campos no pueden estar vacíos", "Fields cannot be empty");
        add("Las contraseñas no coinciden", "Passwords do not match", "The passwords do not match.");
        add("Contraseña cambiada exitosamente", "Password changed successfully");
        add("Éxito", "Success", "Successful Update"); add("Error", "Error");
        add("El email fue actualizado exitosamente.", "The email has been updated successfully.");
        add("El nombre fue actualizado exitosamente.", "The first name has been updated successfully.");
        add("El apellido fue actualizado exitosamente.", "The last name has been updated successfully.");
        add("El teléfono fue actualizado exitosamente.", "The phone has been updated successfully.");
        add("El usuario ya existe en el sistema", "The user already exists in the system");
        add("Habilitar usuarios", "Enable users", "Enable Users");
        add("Agregar usuario", "Add user", "ADD User");
        add("Selecciona un grupo", "Select a group"); add("Filtrar", "Filter");
        add("Cambiar tema", "Change theme"); add("Sonido activado", "Sound enabled");
        add("Sonido desactivado", "Sound disabled"); add("Notificaciones activadas", "Notifications enabled");
        add("Notificaciones desactivadas", "Notifications disabled");
        add("Estado Conexión", "Connection Status"); add("Entorno", "Environment");
        add("Iniciar sesión", "Sign in", "Iniciar sesion"); add("Ingresar", "Sign in");
        add("Recordar usuario y contraseña", "Remember username and password", "Recordar usuario y contrasena");
        add("Ingresa tus credenciales para operar", "Enter your credentials to trade");
        add("Plataforma de trading y ruteo de órdenes", "Trading and order-routing platform", "Plataforma de trading y ruteo de ordenes");
        add("Conversación:", "Conversation:"); add("Escribe un mensaje y presiona Enter", "Type a message and press Enter");
        add("Ambiente", "Environment"); add("Consola de mensajes", "Message Console", "Message Console");
        add("Título", "Title", "Title"); add("Selecciona un grupo", "Select a group", "Select a group");
        add("Grupos", "Groups", "Groups"); add("Subgrupos", "Subgroups", "Sub groups");
        add("Filtrar", "Filter", "Filter"); add("Comentarios", "Comments", "Comments");
        add("Componente", "Component", "Component"); add("Acción", "Action", "Action");
        add("Aviso", "Notice"); add("Confirmación", "Confirmation", "Confirm Action");
        add("Detalle", "Details"); add("Detalle de ejecuciones", "Execution details");
        add("Distribución por precio", "Price distribution"); add("Monto operado", "Traded amount");
        add("Libro de mercado", "Market book"); add("Libro Vertical", "Vertical book");
        add("Libro Horizontal", "Horizontal book"); add("Agregar página", "Add page");
        add("Nuevo portafolio", "New portfolio"); add("Agregar nuevo portafolio", "Add new portfolio");
        add("Ingrese el nombre del portafolio:", "Enter the portfolio name:");
        add("Esta acción removerá la pestaña del portafolio.", "This action will remove the portfolio tab.");
        add("Sin instrumentos", "No instruments"); add("Consultando…", "Loading…"); add("Preparando…", "Preparing…");
        add("Estado de conexiones", "Connection status"); add("Estadísticas de Mercado", "Market Statistics");
        add("Gráfico de Velas", "Candlestick Chart", "Grafico de Velas");
        add("Consola de Noticias", "News Console"); add("Canasta abierta", "Basket open");
        add("Error cargando basket", "Error loading basket"); add("Basket con filas inválidas", "Basket contains invalid rows");
        add("Falta(n) columna(s) en la canasta", "The basket is missing required column(s)");
        add("No se pudo abrir el tab de la canasta", "Could not open the basket tab");
        add("Clase no permitida", "Class not allowed"); add("No permitido", "Not allowed");
        add("El usuario o contraseña no son válidos.", "The username or password is invalid.");
        add("Error al cargar credenciales.", "Error loading credentials.");
        add("Ha ocurrido un error", "An error has occurred");
        add("El margen fue actualizado exitosamente.", "The margin has been updated successfully.");
        add("Ha ocurrido un error durante la actualización.", "An error occurred during the update.");
        add("Actualización disponible", "Update available", "Actualizacion disponible", "Actualización Disponible");
        add("Actualizando…", "Updating…", "Actualizando..."); add("Descargando actualización...", "Downloading update...");
        add("Una nueva versión de la aplicación está disponible.", "A new application version is available.");
        add("¿Deseas actualizar ahora?", "Do you want to update now?");
        add("Actualización Completada", "Update Completed"); add("Resumen de Actualización", "Update Summary");
        add("Doble click para renombrar", "Double-click to rename"); add("Copiado", "Copied");
        add("Exportar configuración", "Export configuration"); add("Importar configuración", "Import configuration");
        add("Exportar detalle de ejecuciones", "Export execution details");
        add("Exportar historial de órdenes", "Export order history");
        add("Solicitud de diagnóstico", "Diagnostic request", "Solicitud de diagnostico");
        add("Soporte solicita un extracto del log de este equipo", "Support is requesting a log extract from this computer");
        add("Sin conexión al core", "No core connection"); add("Desconexión", "Disconnected");
        add("Cargando histórico", "Loading history", "CARGANDO HISTÓRICO");
        add("Cargando trades", "Loading trades", "CARGANDO TRADES");
        add("Histórico Mongo", "Mongo history", "HISTÓRICO MONGO");
        add("Trades del día", "Today's trades", "TRADES DEL DÍA");
        add("Tiempo real", "Real-time", "REAL-TIME"); add("Atrasado", "Delayed", "ATRASADO");
        add("Sin datos reales", "No real data", "SIN DATOS REALES"); add("Sin timestamp", "No timestamp", "SIN TIMESTAMP");
    }

    private static void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) action.run(); else Platform.runLater(action);
    }

    private record Translation(String spanish, String english) {
        private String forLanguage(Language language) {
            return language == Language.ENGLISH ? english : spanish;
        }
    }
}
