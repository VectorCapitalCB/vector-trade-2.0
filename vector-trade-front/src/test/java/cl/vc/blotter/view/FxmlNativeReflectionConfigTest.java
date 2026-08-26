package cl.vc.blotter.view;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ProcessingInstruction;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FxmlNativeReflectionConfigTest {

    private static final Path FXML_DIR = Paths.get("src/main/resources/view");
    private static final Path REFLECT_CONFIG = Paths.get(
            "src/main/resources/META-INF/native-image/reflect-config.json");
    private static final Path POM = Paths.get("pom.xml");

    @Test
    void everyFxmlTypeHasInvokableNativeMetadata() throws Exception {
        Set<String> fxmlTypes = fxmlTypes();
        List<Map<String, Object>> entries = new ObjectMapper().readValue(
                REFLECT_CONFIG.toFile(), new TypeReference<>() { });

        Set<String> methods = new HashSet<>();
        Set<String> constructors = new HashSet<>();
        for (Map<String, Object> entry : entries) {
            String name = (String) entry.get("name");
            if (Boolean.TRUE.equals(entry.get("allPublicMethods"))) methods.add(name);
            if (Boolean.TRUE.equals(entry.get("allPublicConstructors"))) constructors.add(name);
        }

        Set<String> missingMethods = new TreeSet<>(fxmlTypes);
        missingMethods.removeAll(methods);
        Set<String> missingConstructors = new TreeSet<>(fxmlTypes);
        missingConstructors.removeAll(constructors);

        assertTrue(missingMethods.isEmpty(),
                "FXML types without allPublicMethods in native reflection config: " + missingMethods);
        assertTrue(missingConstructors.isEmpty(),
                "FXML types without allPublicConstructors in native reflection config: " + missingConstructors);
    }

    @Test
    void everyFxmlTypeIsReachableFromGluonReflectionList() throws Exception {
        Set<String> reflectionList = reflectionList();

        Set<String> missing = new TreeSet<>(fxmlTypes());
        missing.removeAll(reflectionList);
        assertTrue(missing.isEmpty(), "FXML types missing from Gluon reflectionList: " + missing);
    }

    @Test
    void everyFxmlControllerHasCompleteNativeMetadata() throws Exception {
        Set<String> controllers = fxmlControllers();
        Set<String> reflectionList = reflectionList();
        Set<String> missingReachability = new TreeSet<>(controllers);
        missingReachability.removeAll(reflectionList);
        assertTrue(missingReachability.isEmpty(),
                "FXML controllers missing from Gluon reflectionList: " + missingReachability);

        List<Map<String, Object>> entries = new ObjectMapper().readValue(
                REFLECT_CONFIG.toFile(), new TypeReference<>() { });
        Set<String> complete = new HashSet<>();
        for (Map<String, Object> entry : entries) {
            if (Boolean.TRUE.equals(entry.get("allDeclaredFields"))
                    && Boolean.TRUE.equals(entry.get("allDeclaredMethods"))
                    && Boolean.TRUE.equals(entry.get("allDeclaredConstructors"))) {
                complete.add((String) entry.get("name"));
            }
        }
        Set<String> missingMetadata = new TreeSet<>(controllers);
        missingMetadata.removeAll(complete);
        assertTrue(missingMetadata.isEmpty(),
                "FXML controllers without complete native reflection metadata: " + missingMetadata);
    }

    private static Set<String> reflectionList() throws Exception {
        Document pom = parseXml(POM);
        NodeList lists = pom.getElementsByTagName("list");
        Set<String> result = new HashSet<>();
        for (int i = 0; i < lists.getLength(); i++) {
            result.add(lists.item(i).getTextContent().trim());
        }
        return result;
    }

    private static Set<String> fxmlControllers() throws Exception {
        Set<String> result = new TreeSet<>();
        try (Stream<Path> paths = Files.walk(FXML_DIR)) {
            for (Path path : paths.filter(p -> p.toString().endsWith(".fxml")).toList()) {
                String controller = parseXml(path).getDocumentElement().getAttribute("fx:controller");
                if (!controller.isBlank()) result.add(controller);
            }
        }
        return result;
    }

    private static Set<String> fxmlTypes() throws Exception {
        Set<String> result = new TreeSet<>();
        try (Stream<Path> paths = Files.walk(FXML_DIR)) {
            for (Path path : paths.filter(p -> p.toString().endsWith(".fxml")).toList()) {
                Document document = parseXml(path);
                Map<String, String> explicitImports = new HashMap<>();
                List<String> wildcardImports = new ArrayList<>();
                NodeList children = document.getChildNodes();
                for (int i = 0; i < children.getLength(); i++) {
                    Node node = children.item(i);
                    if (node instanceof ProcessingInstruction instruction
                            && "import".equals(instruction.getTarget())) {
                        String imported = instruction.getData().trim();
                        if (imported.endsWith(".*")) {
                            wildcardImports.add(imported.substring(0, imported.length() - 2));
                        } else {
                            explicitImports.put(imported.substring(imported.lastIndexOf('.') + 1), imported);
                        }
                    }
                }
                collectElementTypes(document.getDocumentElement(), explicitImports, wildcardImports, result, path);
            }
        }
        return result;
    }

    private static void collectElementTypes(Element element, Map<String, String> explicitImports,
                                            List<String> wildcardImports, Set<String> result, Path path)
            throws ClassNotFoundException {
        String tag = element.getTagName();
        if (!tag.isEmpty() && Character.isUpperCase(tag.charAt(0))
                && !tag.contains(".") && !tag.contains(":")) {
            result.add(resolve(tag, explicitImports, wildcardImports, path));
        }
        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) instanceof Element child) {
                collectElementTypes(child, explicitImports, wildcardImports, result, path);
            }
        }
    }

    private static String resolve(String tag, Map<String, String> explicitImports,
                                  List<String> wildcardImports, Path path) throws ClassNotFoundException {
        if (explicitImports.containsKey(tag)) return explicitImports.get(tag);
        for (String packageName : wildcardImports) {
            String candidate = packageName + "." + tag;
            try {
                Class.forName(candidate, false, FxmlNativeReflectionConfigTest.class.getClassLoader());
                return candidate;
            } catch (ClassNotFoundException ignored) {
                // Try the next wildcard import used by this FXML document.
            }
        }
        throw new ClassNotFoundException("Cannot resolve FXML tag " + tag + " in " + path);
    }

    private static Document parseXml(Path path) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        return factory.newDocumentBuilder().parse(path.toFile());
    }
}
