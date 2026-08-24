package cl.vc.blotter.view;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Todos los .fxml del modulo tienen que ser XML bien formado.
 *
 * POR QUE EXISTE: un FXML mal formado compila igual y pasa todos los demas tests — el FXMLLoader
 * recien revienta cuando el operador abre la ventana, en runtime. Paso exactamente eso con
 * Settings.fxml: unos comentarios con guiones de relleno (`&lt;!-- ---- Titulo ---- --&gt;`) tumbaron la
 * pantalla, porque XML prohibe `--` dentro de un comentario. El build estaba verde.
 *
 * Este test NO carga JavaFX: solo parsea el XML, asi que corre headless y en cualquier maquina.
 * No sustituye a los *FxmlFxTest (que si instancian los controllers y necesitan Platform.startup),
 * pero atrapa toda la familia de errores de sintaxis, que es la que se cuela.
 */
public class FxmlWellFormedTest {

    private static List<Path> fxmls() throws Exception {
        Path dir = Paths.get("src/main/resources/view");
        if (!Files.isDirectory(dir)) {
            fail("No existe " + dir.toAbsolutePath() + "; el test corre desde el modulo vector-trade-front");
        }
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".fxml"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    @TestFactory
    public Stream<DynamicTest> todosLosFxmlSonXmlValido() throws Exception {
        List<Path> archivos = fxmls();
        assertTrue(!archivos.isEmpty(), "no se encontro ningun .fxml en src/main/resources/view");
        return archivos.stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            // Sin resolver DTD ni entidades externas: solo interesa la sintaxis, y ademas evita
            // que el test salga a la red.
            f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            f.setFeature("http://xml.org/sax/features/external-general-entities", false);
            f.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            try {
                f.newDocumentBuilder().parse(new File(p.toString()));
            } catch (Exception e) {
                fail(p.getFileName() + " no es XML bien formado: " + e.getMessage()
                        + "  (recordar: '--' esta prohibido dentro de un comentario XML)");
            }
        }));
    }

    /**
     * Guard especifico del error que ya paso, con mensaje explicito. El parseo de arriba tambien lo
     * detecta, pero el motivo no queda obvio en la salida.
     */
    @TestFactory
    public Stream<DynamicTest> ningunComentarioTieneDobleGuion() throws Exception {
        return fxmls().stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> {
            String texto = Files.readString(p);
            int desde = 0;
            while (true) {
                int ini = texto.indexOf("<!--", desde);
                if (ini < 0) break;
                int fin = texto.indexOf("-->", ini + 4);
                if (fin < 0) fail(p.getFileName() + ": comentario sin cerrar desde el offset " + ini);
                String cuerpo = texto.substring(ini + 4, fin);
                if (cuerpo.contains("--")) {
                    int linea = (int) texto.substring(0, ini).chars().filter(c -> c == '\n').count() + 1;
                    fail(p.getFileName() + ":" + linea + " tiene '--' dentro de un comentario XML."
                            + " El FXMLLoader falla en runtime aunque el build este verde.");
                }
                desde = fin + 3;
            }
        }));
    }
}
