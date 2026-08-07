package cl.vc.service.util;

import org.jboss.marshalling.AbstractClassResolver;

import java.util.Set;

/**
 * ClassResolver de JBoss Marshalling (River) con WHITELIST: solo deserializa las clases que
 * realmente aparecen en los datos de Redis del servicio (verificado read-only contra prod):
 * contenedores java.util + wrappers java.lang + protobuf. Cualquier otra clase => SecurityException.
 *
 * Cierra el RCE por deserializacion (el codec por defecto de Redisson resuelve cualquier clase por
 * nombre = ObjectInputStream.readObject). Mantiene el formato River default -> compatible con los
 * datos ya escritos en Redis (solo cambia QUE clases se permiten cargar).
 */
public class SecureRiverClassResolver extends AbstractClassResolver {

    private static final Set<String> ALLOWED_EXACT = Set.of(
            // contenedores
            "java.util.HashMap", "java.util.LinkedHashMap", "java.util.TreeMap",
            "java.util.concurrent.ConcurrentHashMap",
            "java.util.Collections$SynchronizedMap", "java.util.Collections$UnmodifiableMap",
            "java.util.Collections$EmptyMap",
            "java.util.ArrayList", "java.util.LinkedList",
            "java.util.HashSet", "java.util.LinkedHashSet", "java.util.TreeSet",
            "java.util.Collections$SynchronizedList", "java.util.Collections$UnmodifiableList",
            "java.util.Collections$SynchronizedSet",
            // wrappers / basicos
            "java.lang.String", "java.lang.Number", "java.lang.Long", "java.lang.Integer",
            "java.lang.Double", "java.lang.Float", "java.lang.Boolean", "java.lang.Short",
            "java.lang.Byte", "java.lang.Character", "java.math.BigDecimal", "java.math.BigInteger"
    );

    private static final String[] ALLOWED_PREFIX = {
            "com.google.protobuf.",              // GeneratedMessageV3, GeneratedMessageLite$SerializedForm, etc.
            "cl.vc.module.protocolbuff."         // los mensajes de la app (defensivo)
    };

    @Override
    protected ClassLoader getClassLoader() {
        return SecureRiverClassResolver.class.getClassLoader();
    }

    @Override
    protected Class<?> loadClass(String name) throws ClassNotFoundException {
        if (!isAllowed(name)) {
            throw new SecurityException("River: clase no permitida por seguridad al deserializar de Redis: " + name);
        }
        return super.loadClass(name);
    }

    private static boolean isAllowed(String name) {
        String base = name;
        // arrays: "[Lcom.google...;" -> "com.google..."
        while (base.startsWith("[")) base = base.substring(1);
        if (base.startsWith("L") && base.endsWith(";")) {
            base = base.substring(1, base.length() - 1);
        }
        if (ALLOWED_EXACT.contains(base)) return true;
        for (String p : ALLOWED_PREFIX) {
            if (base.startsWith(p)) return true;
        }
        return false;
    }
}
