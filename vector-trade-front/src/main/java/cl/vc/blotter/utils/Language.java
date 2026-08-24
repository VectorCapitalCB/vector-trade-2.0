package cl.vc.blotter.utils;

public enum Language {
    SPANISH("es", "Español"),
    ENGLISH("en", "English");

    private final String code;
    private final String displayName;

    Language(String code, String displayName) {
        this.code = code;
        this.displayName = displayName;
    }

    public String code() {
        return code;
    }

    public String displayName() {
        return displayName;
    }

    public static Language fromCode(String code) {
        return "en".equalsIgnoreCase(code) ? ENGLISH : SPANISH;
    }
}
