package cl.vc.inyectorcandle.fix;

import quickfix.*;

import java.io.FileNotFoundException;

public class FileLogFactoryHelper implements LogFactory {

    private static final String SETTING_FILE_LOG_PATH = "FileLogPath";
    private static final String SETTING_INCLUDE_MILLIS_IN_TIMESTAMP = "FileIncludeMilliseconds";
    private static final String SETTING_INCLUDE_TIMESTAMP_FOR_MESSAGES = "FileIncludeTimeStampForMessages";
    private final SessionSettings settings;

    public FileLogFactoryHelper(SessionSettings settings) {
        this.settings = settings;
    }

    public Log create(SessionID sessionID) {
        try {
            boolean includeMillis = false;
            if (this.settings.isSetting(sessionID, "FileIncludeMilliseconds")) {
                includeMillis = this.settings.getBool(sessionID, "FileIncludeMilliseconds");
            }

            boolean includeTimestampInMessages = false;
            if (this.settings.isSetting(sessionID, "FileIncludeTimeStampForMessages")) {
                includeTimestampInMessages = this.settings.getBool(sessionID, "FileIncludeTimeStampForMessages");
            }

            return new FileLogHelper(this.settings.getString(sessionID, "FileLogPath"), sessionID, includeMillis, includeTimestampInMessages);
        } catch (FieldConvertError | FileNotFoundException | ConfigError var4) {
            throw new RuntimeError(var4);
        }
    }
}
