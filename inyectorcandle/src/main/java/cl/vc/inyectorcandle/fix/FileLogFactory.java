package cl.vc.inyectorcandle.fix;

import quickfix.*;

import java.io.FileNotFoundException;


public class FileLogFactory implements LogFactory {

	private static final String SETTING_FILE_LOG_PATH = "FileLogPath";
	private static final String SETTING_INCLUDE_MILLIS_IN_TIMESTAMP = "FileIncludeMilliseconds";
	private static final String SETTING_INCLUDE_TIMESTAMP_FOR_MESSAGES = "FileIncludeTimeStampForMessages";
	private final SessionSettings settings;

	public FileLogFactory(SessionSettings settings) {
		this.settings = settings;
	}

	@Override
	public Log create(SessionID sessionID) {

		try {

			boolean includeMillis = false;

			if (settings.isSetting(sessionID, SETTING_INCLUDE_MILLIS_IN_TIMESTAMP)) {
				includeMillis = settings.getBool(sessionID, SETTING_INCLUDE_MILLIS_IN_TIMESTAMP);
			}

			boolean includeTimestampInMessages = false;
			if (settings.isSetting(sessionID, SETTING_INCLUDE_TIMESTAMP_FOR_MESSAGES)) {
				includeTimestampInMessages = settings.getBool(sessionID, SETTING_INCLUDE_TIMESTAMP_FOR_MESSAGES);
			}

			return new FileLog(settings.getString(sessionID, FileLogFactory.SETTING_FILE_LOG_PATH),
					sessionID, includeMillis, includeTimestampInMessages);

		} catch (ConfigError | FieldConvertError | FileNotFoundException e) {
			throw new RuntimeError(e);
		}
	}

}
