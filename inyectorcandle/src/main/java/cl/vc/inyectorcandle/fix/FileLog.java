package cl.vc.inyectorcandle.fix;

import org.quickfixj.CharsetSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.FileUtil;
import quickfix.Log;
import quickfix.LogUtil;
import quickfix.SessionID;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;


public class FileLog implements Log {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileLog.class);
	public static final String DATE_FORMAT_ONLY = "yyyyMMdd";
	public static final String TIME_FORMAT_ONLY = "HH-mm-ss";
	public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	private static byte[] timestampDelimeter;

	static {
		try {
			timestampDelimeter = ": ".getBytes(CharsetSupport.getCharset());
		} catch (UnsupportedEncodingException ex) {
			LOGGER.error("Cannot set TIME_STAMP_DELIMETER: ", ex);
		}
	}

	private final SessionID sessionID;
	private final String messagesFileName;
	private final String eventFileName;
	private boolean syncAfterWrite;

	private FileOutputStream messages;
	private FileOutputStream events;

	private final boolean includeMillis;
	private final boolean includeTimestampForMessages;

	FileLog(String path, SessionID sessionID, boolean includeMillis, boolean includeTimestampForMessages)
			throws FileNotFoundException {
		this.sessionID = sessionID;
		String sessionName = FileUtil.sessionIdFileName(sessionID);
		String todayStr = today();
		String prefix = FileUtil.fileAppendPath(path, sessionName + ".");
		messagesFileName = prefix + "messages_" + todayStr + ".log";
		eventFileName = prefix + "events_" + todayStr + ".log";

		File directory = new File(messagesFileName).getParentFile();
		if (!directory.exists()) {
			directory.mkdirs();
		}

		this.includeMillis = includeMillis;
		this.includeTimestampForMessages = includeTimestampForMessages;

		openLogStreams(true);
	}

	private void openLogStreams(boolean append) throws FileNotFoundException {
		messages = new FileOutputStream(messagesFileName, append);
		events = new FileOutputStream(eventFileName, append);
	}

	@Override
	public void onEvent(String message) {
		writeMessage(events, message, true);
	}

	@Override
	public void onIncoming(String message) {
		writeMessage(messages, message, true);
	}

	@Override
	public void onOutgoing(String message) {
		writeMessage(messages, message, true);
	}

	@Override
	public void onErrorEvent(String message) {
		writeMessage(events, message, true);
	}

	private synchronized void writeMessage(FileOutputStream stream, String message, boolean forceTimestamp) {
		try {
			if (forceTimestamp || includeTimestampForMessages) {
				writeTimeStamp(stream);
			}
			stream.write(message.getBytes(CharsetSupport.getCharset()));
			stream.write('\n');
			stream.flush();
			if (syncAfterWrite) {
				stream.getFD().sync();
			}
		} catch (IOException e) {
			LogUtil.logThrowable(sessionID, "error writing message to log", e);
		}
	}

	private void writeTimeStamp(OutputStream out) throws IOException {

		Instant instant = Instant.now();
		ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");

		String formattedTime = zonedDateTime.format(formatter);

		out.write(formattedTime.getBytes(CharsetSupport.getCharset()));
		out.write(timestampDelimeter);

	}

	String getEventFileName() {
		return eventFileName;
	}

	String getMessagesFileName() {
		return messagesFileName;
	}

	public void setSyncAfterWrite(boolean syncAfterWrite) {
		this.syncAfterWrite = syncAfterWrite;
	}

	public void closeFiles() throws IOException {
		messages.close();
		events.close();
	}


	@Override
	public void clear() {
		try {
			closeFiles();
			openLogStreams(false);
		} catch (IOException ex) {
			LOGGER.error("Could not clear log: ", ex);
		}
	}

	public static String today() {
		try {
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_ONLY);
			return sdf.format(cal.getTime());
		} catch (Exception ex) {
			LOGGER.error("Error while finding today(): ", ex);
		}
		return null;
	}
}
