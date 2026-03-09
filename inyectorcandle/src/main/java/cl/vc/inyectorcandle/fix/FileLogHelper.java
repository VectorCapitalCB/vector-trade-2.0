package cl.vc.inyectorcandle.fix;

import org.quickfixj.CharsetSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.*;
import quickfix.field.converter.UtcTimestampConverter;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class FileLogHelper implements Log {

    private static final Logger log = LoggerFactory.getLogger(FileLogHelper.class);

    public static final String DATE_FORMAT_ONLY = "yyyyMMdd";
    public static final String TIME_FORMAT_ONLY = "HH-mm-ss";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static byte[] timestampDelimeter;

    static {
        try {
            timestampDelimeter = ": ".getBytes(CharsetSupport.getCharset());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }

    }

    private final SessionID sessionID;
    private final String messagesFileName;
    private final String eventFileName;
    private final boolean includeMillis;
    private final boolean includeTimestampForMessages;
    private boolean syncAfterWrite;
    private FileOutputStream messages;
    private FileOutputStream events;

    FileLogHelper(String path, SessionID sessionID, boolean includeMillis, boolean includeTimestampForMessages) throws FileNotFoundException {
        this.sessionID = sessionID;
        String sessionName = FileUtil.sessionIdFileName(sessionID);
        String todayStr = today();
        String prefix = FileUtil.fileAppendPath(path, sessionName + ".");
        this.messagesFileName = prefix + "messages_" + todayStr + ".log";
        this.eventFileName = prefix + "events_" + todayStr + ".log";
        File directory = (new File(this.messagesFileName)).getParentFile();
        if (!directory.exists()) {
            directory.mkdirs();
        }

        this.includeMillis = includeMillis;
        this.includeTimestampForMessages = includeTimestampForMessages;
        this.openLogStreams(true);
    }

    public static String today() {
        try {
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            return sdf.format(cal.getTime());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            return null;
        }
    }

    private void openLogStreams(boolean append) throws FileNotFoundException {
        this.messages = new FileOutputStream(this.messagesFileName, append);
        this.events = new FileOutputStream(this.eventFileName, append);
    }

    public void onEvent(String message) {
        this.writeMessage(this.events, message, true);
    }

    public void onIncoming(String message) {
        this.writeMessage(this.messages, message, true);
    }

    public void onOutgoing(String message) {
        this.writeMessage(this.messages, message, true);
    }

    public void onErrorEvent(String message) {
        this.writeMessage(this.events, message, true);
    }

    private synchronized void writeMessage(FileOutputStream stream, String message, boolean forceTimestamp) {
        try {
            if (forceTimestamp || this.includeTimestampForMessages) {
                this.writeTimeStamp(stream);
            }

            stream.write(message.getBytes(CharsetSupport.getCharset()));
            stream.write(10);
            stream.flush();
            if (this.syncAfterWrite) {
                stream.getFD().sync();
            }
        } catch (IOException var5) {
            LogUtil.logThrowable(this.sessionID, "error writing message to log", var5);
        }

    }

    private void writeTimeStamp(OutputStream out) throws IOException {
        String formattedTime = UtcTimestampConverter.convert(SystemTime.getDate(), this.includeMillis);
        out.write(formattedTime.getBytes(CharsetSupport.getCharset()));
        out.write(timestampDelimeter);
    }

    String getEventFileName() {
        return this.eventFileName;
    }

    String getMessagesFileName() {
        return this.messagesFileName;
    }

    public void setSyncAfterWrite(boolean syncAfterWrite) {
        this.syncAfterWrite = syncAfterWrite;
    }

    public void closeFiles() throws IOException {
        this.messages.close();
        this.events.close();
    }

    public void clear() {
        try {
            this.closeFiles();
            this.openLogStreams(false);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }

    }
}
