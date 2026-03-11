package cl.vc.module.protocolbuff.generator;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.FileAppender;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogGenerator {

    public static Logger start(String path, String nameLog) {
        LoggerContext context = (LoggerContext)LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("[%date{ISO8601}] %msg%n");
        encoder.start();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String name = nameLog + "_" + dateFormat.format(new Date());

        File carpeta = new File(path);
        if (!carpeta.exists()) {
            carpeta.mkdirs();
        }

        String completePath = path + name + ".log";
        FileAppender appender = new FileAppender();
        appender.setContext(context);
        appender.setFile(completePath);
        appender.setName(name);
        appender.setAppend(true);
        appender.setEncoder(encoder);
        appender.start();

        Logger fileLog = context.getLogger(name);
        fileLog.setAdditive(false);
        fileLog.setLevel(Level.ALL);
        fileLog.addAppender(appender);
        return fileLog;
    }
}
