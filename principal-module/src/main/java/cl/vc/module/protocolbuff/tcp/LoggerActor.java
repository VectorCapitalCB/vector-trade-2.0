package cl.vc.module.protocolbuff.tcp;

import akka.actor.AbstractActor;
import akka.actor.Props;
import ch.qos.logback.classic.Logger;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggerActor extends AbstractActor {

    private final Logger fileLog;
    private final JsonFormat.Printer printer;
    private final Boolean islog;

    public LoggerActor(Logger fileLog, Boolean islog) {
        this.fileLog = fileLog;
        this.islog = islog;
        this.printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();
    }

    public static Props props(Logger fileLog, Boolean islog) {
        return Props.create(LoggerActor.class, fileLog, islog);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.class, this::getMessage)
                .build();
    }

    private void getMessage(Message message) throws InvalidProtocolBufferException {
        if(islog){
            fileLog.info("{} : {}", message.getClass().getSimpleName(), printer.print(message));
        }
    }

}
