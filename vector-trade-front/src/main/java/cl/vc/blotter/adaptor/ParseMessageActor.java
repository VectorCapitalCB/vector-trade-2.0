package cl.vc.blotter.adaptor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.ws.vectortrade.MessageUtilVT;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;


@Slf4j
public class ParseMessageActor extends AbstractActor {


    private ActorRef client;

    private ParseMessageActor(ActorRef client) {
        this.client = client;
    }

    public static Props props(ActorRef client) {
        return Props.create(ParseMessageActor.class, client);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ByteBuffer.class, this::onByteBuffer)
                .build();

    }

    private void onByteBuffer(ByteBuffer byteBuffer) {
        Message message1 = MessageUtilVT.onDeserializeMessage(byteBuffer);
        client.tell(message1, ActorRef.noSender());
    }

}