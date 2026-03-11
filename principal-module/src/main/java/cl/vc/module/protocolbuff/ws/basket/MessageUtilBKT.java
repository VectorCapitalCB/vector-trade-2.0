package cl.vc.module.protocolbuff.ws.basket;

import cl.vc.algos.bkt.proto.BktStrategyProtos;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generalstrategy.GeneralStrategy;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import java.nio.ByteBuffer;

@Slf4j
public class MessageUtilBKT {


    public static ByteBuffer serializeMessageByteBuffer(Message message) {
        try {

            TopicIdentifierBKT topicIdentifier = TopicIdentifierBKT.valueOf(message.getDescriptorForType().getName());
            byte[] messageBytes = message.toByteArray();
            byte[] data = new byte[messageBytes.length + 1];
            data[0] = topicIdentifier.getId();
            System.arraycopy(messageBytes, 0, data, 1, messageBytes.length);
            return ByteBuffer.wrap(data);
        } catch (IllegalArgumentException e) {
            log.error("error al procesar el mensaje no esta mapeado {}", message.getDescriptorForType().getName());
        }
        return null;
    }


    private static Message deserializeMessage(byte[] data, Message.Builder builder) throws InvalidProtocolBufferException {
        byte[] messageBytes = new byte[data.length - 1];
        System.arraycopy(data, 1, messageBytes, 0, data.length - 1);
        builder.mergeFrom(messageBytes);
        return builder.build();
    }


    public static Message onDeserializeMessage(ByteBuffer byteBuffer) {

        try {

            byte[] data = new byte[byteBuffer.remaining()];
            byteBuffer.get(data);
            TopicIdentifierBKT topic = getTopic(data);




            Message.Builder builder;
            switch (topic) {
                case DeleteBasket:
                    builder = BktStrategyProtos.DeleteBasket.newBuilder();
                    break;
                case User:
                    builder = BlotterMessage.User.newBuilder();
                    break;
                case Pong:
                    builder = SessionsMessage.Pong.newBuilder();
                    break;
                case Ping:
                    builder = SessionsMessage.Ping.newBuilder();
                    break;
                case Subscribe:
                    builder = MarketDataMessage.Subscribe.newBuilder();
                    break;
                case Unsubscribe:
                    builder = MarketDataMessage.Unsubscribe.newBuilder();
                    break;
                case BktSymbolList:
                    builder = BktStrategyProtos.BktSymbolList.newBuilder();
                    break;

                case EtfConfig:
                    builder = BktStrategyProtos.EtfConfig.newBuilder();
                    break;

                case SnapshotBasketRequest:
                    builder = BktStrategyProtos.SnapshotBasketRequest.newBuilder();
                    break;

                case BktSymbol:
                    builder = BktStrategyProtos.BktSymbol.newBuilder();
                    break;

                case Disconnect:
                    builder = SessionsMessage.Disconnect.newBuilder();
                    break;

                case Connect:
                    builder = SessionsMessage.Connect.newBuilder();
                    break;

                case OperationsControl:
                    builder = GeneralStrategy.OperationsControl.newBuilder();
                    break;


                default:
                    log.error("Unknown topic: " + topic);
                    return null;
            }

            return MessageUtilBKT.deserializeMessage(data, builder);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


        return null;

    }

    public static TopicIdentifierBKT getTopic(byte[] data) {
        return TopicIdentifierBKT.fromId(data[0]);
    }
}