package cl.vc.module.protocolbuff.akka;

import akka.actor.ActorRef;
import akka.event.japi.LookupEventBus;
import akka.event.japi.SubchannelEventBus;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;

public class MessageEventBus extends LookupEventBus<Envelope, ActorRef, String> {

	@Override public int mapSize() { return 32768; }

	@Override
	public int compareSubscribers(ActorRef a, ActorRef b) {
		return a.compareTo(b);
	}

	@Override public String classify(Envelope event) {
		return event.getTopic();
	}

	@Override public void publish(Envelope event, ActorRef subscriber) {
		subscriber.tell(event.getPayload(), ActorRef.noSender());
	}

}
