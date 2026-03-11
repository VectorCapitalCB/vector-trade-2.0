package cl.vc.module.protocolbuff.akka;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Envelope {
	private final String topic;
	private final Object payload;
}
