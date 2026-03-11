package cl.vc.module.protocolbuff.akka;

public class Subclassification implements akka.util.Subclassification<String> {

	@Override
	public boolean isEqual(String x, String y) {
		return x.equals(y);
	}

	@Override
	public boolean isSubclass(String x, String y) {
		return x.startsWith(y);
	}

}
