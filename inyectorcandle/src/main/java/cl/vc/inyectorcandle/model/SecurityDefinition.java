package cl.vc.inyectorcandle.model;

public record SecurityDefinition(
        InstrumentKey key,
        String securityId,
        String securityDesc,
        String securityType,
        String bookingRefId
) {
}
