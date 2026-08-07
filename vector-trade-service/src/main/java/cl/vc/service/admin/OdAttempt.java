package cl.vc.service.admin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OdAttempt {
    private long timestamp;
    private String username;
    private String account;
    private String symbol;
    private String attemptedSide;
    private double attemptedPrice;
    private double attemptedQuantity;
    private String attemptedOrderId;
    private String attemptedExchange;
    private String conflictingSide;
    private double conflictingPrice;
    private double conflictingQuantity;
    private String conflictingOrderId;
    private String conflictingStatus;
    private String reason;
}
