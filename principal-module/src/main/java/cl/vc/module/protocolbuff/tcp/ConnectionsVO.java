package cl.vc.module.protocolbuff.tcp;
import lombok.Data;

@Data
public class ConnectionsVO {
    private String host;
    private int port;
    private Boolean status;
}
