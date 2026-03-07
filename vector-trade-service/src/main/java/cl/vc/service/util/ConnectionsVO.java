package cl.vc.service.util;

import lombok.Data;

@Data
public class ConnectionsVO {

    public String host;
    public int port;
    public Boolean status;
}
