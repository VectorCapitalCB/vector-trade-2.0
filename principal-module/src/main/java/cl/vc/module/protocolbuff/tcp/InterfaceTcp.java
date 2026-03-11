package cl.vc.module.protocolbuff.tcp;

import com.google.protobuf.Message;

public interface InterfaceTcp {

    public void sendMessage(String message);
    public void sendMessage(Message message) ;
    public void stopService();
    public void startService();
}
