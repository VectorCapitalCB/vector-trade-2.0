package cl.vc.inyectorcandle.fix;

import quickfix.*;

public class FixInitiatorRunner implements AutoCloseable {

    private final SocketInitiator initiator;

    public FixInitiatorRunner(BcsFixApplication app, String sessionSettingsPath) throws ConfigError {
        SessionSettings settings = new SessionSettings(sessionSettingsPath);
        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new FileLogFactory(settings);
        MessageFactory messageFactory = new DefaultMessageFactory();

        this.initiator = new SocketInitiator(app, storeFactory, settings, logFactory, messageFactory);
    }

    public void start() throws ConfigError {
        initiator.start();
    }

    @Override
    public void close() {
        initiator.stop();
    }
}
