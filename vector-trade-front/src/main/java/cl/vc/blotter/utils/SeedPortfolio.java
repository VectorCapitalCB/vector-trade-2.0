package cl.vc.blotter.utils;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ws.vectortrade.MessageUtilVT;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Utilitario de un solo uso: siembra un portafolio con una lista de simbolos.
 *
 * No hay API administrativa de portafolios en el OMS (los servlets admin cubren cuentas,
 * ordenes, redis, etc., pero no portafolios) y los valores en Redis son objetos
 * serializados con MarshallingCodec de Redisson, imposibles de editar desde fuera de la
 * JVM. Por eso esto habla el protocolo real de la app: mismo header Basic con AES, mismo
 * framing protobuf y el mismo PortfolioRequest/ADD_ASSET que manda el boton "Agregar".
 *
 * Uso:
 *   java -cp <classpath> cl.vc.blotter.utils.SeedPortfolio \
 *        ws://172.16.0.8:8096/websocket/ <usuario> <password> <portafolio> SYM1,SYM2,...
 */
public class SeedPortfolio {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: SeedPortfolio <wsUrl> <usuario> <password> <portafolio> <SYM1,SYM2,...>");
            System.exit(2);
        }
        String wsUrl = args[0];
        String usuario = args[1].trim().toLowerCase();
        String password = args[2];
        String portafolio = args[3];
        String[] simbolos = args[4].split("\\s*,\\s*");

        // Mismo esquema que LoginController: AES por separado y luego Base64 del "u:p".
        String credenciales = AESEncryption.encrypt(usuario) + ":" + AESEncryption.encrypt(password);
        String basic = Base64.getEncoder().encodeToString(credenciales.getBytes(StandardCharsets.UTF_8));

        WebSocketClient client = new WebSocketClient();
        client.getPolicy().setMaxBinaryMessageSize(100 * 1024 * 1024);
        client.start();

        CountDownLatch conectado = new CountDownLatch(1);
        final Session[] sesion = new Session[1];

        WebSocketAdapter adapter = new WebSocketAdapter() {
            @Override public void onWebSocketConnect(Session s) {
                super.onWebSocketConnect(s);
                sesion[0] = s;
                conectado.countDown();
            }
            @Override public void onWebSocketError(Throwable cause) {
                System.err.println("Error de WebSocket: " + cause);
                conectado.countDown();
            }
            @Override public void onWebSocketBinary(byte[] payload, int offset, int len) {
                // Solo interesa el SNAPSHOT_PORTFOLIO del modo --listar; el resto del
                // trafico (market data, ordenes) se ignora.
                try {
                    var msg = MessageUtilVT.onDeserializeMessage(ByteBuffer.wrap(payload, offset, len));
                    if (msg instanceof BlotterMessage.PortfolioResponse resp
                            && resp.getStatusPortfolio() == BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO) {
                        System.out.println("Portafolios de " + resp.getUsername() + ":");
                        resp.getPostfolioList().forEach(p ->
                                System.out.println(String.format("  %-16s %d papeles", p.getNamePortfolio(), p.getAssetCount())));
                    }
                } catch (Exception ignore) {
                    // trafico no relacionado
                }
            }
        };

        ClientUpgradeRequest request = new ClientUpgradeRequest();
        request.setHeader("Authorization", "Basic " + basic);
        client.connect(adapter, new URI(wsUrl), request);

        if (!conectado.await(20, TimeUnit.SECONDS) || sesion[0] == null || !sesion[0].isOpen()) {
            System.err.println("No se pudo conectar/autenticar. Revisa usuario, password y que el OMS responda.");
            client.stop();
            System.exit(1);
        }
        System.out.println("Conectado como " + usuario + ". Sembrando '" + portafolio + "' con " + simbolos.length + " simbolos.");

        // NEW_PORTFOLIO solo si se pide explicitamente: el handler del OMS hace
        // porfolios.put(nombre, nuevo), o sea RESETEA el portafolio si ya existe y se
        // perderian sus assets. Y sin crearlo, ADD_ASSET revienta con NPE porque hace
        // portfolio.toBuilder() sobre null.
        // Modo verificacion: pide el snapshot y muestra que hay realmente en Redis.
        if ("--listar".equalsIgnoreCase(portafolio)) {
            BlotterMessage.PortfolioRequest snap = BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.SNAPSHOT_PORTFOLIO)
                    .setUsername(usuario)
                    .build();
            sesion[0].getRemote().sendBytes(MessageUtilVT.serializeMessageByteBuffer(snap));
            Thread.sleep(4000);
            sesion[0].close();
            client.stop();
            return;
        }

        boolean crear = args.length > 5 && "--crear".equalsIgnoreCase(args[5]);
        if (crear) {
            BlotterMessage.PortfolioRequest nuevo = BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.NEW_PORTFOLIO)
                    .setNamePortfolio(portafolio)
                    .setUsername(usuario)
                    .build();
            sesion[0].getRemote().sendBytes(MessageUtilVT.serializeMessageByteBuffer(nuevo));
            System.out.println("  (portafolio '" + portafolio + "' creado)");
            Thread.sleep(1500);
        }

        int enviados = 0;
        for (String simbolo : simbolos) {
            String s = simbolo.trim();
            if (s.isEmpty()) continue;

            MarketDataMessage.Statistic statistic = MarketDataMessage.Statistic.newBuilder()
                    .setSymbol(s)
                    .setId(IDGenerator.getID())
                    .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                    .setSettlType(RoutingMessage.SettlType.T2)
                    .setSecurityType(RoutingMessage.SecurityType.CS)
                    .build();

            BlotterMessage.Asset asset = BlotterMessage.Asset.newBuilder()
                    .setSymbol(s)
                    .setStatistic(statistic)
                    .setSecurityexchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                    .build();

            BlotterMessage.PortfolioRequest peticion = BlotterMessage.PortfolioRequest.newBuilder()
                    .setStatusPortfolio(BlotterMessage.StatusPortfolio.ADD_ASSET)
                    .setNamePortfolio(portafolio)
                    .setAsset(asset)
                    .setUsername(usuario)
                    .build();

            ByteBuffer payload = MessageUtilVT.serializeMessageByteBuffer(peticion);
            sesion[0].getRemote().sendBytes(payload);
            enviados++;
            System.out.println("  + " + s);
            // El OMS procesa cada ADD_ASSET contra el RMap de Redis; sin pausa la rafaga
            // puede adelantarse a la escritura y perder simbolos.
            Thread.sleep(150);
        }

        Thread.sleep(2000);
        System.out.println("Enviados " + enviados + " ADD_ASSET a '" + portafolio + "'.");
        sesion[0].close();
        client.stop();
    }
}
