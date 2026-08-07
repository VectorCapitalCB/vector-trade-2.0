package cl.vc.service.akka.actors.routing;

import cl.vc.module.protocolbuff.routing.RoutingMessage;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.HashMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ActorGroupPerAccountRedisPersistenceIT {

    @Test
    void persistsEveryPartialFillAndRejectsOnlyTheExactRetransmission() {
        String host = requiredProperty("redis.it.host");
        String port = requiredProperty("redis.it.port");
        String password = requiredProperty("redis.it.password");
        String account = "codex-partial-fill-" + UUID.randomUUID();

        RedissonClient writer = connect(host, port, password);
        RMap<String, HashMap<String, RoutingMessage.Order>> trades = writer.getMap("Trades");

        double[] lastQuantities = {114_673, 25_740, 106_889, 400_000, 334_000, 18_698};
        double[] cumulativeQuantities = {114_673, 140_413, 247_302, 647_302, 981_302, 1_000_000};
        HashMap<String, RoutingMessage.Order> accountTrades = new HashMap<>();

        try {
            for (int i = 0; i < cumulativeQuantities.length; i++) {
                RoutingMessage.Order fill = order(lastQuantities[i], cumulativeQuantities[i]);
                accountTrades.put(ActorGroupPerAccount.tradeExecutionKey(fill), fill);
                trades.put(account, new HashMap<>(accountTrades));
            }

            RoutingMessage.Order retransmission = order(400_000, 647_302);
            accountTrades.put(ActorGroupPerAccount.tradeExecutionKey(retransmission), retransmission);
            trades.put(account, new HashMap<>(accountTrades));
            assertEquals(6, accountTrades.size());
        } finally {
            writer.shutdown();
        }

        RedissonClient reader = connect(host, port, password);
        try {
            HashMap<String, RoutingMessage.Order> restored = reader
                    .<String, HashMap<String, RoutingMessage.Order>>getMap("Trades")
                    .get(account);

            assertEquals(6, restored.size());
            assertEquals(1_000_000d,
                    restored.values().stream().mapToDouble(RoutingMessage.Order::getLastQty).sum());
        } finally {
            reader.<String, HashMap<String, RoutingMessage.Order>>getMap("Trades").fastRemove(account);
            reader.shutdown();
        }
    }

    private static RedissonClient connect(String host, String port, String password) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + host + ":" + port)
                .setPassword(password);
        return Redisson.create(config);
    }

    private static RoutingMessage.Order order(double lastQuantity, double cumulativeQuantity) {
        return RoutingMessage.Order.newBuilder()
                .setId("p18t6hmad3cf")
                .setExecId("-1pvfl6unog35")
                .setSymbol("LTM")
                .setLastQty(lastQuantity)
                .setCumQty(cumulativeQuantity)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .build();
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required system property: " + name);
        }
        return value;
    }
}
