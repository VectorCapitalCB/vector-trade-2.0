package cl.vc.inyectorcandle.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Escritura asincrona hacia Mongo: los hilos de mercado encolan y un unico hilo agrupa
 * por coleccion y envia con bulkWrite no ordenado. El productor nunca hace round-trip.
 */
public final class MongoWriteQueue implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MongoWriteQueue.class);
    private static final BulkWriteOptions UNORDERED = new BulkWriteOptions().ordered(false);
    private static final long OFFER_TIMEOUT_MS = 50L;
    private static final long STATS_LOG_EVERY = 50_000L;

    private final BlockingQueue<Op> queue;
    private final int batchSize;
    private final long flushMs;
    private final Thread flusher;

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong written = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    private volatile boolean running = true;

    public MongoWriteQueue(int queueSize, int batchSize, long flushMs) {
        this.queue = new ArrayBlockingQueue<>(Math.max(1_000, queueSize));
        this.batchSize = Math.max(1, batchSize);
        this.flushMs = Math.max(10L, flushMs);
        this.flusher = new Thread(this::runLoop, "mongo-write-queue");
        this.flusher.setDaemon(true);
        this.flusher.start();
        LOG.info("MongoWriteQueue iniciado queueSize={} batchSize={} flushMs={}", queueSize, this.batchSize, this.flushMs);
    }

    /**
     * @param droppable si la cola esta llena se descarta en vez de frenar el feed. Usar para
     *                  datos reconstruibles (md_events); los trades y candles no son droppable.
     */
    public void submit(MongoCollection<Document> collection, WriteModel<Document> model, boolean droppable) {
        if (!running) {
            return;
        }
        Op op = new Op(collection, model, null);
        if (queue.offer(op)) {
            submitted.incrementAndGet();
            return;
        }
        if (droppable) {
            logDrop(collection);
            return;
        }
        try {
            if (queue.offer(op, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                submitted.incrementAndGet();
            } else {
                logDrop(collection);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logDrop(collection);
        }
    }

    /** Espera a que se escriba todo lo encolado antes de este punto. Usar antes de leer o cerrar. */
    public void flush() {
        if (!running) {
            return;
        }
        CountDownLatch barrier = new CountDownLatch(1);
        try {
            queue.put(new Op(null, null, barrier));
            barrier.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runLoop() {
        List<Op> batch = new ArrayList<>(batchSize);
        while (running || !queue.isEmpty()) {
            try {
                Op first = queue.poll(flushMs, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, batchSize - 1);
                writeBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOG.error("Fallo escribiendo batch a Mongo size={}", batch.size(), e);
            } finally {
                batch.clear();
            }
        }
    }

    private void writeBatch(List<Op> batch) {
        Map<MongoCollection<Document>, List<WriteModel<Document>>> byCollection = new IdentityHashMap<>();
        List<CountDownLatch> barriers = null;

        for (Op op : batch) {
            if (op.barrier != null) {
                if (barriers == null) {
                    barriers = new ArrayList<>(2);
                }
                barriers.add(op.barrier);
                continue;
            }
            byCollection.computeIfAbsent(op.collection, c -> new ArrayList<>()).add(op.model);
        }

        for (Map.Entry<MongoCollection<Document>, List<WriteModel<Document>>> entry : byCollection.entrySet()) {
            List<WriteModel<Document>> models = entry.getValue();
            try {
                entry.getKey().bulkWrite(models, UNORDERED);
                logProgress(written.addAndGet(models.size()));
            } catch (Exception e) {
                // bulkWrite no ordenado aplica lo que puede; se registra y se sigue con el resto.
                LOG.error("bulkWrite fallo collection={} size={}", entry.getKey().getNamespace(), models.size(), e);
            }
        }

        if (barriers != null) {
            barriers.forEach(CountDownLatch::countDown);
        }
    }

    private void logDrop(MongoCollection<Document> collection) {
        long count = dropped.incrementAndGet();
        if (count == 1 || count % 1_000 == 0) {
            LOG.warn("MongoWriteQueue saturada, descartados={} ultimo={} pendientes={}",
                    count, collection.getNamespace(), queue.size());
        }
    }

    private void logProgress(long count) {
        if (count % STATS_LOG_EVERY < batchSize) {
            LOG.info("MongoWriteQueue escritos={} encolados={} descartados={} pendientes={}",
                    count, submitted.get(), dropped.get(), queue.size());
        }
    }

    @Override
    public void close() {
        flush();
        running = false;
        try {
            flusher.join(30_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("MongoWriteQueue cerrado encolados={} escritos={} descartados={}",
                submitted.get(), written.get(), dropped.get());
    }

    private record Op(MongoCollection<Document> collection, WriteModel<Document> model, CountDownLatch barrier) {
    }
}
