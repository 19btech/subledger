package com.reserv.dataloader.service.model.run;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The work queue of a distributed run's chunks: a Pulsar topic with one Shared subscription, so each
 * chunk goes to exactly one dataloader pod.
 *
 * <p>Each pod runs {@code fyntrac.run.chunks-per-pod} consumers, each with a zero-size receiver queue
 * and its own thread: a consumer is handed a chunk only when it asks for one, so chunks go to pods
 * with a free slot instead of being prefetched by a busy one. Running two per pod lets the next
 * chunk's event generation overlap the previous chunk's wait for its last model batches.
 *
 * <p>A message is acknowledged only after its chunk is finished, so a pod that dies mid-chunk has its
 * chunk redelivered to another pod ({@link DistributedRunService#claim} decides what that pod does
 * with it). There is deliberately no ack timeout: a chunk legitimately takes minutes.
 */
@Slf4j
@Component
public class RunChunkQueue {

    /** What a handler wants done with a message once it returns. */
    public enum Outcome { ACK, RETRY_LATER }

    @Value("${spring.pulsar.client.service-url}")
    private String pulsarUrl;

    @Value("${fyntrac.run.chunk-topic:fyntrac-run-chunk}")
    private String topic;

    @Value("${fyntrac.run.chunk-subscription:fyntrac-dataloader-run-chunks}")
    private String subscription;

    // How long a chunk handed back with RETRY_LATER waits before it is offered again.
    @Value("${fyntrac.run.chunk-retry-delay-seconds:60}")
    private long retryDelaySeconds;

    private PulsarClient client;
    private Producer<RunChunkMessage> producer;
    private final List<Consumer<RunChunkMessage>> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    private synchronized PulsarClient client() throws PulsarClientException {
        if (client == null) {
            client = PulsarClient.builder().serviceUrl(pulsarUrl).build();
        }
        return client;
    }

    private synchronized Producer<RunChunkMessage> producer() throws PulsarClientException {
        if (producer == null) {
            // No batching: the consumers use a zero-size receiver queue, which cannot take batched
            // messages — Pulsar closes such a consumer on the first batch it is handed.
            producer = client().newProducer(Schema.JSON(RunChunkMessage.class))
                    .topic(topic)
                    .enableBatching(false)
                    .blockIfQueueFull(true)
                    .create();
        }
        return producer;
    }

    /** Publishes every chunk and returns once the broker has accepted all of them. */
    public void publishAll(List<RunChunkMessage> chunks) throws PulsarClientException {
        Producer<RunChunkMessage> p = producer();
        List<CompletableFuture<?>> sends = new ArrayList<>(chunks.size());
        for (RunChunkMessage chunk : chunks) {
            sends.add(p.newMessage().key(chunk.chunkId()).value(chunk).sendAsync());
        }
        p.flush();
        CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).join();
    }

    /** Starts {@code count} consumers on their own threads, each handing messages to {@code handler}. */
    public synchronized void startConsumers(int count, Function<RunChunkMessage, Outcome> handler) {
        for (int i = 0; i < count; i++) {
            Thread thread = new Thread(() -> consume(handler), "run-chunk-consumer-" + i);
            thread.setDaemon(true);
            thread.start();
        }
        log.info("Starting {} run-chunk consumers on topic {} (subscription {})", count, topic, subscription);
    }

    private synchronized Consumer<RunChunkMessage> subscribe() throws PulsarClientException {
        Consumer<RunChunkMessage> consumer = client().newConsumer(Schema.JSON(RunChunkMessage.class))
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(0)
                .negativeAckRedeliveryDelay(retryDelaySeconds, TimeUnit.SECONDS)
                .subscribe();
        consumers.add(consumer);
        return consumer;
    }

    // One consumer per thread. If Pulsar closes the consumer under us, subscribe a new one — a pod
    // whose consumers silently died would never take another chunk, and runs would stall.
    private void consume(Function<RunChunkMessage, Outcome> handler) {
        Consumer<RunChunkMessage> consumer = null;
        while (running) {
            if (consumer == null) {
                try {
                    consumer = subscribe();
                } catch (PulsarClientException e) {
                    if (!running) return;
                    log.error("Run-chunk subscribe failed; retrying in 5s", e);
                    consumer = null;
                    sleepQuietly(5_000);
                    continue;
                }
            }
            Message<RunChunkMessage> msg;
            try {
                msg = consumer.receive();
            } catch (PulsarClientException.AlreadyClosedException e) {
                if (!running) return;
                log.warn("Run-chunk consumer was closed by the client; subscribing a new one", e);
                consumers.remove(consumer);
                consumer = null;   // re-subscribe on the next pass
                continue;
            } catch (PulsarClientException e) {
                if (!running) return;
                log.error("Run-chunk receive failed; retrying in 5s", e);
                sleepQuietly(5_000);
                continue;
            }
            Outcome outcome;
            try {
                outcome = handler.apply(msg.getValue());
            } catch (Exception e) {
                // The handler records chunk failures itself; anything escaping it is unexpected, and
                // the chunk's state in MongoDB decides what a redelivery does.
                log.error("Run-chunk handler threw for message {}", msg.getMessageId(), e);
                outcome = Outcome.RETRY_LATER;
            }
            try {
                if (outcome == Outcome.ACK) {
                    consumer.acknowledge(msg);
                } else {
                    consumer.negativeAcknowledge(msg);
                }
            } catch (PulsarClientException e) {
                log.error("Failed to {} run-chunk message {}", outcome, msg.getMessageId(), e);
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public synchronized void close() {
        running = false;
        for (Consumer<RunChunkMessage> consumer : consumers) {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close run-chunk consumer", e);
            }
        }
        try {
            if (producer != null) producer.close();
            if (client != null) client.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close run-chunk Pulsar client", e);
        }
    }
}
