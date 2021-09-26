package dap.ingest;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    private static class Record {
        public String id;
        public int order;
        public String value;
        public long timestamp;

        // A no-arg constructor is required
        public Record() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getOrder() {
            return order;
        }

        public void setOrder(int order) {
            this.order = order;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();
        Producer<Record> producer = client.newProducer(JSONSchema.of(Record.class))
            .topic("persistent://public/default/cdc")
            .blockIfQueueFull(true)
            .create();
        List<UUID> uuids = new ArrayList<>();
        Random random = new Random();

        for(int i = 0; i < 1000; i++) {
            uuids.add(UUID.randomUUID());
        }
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        final Instant beginEventTime = new GregorianCalendar(2020, 1, 1).toInstant();
        for(int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                Instant currentEvtTime = beginEventTime;
                while(true) {
                    currentEvtTime = currentEvtTime.plus(1, ChronoUnit.DAYS);
                    for (int j = 0; j < 1000; j++) {
                        Record newR = new Record();
                        int idx = random.nextInt(1000 - 1);
                        newR.id = uuids.get(idx).toString();
                        newR.order = random.nextInt(1000);
                        newR.value = "Value";

                        newR.timestamp = Timestamp.from(currentEvtTime.plus(30, ChronoUnit.SECONDS)).getTime();

                        producer.send(newR);
                    }
                    System.out.println("inserted 1000 messages");
                    Thread.sleep(1000);
                }
            });
        }
    }
}
