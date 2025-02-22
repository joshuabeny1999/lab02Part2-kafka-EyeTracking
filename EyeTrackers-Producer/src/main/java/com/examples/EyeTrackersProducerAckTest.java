package com.examples;

import com.data.Gaze;
import com.google.common.io.Resources;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EyeTrackersProducerAckTest {

    public static void main(String[] args) throws Exception {

        // Specify Topic
        String topic = "gaze-events";

        // Read Kafka properties file
        Properties properties;
        try (InputStream props = Resources.getResource("producer-ack.properties").openStream()) {
            properties = new Properties();
            properties.load(props);
        }

        // Create Kafka producer
        KafkaProducer<String, Gaze> producer = new KafkaProducer<>(properties);

        // Delete existing topic with the same name
        deleteTopic(topic, properties);

        // Create new topic with 2 partitions
        createTopic(topic, 2, properties);

        // Define an array with device IDs
        Integer[] deviceIDs = {0, 1};

        // For collecting latency measurements
        final AtomicLong totalLatency = new AtomicLong(0);
        final AtomicInteger ackCount = new AtomicInteger(0);

        // Send 1000 messages
        for (int counter = 0; counter < 1000; counter++) {
            try {
                Thread.sleep(8);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Select a random device
            int deviceID = getRandomNumber(0, deviceIDs.length);

            // Generate a random gaze event using constructor: Gaze(int eventID, long timestamp, int xPosition, int yPosition, int pupilSize)
            Gaze gazeEvent = new Gaze(counter, System.nanoTime(), getRandomNumber(0, 1920), getRandomNumber(0, 1080), getRandomNumber(3, 4));

            final long startTime = System.nanoTime();

            // Send the gaze event with a callback to capture latency
            producer.send(new ProducerRecord<>(topic, String.valueOf(deviceID), gazeEvent), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending message: " + exception.getMessage());
                } else {
                    long elapsedTime = (System.nanoTime() - startTime) / 1_000_000; // Convert ns to ms
                    totalLatency.addAndGet(elapsedTime);
                    ackCount.incrementAndGet();
                    System.out.println("gazeEvent sent: " + gazeEvent +
                            " from deviceID: " + deviceID +
                            " | Latency: " + elapsedTime + " ms");
                }
            });
        }

        // Flush the producer to ensure all messages are sent and callbacks are executed
        producer.flush();

        // Compute and display average latency
        if (ackCount.get() > 0) {
            long averageLatency = totalLatency.get() / ackCount.get();
            System.out.println("Average Latency for " + ackCount.get() + " messages: " + averageLatency + " ms");
        } else {
            System.out.println("No messages were acknowledged successfully.");
        }

        producer.close();
    }

    // Generate a random number between min (inclusive) and max (exclusive)
    private static int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    // Create topic
    private static void createTopic(String topicName, int numPartitions, Properties properties) throws Exception {
        AdminClient admin = AdminClient.create(properties);

        // Checking if topic already exists
        boolean alreadyExists = admin.listTopics().names().get().stream()
                .anyMatch(existingTopicName -> existingTopicName.equals(topicName));
        if (alreadyExists) {
            System.out.printf("Topic already exists: %s%n", topicName);
        } else {
            System.out.printf("Creating topic: %s%n", topicName);
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 2);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }
    }

    // Delete topic
    private static void deleteTopic(String topicName, Properties properties) {
        try (AdminClient client = AdminClient.create(properties)) {
            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singleton(topicName));
            while (!deleteTopicsResult.all().isDone()) {
                // Wait for the deletion to complete
            }
        } catch (Exception e) {
            System.err.println("Error deleting topic: " + e.getMessage());
        }
    }
}