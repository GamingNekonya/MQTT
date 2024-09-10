package org.example;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * The Publisher class implements the MqttCallback interface to publish messages
 * to an MQTT broker. It subscribes to various request topics and publishes
 * messages to the broker based on the received parameters.
 */
public class Publisher implements MqttCallback {
    private static final String BROKER = "tcp://localhost:1883";
    private static final String CLIENT_ID = "Publisher";
    private static final String TOPIC_PREFIX = "counter/";

    private MqttClient client;
    private int qos = 0;
    private int delay = 0;
    private int instanceCount = 1;
    private final int instanceId;

    /**
     * Constructor for the Publisher class. It initializes the MQTT client and
     * subscribes to the necessary request topics.
     *
     * @param instanceId the ID of this Publisher instance
     * @throws MqttException if an error occurs while connecting to the broker
     */
    public Publisher(int instanceId) throws MqttException {
        this.instanceId = instanceId;
        client = new MqttClient(BROKER, CLIENT_ID + "-" + instanceId, new MemoryPersistence());
        client.setCallback(this);
        connectAndSubscribe();
    }

    /**
     * Connects to the MQTT broker and subscribes to the request topics.
     *
     * @throws MqttException if an error occurs while connecting or subscribing
     */
    private void connectAndSubscribe() throws MqttException {
        client.connect();
        subscribeToRequests();
    }

    /**
     * Subscribes to the request topics to receive publishing parameters.
     */
    private void subscribeToRequests() {
        try {
            client.subscribe("request/qos", 2);
            client.subscribe("request/delay", 2);
            client.subscribe("request/instancecount", 2);
            client.subscribe("state", 2);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts publishing messages to the broker based on the received parameters.
     * Publishes messages for 60 seconds, then sleeps for a period based on the instance ID.
     */
    private void startPublishing() {
        if (instanceId > instanceCount) {
            return;
        }

        long startTime = System.currentTimeMillis();
        int messageCount = 0;

        while (System.currentTimeMillis() - startTime < 60000) { // Publish for 60 seconds
            String topic = TOPIC_PREFIX + instanceId + "/" + qos + "/" + delay;
            String content = Integer.toString(messageCount++);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            try {
                client.publish(topic, message);
                if (delay > 0) {
                    Thread.sleep(delay);
                }
            } catch (MqttException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            // Sleep for instanceId * 1 second before sending the stop signal
            Thread.sleep(instanceId * 1000);
            client.publish("state", "stop".getBytes(), 2, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        try {
            // Attempt to reconnect with exponential backoff
            int attempts = 0;
            while (!client.isConnected() && attempts < 5) {
                try {
                    Thread.sleep((1 << attempts) * 1000); // Exponential backoff
                    connectAndSubscribe();
                    System.out.println("Publisher " + instanceId + " reconnected and resubscribed.");
                } catch (InterruptedException | MqttException e) {
                    e.printStackTrace();
                }
                attempts++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String payload = new String(message.getPayload());
        switch (topic) {
            case "request/qos" -> qos = Integer.parseInt(payload);
            case "request/delay" -> delay = Integer.parseInt(payload);
            case "request/instancecount" -> instanceCount = Integer.parseInt(payload);
            case "state" -> {
                if (payload.equals("start") && this.instanceId <= instanceCount) {
                    new Thread(this::startPublishing).start();
                }
            }
            default -> {}
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // No additional actions required on delivery complete
    }

    /**
     * The main method initializes a Publisher instance and starts it.
     *
     * @param args command-line arguments (expects the instance ID as the first argument)
     * @throws MqttException if an error occurs while initializing the Publisher
     */
    public static void main(String[] args) throws MqttException {
        if (args.length < 1) {
            System.err.println("Please provide an instance ID as an argument.");
            System.exit(1);
        }
        int instanceId = Integer.parseInt(args[0]);
        new Publisher(instanceId);
    }
}