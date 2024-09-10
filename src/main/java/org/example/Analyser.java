package org.example;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.PriorityQueue;

/**
 * The Analyser class implements the MqttCallback interface to analyze messages
 * received from an MQTT broker. It subscribes to various topics and processes
 * messages to generate statistical data, which is then written to a CSV file.
 */
public class Analyser implements MqttCallback {
    private static final String BROKER = "tcp://localhost:1883";
    private static final String CLIENT_ID = "Analyser";
    private static final int[][] testConfigurations = new int[180][4];
    private static final String RESULT_PATH = "mqtt_results.csv";
    private static final String SYS_RESULT_PATH = "sys_results.csv";

    private MqttClient client;
    private static int configIndex = 0;
    private static int receivedMessageCount = 0;
    private static int stopSignalCount = 0;
    private static final int[] previousValue = new int[]{-1, -1, -1, -1, -1};
    private static int outOfOrderMessageCount = 0;
    private static final PriorityQueue<Long> messageIntervals = new PriorityQueue<>();
    private static long lastMessageTimestamp = 0;

    /**
     * Constructor for the Analyser class. It initializes the MQTT client and
     * subscribes to the necessary topics.
     *
     * @throws MqttException if an error occurs while connecting to the broker
     */
    public Analyser() throws MqttException {
        client = new MqttClient(BROKER, CLIENT_ID, new MemoryPersistence());
        client.setCallback(this);
        client.connect();
        client.subscribe("request/qos", 2);
        client.subscribe("request/delay", 2);
        client.subscribe("request/instancecount", 2);
        client.subscribe("state", 2);
        client.subscribe("counter/#", 0); // Initially set QoS to 0
        client.subscribe("$SYS/#", 2); // Subscribe to $SYS topics
    }

    /**
     * Sends a request to the broker with the specified parameters.
     *
     * @param qos the Quality of Service level
     * @param delay the delay between messages
     * @param instanceCount the number of instances
     * @throws MqttException if an error occurs while publishing the request
     */
    private void sendRequest(int qos, int delay, int instanceCount) throws MqttException {
        client.publish("request/qos", Integer.toString(qos).getBytes(), 2, false);
        client.publish("request/delay", Integer.toString(delay).getBytes(), 2, false);
        client.publish("request/instancecount", Integer.toString(instanceCount).getBytes(), 2, false);
        client.publish("state", "start".getBytes(), 2, false);
    }

    @Override
    public void connectionLost(Throwable cause) {
        try {
            client.reconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String payload = new String(message.getPayload());

        if (topic.equals("state")) {
            if (payload.equals("stop")) {
                stopSignalCount++;
                if (stopSignalCount == testConfigurations[configIndex][2]) {
                    processResults();
                    resetCounters();
                    if (++configIndex < testConfigurations.length) {
                        client.unsubscribe("counter/#");
                        client.subscribe("counter/#", testConfigurations[configIndex][3]); // Resubscribe with new QoS
                        sendRequest(testConfigurations[configIndex][0], testConfigurations[configIndex][1], testConfigurations[configIndex][2]);
                    } else {
                        client.disconnect();
                        System.exit(0);
                    }
                }
            }
        } else if (topic.startsWith("counter/")) {
            handleCounterMessage(topic, payload);
        } else if (topic.startsWith("$SYS/")) {
            handleSysMessage(topic, payload);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // This method is required to complete the MqttCallback interface
    }

    /**
     * Handles messages received on the "counter/#" topic.
     *
     * @param topic the topic from which the message was received
     * @param payload the payload of the message
     */
    private void handleCounterMessage(String topic, String payload) {
        long currentTime = System.currentTimeMillis();
        long interval = currentTime - lastMessageTimestamp;
        lastMessageTimestamp = currentTime;
        messageIntervals.add(interval);
        receivedMessageCount++;

        int value = Integer.parseInt(payload);
        int instance = Integer.parseInt(topic.split("/")[1]);
        if (previousValue[instance - 1] > value) {
            outOfOrderMessageCount++;
        }
        previousValue[instance - 1] = value;
    }

    /**
     * Handles messages received on the "$SYS/#" topic.
     *
     * @param topic the topic from which the message was received
     * @param payload the payload of the message
     */
    private void handleSysMessage(String topic, String payload) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(SYS_RESULT_PATH, true))) {
            pw.printf("%s,%s%n", topic, payload);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Processes the results and writes them to a CSV file.
     */
    private void processResults() {
        double messageRate = receivedMessageCount / 60.0;
        int delay = testConfigurations[configIndex][1];
        int instanceCount = testConfigurations[configIndex][2];
        int expectedMessages = (delay == 0) ? 0 : (60000 / delay) * instanceCount;
        double lossRatio = (delay == 0) ? 0 : (1 - ((double) receivedMessageCount / expectedMessages));
        double outOfOrderRatio = (double) outOfOrderMessageCount / receivedMessageCount;
        long medianInterval = 0;
        int halfSize = messageIntervals.size() / 2;
        for (int i = 0; i < halfSize; i++) {
            messageIntervals.poll();
        }
        if (!messageIntervals.isEmpty()) {
            medianInterval = messageIntervals.peek();
        }
        writeResultsToCSV(testConfigurations[configIndex][0], delay, instanceCount, testConfigurations[configIndex][3], messageRate, lossRatio, outOfOrderRatio, medianInterval);
    }

    /**
     * Resets the counters and data structures for the next round of tests.
     */
    private void resetCounters() {
        receivedMessageCount = 0;
        for (int i = 0; i < 5; i++) {
            previousValue[i] = -1;
        }
        outOfOrderMessageCount = 0;
        messageIntervals.clear();
        stopSignalCount = 0;
        lastMessageTimestamp = 0;
    }

    /**
     * Writes the test results to a CSV file.
     *
     * @param pubQos the Quality of Service level for the publisher
     * @param delay the delay between messages
     * @param instanceCount the number of instances
     * @param subQos the Quality of Service level for the subscriber
     * @param msgRate the message rate
     * @param lossRatio the loss ratio
     * @param outOfOrderRatio the out-of-order ratio
     * @param medianInterval the median interval between messages
     */
    private void writeResultsToCSV(int pubQos, int delay, int instanceCount, int subQos, double msgRate, double lossRatio, double outOfOrderRatio, long medianInterval) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(RESULT_PATH, true))) {
            pw.printf("%d,%d,%d,%d,%.2f,%.2f,%.2f,%d%n",
                    pubQos, delay, instanceCount, subQos, msgRate, lossRatio, outOfOrderRatio, medianInterval);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * The main method initializes the Analyser and starts the test process.
     *
     * @param args command-line arguments
     * @throws MqttException if an error occurs while initializing the Analyser
     */
    public static void main(String[] args) throws MqttException {
        Analyser analyser = new Analyser();
        int[] qosLevels = {0, 1, 2};
        int[] delays = {0, 1, 2, 4};
        int[] instanceCounts = {1, 2, 3, 4, 5};
        int index = 0;

        for (int pubQos : qosLevels) {
            for (int delay : delays) {
                for (int instanceCount : instanceCounts) {
                    for (int subQos : qosLevels) {
                        testConfigurations[index][0] = pubQos;
                        testConfigurations[index][1] = delay;
                        testConfigurations[index][2] = instanceCount;
                        testConfigurations[index][3] = subQos;
                        index++;
                    }
                }
            }
        }

        analyser.sendRequest(testConfigurations[configIndex][0], testConfigurations[configIndex][1], testConfigurations[configIndex][2]);
    }
}