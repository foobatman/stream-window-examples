package com.vc.window;

import junit.framework.TestCase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Test;

import java.time.Instant;
import java.util.Properties;

public class ApplicationTest extends TestCase {

    private TopologyTestDriver driver;

    @Override
    @After
    public void tearDown() throws Exception {
        driver.close();
    }

    @Test
    public void testTopology() {
        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(Serdes.String().serializer(),
                Serdes.String().serializer());
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        Topology topology = Application.getTopology("test-input", "test-output");
        System.out.println(topology.describe());
        driver = new TopologyTestDriver(topology, props);
        TestInputTopic<String, String> inputTopic = driver.createInputTopic("test-input", Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic("test-output", Serdes.String().deserializer(), Serdes.Long().deserializer());

        // -----------------------
        // 0----900:900key1 900:1000:key1 1501:1501:key1
        // 0----900:900key2
        inputTopic.pipeInput("key-1", "value-1", Instant.EPOCH.plusMillis(900));
        inputTopic.pipeInput("key-2", "value-2", Instant.EPOCH.plusMillis(900));
        inputTopic.pipeInput("key-1", "value-1", Instant.EPOCH.plusMillis(950));
        while (!outputTopic.isEmpty()) {
            System.out.println(outputTopic.readKeyValue());
        }
        System.out.println("*************");
        inputTopic.pipeInput("key-1", "value-1", Instant.EPOCH.plusMillis(1000));
        inputTopic.pipeInput("key-3", "value-1", Instant.EPOCH.plusMillis(399));
        while (!outputTopic.isEmpty()) {
            System.out.println(outputTopic.readKeyValue());
        }
        System.out.println("*************");
        inputTopic.pipeInput("key-1", "value-1", Instant.EPOCH.plusMillis(1501));
        while (!outputTopic.isEmpty()) {
            System.out.println(outputTopic.readKeyValue());
        }
        System.out.println("*************");


//        System.out.println(driver.readOutput("test-output"));
//        System.out.println(driver.readOutput("test-output", Serdes.String().deserializer(), Serdes.Long().deserializer()));
//        System.out.println(driver.readOutput("test-output", Serdes.String().deserializer(), Serdes.Long().deserializer()));
    }
}