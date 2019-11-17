package com.github.nitin.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private CountDownLatch latch;

    private KafkaConsumer<String,String> consumer;
    public ConsumerThread(CountDownLatch latch,String topic , String bootStrapServers ,String groupId)
    {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        this.latch = latch;
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
    }

    public ConsumerThread(Runnable myConsumerThread) {
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println("Topic : " + consumerRecord.topic() + "key : " + consumerRecord.key() + " value : " + consumerRecord.value());
                    System.out.println("Partition : " + consumerRecord.partition() + " Offset : " + consumerRecord.offset());
                }
            }
        }
        catch (WakeupException ex)
        {
            System.out.println("Received shutdown Signal");
        }
        finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutDown()
    {
        consumer.wakeup();
    }
}
