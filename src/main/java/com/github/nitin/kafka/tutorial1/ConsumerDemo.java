package com.github.nitin.kafka.tutorial1;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void ConsumerDemoMain() {

        final String bootStrapServers = "127.0.0.1:9092";
        final String groupId = "my-fourth-application";
        final String topic = "first_topic";

        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to topic(s)
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true)
        {
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100L));
            for(ConsumerRecord<String,String> consumerRecord : consumerRecords)
            {
                System.out.println("Topic : " + consumerRecord.topic() + "key : " + consumerRecord.key() + " value : " + consumerRecord.value());
                System.out.println("Partition : " + consumerRecord.partition()  + " Offset : " + consumerRecord.offset());
            }
        }
    }
}
