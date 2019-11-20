package com.github.nitin.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoUsingAssignAndSeek {

    public static void ConsumerDemoUsingAssignAndSeekMain ()
    {
        final String bootStrapServers = "127.0.0.1:9092";
        final String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are basically used to replay data or fetch specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        long offsetToReadFrom = 21L;
        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numMessagesReadSoFar = 0;

        //poll for new data
        while (keepOnReading)
        {
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100L));
            for(ConsumerRecord<String,String> consumerRecord : consumerRecords)
            {
                numMessagesReadSoFar++;
                System.out.println("Topic : " + consumerRecord.topic() + "key : " + consumerRecord.key() + " value : " + consumerRecord.value());
                System.out.println("Partition : " + consumerRecord.partition()  + " Offset : " + consumerRecord.offset());
                if(numMessagesReadSoFar >= numberOfMessagesToRead)
                {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
