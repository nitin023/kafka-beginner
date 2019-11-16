package com.github.nitin.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void getProducerDemoCallback()
    {
        //creating producer configs
        final String bootStrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        int iter  = 10;
        while (iter > 0) {
        //Create producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello world " + iter);
            //send data - async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes successfully every-time a record is send or an exception is thrown

                    /*No error case*/
                    if (exception == null) {
                        logger.info("Recieved new metadata. \n" +
                                "Topic : " + metadata.topic() + "\n" +
                                "Partition : " + metadata.partition() + "\n" +
                                "Offset : " + metadata.offset() + "\n" +
                                "TimeStamp : " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            });
            iter--;
        }
        //flush data
        producer.flush();

        //close thr producer
        producer.close();

    }
}
