package com.github.nitin.kafka.tutorial1;

import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        /**
         * Create producer properties
         * create producer
         * send data
         */

        Properties props = new Properties();
        try {
            props.load(new FileInputStream("/home/nitin/IdeaProjects/kafkabeginnercourse/src/log4j.properties"));
            PropertyConfigurator.configure(props);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ProducerDemoWithCallback.getProducerDemoCallback();
    }
}
