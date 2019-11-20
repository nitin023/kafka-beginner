package com.github.nitin.kafka.tutorial1;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class ProducerDemo {

    private static final Logger logger = LogManager.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        if(logger.isInfoEnabled())
            logger.info("Application starting...");

    }
}
