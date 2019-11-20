package com.github.nitin.kafka.tutorial1;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void ConsumerDemoWithThreadMain() {
        run();
    }

    private static void run()
    {
        final String bootStrapServers = "127.0.0.1:9092";
        final String groupId = "my-sixth-application";
        final String topic = "first_topic";

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerThread myConsumerThread = new ConsumerThread(countDownLatch,topic,bootStrapServers,groupId);
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            myConsumerThread.shutDown();
            System.out.println("Caught shutdown hook");
            try
            {
                countDownLatch.await();
            }
            catch (InterruptedException ie)
            {
                ie.printStackTrace();
            }
            System.out.println("Application has exited");
    }));
        try {
            countDownLatch.await();

        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println(new StringBuilder().append("Application got interrupted").append(e.getStackTrace()).toString());
        }
        finally {
            System.out.println("Application is closing");
        }
    }
}
