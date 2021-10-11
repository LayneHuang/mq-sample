package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;

public class RebootTester {

    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        messageQueue.getRange("topic1", 1, 150, 10)
                .forEach((key, value) -> System.out.println("边写边查 topic1: " + key + ": " + new String(value.array())));
        messageQueue.getRange("topic2", 1, 50, 100)
                .forEach((key, value) -> System.out.println("边写边查 topic2 : " + key + ": " + new String(value.array())));
        System.out.println("FINISH");
        System.out.println(System.currentTimeMillis() - start);
    }
}
