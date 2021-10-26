package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.solve.LayneBMessageQueueImpl;
import io.openmessaging.solve.LeoMessageQueueImpl;

public class RebootTester {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        MessageQueue messageQueue = new LayneBMessageQueueImpl();
        messageQueue.getRange("topic0", 1, 5, 10)
                .forEach((key, value) -> System.out.println("边写边查 topic1: " + key + ": " + new String(value.array())));
        messageQueue.getRange("topic15", 1, 495, 10)
                .forEach((key, value) -> System.out.println("边写边查 topic15: " + key + ": " + new String(value.array())));
        messageQueue.getRange("topic29", 1, 995, 10)
                .forEach((key, value) -> System.out.println("边写边查 topic29: " + key + ": " + new String(value.array())));
        messageQueue.getRange("topic15", 1, 995, 0)
                .forEach((key, value) -> System.out.println("边写边查 topic1: " + key + ": " + new String(value.array())));
        System.out.println("FINISH");
        System.out.println(System.currentTimeMillis() - start);
    }
}
