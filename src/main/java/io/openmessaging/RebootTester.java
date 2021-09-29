package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RebootTester {

    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        messageQueue.getRange("A", 1, 9950, 100)
                .forEach((key, value) -> System.out.println(key + ": " + new String(value.array())));
        System.out.println(System.currentTimeMillis() - start);
    }
}
