package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RebootTester {

    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        Map<Integer, ByteBuffer> range;
        range = messageQueue.getRange("A", 1, 100, 100);
        range.forEach((key, value) -> System.out.println(key + ": " + new String(value.array())));
        System.out.println(System.currentTimeMillis() - start);
    }
}
