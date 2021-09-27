package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PerformanceTester {

    public static void main(String[] args) throws InterruptedException {
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        long start = System.currentTimeMillis();
        Thread threadA = new Thread(() -> {
            for (int i = 0; i < 10000; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                messageQueue.append("A", 1, buf);
            }
        });
        Thread threadB = new Thread(() -> {
            for (int i = 10000; i < 20000; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                messageQueue.append("B", 1, buf);
            }
        });
        threadA.start();
        threadA.join();
        threadB.start();
        threadB.join();
        Map<Integer, ByteBuffer> range;
        range = messageQueue.getRange("A", 1, 100, 100);
         range.forEach((key, value) -> System.out.println(key + ": " + new String(value.array())));
        System.out.println(System.currentTimeMillis() - start);
    }
}
