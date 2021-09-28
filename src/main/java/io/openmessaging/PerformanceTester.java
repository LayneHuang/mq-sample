package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PerformanceTester {

    static int i = 0;

    public static void main(String[] args) throws InterruptedException {
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        long start = System.currentTimeMillis();
        Map<Integer, ByteBuffer> range;
        for (; i < 10000; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            messageQueue.append("A", 1, buf);
        }
        Thread threadW1 = new Thread(() -> {
            for (; i < 20000; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                messageQueue.append("A", 1, buf);
            }
        });
        Thread threadR1 = new Thread(() -> {
            System.out.println("边写边查 threadR1");
            messageQueue.getRange("A", 1, 9950, 100)
                    .forEach((key, value) -> System.out.println(key + ": " + new String(value.array())));
        });
//        Thread threadW2 = new Thread(() -> {
//            for (; i < 30000; i++) {
//                String text = String.valueOf(i);
//                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
//                messageQueue.append("B", 1, buf);
//            }
//        });
        threadW1.start();
        threadW1.join();
        threadR1.start();
        threadR1.join();
//        threadW2.start();
//        threadW2.join();
//        System.out.println("最后查");
//        messageQueue.getRange("A", 1, 9950, 100)
//                .forEach((key, value) -> System.out.println("最后查: " + key + ": " + new String(value.array())));
        System.out.println(System.currentTimeMillis() - start);
    }
}
