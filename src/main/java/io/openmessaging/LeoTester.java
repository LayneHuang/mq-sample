package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LeoTester {

    public static void main(String[] args) throws InterruptedException {
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            messageQueue.append("topic1", 1, buf);
        }
        Thread threadW1 = new Thread(() -> {
            for (int i = 100; i < 200; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                messageQueue.append("topic1", 1, buf);
            }
        });
        Thread threadW2 = new Thread(() -> {
            for (int i = 200; i < 300; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                messageQueue.append("topic2", 1, buf);
            }
        });
        threadW1.start();
        threadW2.start();
        Thread.sleep(8_000);
        Thread threadR1 = new Thread(() -> {
            System.out.println("边写边查 threadR1");
            messageQueue.getRange("topic1", 1, 150, 10)
                    .forEach((key, value) -> System.out.println("边写边查 topic1: " + key + ": " + new String(value.array())));
            messageQueue.getRange("topic2", 1, 50, 10)
                    .forEach((key, value) -> System.out.println("边写边查 topic2 : " + key + ": " + new String(value.array())));
            System.out.println("FINISH");
        });
        threadR1.start();
        threadR1.join();
//        Thread threadW2 = new Thread(() -> {
//            for (; i < 30000; i++) {
//                String text = String.valueOf(i);
//                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
//                messageQueue.append("topic2", 1, buf);
//            }
//        });
//        threadW2.start();
//        threadW2.join();
//        System.out.println("最后查");
//        messageQueue.getRange("topic1", 1, 9950, 100)
//                .forEach((key, value) -> System.out.println("最后查: " + key + ": " + new String(value.array())));
        System.out.println(System.currentTimeMillis() - start);
    }
}
