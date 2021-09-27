package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Tester {

    public static void main(String[] args) throws InterruptedException {
        MessageQueue messageQueue = new DefaultMessageQueueImpl();
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
//        ByteBuffer msgBuf = ByteBuffer.allocate(102400);
//        for (int i = 0; i < 1000; i++) {
//            msgBuf.putInt(i);
//        }
//        ByteBuffer duplicate = msgBuf.duplicate();
//        duplicate.flip();
//        Thread threadA = new Thread(() -> {
//            for (int i = 1000; i < 10000; i++) {
//                msgBuf.putInt(i);
//            }
//        });
//        Thread threadB = new Thread(() -> {
//            for (int i = 0; i < 1000; i++) {
//                System.out.println(duplicate.getInt());
//            }
//        });
//        threadA.start();
//        threadA.join();
//        threadB.start();
//        threadB.join();
    }
}
