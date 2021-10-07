package io.openmessaging;

import io.openmessaging.solve.LayneMessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PerformanceTester {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTester.class);

    static int i = 0;

    public static void main(String[] args) throws InterruptedException {
        MessageQueue messageQueue = new LayneMessageQueueImpl();
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
            messageQueue.getRange("A", 1, 19500, 500);
            System.out.println("FINISH");
            log.info("边写边查 threadR1");
            check(messageQueue.getRange("A", 1, 9950, 100));
            log.info("FINISH-1");

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
        log.info("cost: {}", System.currentTimeMillis() - start);
    }

    private static void check(Map<Integer, ByteBuffer> map) {
        map.forEach((key, value) -> {
            if (!String.valueOf(key).equals(new String(value.array()))) {
                log.debug("FUCK");
            }
        });
    }
}
