package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PerformanceTester {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTester.class);

    static int i = 0;

    public static void main(String[] args) throws InterruptedException {
        gao();
    }

    private static void gao() throws InterruptedException {
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        long start = System.currentTimeMillis();
        Map<Integer, ByteBuffer> range;
        for (; i < 10000; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            messageQueue.append("topicA", 1, buf);
        }
        Thread threadW1 = new Thread(() -> {
            for (; i < 20000; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                messageQueue.append("topicA", 1, buf);
            }
        });
        Thread threadR1 = new Thread(() -> {
            messageQueue.getRange("topicA", 1, 19500, 500);
            System.out.println("FINISH");
            log.info("边写边查 threadR1");
            check(messageQueue.getRange("topicA", 1, 9950, 100));
            log.info("FINISH-1");

        });
        threadW1.start();
        threadW1.join();
        threadR1.start();
        threadR1.join();
        log.info("cost: {}", System.currentTimeMillis() - start);
    }

    private static void check(Map<Integer, ByteBuffer> map) {
        map.forEach((key, value) -> {
            String s = new String(value.array());
            if (!String.valueOf(key).equals(s)) {
                log.debug("FUCK: {}, {}", key, s);
            }
//            log.debug("TRUE: {}, {}", key, s);
        });
    }
}
