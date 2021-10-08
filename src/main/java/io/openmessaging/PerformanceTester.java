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
        log.info("cao: {}, {}", (257 & 0xff), ((257 >> 4) & 0xff));
        log.info("res: {}", ((16 << 4) | 1));
        gao();
    }

    private static void gao() throws InterruptedException {
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
        });
    }
}
