package io.openmessaging;

import io.openmessaging.solve.LeoMessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PerformanceTester {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTester.class);

    static int i = 0;

    public static void main(String[] args) throws InterruptedException {
        gao();
    }

    private static void gao() throws InterruptedException {
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        Map<String, ByteBuffer> targetMap = new ConcurrentHashMap<>();

        long start = System.currentTimeMillis();
        String topic = "topic1";
        int queueId = 1;
        for (; i < 10000; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            long offset = messageQueue.append(topic, queueId, buf);
            targetMap.put(toKey(topic, queueId, offset), buf);
        }
        Thread threadW1 = new Thread(() -> {
            for (; i < 20000; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                long offset = messageQueue.append(topic, queueId, buf);
                targetMap.put(toKey(topic, queueId, offset), buf);
            }
        });
        Thread threadR1 = new Thread(() -> {
            long queryOffset = 19500;
            int queryNum = 500;
            Map<Integer, ByteBuffer> ansMap = messageQueue.getRange(topic, queueId, queryOffset, queryNum);
            for (int i = 0; i < queryNum; ++i) {
                if (!(new String(targetMap.get(toKey(topic, queueId, queryOffset + i)).array())).equals(
                        new String(ansMap.get(i).array())
                )) {
                    log.info("FUCK YOU");
                }
            }
            log.info("FINISH-1");
        });
        threadW1.start();
        threadW1.join();
        threadR1.start();
        threadR1.join();
        log.info("cost: {}", System.currentTimeMillis() - start);
    }

    private static String toKey(String topic, int queueId, long offset) {
        return topic + "-" + queueId + "-" + offset;
    }

    private static String toTopic(String key) {
        return key.split("-")[0];
    }

    private static int toQueueId(String key) {
        return Integer.parseInt(key.split("-")[1]);
    }

    private static long toOffset(String key) {
        return Integer.parseInt(key.split("-")[2]);
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
