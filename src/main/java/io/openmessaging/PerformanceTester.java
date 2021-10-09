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
        gao2();
    }

    private static void gao() throws InterruptedException {
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        Map<String, ByteBuffer> targetMap = new ConcurrentHashMap<>();

        long start = System.currentTimeMillis();
        String topic = "topic1";
        int queueId = 1;
        long queryOffset = 0;
        int queryNum = 500;
        for (; i < 100000; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            long offset = messageQueue.append(topic, queueId, buf);
            targetMap.put(toKey(topic, queueId, offset), buf);
            if (offset > queryNum) queryOffset = offset - queryNum;
        }

        Thread threadW1 = new Thread(() -> {
            for (; i < 200000; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                long offset = messageQueue.append(topic, queueId, buf);
                targetMap.put(toKey(topic, queueId, offset), buf);
            }
        });
        log.info("query, offset:{}, fetchNum:{}", queryOffset, queryNum);
        Map<Integer, ByteBuffer> ansMap = messageQueue.getRange(topic, queueId, queryOffset, queryNum);
        for (int i = 0; i < queryNum; ++i) {
            if (!(new String(targetMap.get(toKey(topic, queueId, queryOffset + i)).array())).equals(
                    new String(ansMap.get(i).array())
            )) {
                log.info("FUCK YOU");
            }
        }
        log.info("FINISH-1");
        threadW1.start();
        threadW1.join();
        log.info("cost: {}", System.currentTimeMillis() - start);
    }

    private static void gao2() {
        String topic = "topic1";
        int queueId = 1;
        long queryOffset = 0;
        int queryNum = 200000;
        MessageQueue messageQueue = new LeoMessageQueueImpl();
        Map<Integer, ByteBuffer> ansMap = messageQueue.getRange(topic, queueId, queryOffset, queryNum);
        ansMap.forEach((key, value) -> {
            if (!key.toString().equals(new String(value.array()))) {
                log.info("FUCK YOU~");
            }
        });
        log.info("OK");
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
}
