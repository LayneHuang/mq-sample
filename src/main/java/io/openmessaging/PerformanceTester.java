package io.openmessaging;

import io.openmessaging.solve.LayneBMessageQueueImpl;
import io.openmessaging.solve.LeoMessageQueueImpl;
import io.openmessaging.wal.Idx;
import io.openmessaging.wal.WalInfoBasic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PerformanceTester {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTester.class);

    public static void main(String[] args) throws InterruptedException {
        gao();
//        gao2();
//        check();
    }

    private static void gao() throws InterruptedException {
        MessageQueue messageQueue = new LayneBMessageQueueImpl();
        Map<String, ByteBuffer> targetMap = new ConcurrentHashMap<>();

        long start = System.currentTimeMillis();
        String topic = "topic1";
        int queueId = 1;
        long queryOffset = 0;
        int queryNum = 30;
        for (int i = 0; i < 100; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            long offset = messageQueue.append(topic, queueId, buf);
            targetMap.put(toKey(topic, queueId, offset), buf);
        }

        Thread threadW1 = new Thread(() -> {
            for (int i = 100; i < 200; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                long offset = messageQueue.append(topic, queueId, buf);
                targetMap.put(toKey(topic, queueId, offset), buf);
            }
        });

        Thread threadW2 = new Thread(() -> {
            for (int i = 200; i < 300; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                long offset = messageQueue.append(topic, queueId, buf);
                targetMap.put(toKey(topic, queueId, offset), buf);
            }
        });
        Thread threadR = new Thread(() -> {
            for (int i = 1; i < 300; ++i) {
                log.info("------------------- {} ---------------------", i);
                Map<Integer, ByteBuffer> res = messageQueue.getRange(topic, queueId, i, i + i);

                res.forEach((key, value) -> {
                    log.info("res, {}, {}", key, new String(value.array()));
                });
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        threadW1.start();
        threadW2.start();
        threadR.start();
        threadW1.join();
        threadW2.join();
        threadR.join();
        log.info("WRITE FINISH-1, size: {}", targetMap.size());
        log.info("query, offset:{}, fetchNum:{}", queryOffset, queryNum);
        Map<Integer, ByteBuffer> ansMap = messageQueue.getRange(topic, queueId, queryOffset, queryNum);
        for (int i = 0; i < queryNum; ++i) {
            String s1 = new String(targetMap.get(toKey(topic, queueId, queryOffset + i)).array());
            String s2 = new String(ansMap.get(i).array());
            if (!s1.equals(s2)) {
                log.info("FUCK YOU {}, {}", s1, s2);
            }
        }

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
