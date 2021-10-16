package io.openmessaging;

import io.openmessaging.solve.LayneMessageQueueImpl;
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

    static int i = 0;

    public static void main(String[] args) throws InterruptedException {
        gao();
//        gao2();
//        check();
    }

    public static void check() {
        String text = "SOMETHING_IS_HAPPEN123";
        WalInfoBasic info = new WalInfoBasic(250, 99, ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)));
        info.pOffset = 6676;
        byte[] encodeB = info.encodeToB();
        ByteBuffer buffer = ByteBuffer.allocate(encodeB.length);
        buffer.put(encodeB);
        buffer.flip();
        WalInfoBasic info2 = new WalInfoBasic();
        info2.decode(buffer, true);
        log.info("{}, {}, {}", info.topicId == info2.topicId, info.topicId, info2.topicId);
        log.info("{}", info.queueId == info2.queueId);
        log.info("{}", info.pOffset == info2.pOffset);
        log.info("{}", text.equals(new String(info2.value.array())));
        Idx idx = new Idx();
        idx.add(0, 4, 3, 66556, 888);
        log.info("{}", idx.getWalPart(0) == 3);
        log.info("{}", idx.getWalId(0) == 4);
        log.info("{}", idx.getWalValuePos(0) == 66556);
        log.info("{}", idx.getWalValueSize(0) == 888);
    }

    private static void gao() throws InterruptedException {
        MessageQueue messageQueue = new LayneMessageQueueImpl();
        Map<String, ByteBuffer> targetMap = new ConcurrentHashMap<>();

        long start = System.currentTimeMillis();
        String topic = "topic1";
        int queueId = 1;
        long queryOffset = 0;
        int queryNum = 20;
        for (; i < 100; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            long offset = messageQueue.append(topic, queueId, buf);
            targetMap.put(toKey(topic, queueId, offset), buf);
            if (offset > queryNum) queryOffset = offset - queryNum;
        }

        Thread threadW1 = new Thread(() -> {
            for (; i < 200; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                long offset = messageQueue.append(topic, queueId, buf);
                targetMap.put(toKey(topic, queueId, offset), buf);
            }
        });

        Thread threadW2 = new Thread(() -> {
            for (; i < 300; i++) {
                String text = String.valueOf(i);
                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                long offset = messageQueue.append(topic, queueId, buf);
                targetMap.put(toKey(topic, queueId, offset), buf);
            }
        });

        log.info("WRITE FINISH-1");
        threadW1.start();
        threadW2.start();
        threadW1.join();
        threadW2.join();

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
