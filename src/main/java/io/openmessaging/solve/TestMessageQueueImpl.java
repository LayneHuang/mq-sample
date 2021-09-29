package io.openmessaging.solve;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.openmessaging.MessageQueue;

/**
 * 纯test
 */
public class TestMessageQueueImpl extends MessageQueue {

    private static final ConcurrentHashMap<String, AtomicLong> map = new ConcurrentHashMap<>();

    private static long getOffset(String key) {
        AtomicLong offsetAdder = map.computeIfAbsent(key, k -> new AtomicLong());
        return offsetAdder.getAndIncrement();
    }

    private static final AtomicLong totalCount = new AtomicLong();
    private static final AtomicLong debrisCount = new AtomicLong();

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        totalCount.incrementAndGet();
        if (data.limit() < 4 * 1024 - 5) {
            debrisCount.incrementAndGet();
        }
        return getOffset(topic + queueId);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        System.out.println("totalCount : " + totalCount.get());
        System.out.println("debrisCount : " + debrisCount.get());
        return Collections.emptyMap();
    }
}
