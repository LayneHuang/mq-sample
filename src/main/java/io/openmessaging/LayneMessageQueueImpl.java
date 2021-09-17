package io.openmessaging;

import io.openmessaging.wal.WriteAheadLog;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LayneMessageQueueImpl extends MessageQueue {

    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final WriteAheadLog wal = new WriteAheadLog();

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(Constant.getKey(topic, queueId), k -> new AtomicLong());
        wal.flush(topic, queueId, data);
        return offsetAdder.getAndIncrement();
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = new HashMap<>(fetchNum);
        return dataMap;
    }
}
