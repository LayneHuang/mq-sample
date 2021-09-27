package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LayneMessageQueueImpl extends MessageQueue {

    private static final Logger log = LoggerFactory.getLogger(LayneMessageQueueImpl.class);

    private static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    private static final WriteAheadLog[] walList = new WriteAheadLog[Constant.WAL_FILE_COUNT];

    public LayneMessageQueueImpl() {
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            walList[i] = new WriteAheadLog(i);
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(Constant.getKey(topic, queueId), k -> new AtomicLong());
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        walList[walId].flush(topic, queueId, data);
        return offsetAdder.getAndIncrement();
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        Map<Integer, ByteBuffer> dataMap = new HashMap<>(fetchNum);
        log.info("query, topic: {}, queueId: {}, offset: {}, fetchNum: {}", topic, queueId, offset, fetchNum);
        int idx = 0;
        int[] ansSize = new int[fetchNum];
        long[] ansPos = new long[fetchNum];

        try (FileChannel infoChannel = FileChannel.open(
                Constant.getPath(topicId, queueId), StandardOpenOption.READ)) {
            ByteBuffer infoBuffer = ByteBuffer.allocate(Constant.SIMPLE_MSG_SIZE * fetchNum);
            infoChannel.read(infoBuffer, offset * Constant.SIMPLE_MSG_SIZE);
            while (idx < fetchNum) {
                infoBuffer.flip();
                while (infoBuffer.hasRemaining()) {
                    ansSize[idx] = infoBuffer.getInt();
                    ansPos[idx] = infoBuffer.getLong();
                    idx++;
                }
                infoBuffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileChannel valueChannel = FileChannel.open(
                Constant.getWALValuePath(walId), StandardOpenOption.READ)) {
            for (int i = 0; i < idx; ++i) {
                ByteBuffer buffer = ByteBuffer.allocate(ansSize[i]);
                valueChannel.read(buffer, ansPos[i]);
                dataMap.put((int) offset + i, buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //check(offset, ansSize, ansPos, dataMap);
        //stopBroker();
        return dataMap;
    }

    private void check(long offset, int[] ansSize, long[] ansPos, Map<Integer, ByteBuffer> dataMap) {
        for (int i = 0; i < ansPos.length; ++i) {
            log.info("ans, offset: {}, size: {}, pos: {}, res: {}", (offset + i), ansSize[i], ansPos[i], new String(dataMap.get((int) (offset + i)).array()));
        }
    }

    private void stopBroker() {
        for (WriteAheadLog wal : walList) {
            try {
                wal.stopBroker();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
