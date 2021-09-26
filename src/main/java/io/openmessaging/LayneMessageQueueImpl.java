package io.openmessaging;

import io.openmessaging.wal.WalInfoBasic;
import io.openmessaging.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
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
        for (int i = 0; i < idx; ++i) {
            log.info("ans, offset: {}, size: {}, pos: {}, res: {}", (offset + i), ansSize[i], ansPos[i], new String(dataMap.get((int) (offset + i)).array()));
        }
        return dataMap;
    }
}
