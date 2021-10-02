package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LayneMessageQueueImpl extends MessageQueue {

    private static final Logger log = LoggerFactory.getLogger(LayneMessageQueueImpl.class);

    private static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, AtomicLong> DEAL_OFFSET_MAP = new ConcurrentHashMap<>();

    private static final WriteAheadLog[] walList = new WriteAheadLog[Constant.WAL_FILE_COUNT];
    private static final Broker[] brokers = new Broker[Constant.WAL_FILE_COUNT];

    public LayneMessageQueueImpl() {
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            walList[i] = new WriteAheadLog(i);
            brokers[i] = new Broker(i, new WalOffset(), DEAL_OFFSET_MAP);
//            brokers[i].start();
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        int topicId = IdGenerator.getId(topic);
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(WalInfoBasic.getKey(topicId, queueId), k -> new AtomicLong());
        int walId = topicId % Constant.WAL_FILE_COUNT;
        walList[walId].flush(topic, queueId, data);
        return offsetAdder.getAndIncrement();
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        log.info("query, topic: {}, queueId: {}, offset: {}, fetchNum: {}", topic, queueId, offset, fetchNum);
        help(topicId, queueId, offset, fetchNum);
        long[] ansPos = new long[fetchNum];
        int[] ansSize = new int[fetchNum];
        int dataCount = readInfoListFromPartition(topicId, queueId, offset, fetchNum, ansPos, ansSize);
        return readValueFromWAL(walId, offset, fetchNum, dataCount, ansPos, ansSize);
    }

    private void help(int topicId, int queueId, long offset, int fetchNum) {
        int walId = topicId % Constant.WAL_FILE_COUNT;
        while (true) {
            // topic 块的处理偏移
            long targetOffset = offset + fetchNum;
            long dealOffset = DEAL_OFFSET_MAP.computeIfAbsent(
                    WalInfoBasic.getKey(topicId, queueId),
                    k -> new AtomicLong()
            ).get();
            log.info("dealing offset: {}, target: {}", dealOffset, targetOffset);
            if (dealOffset >= targetOffset) break;
            // 日志的处理偏移
            int dealingCount = walList[walId].offset.dealingCount.getAndIncrement();
            int logCount = walList[walId].offset.logCount.get();
            long dealingBeginPos = (long) dealingCount * Constant.READ_BEFORE_QUERY;
            long dealingEndPos = Math.min(
                    dealingBeginPos + Constant.READ_BEFORE_QUERY,
                    (long) logCount * Constant.MSG_SIZE
            );
            log.info("dealing, wal: {}, dealing: {}(b), {}(e), log: {}",
                    walId, dealingBeginPos, dealingEndPos, (logCount * Constant.MSG_SIZE));
            if (dealingBeginPos >= dealingEndPos) break;
            Page page = new Page();
            if (dealingEndPos == (long) logCount * Constant.MSG_SIZE) page.forceUpdate = true;
            try (FileChannel infoChannel = FileChannel.open(Constant.getWALInfoPath(walId), StandardOpenOption.READ)) {
                MappedByteBuffer buffer = infoChannel.map(FileChannel.MapMode.READ_ONLY, dealingBeginPos, Constant.READ_BEFORE_QUERY);
                while (buffer.hasRemaining()) {
                    WalInfoBasic msgInfo = new WalInfoBasic();
                    msgInfo.decode(buffer);
                    page.partition(msgInfo);
                }
//                brokers[walId].saveAsync(page);
                brokers[walId].save(page);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private int readInfoListFromPartition(int topicId, int queueId, long offset,
                                          int fetchNum, long[] ansPos, int[] ansSize) {
        int size = 0;
        try (FileChannel infoChannel = FileChannel.open(
                Constant.getPath(topicId, queueId), StandardOpenOption.READ)) {
            ByteBuffer infoBuffer = ByteBuffer.allocate(Constant.SIMPLE_MSG_SIZE * fetchNum);
            infoChannel.read(infoBuffer, offset * Constant.SIMPLE_MSG_SIZE);
            while (size < fetchNum) {
                infoBuffer.flip();
                while (infoBuffer.hasRemaining()) {
                    ansSize[size] = infoBuffer.getInt();
                    ansPos[size] = infoBuffer.getLong();
                    size++;
                }
                infoBuffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return size;
    }

    private Map<Integer, ByteBuffer> readValueFromWAL(int walId, long offset, int fetchNum,
                                                      int dataCount, long[] ansPos, int[] ansSize) {
        Map<Integer, ByteBuffer> result = new HashMap<>(fetchNum);
        try (FileChannel valueChannel = FileChannel.open(
                Constant.getWALValuePath(walId),
                StandardOpenOption.READ)
        ) {
            for (int i = 0; i < dataCount; ++i) {
                ByteBuffer buffer = ByteBuffer.allocate(ansSize[i]);
                valueChannel.read(buffer, ansPos[i]);
                result.put((int) offset + i, buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        check(offset, ansSize, ansPos, dataMap);
//        stopBroker();
        return result;
    }

    private void check(long offset, int[] ansSize, long[] ansPos, Map<Integer, ByteBuffer> dataMap) {
        for (int i = 0; i < ansPos.length; ++i) {
            log.info("ans, offset: {}, size: {}, pos: {}, res: {}", (offset + i), ansSize[i], ansPos[i], new String(dataMap.get((int) (offset + i)).array()));
        }
    }
}
