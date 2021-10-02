package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.Broker;
import io.openmessaging.wal.Partition;
import io.openmessaging.wal.WalInfoBasic;
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

    private static final ConcurrentHashMap<String, AtomicLong> PARTITION_OFFSET_MAP = new ConcurrentHashMap<>();

    private static final WriteAheadLog[] walList = new WriteAheadLog[Constant.WAL_FILE_COUNT];

    private static final Partition[] partitions = new Partition[Constant.WAL_FILE_COUNT];

    private static final Broker[] brokers = new Broker[Constant.WAL_FILE_COUNT];

    public LayneMessageQueueImpl() {
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            walList[i] = new WriteAheadLog(i);
            partitions[i] = new Partition(i);
            brokers[i] = new Broker(i, PARTITION_OFFSET_MAP, partitions[i].writeBq);
            partitions[i].start();
            brokers[i].start();
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        int topicId = IdGenerator.getId(topic);
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(WalInfoBasic.getKey(topicId, queueId), k -> new AtomicLong());
        int walId = topicId % Constant.WAL_FILE_COUNT;
        walList[walId].flush(topic, queueId, data);
        asyncReadWal(walId);
        return offsetAdder.getAndIncrement();
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        long[] ansPos = new long[fetchNum];
        int[] ansSize = new int[fetchNum];
        long partitionCount = PARTITION_OFFSET_MAP
                .computeIfAbsent(WalInfoBasic.getKey(topicId, queueId), k -> new AtomicLong())
                .get();
        int partitionFetchNum = 0;
        String qLog = "";
        if (offset < partitionCount) {
            partitionFetchNum = (int) (Math.min(partitionCount, offset + fetchNum) - offset);
            readInfoListFromPartition(topicId, queueId, offset, partitionFetchNum, ansPos, ansSize);
            qLog += "read form partition, fetNum: " + partitionFetchNum;
        }
        int walFetchNum = fetchNum - partitionFetchNum;
        if (walFetchNum > 0) {
            qLog += ", read form wal";
        }
        log.info("{}, topic: {}, queueId: {}, offset: {}, fetchNum: {}, partitionCount: {}",
                qLog, topic, queueId, offset, fetchNum, partitionCount);
        return readValueFromWAL(walId, offset, fetchNum, fetchNum, ansPos, ansSize);
    }

    private void asyncReadWal(int walId) {
        long curWalPos = walList[walId].offset.logCount.get() * Constant.SIMPLE_MSG_SIZE;
        if (curWalPos > 0 && curWalPos % Constant.READ_BEFORE_QUERY == 0) {
            try {
                partitions[walId].readBq.put(curWalPos - Constant.READ_BEFORE_QUERY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void readInfoListFromPartition(int topicId, int queueId, long offset,
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
        check(offset, ansSize, ansPos, result);
//        stopBroker();
        return result;
    }

    private void check(long offset, int[] ansSize, long[] ansPos, Map<Integer, ByteBuffer> dataMap) {
        for (int i = 0; i < ansPos.length; ++i) {
            log.info("ans, offset: {}, size: {}, pos: {}, res: {}", (offset + i), ansSize[i], ansPos[i], new String(dataMap.get((int) (offset + i)).array()));
        }
    }
}
