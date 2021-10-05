package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private static final InfoReader partitionInfoReader = new PartitionInfoReader();
    private static final InfoReader walInfoReader = new WalInfoReader();

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
        long result = offsetAdder.getAndIncrement();
        int walId = topicId % Constant.WAL_FILE_COUNT;
        walList[walId].flush(topic, queueId, data, result);
        asyncReadWal(walId);
        return result;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        long partitionCount = PARTITION_OFFSET_MAP
                .computeIfAbsent(WalInfoBasic.getKey(topicId, queueId), k -> new AtomicLong())
                .get();
        int partitionFetchNum = 0;
        List<WalInfoBasic> infoList = new ArrayList<>();
        if (offset < partitionCount) {
            partitionFetchNum = (int) (Math.min(partitionCount, offset + fetchNum) - offset);
            infoList.addAll(partitionInfoReader.read(topicId, queueId, offset, partitionFetchNum));
        }
        long walFetchOffset = offset + partitionFetchNum;
        int walFetchNum = fetchNum - partitionFetchNum;
        if (walFetchNum > 0) {
            infoList.addAll(walInfoReader.read(topicId, queueId, walFetchOffset, walFetchNum));
        }
        log.info("topic: {}, queueId: {}, offset: {}, fetchNum: {}, partitionCount: {}, partitionFetchNum: {}, walFetchNum: {}",
                topic, queueId, offset, fetchNum, partitionCount, partitionFetchNum, walFetchNum);
        return readValueFromWAL(walId, offset, fetchNum, infoList);
    }

    private void asyncReadWal(int walId) {
        long curWalPos = (long) walList[walId].offset.logCount.get() * Constant.MSG_SIZE;
        if (curWalPos > 0 && curWalPos % Constant.READ_BEFORE_QUERY == 0) {
            try {
                partitions[walId].readBq.put(curWalPos - Constant.READ_BEFORE_QUERY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<Integer, ByteBuffer> readValueFromWAL(int walId, long offset, int fetchNum,
                                                      List<WalInfoBasic> infoList) {
        Map<Integer, ByteBuffer> result = new HashMap<>(fetchNum);
        try (FileChannel valueChannel = FileChannel.open(
                Constant.getWALValuePath(walId),
                StandardOpenOption.READ)
        ) {
            int idx = 0;
            for (WalInfoBasic infoBasic : infoList) {
                ByteBuffer buffer = ByteBuffer.allocate(infoBasic.valueSize);
                valueChannel.read(buffer, infoBasic.valuePos);
                result.put((int) offset + idx, buffer);
                idx++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        check(offset, ansSize, ansPos, result);
//        stopBroker();
        return result;
    }

    private void check(long offset, int[] ansSize, long[] ansPos, Map<Integer, ByteBuffer> dataMap) {
        for (int i = 0; i < ansPos.length; ++i) {
            log.info("ans, offset: {}, size: {}, pos: {}, res: {}", (offset + i), ansSize[i], ansPos[i], new String(dataMap.get((int) (offset + i)).array()));
        }
    }
}
