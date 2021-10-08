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
import java.util.HashMap;
import java.util.Map;

public class LayneMessageQueueImpl extends MessageQueue {
    private static final Logger log = LoggerFactory.getLogger(LayneMessageQueueImpl.class);

//    private static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

//    private static final ConcurrentHashMap<String, AtomicLong> PARTITION_OFFSET_MAP = new ConcurrentHashMap<>();

    private static final WriteAheadLog[] walList = new WriteAheadLog[Constant.WAL_FILE_COUNT];

//    private static final Partition[] partitions = new Partition[Constant.WAL_FILE_COUNT];

    private static final Broker[] brokers = new Broker[Constant.WAL_FILE_COUNT];
    private static final Loader[] loader = new Loader[Constant.WAL_FILE_COUNT];

//    private static final PartitionInfoReader partitionInfoReader = new PartitionInfoReader();

    private static final WalInfoReader walInfoReader = new WalInfoReader();

    public LayneMessageQueueImpl() {
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            walList[i] = new WriteAheadLog();
        }
        reload();
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            brokers[i] = new Broker(i, walList[i].readBq);
            brokers[i].start();
        }
    }

    private void reload() {
        IdGenerator.load();
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            loader[i] = new Loader(i, walList[i]);
            loader[i].start();
        }
    }

    private int okCnt = 0;
    private int forceCnt = 0;
    private int queryCnt = 0;
    private long start = 0;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        queryCnt++;
        if (queryCnt % 10000 == 0) {
            log.info("queryCount: {}, ok: {} , force:{} ", queryCnt, okCnt, forceCnt);
        }
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        WalInfoBasic submitResult = walList[walId].submit(topicId, queueId, data);
        int wait = 0;
        while (true) {
            if (submitResult.walPos <= brokers[walId].walPos.get()) {
                okCnt++;
                break;
            }
            wait++;
            if (wait > 2000) {
                walList[walId].force();
                forceCnt++;
            }
        }
        return submitResult.pOffset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            System.out.println("75G cost: " + (System.currentTimeMillis() - start));
            return null;
        }
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        WriteAheadLog.Idx idx = walList[walId].IDX.get(WalInfoBasic.getKey(topicId, queueId));
//        log.info("query, idxSize: {}", idx.size);
        return readValueFromWAL(walId, (int) offset, fetchNum, idx.list);
    }

    private Map<Integer, ByteBuffer> readValueFromWAL(int walId, int offset, int fetchNum,
                                                      int[] idx) {
        Map<Integer, ByteBuffer> result = new HashMap<>(fetchNum);
        try (FileChannel valueChannel = FileChannel.open(
                Constant.getWALInfoPath(walId),
                StandardOpenOption.READ)
        ) {
            for (int i = 0; i < fetchNum; ++i) {
                int key = offset + i;
                if ((key << 1) >= idx.length) continue;
                ByteBuffer buffer = ByteBuffer.allocate(idx[(key << 1) | 1]);
                valueChannel.read(buffer, idx[key << 1]);
                result.put(key, buffer);
//                log.info("key: {}, value:{}, pos: {}, size: {}", key, new String(buffer.array()), idx[key << 1], idx[(key << 1) | 1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
