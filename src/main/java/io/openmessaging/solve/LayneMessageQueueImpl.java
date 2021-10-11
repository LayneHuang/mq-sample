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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LayneMessageQueueImpl extends MessageQueue {
    private static final Logger log = LoggerFactory.getLogger(LayneMessageQueueImpl.class);

    private static final WriteAheadLog[] walList = new WriteAheadLog[Constant.WAL_FILE_COUNT];

    private static final Broker[] brokers = new Broker[Constant.WAL_FILE_COUNT];
    private static final Encoder[] encoders = new Encoder[Constant.WAL_FILE_COUNT];
    private static final Loader[] loader = new Loader[Constant.WAL_FILE_COUNT];
    private static final Lock[] locks = new ReentrantLock[Constant.WAL_FILE_COUNT];
    private static final Condition[] conditions = new Condition[Constant.WAL_FILE_COUNT];

    public Map<Integer, Idx> IDX = new ConcurrentHashMap<>();

    public LayneMessageQueueImpl() {
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            locks[i] = new ReentrantLock();
            conditions[i] = locks[i].newCondition();
            brokers[i] = new Broker(i, locks[i], conditions[i]);
            brokers[i].start();
        }

        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            encoders[i] = new Encoder(brokers[i].writeBq, IDX);
            encoders[i].start();
        }

        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
//            walList[i] = new WriteAheadLog();
            walList[i] = new WriteAheadLog(encoders[i].encodeBq);
        }
    }

    private void reload() {
        if (!IdGenerator.load()) {
            return;
        }
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            loader[i] = new Loader(i, walList[i]);
            loader[i].start();
        }
    }

    private long start = 0;

    private boolean isDown(int walId, long logCount) {
        return logCount <= brokers[walId].logCount.get();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, data);
        try {
            locks[walId].lock();
            walList[walId].submitEncoder(result);
//            walList[walId].submit(result);
//            encoders[walId].submit(result);
            while (!isDown(walId, result.logCount)) {
//                conditions[walId].await(200, TimeUnit.MILLISECONDS);
                conditions[walId].await();
//                if (!isDown(walId, result.logCount)) {
//                    encoders[walId].force();
//                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            locks[walId].unlock();
        }
        long cost = System.currentTimeMillis() - start;
        if (cost > 10 * 60 * 1000) {
            log.info("time over: {}", result.logCount);
            return 0;
        }
//        log.debug("check now: {}, {}, {}", topic, queueId, new String(getRange(topic, queueId, result.pOffset, 1).get(0).array()));
        return result.pOffset;
    }

    private long queryCnt;

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            log.info("75G cost: " + (System.currentTimeMillis() - start));
        }
        queryCnt++;
        if (queryCnt > 3) return null;
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        Idx idx = IDX.get(WalInfoBasic.getKey(topicId, queueId));
        return readValueFromWAL(walId, (int) offset, fetchNum, idx);
    }

    private Map<Integer, ByteBuffer> readValueFromWAL(int walId, int offset, int fetchNum, Idx idx) {
        Map<Integer, ByteBuffer> result = new HashMap<>(fetchNum);
        FileChannel valueChannel = null;
        int curPart = -1;
        try {
            for (int i = 0; i < fetchNum; ++i) {
                int key = offset + i;
                if ((key << 1) >= idx.getSize()) continue;
                int part = idx.getWalPart(key);
                if (valueChannel == null || part != curPart) {
                    if (valueChannel != null) {
                        valueChannel.close();
                    }
                    valueChannel = FileChannel.open(
                            Constant.getWALInfoPath(walId, part),
                            StandardOpenOption.READ);
                    curPart = part;
                }
                int pos = idx.getWalValuePos(key);
                int size = idx.getWalValueSize(key);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                valueChannel.read(buffer, pos);
                buffer.flip();
                result.put(i, buffer);
//                log.debug("key: {}, value: {}, pos: {}, size: {}", key, new String(buffer.array()), pos, size);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
