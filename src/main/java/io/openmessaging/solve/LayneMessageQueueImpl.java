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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
//    private final BrokerManager brokerManager;

    public LayneMessageQueueImpl() {
        reload();
        // wal
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            walList[i] = new WriteAheadLog();
        }
        // 分块
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            encoders[i] = new Encoder(i, walList[i].logsBq, IDX);
            encoders[i].start();
        }
        // 落盘
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            locks[i] = new ReentrantLock();
            conditions[i] = locks[i].newCondition();
            brokers[i] = new Broker(i, encoders[i].writeBq, locks[i], conditions[i]);
            brokers[i].start();
        }
//        List<BlockingQueue<WritePage>> producer = new ArrayList<>();
//        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) producer.add(encoders[i].writeBq);
//        brokerManager = new BrokerManager(producer, locks, conditions);
//        brokerManager.start();
    }

    private void reload() {
        if (!IdGenerator.load()) {
            return;
        }
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            loader[i] = new Loader(i, IDX);
            loader[i].start();
        }
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            try {
                loader[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private long start = 0;

    private boolean isDown(int walId, long logCount) {
        return logCount <= brokers[walId].logCount.get();
    }

    private long appendCnt = 0;

    private final Map<Integer, Integer> WAL_ID_MAP = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> WAL_ID_CNT_MAP = new ConcurrentHashMap<>();

    private int curWalId = 0;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        appendCnt++;
        long cost = System.currentTimeMillis() - start;
        if (cost > 15 * 60 * 1000) {
            log.info("APPEND TIME OVER: {}", appendCnt);
            return 0;
        }
        int topicId = IdGenerator.getId(topic);
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, data);
        //        int walId = topicId % Constant.WAL_FILE_COUNT;
        int key = result.getKey();
        AtomicInteger partitionCnt = WAL_ID_CNT_MAP.computeIfAbsent(key, k -> new AtomicInteger());
        partitionCnt.incrementAndGet();
        int walId = WAL_ID_MAP.computeIfAbsent(key, k -> curWalId % Constant.WAL_FILE_COUNT);
        result.walId = walId;
        curWalId++;
        try {
            locks[walId].lock();
            walList[walId].submit(result);
            while (!isDown(walId, result.logCount)) {
                conditions[walId].await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            locks[walId].unlock();
            int restCnt = partitionCnt.decrementAndGet();
            if (restCnt == 0) WAL_ID_MAP.remove(key);
        }
        return result.pOffset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            log.info("75G cost: {}", (System.currentTimeMillis() - start));
            start = -1;
        }
        int topicId = IdGenerator.getId(topic);
        Idx idx = IDX.get(WalInfoBasic.getKey(topicId, queueId));
        return readValueFromWAL((int) offset, fetchNum, idx);
    }

    private Map<Integer, ByteBuffer> readValueFromWAL(int offset, int fetchNum, Idx idx) {
        Map<Integer, ByteBuffer> result = new HashMap<>(fetchNum);
        FileChannel valueChannel = null;
        List<WalInfoBasic> idxList = new ArrayList<>(fetchNum);
        for (int i = 0; i < fetchNum; ++i) {
            int key = offset + i;
            if ((key << 1) >= idx.getSize()) break;
            int walId = idx.getWalId(key);
            int part = idx.getWalPart(key);
            int pos = idx.getWalValuePos(key);
            int size = idx.getWalValueSize(key);
            idxList.add(new WalInfoBasic(i, walId, part, pos, size));
        }

        idxList.sort(Comparator.comparingInt((WalInfoBasic o) -> o.walId).thenComparingInt(o -> o.walPart));

        int curWalId = -1;
        int curPart = -1;
        try {
            for (WalInfoBasic info : idxList) {
                if (valueChannel == null || info.walId != curWalId || info.walPart != curPart) {
                    if (valueChannel != null) {
                        valueChannel.close();
                    }
                    valueChannel = FileChannel.open(
                            Constant.getWALInfoPath(info.walId, info.walPart),
                            StandardOpenOption.READ);
                    curWalId = info.walId;
                    curPart = info.walPart;
                }
                ByteBuffer buffer = ByteBuffer.allocate(info.valueSize);
                valueChannel.read(buffer, info.walPos);
                buffer.flip();
                result.put((int) info.pOffset, buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
