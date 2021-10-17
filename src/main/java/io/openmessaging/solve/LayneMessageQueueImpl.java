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

    public LayneMessageQueueImpl() {
//        reload();
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
    }

    private void reload() {
        if (!IdGenerator.load()) {
            return;
        }
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            loader[i] = new Loader(i, IDX, APPEND_OFFSET_MAP);
            loader[i].start();
        }
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            try {
                loader[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("reload finished.");
    }

    private long start = 0;

    private boolean isDown(int walId, long logCount) {
        return logCount <= brokers[walId].logCount.get();
    }

    private final Map<Integer, Integer> WAL_ID_MAP = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> WAL_ID_CNT_MAP = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    private int curWalId = 0;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        int topicId = IdGenerator.getId(topic);
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, data);
        // 某块计算处理中的 msg 数目
        int key = result.getKey();
        AtomicInteger partitionCnt = WAL_ID_CNT_MAP.computeIfAbsent(key, k -> new AtomicInteger());
        partitionCnt.incrementAndGet();
        // 映射到对应的 wal, 循环分配
        int walId = WAL_ID_MAP.computeIfAbsent(key, k -> (curWalId++) % Constant.WAL_FILE_COUNT);
        result.walId = walId;
        try {
            locks[walId].lock();
            // 获取偏移
            result.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(
                    result.getKey(),
                    k -> new AtomicInteger()
            ).getAndIncrement();
            // 提交 log 号
            walList[walId].submit(result);
            while (!isDown(walId, result.logCount)) {
                conditions[walId].await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            locks[walId].unlock();
            int restCnt = partitionCnt.decrementAndGet();
//            if (restCnt == 0) WAL_ID_MAP.remove(key);
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
        int key = WalInfoBasic.getKey(topicId, queueId);
        int pOffset = APPEND_OFFSET_MAP.getOrDefault(key, new AtomicInteger()).get()
                - WAL_ID_CNT_MAP.getOrDefault(key, new AtomicInteger()).get();
        fetchNum = Math.min(fetchNum, (int) (pOffset - offset));
        return readValueFromWAL((int) offset, fetchNum, idx);
    }

    private Map<Integer, ByteBuffer> readValueFromWAL(int offset, int fetchNum, Idx idx) {
        Map<Integer, ByteBuffer> result = new HashMap<>(fetchNum);
        if (idx == null || fetchNum <= 0) return result;
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

        idxList.sort(Comparator.comparingInt((WalInfoBasic o) -> o.walId)
                .thenComparingInt(o -> o.walPart));

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
        } finally {
            if (valueChannel != null) {
                try {
                    valueChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
}
