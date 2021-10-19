package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.BufferEncoder;
import io.openmessaging.wal.Idx;
import io.openmessaging.wal.Loader;
import io.openmessaging.wal.WalInfoBasic;
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
import java.util.concurrent.atomic.AtomicInteger;

public class LayneBMessageQueueImpl extends MessageQueue {
    private static final Logger log = LoggerFactory.getLogger(LayneBMessageQueueImpl.class);
    private static final Loader[] loader = new Loader[Constant.WAL_FILE_COUNT];
    public Map<Integer, Idx> IDX = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> WAL_ID_CNT_MAP = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public LayneBMessageQueueImpl() {
        reload();
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


    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        return allIn(topic, queueId, data);
    }

    public static ThreadLocal<BufferEncoder> BLOCK_TL = new ThreadLocal<>();
    public static ConcurrentHashMap<Integer, BufferEncoder> BLOCKS = new ConcurrentHashMap<>(40);

    private long allIn(String topic, int queueId, ByteBuffer data) {
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        WalInfoBasic info = new WalInfoBasic(topicId, queueId, data);
        info.walId = walId;
        BufferEncoder encoder = BLOCK_TL.get();
        if (encoder == null) {
            encoder = BLOCKS.computeIfAbsent(walId, key -> new BufferEncoder());
            BLOCK_TL.set(encoder);
        }

        // 某块计算处理中的 msg 数目
        int key = info.getKey();
        AtomicInteger partitionCnt = WAL_ID_CNT_MAP.computeIfAbsent(key, k -> new AtomicInteger());
        partitionCnt.incrementAndGet();

        // 获取偏移
        info.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(
                key,
                k -> new AtomicInteger()
        ).getAndIncrement();
        try {
            encoder.submit(info);
            encoder.holdOn(info);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 索引
        Idx idx = IDX.computeIfAbsent(key, k -> new Idx());
        idx.add((int) info.pOffset, info.walId, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);

        partitionCnt.decrementAndGet();
        return info.pOffset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            log.info("75G cost: {}", (System.currentTimeMillis() - start));
            start = -1;
//            return null;
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
            return new HashMap<>();
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
