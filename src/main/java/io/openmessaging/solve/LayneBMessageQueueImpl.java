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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LayneBMessageQueueImpl extends MessageQueue {
    private static final Logger log = LoggerFactory.getLogger(LayneBMessageQueueImpl.class);
    private static final Loader[] loader = new Loader[Constant.WAL_FILE_COUNT];
    public Map<Integer, Idx> IDX = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> DOING_OFFSET_MAP = new ConcurrentHashMap<>();

    public LayneBMessageQueueImpl() {
        reload();
    }

    private void reload() {
        if (!IdGenerator.getIns().load()) {
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
        int topicId = IdGenerator.getIns().getId(topic);
        WalInfoBasic info = new WalInfoBasic(topicId, queueId, data);
        BufferEncoder encoder = BLOCK_TL.get();
        if (encoder == null) {
            int walId = topicId % Constant.WAL_FILE_COUNT;
            encoder = BLOCKS.computeIfAbsent(walId, key -> new BufferEncoder(walId));
            BLOCK_TL.set(encoder);
        }
        info.walId = encoder.id;
        // 某块计算处理中的 msg 数目
        int key = info.getKey();
        DOING_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
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
        idx.add((int) info.pOffset, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);
        DOING_OFFSET_MAP.get(key).decrementAndGet();
        return info.pOffset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            log.info("75G cost: {}", (System.currentTimeMillis() - start));
            start = -1;
//            return null;
        }
        int topicId = IdGenerator.getIns().getId(topic);
        int key = WalInfoBasic.getKey(topicId, queueId);
        Idx idx = IDX.get(key);
        int append = APPEND_OFFSET_MAP.getOrDefault(key, new AtomicInteger()).get();
        int doing = DOING_OFFSET_MAP.getOrDefault(key, new AtomicInteger()).get();
        int pOffset = append - doing;

        fetchNum = Math.min(fetchNum, (int) (pOffset - offset));

        Map<Integer, ByteBuffer> result = new HashMap<>();
        if (idx == null || fetchNum <= 0) return result;
        FileChannel valueChannel = null;
        int walId = topicId % Constant.WAL_FILE_COUNT;
        List<WalInfoBasic> idxList = new ArrayList<>();
        for (int i = 0; i < fetchNum; ++i) {
            int idxPos = (int) offset + i;
            if ((idxPos << 1 | 1) >= idx.getSize()) break;
            int part = idx.getWalPart(idxPos);
            int pos = idx.getWalValuePos(idxPos);
            int size = idx.getWalValueSize(idxPos);
            idxList.add(new WalInfoBasic(i, part, pos, size));
        }
        int curPart = -1;
        try {
            for (WalInfoBasic info : idxList) {
                if (valueChannel == null || info.walPart != curPart) {
                    curPart = info.walPart;
                    if (valueChannel != null) {
                        valueChannel.close();
                    }
                    valueChannel = FileChannel.open(
                            Constant.getWALInfoPath(walId, info.walPart),
                            StandardOpenOption.READ);
                }
                ByteBuffer buffer = ByteBuffer.allocate(info.valueSize);
                valueChannel.read(buffer, info.walPos);
                buffer.flip();
                result.put((int) info.pOffset, buffer);
            }
        } catch (IOException e) {
            BufferEncoder encoder = BLOCKS.computeIfAbsent(walId, k -> new BufferEncoder(walId));
            int createFilePart = 1;
            while (Constant.getWALInfoPath(walId, createFilePart).toFile().exists()) createFilePart++;
            int queryMaxPart = idxList.get(fetchNum - 1).walPart;
            int queryMaxPos = idxList.get(fetchNum - 1).walPos;
            log.error("now walId: {}, append: {}, doing: {}, part: {}, pos: {}, createFilePart: {}, queryMaxPart: {}, queryMaxPos: {},resultSize: {}, e: {}",
                    walId, append, doing, encoder.part, encoder.pos, createFilePart - 1, queryMaxPart, queryMaxPos, result.size(), e.getMessage());
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
