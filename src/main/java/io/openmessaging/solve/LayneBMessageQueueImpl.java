package io.openmessaging.solve;

import com.intel.pmem.llpl.MemoryBlock;
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

public class LayneBMessageQueueImpl extends MessageQueue {
    private static final Logger log = LoggerFactory.getLogger(LayneBMessageQueueImpl.class);
    public Map<Integer, Idx> IDX = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    //    private final Map<Integer, AtomicInteger> DOING_OFFSET_MAP = new ConcurrentHashMap<>();
    public static ThreadLocal<BufferEncoder> BLOCK_TL = new ThreadLocal<>();
    public static ConcurrentHashMap<Integer, BufferEncoder> BLOCKS = new ConcurrentHashMap<>(40);

    public LayneBMessageQueueImpl() {
//        reload();
    }

    private void reload() {
        if (!IdGenerator.getIns().load()) {
            return;
        }
        Loader load = new Loader(IDX, APPEND_OFFSET_MAP);
        load.run();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        int topicId = IdGenerator.getIns().getId(topic);
        BufferEncoder encoder = BLOCK_TL.get();
        if (encoder == null) {
            int walId = topicId % Constant.WAL_FILE_COUNT;
            encoder = BLOCKS.computeIfAbsent(walId, key -> new BufferEncoder(walId, new Cache(walId, IDX)));
            BLOCK_TL.set(encoder);
        }
        WalInfoBasic info = new WalInfoBasic(encoder.id, topicId, queueId, data);
        // 某块计算处理中的 msg 数目
        int key = info.getKey();
//        DOING_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
        // 获取偏移
        info.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(
                key,
                k -> new AtomicInteger()
        ).getAndIncrement();
        encoder.submit(info);
        encoder.holdOn(info);
        // 索引
        Idx idx = IDX.computeIfAbsent(key, k -> new Idx());
        idx.add((int) info.pOffset, info.walId, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);
//        DOING_OFFSET_MAP.get(key).decrementAndGet();
        return info.pOffset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        int topicId = IdGenerator.getIns().getId(topic);
        int key = WalInfoBasic.getKey(topicId, queueId);
        Idx idx = IDX.get(key);
        int append = APPEND_OFFSET_MAP.getOrDefault(key, new AtomicInteger()).get();
//        int doing = DOING_OFFSET_MAP.getOrDefault(key, new AtomicInteger()).get();
//        int pOffset = append - doing;
        fetchNum = Math.min(fetchNum, (int) (append - offset));
        Map<Integer, ByteBuffer> result = new HashMap<>();
        if (idx == null || fetchNum <= 0) return result;
        FileChannel valueChannel = null;
        List<WalInfoBasic> idxList = new ArrayList<>();
        for (int i = 0; i < fetchNum; ++i) {
            int idxPos = (int) offset + i;
            if ((idxPos << 1 | 1) >= idx.getSize()) break;
            int walId = idx.getWalId(idxPos);
            int part = idx.getWalPart(idxPos);
            int pos = idx.getWalValuePos(idxPos);
            int size = idx.getWalValueSize(idxPos);
            boolean isPmem = idx.isPmem(idxPos);
            idxList.add(new WalInfoBasic(idxPos, walId, part, pos, size, isPmem));
        }
        idxList.sort(
                Comparator.comparingInt((WalInfoBasic o) -> o.walId)
                        .thenComparingInt(o -> o.walPart)
                        .thenComparingInt(o -> o.walPos)
        );
        int curPart = -1;
        int curId = -1;
        try {
            for (WalInfoBasic info : idxList) {
                info.value = ByteBuffer.allocate(info.valueSize);
                // 在傲腾上
                if (info.isPmem) {
                    BufferEncoder encoder = BLOCKS.get(info.walId);
                    encoder.cache.get(info);
                    result.put((int) (info.pOffset - offset), info.value);
                    continue;
                }
                // ESSD上
                if (valueChannel == null || info.walId != curId || info.walPart != curPart) {
                    curId = info.walId;
                    curPart = info.walPart;
                    if (valueChannel != null) {
                        valueChannel.close();
                    }
                    valueChannel = FileChannel.open(
                            Constant.getWALInfoPath(info.walId, info.walPart),
                            StandardOpenOption.READ);
                }
                valueChannel.read(info.value, info.walPos);
                info.value.flip();
                result.put((int) (info.pOffset - offset), info.value);
            }
        } catch (IOException e) {
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
