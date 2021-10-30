package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.*;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Constant.KB;
import static io.openmessaging.leo2.Utils.UNSAFE;

public class LayneBMessageQueueImpl extends MessageQueue {
    public static final Map<Integer, Idx> IDX = new ConcurrentHashMap<>();
    public static final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    public static ThreadLocal<BufferEncoder> BLOCK_TL = new ThreadLocal<>();
    public static ConcurrentHashMap<Integer, BufferEncoder> BLOCKS = new ConcurrentHashMap<>(Constant.WAL_FILE_COUNT);
    public static boolean GET_RANGE_START = false;
    public static ThreadLocal<ByteBuffer> READ_BUF_TL = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(18 * KB));

    public LayneBMessageQueueImpl() {
        reload();
    }

    private void reload() {
        if (!IdGenerator.getIns().load()) {
            return;
        }
        Loader load = new Loader();
        load.run();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        int topicId = IdGenerator.getIns().getId(topic);
        BufferEncoder encoder = BLOCK_TL.get();
        if (encoder == null) {
            int walId = topicId % Constant.WAL_FILE_COUNT;
            encoder = BLOCKS.computeIfAbsent(walId, BufferEncoder::new);
            BLOCK_TL.set(encoder);
        }
        WalInfoBasic info = new WalInfoBasic(encoder.id, topicId, queueId, data);
        // 获取偏移
        info.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(
                info.getKey(),
                k -> new AtomicInteger()
        ).getAndIncrement();
        // 每个步骤不放入写文件的同一个同步中, 锁粒度更小, 并发更高
        // 体积较小(12KB)的写傲腾(12KB->17KB, 60G->125G)
        int[] cacheResult = null;
        if (info.valueSize < 8 * KB || GET_RANGE_START) {
            cacheResult = encoder.cache.write(info);
        }
        // 再写文件
        encoder.submit(info);
        // 写索引
        boolean isPmem = cacheResult != null;
        Idx idx = IDX.computeIfAbsent(info.getKey(), k -> new Idx());
        idx.add(
                (int) info.pOffset,
                info.walId,
                isPmem ? cacheResult[0] : info.walPart,
                isPmem ? cacheResult[1] : info.walPos + WalInfoBasic.BYTES,
                info.valueSize,
                isPmem
        );
        // 最后等待
        encoder.holdOn(info);
        return info.pOffset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        GET_RANGE_START = true;
        int topicId = IdGenerator.getIns().getId(topic);
        int key = WalInfoBasic.getKey(topicId, queueId);
        Idx idx = IDX.get(key);
        int append = APPEND_OFFSET_MAP.getOrDefault(key, new AtomicInteger()).get();
        fetchNum = Math.min(fetchNum, (int) (append - offset));
        Map<Integer, ByteBuffer> result = new HashMap<>();
        if (idx == null || fetchNum <= 0) return result;
        List<WalInfoBasic> idxList = new ArrayList<>();
        for (int i = 0; i < fetchNum; ++i) {
            int idxPos = (int) offset + i;
            WalInfoBasic query = idx.getInfo(idxPos);
            if (query == null) break;
            idxList.add(query);
        }
        idxList.sort(
                Comparator.comparingInt((WalInfoBasic o) -> o.isPmem ? -1 : 1)
                        .thenComparing(o -> o.walId)
                        .thenComparingInt(o -> o.walPart)
                        .thenComparingInt(o -> o.walPos)
        );
        FileChannel valueChannel = null;
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
                ByteBuffer readBuf = READ_BUF_TL.get();
                readBuf.clear();
                valueChannel.read(readBuf, info.walPos);
                long address = ((DirectBuffer) readBuf).address();
                UNSAFE.copyMemory(null, address, info.value.array(), 16, info.valueSize);
                info.value.limit(info.valueSize);
                result.put((int) (info.pOffset - offset), info.value);
            }
        } catch (IOException ignored) {
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
