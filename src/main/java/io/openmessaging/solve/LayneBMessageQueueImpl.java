package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Constant.KB;

public class LayneBMessageQueueImpl extends MessageQueue {
    public Map<Integer, Idx> IDX = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    public static ThreadLocal<BufferEncoder> BLOCK_TL = new ThreadLocal<>();
    public static ConcurrentHashMap<Integer, BufferEncoder> BLOCKS = new ConcurrentHashMap<>(40);
    public static boolean GET_RANGE_START = false;
//    public static ThreadLocal<ByteBuffer> PAGE_BUFFER = new ThreadLocal<>();

    public LayneBMessageQueueImpl() {
        reload();
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
            // 多读出来的放这
//            Map<String, WalInfoBasic> restMap = new HashMap<>();
            for (WalInfoBasic info : idxList) {
                info.value = ByteBuffer.allocate(info.valueSize);
                // 在多余的内存上
//                if (restMap.containsKey(info.getStrKey())) {
//                    WalInfoBasic resInfo = restMap.get(info.getStrKey());
//                    result.put((int) (info.pOffset - offset), resInfo.value);
//                    continue;
//                }
                // 在傲腾上
                if (info.isPmem) {
                    BufferEncoder encoder = BLOCKS.get(info.walId);
                    encoder.cache.get(info);
                    result.put((int) (info.pOffset - offset), info.value);
                    continue;
                }
                // ESSD上
                // 每次读32K
//                ByteBuffer pageBuffer = PAGE_BUFFER.get();
//                if (pageBuffer == null) {
//                    pageBuffer = ByteBuffer.allocate(64 * KB);
//                    PAGE_BUFFER.set(pageBuffer);
//                }
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
//                pageBuffer.clear();
                valueChannel.read(info.value, info.walPos);
//                pageBuffer.flip();
                // 拿出答案
//                for (int i = 0; i < info.valueSize; ++i) info.value.put(pageBuffer.get());
                info.value.flip();
                result.put((int) (info.pOffset - offset), info.value);
                // 剩下的放内存
//                while (pageBuffer.hasRemaining()) {
//                    if (pageBuffer.remaining() < WalInfoBasic.BYTES) break;
//                    WalInfoBasic restInfo = new WalInfoBasic();
//                    restInfo.decode(pageBuffer, false);
//                    if (pageBuffer.remaining() < restInfo.valueSize) break;
//                    restInfo.value = ByteBuffer.allocate(restInfo.valueSize);
//                    for (int i = 0; i < restInfo.valueSize; ++i) restInfo.value.put(pageBuffer.get());
//                    restInfo.value.flip();
//                    // 放入多余先放内存
////                    restMap.put(restInfo.getStrKey(), restInfo);
//                    BufferEncoder encoder = BLOCKS.get(restInfo.walId);
//                    encoder.cache.write(restInfo);
//                }
            }
            // 把读出来多余的放傲腾
//            restMap.forEach((infoStrKey, info) -> {
//                BufferEncoder encoder = BLOCKS.get(info.walId);
//                encoder.cache.write(info);
//            });
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
