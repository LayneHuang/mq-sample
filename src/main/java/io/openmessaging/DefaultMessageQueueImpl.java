package io.openmessaging;

import sun.misc.Cleaner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {

    public static final String DIR_PMEM = "/pmem";
    public static final Path DIR_WORK = Paths.get(System.getProperty("user.dir")).resolve("target");
    public static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, AtomicLong>> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        ConcurrentHashMap<Integer, AtomicLong> queueOffsetMap = APPEND_OFFSET_MAP.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
        AtomicLong offsetAdder = queueOffsetMap.computeIfAbsent(queueId, k -> new AtomicLong());
        long offset = offsetAdder.getAndIncrement();
        // 更新最大位点
        // 保存 data 中的数据
        try {
            Path queuePath = DIR_WORK.resolve(topic);
            Files.createDirectories(queuePath);
            FileChannel dataChannel = FileChannel.open(
                    queuePath.resolve(queueId + ".d"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.SYNC
            );
            dataChannel.force(true);
            long position = dataChannel.position();
            ByteBuffer lenBufWrite = ByteBuffer.allocate(Short.BYTES);
            lenBufWrite.putShort((short) data.limit());
            lenBufWrite.flip();
            dataChannel.write(lenBufWrite);
            lenBufWrite.flip();
            dataChannel.write(data);
            dataChannel.close();
            // 索引
            if (offset % 64 == 0) {
                ByteBuffer indexBuf = ByteBuffer.allocate(16);
                indexBuf.putLong(offset);
                indexBuf.putLong(position);
                indexBuf.flip();
                FileChannel indexChannel = FileChannel.open(
                        queuePath.resolve(queueId + ".i"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.SYNC
                );
                indexChannel.force(true);
                indexChannel.write(indexBuf);
                indexChannel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = null;
        try {
            FileChannel indexChannel = FileChannel.open(
                    DIR_WORK.resolve(topic).resolve(queueId + ".i"),
                    StandardOpenOption.READ
            );
            MappedByteBuffer indexMapBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            // 找数据偏移量
            long prevOffset = 0;
            long position = 0;
            while (indexMapBuf.hasRemaining()) {
                long curOffset = indexMapBuf.getLong();
                if (curOffset > offset) {
                    break;
                }
                prevOffset = curOffset;
                position = indexMapBuf.getLong();
            }
            Cleaner cleaner = ((sun.nio.ch.DirectBuffer) indexMapBuf).cleaner();
            if (cleaner != null) {
                cleaner.clean();
            }
            indexChannel.close();
            dataMap = new HashMap<>(fetchNum);
            FileChannel dataChannel = FileChannel.open(
                    DIR_WORK.resolve(topic).resolve(queueId + ".d"),
                    StandardOpenOption.READ
            );
            dataChannel.position(position);
            int key = 0;
            ByteBuffer lenBufRead = ByteBuffer.allocate(Short.BYTES);
            while (true) {
                if (dataChannel.read(lenBufRead) <= 0) {
                    break;
                }
                lenBufRead.flip();
                short length = lenBufRead.getShort();
                lenBufRead.flip();
                ByteBuffer msgBuf = ByteBuffer.allocate(length);
                dataChannel.read(msgBuf);
                msgBuf.flip();
                if (prevOffset >= offset) {
                    dataMap.put(key, msgBuf);
                    key++;
                    if (key == fetchNum) {
                        break;
                    }
                }
                prevOffset++;
            }
            dataChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataMap;
    }
}
