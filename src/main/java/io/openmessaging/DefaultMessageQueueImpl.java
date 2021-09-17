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
    public static final Path DIR_ESSD = Paths.get("/essd");
    //    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    public static FileChannel WAL;

    static {
        try {
            WAL = FileChannel.open(
                    DIR_ESSD.resolve("wal"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                    , StandardOpenOption.DSYNC
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(topic + queueId, k -> new AtomicLong());
        long offset = offsetAdder.getAndIncrement();
        // 更新最大位点
        // 保存 data 中的数据
        try {
            // msg长度
            ByteBuffer lenBufWrite = ByteBuffer.allocate(Short.BYTES);
            lenBufWrite.putShort((short) data.limit());
            lenBufWrite.flip();
            // 落 wal
            WAL.write(lenBufWrite);
            lenBufWrite.rewind(); // 重复读
            WAL.write(data);
            data.rewind();
            // 落具体 queue
            Path queuePath = DIR_ESSD.resolve(topic);
            Files.createDirectories(queuePath);
            FileChannel dataChannel = FileChannel.open(
                    queuePath.resolve(queueId + ".d"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
            );
            long position = dataChannel.position();
            dataChannel.write(lenBufWrite);
            lenBufWrite.flip();
            dataChannel.write(data);
            dataChannel.close();
            // 索引
            if (offset % 128 == 0) {
                ByteBuffer indexBuf = ByteBuffer.allocate(16);
                indexBuf.putLong(offset);
                indexBuf.putLong(position);
                indexBuf.flip();
                FileChannel indexChannel = FileChannel.open(
                        queuePath.resolve(queueId + ".i"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                );
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
        Map<Integer, ByteBuffer> dataMap = new HashMap<>(fetchNum);
        Path indexPath = DIR_ESSD.resolve(topic).resolve(queueId + ".i");
        Path dataPath = DIR_ESSD.resolve(topic).resolve(queueId + ".d");
        try {
            long prevOffset = 0;
            long position = 0;
            if (Files.exists(indexPath)) {
                FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
                MappedByteBuffer indexMapBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
                // 找数据偏移量
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
            } else {
                System.out.println(indexPath + "不存在");
            }
            if (Files.exists(dataPath)) {
                FileChannel dataChannel = FileChannel.open(dataPath, StandardOpenOption.READ);
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
            } else {
                System.out.println(dataPath + "不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataMap;
    }
}
