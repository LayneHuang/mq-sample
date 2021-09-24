package io.openmessaging;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");
    public static AtomicInteger LOG_ADDER = new AtomicInteger();
    public static FileChannel LOG_FILE_CHANNEL;
    public static MappedByteBuffer LOG_MAPPED_BUF;

    static {
        try {
            Files.createDirectories(LOGS_PATH);
            openLog();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static ByteBuffer writeLog(ByteBuffer data) throws IOException {
        if (LOG_MAPPED_BUF.remaining() < data.limit()) {
            unmap(LOG_MAPPED_BUF);
            LOG_FILE_CHANNEL.close();
            openLog();
        }
        int position = LOG_MAPPED_BUF.position();
        LOG_MAPPED_BUF.put(data);
        LOG_MAPPED_BUF.force();
        ByteBuffer indexBuf = ByteBuffer.allocate(8);
        indexBuf.putShort((short) LOG_ADDER.get());
        indexBuf.putInt(position);
        indexBuf.putShort((short) data.limit());
        indexBuf.flip();
        return indexBuf;
    }

    public static void openLog() throws IOException {
        int logNum = LOG_ADDER.getAndIncrement();
        Path logPath = LOGS_PATH.resolve(String.valueOf(logNum));
        Files.createFile(logPath);
        LOG_FILE_CHANNEL = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        LOG_MAPPED_BUF = LOG_FILE_CHANNEL.map(MapMode.READ_WRITE, 0, 1_073_741_824);// 1G
    }

    public static void unmap(MappedByteBuffer indexMapBuf) throws IOException {
        Cleaner cleaner = ((DirectBuffer) indexMapBuf).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        String key = (topic + " + " + queueId).intern();
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicLong());
        long offset = offsetAdder.getAndIncrement();
        // 更新最大位点
        // 保存 data 中的数据
        synchronized (key) {
            try {
                // 落 wal
                ByteBuffer indexBuf = writeLog(data);
                // 索引
                Path topicPath = DIR_ESSD.resolve(topic);
                try {
                    Files.createDirectories(topicPath);
                } catch (IOException e) {
                }
                FileChannel indexChannel = FileChannel.open(
                        topicPath.resolve(String.valueOf(queueId)),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
//                            , StandardOpenOption.DSYNC
                );
//                indexChannel.force(true);
                indexChannel.write(indexBuf);
                indexChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = null;
        Path indexPath = DIR_ESSD.resolve(topic).resolve(queueId + ".i");
        try {
            if (Files.exists(indexPath)) {
                FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
                long fileSize = indexChannel.size();
                long start = offset * 6;
                if (start < fileSize) {
                    dataMap = new HashMap<>(fetchNum);
                    int key = 0;
                    long end = Math.min(start + fetchNum * 6L, fileSize);
                    long mappedSize = end - start;
                    MappedByteBuffer indexMappedBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, start, mappedSize);
                    short logNum, msgLen;
                    long position;
                    Path logPath;
                    FileChannel logChannel;
                    while (indexMappedBuf.hasRemaining()) {
                        logNum = indexMappedBuf.getShort();
                        position = indexMappedBuf.getInt();
                        msgLen = indexMappedBuf.getShort();
                        logPath = LOGS_PATH.resolve(String.valueOf(logNum));
                        logChannel = FileChannel.open(logPath, StandardOpenOption.READ);
//                        MappedByteBuffer msgMappedBuf = logChannel.map(FileChannel.MapMode.READ_ONLY, position, msgLen);
                        ByteBuffer msgBuf = ByteBuffer.allocate(msgLen);
                        logChannel.read(msgBuf, position);
                        logChannel.close();
                        msgBuf.flip();
                        dataMap.put(key, msgBuf);
                        key++;
                    }
                    unmap(indexMappedBuf);
                    indexChannel.close();
                }
            } else {
                System.out.println(indexPath + "不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (dataMap != null) {
            return dataMap;
        } else {
            return Collections.emptyMap();
        }
    }
}
