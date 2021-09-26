package io.openmessaging.leo;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class DataManager {

    public static final String DIR_PMEM = "/pmem";
    //    public static final Path DIR_ESSD = Paths.get("/essd");
    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");

    public static final short INDEX_BUF_SIZE = 10;
    public static AtomicInteger PARTITION_ID_ADDER = new AtomicInteger();
    public static ConcurrentHashMap<Integer, DataPartition> PARTITIONS = new ConcurrentHashMap<>(40);
    public static ThreadLocal<DataPartition> PARTITION_TL = new ThreadLocal<>();

    static {
        try {
            Files.createDirectories(LOGS_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeLog(String topic, int queueId, ByteBuffer data) {
        DataPartition partition = PARTITION_TL.get();
        if (partition == null) {
            partition = new DataPartition();
            int id = PARTITION_ID_ADDER.getAndIncrement();
            partition.init((short) id);
            try {
                partition.openLog();
            } catch (IOException e) {
                e.printStackTrace();
            }
            PARTITIONS.put(id, partition);
            PARTITION_TL.set(partition);
        }
        partition.writeLog(topic, queueId, data);
    }

    public static Map<Integer, ByteBuffer> readLog(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = null;
        Path indexPath = DIR_ESSD.resolve(topic).resolve(String.valueOf(queueId));
        try {
            if (Files.exists(indexPath)) {
                FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
                long fileSize = indexChannel.size();
                long start = offset * INDEX_BUF_SIZE;
                if (start < fileSize) {
                    dataMap = new HashMap<>(fetchNum);
                    int key = 0;
                    long end = Math.min(start + fetchNum * INDEX_BUF_SIZE, fileSize);
                    long mappedSize = end - start;
                    MappedByteBuffer indexMappedBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, start, mappedSize);
                    short partitionId, logNum, msgLen;
                    long position;
                    Path logPath;
                    FileChannel logChannel;
                    while (indexMappedBuf.hasRemaining()) {
                        partitionId = indexMappedBuf.getShort();
                        logNum = indexMappedBuf.getShort();
                        position = indexMappedBuf.getInt();
                        msgLen = indexMappedBuf.getShort();
                        logPath = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNum));
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
        return dataMap;
    }

    public static void unmap(MappedByteBuffer indexMapBuf) throws IOException {
        Cleaner cleaner = ((DirectBuffer) indexMapBuf).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }

    public static long getOffset(String key) {
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicLong());
        return offsetAdder.getAndIncrement();
    }

}
