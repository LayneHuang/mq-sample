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
    public static final Path DIR_ESSD = Paths.get("/essd");
    //    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");

    public static final short INDEX_BUF_SIZE = 8;
    public static final short INDEX_TEMP_BUF_SIZE = INDEX_BUF_SIZE * 2048;
    public static AtomicInteger PARTITION_ID_ADDER = new AtomicInteger();
    public static ConcurrentHashMap<Integer, DataPartition> PARTITIONS = new ConcurrentHashMap<>(40);
    public static ThreadLocal<DataPartition> PARTITION_TL = new ThreadLocal<>();
    public static ConcurrentHashMap<String, Indexer> INDEXERS = new ConcurrentHashMap<>(1000_000);

    static {
        try {
            if (Files.notExists(LOGS_PATH)) {
                Files.createDirectories(LOGS_PATH);
            } else {
                // 重启
//                Files.list(LOGS_PATH).forEach(path -> {
//                    40 * INDEX_TEMP_BUF_SIZE
//                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeLog(int topic, int queueId, long offset, ByteBuffer data) {
        DataPartition partition = PARTITION_TL.get();
        if (partition == null) {
            partition = new DataPartition();
            int id = PARTITION_ID_ADDER.getAndIncrement();
            partition.init((byte) id);
            try {
                partition.openLog();
            } catch (IOException e) {
                e.printStackTrace();
            }
            PARTITIONS.put(id, partition);
            PARTITION_TL.set(partition);
        }
        Indexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new Indexer(topic, queueId));
        partition.writeLog(topic, queueId, offset, data, indexer);
    }

    public static Map<Integer, ByteBuffer> readLog(int topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = null;
        Path indexPath = DIR_ESSD.resolve(String.valueOf(topic)).resolve(String.valueOf(queueId));
        try {
            if (Files.exists(indexPath)) {
                FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
                long fileSize = indexChannel.size();
                long start = offset * INDEX_BUF_SIZE;
                dataMap = new HashMap<>(fetchNum);
                int key = 0;
                if (start < fileSize) {
                    long end = Math.min(start + fetchNum * INDEX_BUF_SIZE, fileSize);
                    long mappedSize = end - start;
                    MappedByteBuffer indexMappedBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, start, mappedSize);
                    while (indexMappedBuf.hasRemaining()) {
                        readLog(dataMap, key, indexMappedBuf);
                        key++;
                    }
                    unmap(indexMappedBuf);
                    indexChannel.close();
                }
                if (key < fetchNum) {
                    Indexer indexer = INDEXERS.get(topic + "+" + queueId);
                    ByteBuffer tempBuf = indexer.getTempBuf();
                    if (tempBuf.position() != 0) {
                        while (tempBuf.hasRemaining() && key < fetchNum) {
                            readLog(dataMap, key, tempBuf);
                            key++;
                        }
                    }
                }
            } else {
                System.out.println(indexPath + "不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataMap;
    }

    private static void readLog(Map<Integer, ByteBuffer> dataMap, int key, ByteBuffer indexBuf) throws IOException {
        byte partitionId = indexBuf.get();
        byte logNum = indexBuf.get();
        int position = indexBuf.getInt();
        short dataSize = indexBuf.getShort();
        Path logFile = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNum));
        if (Files.exists(logFile)) {
            FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ);
            long logSize = logChannel.size();
//      MappedByteBuffer msgMappedBuf = logChannel.map(FileChannel.MapMode.READ_ONLY, position, msgLen);
            ByteBuffer dataBuf = ByteBuffer.allocate(dataSize);
            logChannel.read(dataBuf, position);
            logChannel.close();
            dataBuf.flip();
            try {
                int topic = dataBuf.getInt();
                int queueId = dataBuf.getInt();
                long offset = dataBuf.getLong();
                short msgLen = dataBuf.getShort();
                ByteBuffer msgBuf = ByteBuffer.allocate(msgLen);
                msgBuf.put(dataBuf);
                msgBuf.flip();
                dataMap.put(key, msgBuf);
            } catch (Exception e) {
                System.out.println("readLog : " + partitionId + ", " + logNum + ", " + position + ", " + dataSize + ", logSize: " + logSize);
            }
        } else {
            System.out.println("readLog : " + partitionId + ", " + logNum + " 不存在");
        }
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
