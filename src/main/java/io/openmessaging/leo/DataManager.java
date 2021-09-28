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
    //        public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
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
            int id = PARTITION_ID_ADDER.getAndIncrement();
            System.out.println("PARTITION " + id);
            partition = new DataPartition((byte) id);
            PARTITIONS.put(id, partition);
            PARTITION_TL.set(partition);
        }
        Indexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new Indexer(topic, queueId));
        partition.writeLog(topic, queueId, offset, data, indexer);
    }

    static int topic22hash = "topic22".hashCode();

    public static Map<Integer, ByteBuffer> readLog(int topic, int queueId, long offset, int fetchNum) {
        boolean DEBUG = topic == topic22hash && queueId == 937 && offset == 24 && fetchNum == 4;
        Map<Integer, ByteBuffer> dataMap = null;
        Path indexPath = DIR_ESSD.resolve(String.valueOf(topic)).resolve(String.valueOf(queueId));
        try {
            if (Files.exists(indexPath)) {
                if (DEBUG) System.out.println("exists " + indexPath.toAbsolutePath());
                FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
                long fileSize = indexChannel.size();
                if (DEBUG) System.out.println("fileSize " + fileSize);
                long start = offset * INDEX_BUF_SIZE;
                if (DEBUG) System.out.println("start " + start);
                dataMap = new HashMap<>(fetchNum);
                int key = 0;
                if (start < fileSize) {
                    long end = Math.min(start + (long) fetchNum * INDEX_BUF_SIZE, fileSize);
                    if (DEBUG) System.out.println("end " + end);
                    long mappedSize = end - start;
                    if (DEBUG) System.out.println("mappedSize " + mappedSize);
                    MappedByteBuffer indexMappedBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, start, mappedSize);
                    while (indexMappedBuf.hasRemaining()) {
                        readLog(dataMap, key, indexMappedBuf, DEBUG);
                        key++;
                    }
                    unmap(indexMappedBuf);
                    indexChannel.close();
                }
                if (DEBUG) System.out.println("key1 " + key);
                if (key < fetchNum) {
                    Indexer indexer = INDEXERS.get(topic + "+" + queueId);
                    ByteBuffer tempBuf = indexer.getTempBuf();
                    if (DEBUG) System.out.println("tempBuf.limit() " + tempBuf.limit());
                    if (DEBUG) System.out.println("tempBuf.position() " + tempBuf.position());
                    while (tempBuf.hasRemaining() && key < fetchNum) {
                        readLog(dataMap, key, tempBuf, DEBUG);
                        key++;
                    }
                }
                if (DEBUG) System.out.println("dataMap.size() " + dataMap.size());
            } else {
                System.out.println(indexPath + "不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataMap;
    }

    private static void readLog(Map<Integer, ByteBuffer> dataMap, int key, ByteBuffer indexBuf, boolean DEBUG) throws IOException {
        byte partitionId = indexBuf.get();
        if (DEBUG) System.out.println("partitionId " + partitionId);
        byte logNum = indexBuf.get();
        if (DEBUG) System.out.println("logNum " + logNum);
        int position = indexBuf.getInt();
        if (DEBUG) System.out.println("position " + position);
        short dataSize = indexBuf.getShort();
        if (DEBUG) System.out.println("dataSize " + dataSize);
        Path logFile = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNum));
        if (DEBUG) System.out.println("logFile " + logFile.toAbsolutePath());
        FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ);
        long logSize = logChannel.size();
        if (DEBUG) System.out.println("logSize " + logSize);
//      MappedByteBuffer msgMappedBuf = logChannel.map(FileChannel.MapMode.READ_ONLY, position, msgLen);
        ByteBuffer dataBuf = ByteBuffer.allocate(dataSize);
        logChannel.read(dataBuf, position);
        logChannel.close();
        dataBuf.flip();
        try {
            int topic = dataBuf.getInt();
            if (DEBUG) System.out.println("topic " + topic);
            int queueId = dataBuf.getInt();
            if (DEBUG) System.out.println("queueId " + queueId);
            long offset = dataBuf.getLong();
            if (DEBUG) System.out.println("offset " + offset);
            short msgLen = dataBuf.getShort();
            if (DEBUG) System.out.println("msgLen " + msgLen);
            ByteBuffer msgBuf = ByteBuffer.allocate(msgLen);
            msgBuf.put(dataBuf);
            msgBuf.flip();
            dataMap.put(key, msgBuf);
        } catch (Exception e) {
            System.out.println("readLog : " + partitionId + ", " + logNum + ", " + position + ", " + dataSize + ", logSize: " + logSize);
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
