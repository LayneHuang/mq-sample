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
//            public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");

    public static final short INDEX_BUF_SIZE = 8;
    public static final short INDEX_TEMP_BUF_SIZE = INDEX_BUF_SIZE * 2048;
    public static AtomicInteger PARTITION_ID_ADDER = new AtomicInteger();
    public static ConcurrentHashMap<Integer, DataPartition> PARTITIONS = new ConcurrentHashMap<>(40);
    public static ThreadLocal<DataPartition> PARTITION_TL = new ThreadLocal<>();
    public static ConcurrentHashMap<String, Indexer> INDEXERS = new ConcurrentHashMap<>(1000_000);

    // 索引位置落盘
    public static final short INDEX_POS_SIZE = 5;
    public static final Path INDEX_POS_PATH = DIR_ESSD.resolve("index");
    public static FileChannel INDEX_POS_FILE;
    public static MappedByteBuffer INDEXER_POS_BUF;

    static {
        try {
            if (Files.notExists(LOGS_PATH)) {
                Files.createDirectories(LOGS_PATH);
            }
            if (Files.notExists(INDEX_POS_PATH)) {
                Files.createFile(INDEX_POS_PATH);
                INDEX_POS_FILE = FileChannel.open(INDEX_POS_PATH, StandardOpenOption.READ, StandardOpenOption.WRITE);
                INDEXER_POS_BUF = INDEX_POS_FILE.map(FileChannel.MapMode.READ_WRITE, 0, 40 * INDEX_POS_SIZE);
            } else {
                // 重启
                INDEX_POS_FILE = FileChannel.open(INDEX_POS_PATH, StandardOpenOption.READ);
                INDEXER_POS_BUF = INDEX_POS_FILE.map(FileChannel.MapMode.READ_ONLY, 0, 40 * INDEX_POS_SIZE);
                byte partitionId = 0;
                while (INDEXER_POS_BUF.hasRemaining()) {
                    byte logNumAdder = INDEXER_POS_BUF.get();
                    int position = INDEXER_POS_BUF.getInt();
                    Path logFile = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNumAdder));
                    FileChannel logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                    long fileSize = logFileChannel.size();
                    if (fileSize > position) {
                        MappedByteBuffer logBuf = logFileChannel.map(FileChannel.MapMode.READ_ONLY, position, fileSize);
                        while (logBuf.hasRemaining()) {
                            ByteBuffer indexBuf = ByteBuffer.allocate(INDEX_BUF_SIZE);
                            int topic = logBuf.getInt();
                            int queueId = logBuf.getInt();
                            long offset = logBuf.getLong();
                            short msgLen = logBuf.getShort();
                            for (int i = 0; i < msgLen; i++) {
                                logBuf.get();
                            }
                            short dataSize = (short) (18 + msgLen);
                            // index
                            indexBuf.put(partitionId);
                            indexBuf.put(logNumAdder);
                            indexBuf.putInt(position);
                            indexBuf.putShort(dataSize);
                            indexBuf.flip();
                            Indexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new Indexer(topic, queueId));
                            indexer.writeIndex(indexBuf);
                        }
                        unmap(logBuf);
                    }
                    logFileChannel.close();
                    partitionId++;
                }
                unmap(INDEXER_POS_BUF);
                INDEX_POS_FILE.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeLog(int topic, int queueId, long offset, ByteBuffer data) {
        DataPartition partition = PARTITION_TL.get();
        if (partition == null) {
            int id = PARTITION_ID_ADDER.getAndIncrement();
            partition = new DataPartition((byte) id);
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
                    long end = Math.min(start + (long) fetchNum * INDEX_BUF_SIZE, fileSize);
                    long mappedSize = end - start;
                    MappedByteBuffer indexMappedBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, start, mappedSize);
                    while (indexMappedBuf.hasRemaining()) {
                        readLog(dataMap, key, indexMappedBuf);
                        key++;
                    }
                    unmap(indexMappedBuf);
                }
                indexChannel.close();
                if (key < fetchNum) {
                    Indexer indexer = INDEXERS.get(topic + "+" + queueId);
                    ByteBuffer tempBuf = indexer.getTempBuf();
                    if (start > fileSize) { // 需要跳过
                        int skipPos = (int) (start - fileSize);
                        if (skipPos < tempBuf.limit()) {
                            tempBuf.position(skipPos);
                            while (tempBuf.hasRemaining() && key < fetchNum) {
                                readLog(dataMap, key, tempBuf);
                                key++;
                            }
                        }
                    } else {
                        while (tempBuf.hasRemaining() && key < fetchNum) {
                            readLog(dataMap, key, tempBuf);
                            key++;
                        }
                    }
                }
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
