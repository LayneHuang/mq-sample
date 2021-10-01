package io.openmessaging.leo;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DataManager {

    public static final String DIR_PMEM = "/pmem";
    public static final Path DIR_ESSD = Paths.get("D:/essd");
    //    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");
    public static final Path INDEX_PATH = DIR_ESSD.resolve("index");

    public static final short INDEX_BUF_SIZE = 8;
    public static final short INDEX_TEMP_BUF_SIZE = INDEX_BUF_SIZE * 2048;
    public static AtomicInteger PARTITION_ID_ADDER = new AtomicInteger();
    public static ConcurrentHashMap<Integer, DataPartition> PARTITIONS = new ConcurrentHashMap<>(40);
    public static ThreadLocal<DataPartition> PARTITION_TL = new ThreadLocal<>();
    public static ConcurrentHashMap<String, MemoryIndexer> INDEXERS = new ConcurrentHashMap<>(1000_000);
//    public static ConcurrentHashMap<String, Indexer> INDEXERS = new ConcurrentHashMap<>(1000_000);

    // 索引位置落盘
//    public static final short INDEX_POS_SIZE = 5;
//    public static final Path INDEX_POS_PATH = DIR_ESSD.resolve("index");
//    public static FileChannel INDEX_POS_FILE;
//    public static MappedByteBuffer INDEXER_POS_BUF;

    static {
        try {
            if (Files.notExists(LOGS_PATH)) {
                Files.createDirectories(LOGS_PATH);
            } else {
                // 重启
                List<Path> logDirs = Files.list(LOGS_PATH).collect(Collectors.toList());
                Thread[] threads = new Thread[logDirs.size()];
                for (int i = 0; i < logDirs.size(); i++) {
                    Path logDir = logDirs.get(i);
                    threads[i] = new Thread(() -> {
                        try {
                            String partitionId = logDir.toFile().getName();
                            List<Path> logFiles = Files.list(logDir).collect(Collectors.toList());
                            for (Path logFile : logFiles) {
                                String logNumAdder = logDir.toFile().getName();
                                FileChannel logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                                long fileSize = logFileChannel.size();
                                MappedByteBuffer logBuf = logFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
                                while (logBuf.hasRemaining()) {
                                    int topic = logBuf.getInt();
                                    int queueId = logBuf.getInt();
                                    long offset = logBuf.getLong();
                                    short msgLen = logBuf.getShort();
                                    for (int i1 = 0; i1 < msgLen; i1++) {
                                        logBuf.get();
                                    }
                                    // index
                                    MemoryIndexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new MemoryIndexer(topic, queueId));
                                    indexer.writeIndex(Byte.parseByte(partitionId), Byte.parseByte(logNumAdder), logBuf.position(), msgLen);
                                }
                                unmap(logBuf);
                                logFileChannel.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
                for (Thread thread : threads) {
                    thread.start();
                }
                for (Thread thread : threads) {
                    thread.join();
                }
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
        MemoryIndexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new MemoryIndexer(topic, queueId));
        partition.writeLog(topic, queueId, offset, data, indexer);
    }

    public static Map<Integer, ByteBuffer> readLog(int topic, int queueId, long offset, int fetchNum) {
        MemoryIndexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new MemoryIndexer(topic, queueId));
        try {
            return indexer.getRange(offset, fetchNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
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
