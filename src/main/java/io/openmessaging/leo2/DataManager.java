package io.openmessaging.leo2;

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
import java.util.concurrent.atomic.AtomicLong;

public class DataManager {

    public static final String DIR_PMEM = "/pmem";
    public static final Path DIR_ESSD = Paths.get("/essd");
    //    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");

    public static final short MSG_META_SIZE = 9;
    public static final short INDEX_BUF_SIZE = 8;
    public static final short INDEX_TEMP_BUF_NUM = 2048;
    public static final short INDEX_TEMP_BUF_SIZE = INDEX_BUF_SIZE * INDEX_TEMP_BUF_NUM;
    public static ConcurrentHashMap<String, Indexer> INDEXERS = new ConcurrentHashMap<>(1000_000);

    static {
        try {
            if (Files.notExists(LOGS_PATH)) {
                Files.createDirectories(LOGS_PATH);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DataBlock dataBlock = new DataBlock();

    public static void writeLog(byte topic, short queueId, int offset, ByteBuffer data) {
        dataBlock.writeLog(topic, queueId, offset, data);
    }

    private static Indexer getIndexer(byte topic, short queueId) {
        return INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new Indexer(topic, queueId));
    }

    public static Map<Integer, ByteBuffer> readLog(byte topic, short queueId, int offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = null;
        try {
            long start = (long) offset * INDEX_BUF_SIZE;
            dataMap = new HashMap<>(fetchNum);
            int key = 0;
            Indexer indexer = getIndexer(topic, queueId);
            for (int i = 0; i < indexer.fullBufs.size(); i++) {
                if (start >= INDEX_TEMP_BUF_SIZE) {
                    start -= INDEX_TEMP_BUF_SIZE;
                    continue;
                }
                ByteBuffer fullBuf = indexer.fullBufs.get(i);
                if (start > 0) {
                    fullBuf.position((int) start);
                }
                while (fullBuf.hasRemaining() && key < fetchNum) {
                    readLog(dataMap, key, fullBuf);
                    key++;
                }
                fullBuf.rewind();
                if (key == fetchNum) break;
            }
            if (key < fetchNum) {
                ByteBuffer tempBuf = indexer.getTempBuf();
                if (start > 0) { // 需要跳过
                    if (start < tempBuf.limit()) {
                        tempBuf.position((int) start);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataMap;
    }

    private static void readLog(Map<Integer, ByteBuffer> dataMap, int key, ByteBuffer indexBuf) throws IOException {
        byte partitionId = indexBuf.get();
        byte logNum = indexBuf.get();
        int position = indexBuf.getInt() + MSG_META_SIZE;
        short dataSize = (short) (indexBuf.getShort() - MSG_META_SIZE);
        Path logFile = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNum));
        FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ);
        try {
            ByteBuffer msgBuf = ByteBuffer.allocate(dataSize);
            logChannel.read(msgBuf, position);
            logChannel.close();
            msgBuf.flip();
            dataMap.put(key, msgBuf);
        } catch (Exception e) {
            System.out.println("readLog : " + partitionId + ", " + logNum + ", " + position + ", " + dataSize);
        }

    }

    public static void unmap(MappedByteBuffer indexMapBuf) throws IOException {
        Cleaner cleaner = ((DirectBuffer) indexMapBuf).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }

    public static long getOffset(byte topicId, short queueId) {
        String key = (topicId + "+" + queueId).intern();
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicLong());
        return offsetAdder.getAndIncrement();
    }

}