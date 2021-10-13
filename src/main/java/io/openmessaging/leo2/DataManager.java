package io.openmessaging.leo2;

import io.openmessaging.leo.Indexer;
import io.openmessaging.leo.OffsetBuf;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DataManager {

    public static final Path DIR_ESSD = Paths.get("/essd");
    //    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");

    public static final short THREAD_MAX = 40;
    public static final short MSG_META_SIZE = 9;
    public static final short INDEX_BUF_SIZE = 8;
    public static final short INDEX_TEMP_BUF_NUM = 2048;
    public static final short INDEX_TEMP_BUF_SIZE = INDEX_BUF_SIZE * INDEX_TEMP_BUF_NUM;
    public static ConcurrentHashMap<String, Indexer> INDEXERS = new ConcurrentHashMap<>(1000_000);

    public static ThreadLocal<DataBlock> BLOCK_TL = new ThreadLocal<>();
    public static AtomicInteger BLOCK_ID_ADDER = new AtomicInteger();
    public static ConcurrentHashMap<Integer, DataBlock> BLOCKS = new ConcurrentHashMap<>(THREAD_MAX);

    static {
        try {
            if (Files.notExists(LOGS_PATH)) {
                Files.createDirectories(LOGS_PATH);
            } else {
                restartLogic();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void restartLogic() throws IOException {
        Map<Byte, Map<Short, PriorityQueue<OffsetBuf>>> topicQueueBufMap = new HashMap<>(100);
        Files.list(LOGS_PATH).forEach(partitionDir -> {
            byte partitionId = Byte.parseByte(String.valueOf(partitionDir.getFileName()));
            try {
                Files.list(partitionDir).forEach(logFile -> {
                    byte logNumAdder = Byte.parseByte(String.valueOf(logFile.getFileName()));
                    try {
                        FileChannel logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                        long fileSize = logFileChannel.size();
                        MappedByteBuffer logBuf = logFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
                        while (logBuf.remaining() > MSG_META_SIZE) {
                            ByteBuffer indexBuf = ByteBuffer.allocate(INDEX_BUF_SIZE);
                            int position = logBuf.position();
                            byte topic = logBuf.get();
                            short queueId = logBuf.getShort();
                            int offset = logBuf.getInt();
                            short msgLen = logBuf.getShort();
                            if (msgLen == 0) break;
                            for (int i = 0; i < msgLen; i++) {
                                logBuf.get();
                            }
                            short dataSize = (short) (MSG_META_SIZE + msgLen);
                            // index
                            indexBuf.put(partitionId);
                            indexBuf.put(logNumAdder);
                            indexBuf.putInt(position);
                            indexBuf.putShort(dataSize);
                            indexBuf.flip();
                            topicQueueBufMap.putIfAbsent(topic, new HashMap<>());
                            Map<Short, PriorityQueue<OffsetBuf>> queueMap = topicQueueBufMap.get(topic);
                            queueMap.putIfAbsent(queueId,
                                    new PriorityQueue<>(Comparator.comparingInt(o -> o.offset))
                            );
                            PriorityQueue<OffsetBuf> bufList = queueMap.get(queueId);
                            bufList.add(new OffsetBuf(offset, indexBuf));
                        }
                        unmap(logBuf);
                        logFileChannel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println("读完了");
        // 根据 offset 排序后统一插入
        Iterator<Map.Entry<Byte, Map<Short, PriorityQueue<OffsetBuf>>>> iterator = topicQueueBufMap.entrySet().iterator();
        Map.Entry<Byte, Map<Short, PriorityQueue<OffsetBuf>>> topicMapEntry;
        while (iterator.hasNext()) {
            topicMapEntry = iterator.next();
            iterator.remove();
            byte topic = topicMapEntry.getKey();
            Map<Short, PriorityQueue<OffsetBuf>> queueMap = topicMapEntry.getValue();
            Iterator<Map.Entry<Short, PriorityQueue<OffsetBuf>>> iterator1 = queueMap.entrySet().iterator();
            Map.Entry<Short, PriorityQueue<OffsetBuf>> queueMapEntry;
            while (iterator1.hasNext()) {
                queueMapEntry = iterator1.next();
                iterator1.remove();
                short queueId = queueMapEntry.getKey();
                PriorityQueue<OffsetBuf> bufList = queueMapEntry.getValue();
                Indexer indexer = getIndexer(topic, queueId);
                ByteBuffer buf;
                while (!bufList.isEmpty()) {
                    buf = bufList.poll().buf;
                    indexer.writeIndex(buf.get(), buf.get(), buf.getInt(), buf.getShort());
                }
            }
        }
//        topicQueueBufMap.forEach((topic, queueMap) -> {
//            queueMap.forEach((queueId, bufList) -> {
//                Indexer indexer = getIndexer(topic, queueId);
//                ByteBuffer buf;
//                while (!bufList.isEmpty()) {
//                    buf = bufList.poll().buf;
//                    indexer.writeIndex(buf.get(), buf.get(), buf.getInt(), buf.getShort());
//                }
//            });
//        });
    }

    // block 2 64k 75G cost: 330965

    public static void writeLog(byte topic, short queueId, int offset, ByteBuffer data) {
        DataBlock dataBlock = BLOCK_TL.get();
        if (dataBlock == null) {
            int id = BLOCK_ID_ADDER.getAndIncrement() % 2;
            dataBlock = BLOCKS.computeIfAbsent(id, key -> new DataBlock(key.byteValue()));
            BLOCK_TL.set(dataBlock);
        }
        Indexer indexer = getIndexer(topic, queueId);
        dataBlock.writeLog(topic, queueId, offset, data, indexer);
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
