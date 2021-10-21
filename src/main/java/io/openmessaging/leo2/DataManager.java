package io.openmessaging.leo2;

import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.leo.Indexer;
import io.openmessaging.leo.OffsetBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.leo2.Utils.unmap;

public class DataManager {

    public static final Path DIR_ESSD = Paths.get("/essd");
    //    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target").resolve("work");
    public static final ConcurrentHashMap<String, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");

    public static final int G1 = 1024 * 1024 * 1024;
    public static final short THREAD_MAX = 40;
    public static final short MSG_META_SIZE = 9;
    public static final short INDEX_BUF_SIZE = 8;
    public static final ConcurrentHashMap<String, Indexer> INDEXERS = new ConcurrentHashMap<>(1000_000);

    public static final ThreadLocal<DataBlock2> BLOCK_TL = new ThreadLocal<>();
    public static final AtomicInteger BLOCK_ID_ADDER = new AtomicInteger();
    public static final ConcurrentHashMap<Byte, DataBlock2> BLOCKS = new ConcurrentHashMap<>(THREAD_MAX);

    public DataManager() {
        try {
            if (Files.notExists(LOGS_PATH)) {
                Files.createDirectories(LOGS_PATH);
            } else {
                // 重启
                Files.list(LOGS_PATH).forEach(partitionDir -> {
                    byte partitionId = Byte.parseByte(String.valueOf(partitionDir.getFileName()));
                    try {
                        Files.list(partitionDir).forEach(logFile -> {
                            byte logNum = Byte.parseByte(String.valueOf(logFile.getFileName()));
                            try {
                                FileChannel logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                                long fileSize = logFileChannel.size();
                                MappedByteBuffer logBuf = logFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
                                while (logBuf.remaining() > MSG_META_SIZE) {
                                    int position = logBuf.position();
                                    byte topic = logBuf.get();
                                    short queueId = logBuf.getShort();
                                    int offset = logBuf.getInt();
                                    short msgLen = logBuf.getShort();
                                    if (msgLen == 0) break;
                                    logBuf.position(logBuf.position() + msgLen);
                                    short dataSize = (short) (MSG_META_SIZE + msgLen);
                                    // index
                                    ByteBuffer indexBuf = ByteBuffer.allocate(INDEX_BUF_SIZE);
                                    indexBuf.put(partitionId);
                                    indexBuf.put(logNum);
                                    indexBuf.putInt(position);
                                    indexBuf.putShort(dataSize);
                                    indexBuf.flip();
                                    Indexer indexer = getIndexer(topic, queueId);
                                    indexer.writeIndex(new OffsetBuf(offset, indexBuf));
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
                // 根据 offset 排序
                INDEXERS.values().forEach(indexer -> indexer.fullBufs.sort(Comparator.comparingInt(o -> o.offset)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeLog(byte topic, short queueId, int offset, ByteBuffer data) {
        DataBlock2 dataBlock = BLOCK_TL.get();
        if (dataBlock == null) {
            byte id = (byte) (BLOCK_ID_ADDER.getAndIncrement() % 2);
            dataBlock = BLOCKS.computeIfAbsent(id, DataBlock2::new);
            BLOCK_TL.set(dataBlock);
        }
        Indexer indexer = getIndexer(topic, queueId);
        dataBlock.writeLog(topic, queueId, offset, data, indexer);
    }

    private static Indexer getIndexer(byte topic, short queueId) {
        return INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new Indexer(topic, queueId));
    }

    public Map<Integer, ByteBuffer> readLog(byte topic, short queueId, int offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = null;
        try {
            dataMap = new HashMap<>(fetchNum);
            int key = 0;
            Indexer indexer = getIndexer(topic, queueId);
            int maxCount = Math.min(indexer.fullBufs.size(), offset + fetchNum);
            while (offset < maxCount) {
                OffsetBuf fullBuf = indexer.fullBufs.get(offset);
                readLog(dataMap, key, fullBuf.buf);
                key++;
                offset++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataMap;
    }

    private void readLog(Map<Integer, ByteBuffer> dataMap, int key, ByteBuffer indexBuf) throws IOException {
        byte partitionId = indexBuf.get();
        byte logNum = indexBuf.get();
        int position = indexBuf.getInt() + MSG_META_SIZE;
        short dataSize = (short) (indexBuf.getShort() - MSG_META_SIZE);
        indexBuf.rewind();
        ByteBuffer msgBuf = readCache(partitionId, logNum, position, dataSize);
        if (msgBuf == null) {
            System.out.println("未命中");
            Path logFile = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNum));
            FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ);
            try {
                msgBuf = ByteBuffer.allocate(dataSize);
                logChannel.read(msgBuf, position);
                logChannel.close();
                msgBuf.flip();
            } catch (Exception e) {
                System.out.println("readLog : " + partitionId + ", " + logNum + ", " + position + ", " + dataSize);
            }
        } else {
            System.out.println("命中");
        }
        dataMap.put(key, msgBuf);
    }

    private ByteBuffer readCache(byte partitionId, byte logNum, int position, short dataSize) {
        DataBlock2 dataBlock2 = BLOCKS.get(partitionId);
        if (dataBlock2 == null) return null;
        MemoryBlock mb = dataBlock2.cache.getMb(logNum);
        if (mb == null) return null;
        ByteBuffer msgBuf = ByteBuffer.allocate(dataSize);
        for (int i = 0; i < dataSize; i++) {
            msgBuf.put(mb.getByte(position + i));
        }
        msgBuf.flip();
        return msgBuf;
    }

    public static int getOffset(byte topicId, short queueId) {
        String key = (topicId + "+" + queueId).intern();
        AtomicInteger offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicInteger());
        return offsetAdder.getAndIncrement();
    }

}
