package io.openmessaging.finkys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.openmessaging.leo.OffsetBuf;

public class BulletManager {

    public static final int GUN_AMOUNT = 2;
    public static final String DIR_PMEM = "/pmem";
    public static final Path DIR_ESSD = Paths.get("/essd");
    public static final Path LOGS_PATH = DIR_ESSD.resolve("log");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, BulletIndexer> INDEXERS = new ConcurrentHashMap<>(1000_000);
    public static Gun[] guns = new Gun[GUN_AMOUNT];
    public static LinkedBlockingQueue<Bullet> clip = new LinkedBlockingQueue<>();

    static {
        try {
            if (Files.notExists(LOGS_PATH)) {
                Files.createDirectories(LOGS_PATH);
                for (int i = 0; i < GUN_AMOUNT; i++) {
                    guns[i] = new Gun((byte) i);
//                    guns[i].start();
                }
            } else {
                // 重启
//                Map<Byte, Map<Short, PriorityQueue<OffsetBuf>>> topicQueueBufMap = new HashMap<>(100);
//                Files.list(LOGS_PATH).forEach(partitionDir -> {
//                    byte partitionId = Byte.parseByte(String.valueOf(partitionDir.getFileName()));
//                    try {
//                        Files.list(partitionDir).forEach(logFile -> {
//                            byte logNumAdder = Byte.parseByte(String.valueOf(logFile.getFileName()));
//                            try {
//                                FileChannel logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
//                                long fileSize = logFileChannel.size();
//                                MappedByteBuffer logBuf = logFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
//                                while (logBuf.hasRemaining()) {
//                                    ByteBuffer indexBuf = ByteBuffer.allocate(8);
//                                    int position = logBuf.position();
//                                    byte topic = logBuf.get();
//                                    short queueId = logBuf.getShort();
//                                    int offset = logBuf.getInt();
//                                    short msgLen = logBuf.getShort();
//                                    if (msgLen == 0) break;
//                                    for (int i = 0; i < msgLen; i++) {
//                                        logBuf.get();
//                                    }
//                                    short dataSize = (short) (9 + msgLen);
//                                    // index
//                                    indexBuf.put(partitionId);
//                                    indexBuf.put(logNumAdder);
//                                    indexBuf.putInt(position);
//                                    indexBuf.putShort(dataSize);
//                                    indexBuf.flip();
//                                    topicQueueBufMap.putIfAbsent(topic, new HashMap<>());
//                                    Map<Short, PriorityQueue<OffsetBuf>> queueMap = topicQueueBufMap.get(topic);
//                                    queueMap.putIfAbsent(queueId,
//                                            new PriorityQueue<>(Comparator.comparingInt(o -> o.offset))
//                                    );
//                                    PriorityQueue<OffsetBuf> bufList = queueMap.get(queueId);
//                                    bufList.add(new OffsetBuf(offset, indexBuf));
//                                }
//                                unmap(logBuf);
//                                logFileChannel.close();
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }
//                        });
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//                // 根据 offset 排序后统一插入
//                topicQueueBufMap.forEach((topic, queueMap) -> {
//                    queueMap.forEach((queueId, bufList) -> {
//                        BulletIndexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new BulletIndexer(topic, queueId));
//                        ByteBuffer buf;
//                        while (!bufList.isEmpty()) {
//                            buf = bufList.poll().buf;
//                            indexer.writeIndex(buf.get(), buf.get(), buf.getInt(), buf.getShort());
//                        }
//                    });
//                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ThreadLocal<Gun> GUN_TL = new ThreadLocal<>();
    public static AtomicInteger GUN_ID_ADDER = new AtomicInteger();

    public static long append(String topic, int queueId, ByteBuffer data) {
        int topicHash = topic.hashCode();
        String key = (topicHash + " + " + queueId).intern();
        long offset = getOffset(key);
//        Bullet bullet = new Bullet(topicHash, queueId, offset, data);
//        bullet.acquire();
        Gun gun = GUN_TL.get();
        if (gun == null) {
            int id = GUN_ID_ADDER.getAndIncrement();
            gun = guns[id % GUN_AMOUNT];
            GUN_TL.set(gun);
        }
        gun.append(topicHash,queueId,offset,data);
//        gun.clip.offer(bullet);
//        clip.offer(bullet);
//        bullet.acquire();
        return offset;
    }


    public static Map<Integer, ByteBuffer> readLog(int topic, int queueId, long offset, int fetchNum) {
        BulletIndexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new BulletIndexer(topic, queueId));
        try {
            return indexer.getRange(offset, fetchNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }


    private static long getOffset(String key) {
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicLong());
        return offsetAdder.getAndIncrement();
    }
}
