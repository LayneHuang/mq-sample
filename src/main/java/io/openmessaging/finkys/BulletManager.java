package io.openmessaging.finkys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BulletManager {

    public static final int GUN_AMOUNT = 4;
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
//                List<Path> logDirs = Files.list(LOGS_PATH).collect(Collectors.toList());
//                Thread[] threads = new Thread[logDirs.size()];
//                for (int i = 0; i < logDirs.size(); i++) {
//                    Path logDir = logDirs.get(i);
//                    threads[i] = new Thread(() -> {
//                        try {
//                            String partitionId = logDir.toFile().getName();
//                            List<Path> logFiles = Files.list(logDir).collect(Collectors.toList());
//                            for (Path logFile : logFiles) {
//                                String logNumAdder = logDir.toFile().getName();
//                                FileChannel logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
//                                long fileSize = logFileChannel.size();
//                                MappedByteBuffer logBuf = logFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
//                                while (logBuf.hasRemaining()) {
//                                    int topic = logBuf.getInt();
//                                    int queueId = logBuf.getInt();
//                                    long offset = logBuf.getLong();
//                                    short msgLen = logBuf.getShort();
//                                    for (int i1 = 0; i1 < msgLen; i1++) {
//                                        logBuf.get();
//                                    }
//                                    // index
//                                    MemoryIndexer indexer = INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new MemoryIndexer(topic, queueId));
//                                    indexer.writeIndex(Byte.parseByte(partitionId), Byte.parseByte(logNumAdder), logBuf.position(), msgLen);
//                                }
//                                unmap(logBuf);
//                                logFileChannel.close();
//                            }
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    });
//                }
//                for (Thread thread : threads) {
//                    thread.start();
//                }
//                for (Thread thread : threads) {
//                    thread.join();
//                }
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
