package io.openmessaging;

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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {

    public static final long SEGMENT_MAX = 536870912; // 512M
    //    public static final long SEGMENT_MAX = 128;
    public static final long INDEX_GAP = 128;
    public static final String DIR_PMEM = "/pmem";
    public static final Path DIR_ESSD = Paths.get("/essd");
    //    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir")).resolve("target");
    public static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, String> APPEND_PATH_MAP = new ConcurrentHashMap<>();
    Semaphore semaphore = new Semaphore(1000);

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        AtomicLong offsetAdder = APPEND_OFFSET_MAP.computeIfAbsent(topic + "+" + queueId, k -> new AtomicLong());
        // 更新最大位点
        long offset = offsetAdder.getAndIncrement();
        // 保存 data 中的数据
        try {
            // 落具体 queue
            Path queueDir = DIR_ESSD.resolve(topic).resolve(String.valueOf(queueId));
            Files.createDirectories(queueDir);
            String segmentName = APPEND_PATH_MAP.compute(topic + "+" + queueId, (k, old) -> {
                String segmentNew = String.valueOf(offset);
                if (old == null) {
                    return segmentNew;
                }
                Path oldPath = queueDir.resolve(old); // 直接用 offset 当文件名吧
                try {
                    if (Files.notExists(oldPath) || Files.size(oldPath) < SEGMENT_MAX) {
                        return old;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return segmentNew;
            });
            Path logPath = queueDir.resolve(segmentName);
            try {
                Files.createFile(logPath);
            } catch (IOException e) {
            }
            semaphore.acquire();
            FileChannel dataChannel = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long position = dataChannel.size();
            MappedByteBuffer logMapBuf = dataChannel.map(FileChannel.MapMode.READ_WRITE, position, Short.BYTES + data.limit());
            logMapBuf.putShort((short) data.limit()); // msg长度
            logMapBuf.put(data);
            logMapBuf.force();
            clean(logMapBuf);
            dataChannel.close();
            semaphore.release();
            // 索引
            if (offset % INDEX_GAP == 0) {
                Path indexPath = queueDir.resolve(segmentName + ".i");
                try {
                    Files.createFile(indexPath);
                } catch (IOException e) {
                }
                semaphore.acquire();
                FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                MappedByteBuffer indexMapBuf = indexChannel.map(FileChannel.MapMode.READ_WRITE, indexChannel.size(), 16);
                indexMapBuf.putLong(offset);
                indexMapBuf.putLong(position);
                indexMapBuf.force();
                clean(indexMapBuf);
                indexChannel.close();
                semaphore.release();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = null;
        Path queueDir = DIR_ESSD.resolve(topic).resolve(String.valueOf(queueId));
        if (Files.notExists(queueDir)) {
            return new HashMap<>();
        }
        try {
            // 找数据块
            Path indexPath;
            Path logPath;
            Optional<Long> max = Files.list(queueDir)
                    .map(path -> path.getFileName().toString())
                    .filter(fileName -> !fileName.endsWith(".i"))
                    .map(Long::valueOf)
                    .filter(aLong -> aLong <= offset)
                    .max((o1, o2) -> (int) (o1 - o2));
            String segmentName;
            if (max.isPresent()) {
                segmentName = max.get().toString();
            } else {
                return new HashMap<>();
            }
            logPath = queueDir.resolve(segmentName);
            indexPath = queueDir.resolve(segmentName);
            // 找数据偏移量
            long prevOffset = 0;
            long position = 0;
            if (Files.exists(indexPath)) {
                semaphore.acquire();
                FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
                MappedByteBuffer indexMapBuf = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
                while (indexMapBuf.hasRemaining()) {
                    long curOffset = indexMapBuf.getLong();
                    if (curOffset > offset) {
                        break;
                    }
                    prevOffset = curOffset;
                    position = indexMapBuf.getLong();
                }
                clean(indexMapBuf);
                indexChannel.close();
                semaphore.release();
            } else {
                System.out.println(indexPath + "不存在");
            }
            if (Files.exists(logPath)) {
                dataMap = new HashMap<>(fetchNum);
                semaphore.acquire();
                FileChannel dataChannel = FileChannel.open(logPath, StandardOpenOption.READ);
                dataChannel.position(position);
                int key = 0;
                ByteBuffer lenBufRead = ByteBuffer.allocate(Short.BYTES);
                while (true) {
                    if (dataChannel.read(lenBufRead) <= 0) {
                        // 当前块读完了
                        // todo
                        break;
                    }
                    lenBufRead.flip();
                    short length = lenBufRead.getShort();
                    lenBufRead.flip();
                    ByteBuffer msgBuf = ByteBuffer.allocate(length);
                    dataChannel.read(msgBuf);
                    msgBuf.flip();
                    if (prevOffset >= offset) {
                        dataMap.put(key, msgBuf);
                        key++;
                        if (key == fetchNum) {
                            break;
                        }
                    }
                    prevOffset++;
                }
                dataChannel.close();
                semaphore.release();
            } else {
                System.out.println(logPath + "不存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataMap;
    }

    private void clean(MappedByteBuffer indexMapBuf) {
        Cleaner cleaner = ((DirectBuffer)indexMapBuf).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }
}
