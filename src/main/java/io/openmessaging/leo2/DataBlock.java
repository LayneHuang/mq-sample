package io.openmessaging.leo2;

import io.openmessaging.leo.Indexer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static io.openmessaging.leo2.DataManager.*;

public class DataBlock {

    public byte id;
    public Path logDir;
    public byte logNumAdder = Byte.MIN_VALUE;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;

    public DataBlock(byte id) {
        this.id = id;
        logDir = LOGS_PATH.resolve(String.valueOf(this.id));
        try {
            Files.createDirectories(logDir);
            setupLog();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openLog() throws IOException {
        logNumAdder++;
        setupLog();
    }

    private void setupLog() throws IOException {
        Path logFile = logDir.resolve(String.valueOf(logNumAdder));
        Files.createFile(logFile);
        logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        logMappedBuf = logFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024);// 1G
    }

    public static final int barrierCount = THREAD_MAX / 2;
    public static final long G125 = 1024L * 1024 * 1024 * 125 - 10_000;
    private final Object WRITE_LOCKER = new Object();
    private CyclicBarrier barrier = new CyclicBarrier(barrierCount);
    private static final LongAdder appendAdder = new LongAdder();
    private static final LongAdder forceAdder = new LongAdder();
    private volatile int addSize = 0;

    public void writeLog(byte topic, short queueId, int offset, ByteBuffer data, Indexer indexer) {
        short msgLen = (short) data.limit();
        appendAdder.add(msgLen);
        short dataSize = (short) (MSG_META_SIZE + msgLen);
        try {
            MappedByteBuffer tempBuf;
            synchronized (WRITE_LOCKER) {
                if (logMappedBuf.remaining() < dataSize) {
                    logMappedBuf.force();
                    forceAdder.add(addSize);
                    addSize = 0;
                    unmap(logMappedBuf);
                    logFileChannel.close();
                    openLog();
                }
                tempBuf = logMappedBuf;
                int position = logMappedBuf.position();
                logMappedBuf.put(topic); // 1
                logMappedBuf.putShort(queueId); // 2
                logMappedBuf.putInt(offset); // 4
                logMappedBuf.putShort(msgLen); // 2
                logMappedBuf.put(data);
                addSize += msgLen;
                indexer.writeIndex(id, logNumAdder, position, dataSize);
            }
            try {
                int arrive = barrier.await(250, TimeUnit.MILLISECONDS);
                if (arrive == 0) {
                    System.out.println("Full-F");
                    synchronized (WRITE_LOCKER) {
                        try {
                            logMappedBuf.force();
                            forceAdder.add(addSize);
                            addSize = 0;
                        } catch (Exception ignored) {
                        }
                    }
                }
            } catch (TimeoutException e) {
                // 只有一个超时，其他都是 BrokenBarrierException
                System.out.println("Timeout-F");
                barrier = new CyclicBarrier(barrierCount);
                synchronized (WRITE_LOCKER) {
                    try {
                        tempBuf.force();
                        forceAdder.add(addSize);
                        addSize = 0;
                    } catch (Exception ignored) {
                    }
                }
            } catch (BrokenBarrierException e) {
                System.out.println("Broken");
            } catch (InterruptedException e) {
                System.out.println("Interrupted");
            }
            if (G125 < appendAdder.sum()) {
                System.out.println("append " + appendAdder.sum() + "force" + forceAdder.sum());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
