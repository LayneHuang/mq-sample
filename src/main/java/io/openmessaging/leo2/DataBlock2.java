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

import static io.openmessaging.leo2.DataManager.*;
import static io.openmessaging.leo2.Utils.unmap;

public class DataBlock2 {

    public byte id;
    public Path logDir;
    public byte logNumAdder = Byte.MIN_VALUE;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;

    public DataBlock2(byte id) {
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
        logMappedBuf = logFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, G1);// 1G
    }

    private final Object WRITE_LOCKER = new Object();
    private volatile int barrierCount = THREAD_MAX / 2;
    private volatile CyclicBarrier barrier = new CyclicBarrier(barrierCount);
    private volatile int noFuck = 0;
    private volatile int fuck = 0;
    private volatile int timeoutTimes = 0;
    private volatile int fullTimes = 0;

    public void writeLog(byte topic, short queueId, int offset, ByteBuffer data, Indexer indexer) {
        short msgLen = (short) data.limit();
        short dataSize = (short) (MSG_META_SIZE + msgLen);
        try {
            MappedByteBuffer tempBuf;
            CyclicBarrier tempBarrier = barrier;
            synchronized (WRITE_LOCKER) {
                if (logMappedBuf.remaining() < dataSize) {
                    logMappedBuf.force();
                    unmap(logMappedBuf);
                    logFileChannel.close();
                    openLog();
                }
                tempBuf = logMappedBuf;
                int position = logMappedBuf.position();
                tempBuf.put(topic); // 1
                tempBuf.putShort(queueId); // 2
                tempBuf.putInt(offset); // 4
                tempBuf.putShort(msgLen); // 2
                tempBuf.put(data);
                indexer.writeIndex(id, logNumAdder, position, dataSize);
            }
            try {
                int arrive = 0;
                if (barrierCount > 1) {
                    arrive = tempBarrier.await(10L * barrierCount, TimeUnit.MILLISECONDS);
                }
                if (arrive == 0) {
                    fullTimes++;
                    noFuck++;
                    okWrite(tempBuf);
                }
            } catch (TimeoutException e) {
                // 只有一个超时，其他都是 BrokenBarrierException
                timeoutTimes++;
                fuck++;
                if (timeoutTimes % 50 == 0) {
                    System.out.println("TIMEOVER:" + timeoutTimes + ", FULL:" + fullTimes + ", BC:" + barrierCount);
                }
                timeoutWrite(tempBuf);
            } catch (BrokenBarrierException | InterruptedException ignored) {
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void okWrite(MappedByteBuffer tempBuf) {
        synchronized (WRITE_LOCKER) {
            if (noFuck > 2 && barrierCount < 20) {
                noFuck = 0;
                fuck = 0;
                barrierCount++;
                barrier = new CyclicBarrier(barrierCount);
            }
            try {
                tempBuf.force();
            } catch (Exception ignored) {
            }
        }
    }

    private void timeoutWrite(MappedByteBuffer tempBuf) {
        synchronized (WRITE_LOCKER) {
            barrier.reset();
            if (fuck > 2 && barrierCount >= 2) {
                barrierCount--;
                fuck = 0;
                noFuck = 0;
                if (barrierCount > 1) {
                    barrier = new CyclicBarrier(barrierCount);
                }
            }
            try {
                tempBuf.force();
            } catch (Exception ignored) {
            }
        }
    }

}
