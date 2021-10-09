package io.openmessaging.leo2;

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

import static io.openmessaging.leo.DataManager.*;
import static io.openmessaging.leo2.DataManager.THREAD_MAX;

public class DataBlock {

    public byte id;
    public Path logDir;
    public byte logNumAdder = Byte.MIN_VALUE;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;
    private final Object LOCKER = new Object();

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

    private int tempSize = 0;
    private int barrierCount = THREAD_MAX / 4;
    private CyclicBarrier barrier = new CyclicBarrier(barrierCount);

    public void writeLog(byte topic, short queueId, int offset, ByteBuffer data) {
        short msgLen = (short) data.limit();
        short dataSize = (short) (MSG_META_SIZE + msgLen);
        try {
            boolean forced;
            synchronized (LOCKER) {
                if (logMappedBuf.remaining() < dataSize) {
                    if (tempSize > 0) {
                        logMappedBuf.force();
                    }
                    unmap(logMappedBuf);
                    logFileChannel.close();
                    openLog();
                }
                int position = logMappedBuf.position();
                logMappedBuf.put(topic); // 1
                logMappedBuf.putShort(queueId); // 2
                logMappedBuf.putInt(offset); // 4
                logMappedBuf.putShort(msgLen); // 2
                logMappedBuf.put(data);
                tempSize += dataSize;
                forced = tempSize >= 1024 * 64;
                if (forced) {
                    tempSize = 0;
                }
            }
            if (forced) {
                logMappedBuf.force();
                barrier.reset();
            } else {
                try {
                    int arrive = barrier.await(500, TimeUnit.MILLISECONDS);
                    if (arrive == 0) {
                        System.out.println("不够 force");
                        logMappedBuf.force();
                    }
                } catch (TimeoutException e) {
                    System.out.println("超时 force");
                    synchronized (LOCKER) {
                        logMappedBuf.force();
                        barrierCount--;
                        barrier = new CyclicBarrier(barrierCount);
                    }
                } catch (BrokenBarrierException ignored) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
