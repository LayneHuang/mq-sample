package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferEncoder {
    public static final Logger log = LoggerFactory.getLogger(BufferEncoder.class);
    private volatile int waitCnt = Constant.DEFAULT_MAX_THREAD_PER_WAL;
    private CyclicBarrier barrier = new CyclicBarrier(Constant.DEFAULT_MAX_THREAD_PER_WAL);
    private FileChannel channel = null;
    private MappedByteBuffer buffer = null;
    private final Object LOCK = new Object();
    private int pos = 0;

    private int part = 0;

    public BufferEncoder() {
    }

    public void submit(WalInfoBasic info) throws InterruptedException {
        synchronized (LOCK) {
            // wal 分段
            fetchBuffer(info);
            info.walPart = part;
            info.walPos = pos;
            // topicId
            buffer.put((byte) info.topicId);
            // queueId
            buffer.put((byte) ((info.queueId >> 8) & 0xff));
            buffer.put((byte) (info.queueId & 0xff));
            // pOffset
            buffer.put((byte) ((info.pOffset >> 8) & 0xff));
            buffer.put((byte) (info.pOffset & 0xff));
            // value
            buffer.put((byte) ((info.valueSize >> 8) & 0xff));
            buffer.put((byte) (info.valueSize & 0xff));
            // value
            buffer.put(info.value);
            pos += info.getSize();
        }
    }

    private volatile int noFuck = 0;
    private volatile int fuck = 0;
    private volatile int timeoutTimes = 0;
    private volatile int fullTimes = 0;

    public void holdOn() {
        try {
            int arrive = 0;
            if (waitCnt > 1) {
                arrive = barrier.await(10L * waitCnt, TimeUnit.MILLISECONDS);
            }
            if (arrive == 0) {
                fullTimes++;
                noFuck++;
                okWrite();
            }
        } catch (TimeoutException e) {
            // 只有一个超时，其他都是 BrokenBarrierException
            timeoutTimes++;
            fuck++;
            if (timeoutTimes % 50 == 0) {
                log.info("TIMEOVER: {}, FULL: {}, , BC: {}", timeoutTimes, fullTimes, waitCnt);
            }
            timeoutWrite();
        } catch (BrokenBarrierException | InterruptedException ignored) {
        }
    }

    private void okWrite() {
        synchronized (LOCK) {
            if (noFuck > 2 && waitCnt < 20) {
                noFuck = 0;
                fuck = 0;
                waitCnt++;
                barrier = new CyclicBarrier(waitCnt);
            }
            buffer.force();
        }
    }

    private void timeoutWrite() {
        barrier.reset();
        synchronized (LOCK) {
            if (fuck > 2 && waitCnt >= 2) {
                waitCnt--;
                fuck = 0;
                noFuck = 0;
                if (waitCnt > 1) {
                    barrier = new CyclicBarrier(waitCnt);
                }
            }
            buffer.force();
        }
    }

    private void fetchBuffer(WalInfoBasic info) {
        if (channel == null || pos + info.getSize() >= Constant.WRITE_BEFORE_QUERY) {
            try {
                if (channel != null) {
                    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                    if (cleaner != null) {
                        cleaner.clean();
                    }
                    channel.close();
                }
                channel = FileChannel.open(
                        Constant.getWALInfoPath(info.walId, part),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE);
                buffer = channel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Constant.WRITE_BEFORE_QUERY
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
            pos = 0;
            part++;
        }
    }
}
