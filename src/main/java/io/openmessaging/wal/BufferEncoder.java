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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferEncoder {
    public static final Logger log = LoggerFactory.getLogger(BufferEncoder.class);
    private volatile int waitCnt = Constant.DEFAULT_MAX_THREAD_PER_WAL;
    private FileChannel channel = null;
    private MappedByteBuffer buffer = null;
    private final Object LOCK = new Object();
    public volatile int pos = 0;
    public volatile int part = 0;

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
    private volatile int writtenPos = 0;

    private final Lock waitLock = new ReentrantLock();
    private final Condition condition = waitLock.newCondition();
    private int nowWaitCnt = 0;

    public void holdOn(WalInfoBasic info) {
        if (waitCnt <= 1) {
            okWrite(info);
            return;
        }
        try {
            waitLock.lock();
            nowWaitCnt++;
            if (nowWaitCnt < waitCnt) {
                condition.await(10L * waitCnt, TimeUnit.MILLISECONDS);
                timeoutWrite(info);
            } else {
                okWrite(info);
                condition.signalAll();
                if (fullTimes % 1000 == 0) {
                    log.info("TIME OVER: {}, FULL: {}, FINISH: {}, WAIT CNT: {}",
                            timeoutTimes, fullTimes, writtenPos, waitCnt);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            waitLock.unlock();
        }
    }

    private void okWrite(WalInfoBasic info) {
        synchronized (LOCK) {
            if (forced(info)) return;
            fullTimes++;
            noFuck++;
            if (noFuck > 2 && waitCnt < 20) {
                noFuck = 0;
                fuck = 0;
                waitCnt++;
            }
            writtenPos = buffer.position();
            buffer.force();
            nowWaitCnt = 0;
        }
    }

    private void timeoutWrite(WalInfoBasic info) {
        synchronized (LOCK) {
            if (forced(info)) return;
            timeoutTimes++;
            fuck++;
            if (fuck > 2 && waitCnt >= 2) {
                waitCnt--;
                fuck = 0;
                noFuck = 0;
            }
            writtenPos = buffer.position();
            buffer.force();
            nowWaitCnt = 0;
        }
    }

    private boolean forced(WalInfoBasic info) {
        if (info.walPart < part) return true;
        return info.walPart == part && info.getEndPos() <= writtenPos;
    }

    private void fetchBuffer(WalInfoBasic info) {
        synchronized (LOCK) {
            if (channel == null || pos + info.getSize() >= Constant.WRITE_BEFORE_QUERY) {
                try {
                    if (channel != null) {
                        buffer.force();
                        channel.close();
                        Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                        if (cleaner != null) {
                            cleaner.clean();
                        }
                    }
                    channel = FileChannel.open(
                            Constant.getWALInfoPath(info.walId, ++part),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE);
                    buffer = channel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0,
                            Constant.WRITE_BEFORE_QUERY
                    );
                    pos = 0;
                    nowWaitCnt = 0;
                    writtenPos = buffer.position();
                    log.info("change file: {}, {}", part, writtenPos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
