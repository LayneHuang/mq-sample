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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * WalCache
 *
 * @author layne
 * @since 2021/9/23
 */
public class JustBroker extends Thread {
    public static final Logger log = LoggerFactory.getLogger(JustBroker.class);

    private final int walId;

    public final BlockingQueue<WalInfoBasic> writeBq = new LinkedBlockingQueue<>(Constant.BQ_SIZE);

    private int maxLogCnt = 0;

    public AtomicLong logCount = new AtomicLong();

    private final Lock lock;

    private final Condition condition;

    private FileChannel channel = null;

    private MappedByteBuffer buffer = null;

    private int curPart = -1;

    private int unForceCnt = 0;

    private int nullCnt = 0;

    public JustBroker(int walId, Lock lock, Condition condition) {
        this.walId = walId;
        this.lock = lock;
        this.condition = condition;
    }

    @Override
    public void run() {
        try {
            while (true) {
                WalInfoBasic info = writeBq.poll(20, TimeUnit.MILLISECONDS);
                if (info == null && unForceCnt == 0) {
                    nullCnt++;
                    if (nullCnt > 100) {
                        log.debug("Broker {} End", walId);
                        break;
                    }
                    continue;
                }
                if (info == null) {
                    force();
                    continue;
                }
                if (channel == null || buffer.remaining() < info.getSize()) {
                    if (channel != null) {
                        if (unForceCnt > 0) {
                            buffer.force();
                        }
                        Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                        if (cleaner != null) {
                            cleaner.clean();
                        }
                        channel.close();
                    }
                    // 当前部分
                    curPart++;
                    unForceCnt = 0;
                    channel = FileChannel.open(
                            Constant.getWALInfoPath(walId, curPart),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE);
                    buffer = channel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0,
                            Constant.WRITE_BEFORE_QUERY
                    );
                }
                maxLogCnt++;
                info.walPart = curPart;
                info.walPos = buffer.position();
                // topicId
                buffer.put((byte) info.topicId);
                tryForce();
                // queueId
                buffer.put((byte) ((info.queueId >> 8) & 0xff));
                tryForce();
                buffer.put((byte) (info.queueId & 0xff));
                tryForce();
                // value size
                buffer.put((byte) ((info.valueSize >> 8) & 0xff));
                tryForce();
                buffer.put((byte) (info.valueSize & 0xff));
                tryForce();
                // value
//                log.info("value : {}", new String(info.value.array()));
//                info.value.flip();
                for (int i = 0; i < info.valueSize; ++i) {
                    buffer.put(info.value.get());
                    tryForce(i == info.valueSize - 1);
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private void tryForce() {
        tryForce(false);
    }

    private int timeOverForceCnt = 0;

    private int autoForceCnt = 0;

    private void force() {
        buffer.force();
        logCount.set(maxLogCnt);
        unForceCnt = 0;
        timeOverForceCnt++;
        if (timeOverForceCnt % 100 == 0) log.info("t cnt: {}, a cnt: {}", timeOverForceCnt, autoForceCnt);
        signal();
    }

    private void tryForce(boolean isLast) {
        unForceCnt++;
        if (unForceCnt < Constant.WRITE_SIZE) return;
        buffer.force();
        if (isLast) logCount.set(maxLogCnt);
        unForceCnt = 0;
        autoForceCnt++;
        signal();
    }

    private void signal() {
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
