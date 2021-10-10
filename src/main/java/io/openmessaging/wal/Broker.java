package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * WalCache
 *
 * @author layne
 * @since 2021/9/23
 */
public class Broker extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final int walId;

    public final BlockingQueue<WritePage> writeBq = new LinkedBlockingQueue<>(Constant.BQ_SIZE);

    public AtomicLong logCount = new AtomicLong();

    private final Lock lock;
    private final Condition condition;

    public Broker(int walId, Lock lock, Condition condition) {
        this.walId = walId;
        this.lock = lock;
        this.condition = condition;
    }

    @Override
    public void run() {
        try {
            FileChannel channel = FileChannel.open(
                    Constant.getWALInfoPath(walId, 0),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Constant.WRITE_BEFORE_QUERY
            );
            int curPart = 0;
            while (true) {
                WritePage page = writeBq.poll(2, TimeUnit.SECONDS);
                if (page == null) {
                    log.debug("Broker {} End", walId);
                    break;
                }
                if (page.part != curPart) {
                    channel.close();
                    channel = FileChannel.open(
                            Constant.getWALInfoPath(walId, page.part),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE);
                    buffer = channel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0,
                            Constant.WRITE_BEFORE_QUERY
                    );
                    curPart = page.part;
                }
                buffer.put(page.value);
                buffer.force();
                logCount.set(page.logCount);
                signal();
                if (page.logCount % 10000 == 0)
                    log.debug("Broker {}, write part: {}, pos: {}, logCnt: {}", walId, page.part, page.pos, page.logCount);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
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
