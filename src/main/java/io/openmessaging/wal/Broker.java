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
    public final BlockingQueue<WritePage> writeBq;
    public AtomicLong logCount = new AtomicLong();
    private final Lock lock;
    private final Condition condition;
    private FileChannel channel = null;
    private MappedByteBuffer buffer = null;
    private int curPart = 0;

    public Broker(int walId, BlockingQueue<WritePage> writeBq, Lock lock, Condition condition) {
        this.walId = walId;
        this.lock = lock;
        this.writeBq = writeBq;
        this.condition = condition;
    }

    @Override
    public void run() {
        try {
            while (true) {
                WritePage page = writeBq.take();
                write(page);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private void write(WritePage page) throws IOException {
        if (channel == null || page.part != curPart) {
            if (channel != null) {
                Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                if (cleaner != null) {
                    cleaner.clean();
                }
                channel.close();
            }
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
