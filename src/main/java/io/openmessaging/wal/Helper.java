package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WalCache
 *
 * @author layne
 * @since 2021/9/23
 */
public class Helper {
    public static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final int walId;

    public AtomicInteger finishNum = new AtomicInteger();

    private FileChannel channel;

    private MappedByteBuffer buffer;

    private int curPart;

    private Lock lock = new ReentrantLock();

    public Helper(int walId) {
        this.walId = walId;
        try {
            channel = FileChannel.open(
                    Constant.getWALInfoPath(walId, 0),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            buffer = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Constant.WRITE_BEFORE_QUERY
            );
            curPart = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void help(WritePage page) {
        try {
            lock.lock();
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
            int fn = finishNum.incrementAndGet();
            log.debug("Broker {}, write part: {}, pos: {}, fn: {}", walId, page.part, page.pos, fn);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
