package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WalCache
 *
 * @author layne
 * @since 2021/9/23
 */
public class Broker extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final int walId;

    private final BlockingQueue<WritePage> writeBq;

    public AtomicInteger finishNum = new AtomicInteger();

    public Broker(int walId, BlockingQueue<WritePage> writeBq) {
        this.walId = walId;
        this.writeBq = writeBq;
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
                WritePage page = writeBq.poll(5, TimeUnit.SECONDS);
                if (page == null) break;
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
                finishNum.incrementAndGet();
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
