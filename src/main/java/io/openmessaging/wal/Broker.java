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

    private final BlockingQueue<byte[]> writeBq;

    public AtomicInteger walPart = new AtomicInteger();

    public AtomicInteger walPos = new AtomicInteger();

    public Broker(int walId, BlockingQueue<byte[]> writeBq) {
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
            while (true) {
                byte[] bs = writeBq.poll(5, TimeUnit.SECONDS);
                if (bs == null) break;
                if (bs.length == 0) {
                    channel.close();
                    channel = FileChannel.open(
                            Constant.getWALInfoPath(walId, walPart.incrementAndGet()),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE);
                    buffer = channel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0,
                            Constant.WRITE_BEFORE_QUERY
                    );
                    walPos.set(0);
                } else {
                    if (buffer.remaining() < bs.length) {
                        buffer = channel.map(
                                FileChannel.MapMode.READ_WRITE,
                                walPos.get(),
                                Constant.WRITE_BEFORE_QUERY
                        );
                    }
                    buffer.put(bs);
                    buffer.force();
                    walPos.addAndGet(bs.length);
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
