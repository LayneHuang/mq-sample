package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
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

    public AtomicInteger walPos = new AtomicInteger();

    public Broker(int walId, BlockingQueue<byte[]> writeBq) {
        this.walId = walId;
        this.writeBq = writeBq;
    }

    @Override
    public void run() {
        try (FileChannel fileChannel = FileChannel.open(
                Constant.getWALInfoPath(walId),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        )) {
            MappedByteBuffer buffer = null;
            while (true) {
                try {
                    byte[] bs = writeBq.take();
                    if (bs.length == 0) break;
                    if (buffer == null || buffer.remaining() < bs.length) {
                        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, walPos.get(), Constant.WRITE_BEFORE_QUERY);
                    }
                    buffer.put(bs);
                    buffer.force();
                    walPos.addAndGet(bs.length);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
