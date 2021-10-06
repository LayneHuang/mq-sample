package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WalCache
 *
 * @author layne
 * @since 2021/9/23
 */
public class Broker extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final int walId;

    private final BlockingQueue<PageForWrite> writeBq;

    private final ConcurrentHashMap<String, AtomicLong> pageOffset;

    public Broker(int walId,
                  ConcurrentHashMap<String, AtomicLong> pageOffset,
                  BlockingQueue<PageForWrite> writeBq) {
        this.walId = walId;
        this.pageOffset = pageOffset;
        this.writeBq = writeBq;
    }

    @Override
    public void run() {
//        log.debug("Broker :{} , Start", walId);
        while (true) {
            PageForWrite page = null;
            try {
                page = writeBq.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (page == null) {
                log.debug("Broker :{} , End", walId);
                break;
            }
            int msgCount = page.buffer.position() / Constant.SIMPLE_MSG_SIZE;
            write(page);
            long pCount = pageOffset.computeIfAbsent(
                    WalInfoBasic.getKey(page.topicId, page.queueId),
                    k -> new AtomicLong()
            ).addAndGet(msgCount);
//            log.debug("write, topic:{}, queue:{}, pCount:{}", page.topicId, page.queueId, pCount);
        }
    }

    private void write(PageForWrite page) {
        try (FileChannel fileChannel = FileChannel.open(
                Constant.getPath(page.topicId, page.queueId),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            page.buffer.flip();
            fileChannel.write(page.buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
