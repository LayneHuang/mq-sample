package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.PageCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;

/**
 * WalCache
 *
 * @author layne
 * @since 2021/9/23
 */
public class Broker extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final int walId;

    private final WalOffset offset;

    private final ByteBuffer pageBuffer = ByteBuffer.allocate(Constant.SIMPLE_MSG_SIZE * Constant.CACHE_LEN);

    private final BlockingQueue<WalInfoBasic> bq;

    public Broker(int walId, WalOffset offset, BlockingQueue<WalInfoBasic> bq) {
        this.walId = walId;
        this.offset = offset;
        this.bq = bq;
    }

    @Override
    public void run() {
        log.info("Broker :{} , Start", walId);
        while (true) {
            WalInfoBasic info = null;
            try {
                info = bq.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (info == null || info.size == 0) {
                log.info("Broker :{} , End", walId);
                break;
            }
            partition(info);
        }
    }

    private void partition(WalInfoBasic info) {
        PageCache.getIns().add(info);
        if (PageCache.getIns().isFull(info.topicId, info.queueId)) {
            write(info, PageCache.getIns().encode(info.topicId, info.queueId, pageBuffer));
            PageCache.getIns().clear(info.topicId, info.queueId);
        }
    }

    /**
     * 写入 topic_queue 文件
     */
    private void write(WalInfoBasic info, ByteBuffer buffer) {
        // log.info("write, topic: {} , queue: {}", info.topicId, info.queueId);
        try (FileChannel fileChannel = FileChannel.open(
                Constant.getPath(info.topicId, info.queueId),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            buffer.flip();
            fileChannel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
