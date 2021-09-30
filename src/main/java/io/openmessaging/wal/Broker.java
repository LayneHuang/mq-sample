package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.PageCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    private final BlockingQueue<Page> bq = new LinkedBlockingQueue<>(Constant.CACHE_LEN);

    private final Page page = new Page();

    public Broker(int walId, WalOffset offset) {
        this.walId = walId;
        this.offset = offset;
    }

    @Override
    public void run() {
        log.info("Broker :{} , Start", walId);
        while (true) {
            Page page = null;
            try {
                page = bq.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (page == null) {
                log.info("Broker :{} , End", walId);
                break;
            }
            this.page.union(page);
            for (String key : this.page.data.keySet()) {
                List<String> list = this.page.data.get(key);
                if (list.size() * Constant.SIMPLE_MSG_SIZE < Constant.WRITE_BEFORE_QUERY) continue;
                ByteBuffer buffer = ByteBuffer.allocate(list.size() * Constant.SIMPLE_MSG_SIZE);
                for (String posStr : list) {
                    String[] ps = posStr.split("-");
                    buffer.putInt(Integer.parseInt(ps[0]));
                    buffer.putLong(Long.parseLong(ps[1]));
                }
                String[] indexes = key.split("-");
                int topicId = Integer.parseInt(indexes[0]);
                int queueId = Integer.parseInt(indexes[1]);
                write(topicId, queueId, buffer);
                this.page.data.remove(key);
            }
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
        write(info.topicId, info.queueId, buffer);
    }

    private void write(int topicId, int queueId, ByteBuffer buffer) {
        try (FileChannel fileChannel = FileChannel.open(
                Constant.getPath(topicId, queueId),
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

    public void save(Page page) {
        try {
            bq.put(page);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
