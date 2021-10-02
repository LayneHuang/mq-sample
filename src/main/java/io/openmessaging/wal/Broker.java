package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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

    private final WalOffset offset;

    private final BlockingQueue<Page> bq = new LinkedBlockingQueue<>(Constant.CACHE_LEN);

    private final ConcurrentHashMap<String, AtomicLong> pageOffset;

    private final Page page = new Page();

    public Broker(int walId, WalOffset offset, ConcurrentHashMap<String, AtomicLong> pageOffset) {
        this.walId = walId;
        this.offset = offset;
        this.pageOffset = pageOffset;
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
                int listSize = list.size();
                if (listSize == 0) continue;
                if (!page.forceUpdate && listSize * Constant.SIMPLE_MSG_SIZE < Constant.WRITE_BEFORE_QUERY) continue;
                ByteBuffer buffer = ByteBuffer.allocate(listSize * Constant.SIMPLE_MSG_SIZE);
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
                long pOffset = pageOffset.computeIfAbsent(
                        WalInfoBasic.getKey(topicId, queueId),
                        k -> new AtomicLong()
                ).addAndGet(listSize);
                log.info("topic: {}, queue: {}, list size: {}, save to db, pageOffset: {}",
                        topicId, queueId, listSize, pOffset);
            }
        }
    }

    public void save(Page page) {

    }

    public void saveAsync(Page page) {
        try {
            bq.put(page);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

}
