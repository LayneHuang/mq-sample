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
                int listSize = list.size();
                if (listSize * Constant.SIMPLE_MSG_SIZE < Constant.WRITE_BEFORE_QUERY) continue;
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
                offset.dealingCount.addAndGet(listSize);
            }
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

    public void save(Page page) {
        try {
            bq.put(page);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
