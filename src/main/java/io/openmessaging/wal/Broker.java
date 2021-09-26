package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.PageCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

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

    private final MyBlockingQueue bq;

    public Broker(int walId, WalOffset offset, MyBlockingQueue bq) {
        this.walId = walId;
        this.offset = offset;
        this.bq = bq;
    }

    @Override
    public void run() {
        log.info("Broker :{} , Start", walId);
        while (true) {
            WalInfoBasic info = bq.poll();
            if (info == null) break;
            partition(info);
        }
    }

    private void readFromWAL() {
//        try (FileChannel channel = FileChannel.open(Constant.getWALPath(walId), StandardOpenOption.READ)) {
//            while (true) {
//                long logPos = bq.poll();
//                if (logPos == -1) break;
//                long begin = logPos * Constant.MSG_SIZE;
//                long end = (logPos + Constant.LOG_SEGMENT_SIZE) * Constant.MSG_SIZE;
//                log.info("read wal: {}, begin: {}, end: {}, channel size: {}", walId, begin, end, channel.size());
//                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin, end);
//                partition(buffer);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * 分区
     *
     * @param buffer WAL
     */
    private void partition(ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            WalInfoBasic info = new WalInfoBasic();
            info.decode(buffer);
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
