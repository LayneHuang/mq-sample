package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.PageCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * WalCache
 *
 * @author layne
 * @since 2021/9/23
 */
public class Broker extends Thread {

    private final int walId;

    private final WalOffset offset;

    public Broker(int walId, WalOffset offset) {
        this.walId = walId;
        this.offset = offset;
    }

    @Override
    public void run() {
        try (FileChannel channel = FileChannel.open(Constant.getWALPath(walId))) {
            while (true) {
                if (offset.hasLogSegment()) {
                    MappedByteBuffer buffer = channel.map(
                            FileChannel.MapMode.READ_ONLY,
                            getBegin(),
                            getEnd()
                    );
                    partition(buffer);
                }
                offset.dealCount += Constant.LOG_SEGMENT_SIZE;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            PageCache.add(info);
            if (PageCache.isFull(info.topicId, info.queueId)) {
                write();
            }
        }
    }

    /**
     * 写入 topic_queue 文件
     */
    private void write() {

    }

    private long getBegin() {
        return (long) offset.dealCount * Constant.MSG_SIZE;
    }

    private long getEnd() {
        return (long) (offset.dealCount + Constant.LOG_SEGMENT_SIZE) * Constant.MSG_SIZE;
    }
}
