package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * WalCache
 *
 * @author 86188
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
        while (true) {
            if (offset.hasLogSegment()) {
                partition();
                offset.dealCount += Constant.LOG_SEGMENT_SIZE;
            }
        }
    }

    private void partition() {
        try (FileChannel channel = FileChannel.open(Constant.getWALPath(walId))) {
            MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    getBegin(),
                    getEnd()
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private long getBegin() {
        return (long) offset.dealCount * Constant.MSG_SIZE;
    }

    private long getEnd() {
        return (long) offset.logCount.get() * Constant.MSG_SIZE;
    }
}
