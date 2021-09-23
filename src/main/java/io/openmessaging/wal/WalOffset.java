package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WALInfo
 *
 * @author 86188
 * @since 2021/9/17
 */
public class WalOffset {

    /**
     * 已经处理过 info 个数
     */
    public int dealCount;

    /**
     * 未处理过 info 个数
     */
    public AtomicInteger logCount = new AtomicInteger();

    /**
     * 文件末尾
     */
    public long valueEndOffset;


    public boolean hasLogSegment() {
        int count = logCount.get();
        return count - dealCount >= Constant.LOG_SEGMENT_SIZE;
    }

}
