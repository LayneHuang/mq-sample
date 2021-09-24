package io.openmessaging.wal;

import io.openmessaging.Constant;

/**
 * WALInfo
 *
 * @author 86188
 * @since 2021/9/17
 */
public class WalOffset {
    /**
     * write ahead log 个数
     */
    public int logCount;

    /**
     * 当前处理到的 log 号
     */
    public int dealCount;

    /**
     * 文件末尾
     */
    public long msgPos;

    public boolean hasLogSegment() {
        return logCount - dealCount >= Constant.LOG_SEGMENT_SIZE;
    }

}
