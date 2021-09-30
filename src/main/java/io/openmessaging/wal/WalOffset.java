package io.openmessaging.wal;

import java.util.concurrent.atomic.AtomicInteger;

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
    public AtomicInteger logCount = new AtomicInteger();
    ;

    /**
     * 当前处理到的 log 号
     */
    public AtomicInteger dealingCount = new AtomicInteger();

    /**
     * wal基础信息偏移
     */
    public long infoPos;

    /**
     * wal内容(buffer)偏移
     */
    public long valuePos;
}
