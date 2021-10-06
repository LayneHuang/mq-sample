package io.openmessaging.wal;

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
     * wal 索引偏移
     */
    public int walIndexPos;

    /**
     * wal基础信息偏移
     */
    public long infoPos;

    /**
     * wal内容(buffer)偏移
     */
    public long valuePos;
}
