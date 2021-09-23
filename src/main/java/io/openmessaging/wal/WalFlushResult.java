package io.openmessaging.wal;

/**
 * WalFlushResult
 *
 * @author 86188
 * @since 2021/9/23
 */
public class WalFlushResult {

    public int walId;

    /**
     * 需要读入地址
     */
    public long begin;
}
