package io.openmessaging.wal;

/**
 * WALInfo
 *
 * @author 86188
 * @since 2021/9/17
 */
public class WalOffset {

    /**
     * 已经处理过 offset
     */
    public int beginOffset;

    /**
     * 文件末尾
     */
    public int endOffset;
}
