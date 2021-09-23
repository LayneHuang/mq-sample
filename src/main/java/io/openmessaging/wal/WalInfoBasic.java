package io.openmessaging.wal;

/**
 * WalInfoBasic
 *
 * @author 86188
 * @since 2021/9/23
 */
public class WalInfoBasic {
    public int topicId;

    public int queueId;

    public int valueBeginOffset;

    public int valueEndOffset;
}
