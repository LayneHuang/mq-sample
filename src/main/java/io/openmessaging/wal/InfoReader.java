package io.openmessaging.wal;

import java.util.List;

/**
 * Reader
 *
 * @author 86188
 * @since 2021/10/5
 */
public interface InfoReader {

    List<WalInfoBasic> read(int topicId, int queueId, long offset, int fetchNum);
}
