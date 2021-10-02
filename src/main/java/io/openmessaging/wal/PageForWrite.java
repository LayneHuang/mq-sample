package io.openmessaging.wal;

import java.nio.ByteBuffer;

public final class PageForWrite {
    public int topicId;
    public int queueId;
    public ByteBuffer buffer;

    public PageForWrite(int topicId, int queueId, ByteBuffer buffer) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.buffer = buffer;
    }
}
