package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.nio.ByteBuffer;

/**
 * WalInfoBasic
 *
 * @author 86188
 * @since 2021/9/23
 */
public class WalInfoBasic {
    public int topicId;

    public int queueId;

    public int size;

    public long pos;

    public WalInfoBasic() {
    }

    public WalInfoBasic(int topicId, int queueId, int size, long pos) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.size = size;
        this.pos = pos;
    }

    public ByteBuffer encode() {
        ByteBuffer infoBuffer = ByteBuffer.allocate(Constant.MSG_SIZE);
        return this.encode(infoBuffer);
    }

    public ByteBuffer encode(ByteBuffer infoBuffer) {
        // topic
        infoBuffer.putInt(topicId);
        // queueId
        infoBuffer.putInt(queueId);
        // buffer size
        infoBuffer.putInt(size);
        // buffer pos
        infoBuffer.putLong(pos);
        return infoBuffer;
    }

    public void decode(ByteBuffer buffer) {
        this.topicId = buffer.getInt();
        this.queueId = buffer.getInt();
        this.size = buffer.getInt();
        this.pos = buffer.getLong();
    }
}
