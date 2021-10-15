package io.openmessaging.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * WalInfoBasic
 *
 * @author 86188
 * @since 2021/9/23
 */
public class WalInfoBasic {
    private static final Logger log = LoggerFactory.getLogger(WalInfoBasic.class);

    public int topicId;

    public int queueId;

    public long pOffset;

    public int valueSize;

    public int walId;

    public int walPart;

    public int walPos;

    public long logCount;

    public ByteBuffer value;

    public WalInfoBasic() {
    }

    public WalInfoBasic(int pOffset, int walId, int walPart, int walPos, int valueSize) {
        this.pOffset = pOffset;
        this.walId = walId;
        this.walPart = walPart;
        this.walPos = walPos;
        this.valueSize = valueSize;
    }

    public WalInfoBasic(int topicId, int queueId, ByteBuffer value) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = value.limit();
        this.value = value;
    }

    public static final int BYTES = 1 + 2 * 2;

    public byte[] encodeToB() {
        byte[] result = new byte[BYTES + this.valueSize];
        // topicId
        result[0] = (byte) topicId;
        // queueId
        result[1] = (byte) ((queueId >> 8) & 0xff);
        result[2] = (byte) (queueId & 0xff);
        // value size
        result[3] = (byte) ((valueSize >> 8) & 0xff);
        result[4] = (byte) (valueSize & 0xff);
        // value
        System.arraycopy(value.array(), 0, result, BYTES, result.length - BYTES);
        return result;
    }

    private byte checkAndGetByte(FileChannel channel, ByteBuffer buffer) {
        if (buffer.hasRemaining()) return buffer.get();
        buffer.clear();
        try {
            channel.read(buffer);
            buffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.get();
    }

    public void decode(FileChannel channel, ByteBuffer buffer, boolean hasValue) {
        // topicId
        topicId = checkAndGetByte(channel, buffer) & 0xff;
        // queueId
        queueId = checkAndGetByte(channel, buffer) & 0xff;
        queueId <<= 8;
        queueId |= checkAndGetByte(channel, buffer) & 0xff;
        // valueSize
        valueSize = checkAndGetByte(channel, buffer) & 0xff;
        valueSize <<= 8;
        valueSize |= checkAndGetByte(channel, buffer) & 0xff;
        // value
        if (!hasValue) {
            return;
        }
        value = ByteBuffer.allocate(valueSize);
        for (int i = 0; i < valueSize; ++i) {
            value.put(checkAndGetByte(channel, buffer));
        }
    }

    public int getSize() {
        return BYTES + this.valueSize;
    }

    public static int getKey(int topicId, int queueId) {
        return queueId * 100 + (topicId - 1);
    }

    public int getKey() {
        return queueId * 100 + (topicId - 1);
    }
}
