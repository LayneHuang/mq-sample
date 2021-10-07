package io.openmessaging.leo;

import java.nio.ByteBuffer;

import static io.openmessaging.leo.DataManager.INDEX_TEMP_BUF_SIZE;

public class OffsetBuf {

    public int offset;
    public ByteBuffer buf;

    public OffsetBuf(int offset, ByteBuffer buf) {
        this.offset = offset;
        this.buf = buf;
    }

}
