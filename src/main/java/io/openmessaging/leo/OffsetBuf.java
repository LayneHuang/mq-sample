package io.openmessaging.leo;

import java.nio.ByteBuffer;

import static io.openmessaging.leo.DataManager.INDEX_TEMP_BUF_SIZE;

public class OffsetBuf {

    public long offset;
    public ByteBuffer buf;

    public OffsetBuf(long offset, ByteBuffer buf) {
        this.offset = offset;
        this.buf = buf;
    }

}
