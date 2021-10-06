package io.openmessaging.finkys;

import java.nio.ByteBuffer;

public class OffsetBuf {

    public long offset;
    public ByteBuffer buf;

    public OffsetBuf(long offset, ByteBuffer buf) {
        this.offset = offset;
        this.buf = buf;
    }

}
