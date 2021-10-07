package io.openmessaging.leo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.leo.DataManager.INDEX_TEMP_BUF_SIZE;

public class Indexer {

    public byte topic;
    public short queueId;
    public List<ByteBuffer> fullBufs = new ArrayList<>();
    private ByteBuffer tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);

    public Indexer(byte topic, short queueId) {
        this.topic = topic;
        this.queueId = queueId;
    }

    public synchronized void writeIndex(ByteBuffer indexBuf) {
        if (tempBuf.position() == tempBuf.limit()) {
            tempBuf.flip();
            fullBufs.add(tempBuf);
            tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);
        }
        tempBuf.put(indexBuf);
    }

    public synchronized ByteBuffer getTempBuf() {
        ByteBuffer clone = ByteBuffer.allocate(tempBuf.position());
        tempBuf.rewind();
        while (clone.hasRemaining()) {
            clone.put(tempBuf.get());
        }
        clone.flip();
        return clone;
    }

}
