package io.openmessaging.leo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.leo.DataManager.INDEX_TEMP_BUF_SIZE;

public class Indexer {

    public int topic;
    public int queueId;
    public List<ByteBuffer> fullBufs = new ArrayList<>();
    private ByteBuffer tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);
    public final Object LOCKER = new Object();

    public Indexer(int topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;
    }

    public void writeIndex(ByteBuffer indexBuf) {
        if (tempBuf.position() == tempBuf.limit()) {
            tempBuf.flip();
            fullBufs.add(tempBuf);
            tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);
        }
        tempBuf.put(indexBuf);
    }

    public ByteBuffer getTempBuf() {
        synchronized (LOCKER) {
            ByteBuffer clone = ByteBuffer.allocate(tempBuf.position());
            tempBuf.rewind();
            while (clone.hasRemaining()) {
                clone.put(tempBuf.get());
            }
            clone.flip();
            return clone;
        }
    }

}
