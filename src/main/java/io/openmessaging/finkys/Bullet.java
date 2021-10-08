package io.openmessaging.finkys;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

public class Bullet {

    private Semaphore semaphore = new Semaphore(1);
    private int topicHash;
    private int queueId;
    private long offset;
    private ByteBuffer data;

    public Bullet(int topicHash, int queueId, long offset, ByteBuffer data) {
        this.topicHash = topicHash;
        this.queueId = queueId;
        this.offset = offset;
        this.data = data;
    }

    public int getTopicHash() {
        return topicHash;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void acquire(){
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void release(){
        semaphore.release();
    }

}
