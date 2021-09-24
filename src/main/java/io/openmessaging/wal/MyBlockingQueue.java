package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public final class MyBlockingQueue {

    private final BlockingQueue<WalInfoBasic> bq = new LinkedBlockingDeque<>(Constant.LOG_SEGMENT_SIZE);

    public WalInfoBasic poll() {
        try {
            return bq.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(WalInfoBasic pos) {
        try {
            bq.put(pos);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
