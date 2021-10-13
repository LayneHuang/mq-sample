package io.openmessaging.wal;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Idx {
    private static final int IDX_SIZE = 4;
    private static final int BASE_DIS = 16;
    private static final int BASE = (1 << BASE_DIS) - 1;
    private int[] list = new int[128];
    private final Lock lock = new ReentrantLock();

    public void add(int pos, int walPart, int walPos, int valueSize) {
        int maxPos = pos << 1 | 1;
        if (maxPos + IDX_SIZE > list.length) {
            lock.lock();
            int[] nList = new int[list.length + (list.length >> 1)];
            System.arraycopy(list, 0, nList, 0, list.length);
            list = nList;
            lock.unlock();
        }
        list[pos << 1] = walPos;
        list[pos << 1 | 1] = ((walPart & BASE) << BASE_DIS) | (valueSize & BASE);
    }

    public int getSize() {
        return list.length;
    }

    public int getWalPart(int pos) {
        int p = (pos << 1) | 1;
        return (list[p] >> BASE_DIS) & BASE;
    }

    public int getWalValueSize(int pos) {
        int p = (pos << 1) | 1;
        return list[p] & BASE;
    }

    public int getWalValuePos(int pos) {
        return list[pos << 1];
    }
}