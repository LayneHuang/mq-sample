package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.util.concurrent.locks.ReentrantLock;

public class Idx {
    private static final int IDX_SIZE = 4;
    private static final int BASE_DIS = 16;
    private static final int BASE = (1 << BASE_DIS) - 1;
    // (0 ~ 1) 0 : ESSD, 1: PMEM
    private static final int WAL_IS_PMEM_DIS = 1;
    // (0 ~ 2^2-1) 0 ~ 3 : WAL 文件前编号
    private static final int WAL_ID_DIS = 2;

    private static final int WAL_ID_BASE = (1 << WAL_ID_DIS) - 1;
    private static final int WAL_VALUE_BASE = (1 << Constant.VALUE_POS_DIS) - 1;
    private int[] list = new int[128];
    //    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantLock lock = new ReentrantLock();

    public void add(int pos, int walId, int walPart, int walPos, int valueSize) {
        this.add(pos, walId, walPart, walPos, valueSize, false);
    }

    public void add(int pos, int walId, int walPart, int walPos, int valueSize, boolean isPmem) {
        int maxPos = pos << 1 | 1;
        if (maxPos + IDX_SIZE > list.length) {
            lock.lock();
            int[] nList = new int[list.length + (list.length >> 1)];
            System.arraycopy(list, 0, nList, 0, list.length);
            list = nList;
            lock.unlock();
        }
        // 已经在傲腾上
        if ((list[pos << 1] & 1) == 1) return;
        // 第 1 位 表示是否在傲腾上
        // 2 ~ 3 位表示 wal log 文件编号 (0~3)
        // 4 ~ 32 位表示在文件中的物理位置
        list[pos << 1] = ((walPos & WAL_VALUE_BASE) << 3) | ((walId & 3) << 1) | (isPmem ? 1 : 0);
        // 1 ~ 16 位表示 value 在 (PMEM or ESSD) 的位置
        // 17 ~ 32 位表示在 wal log 的哪个分片上
        list[pos << 1 | 1] = ((walPart & BASE) << BASE_DIS) | (valueSize & BASE);
    }

    public WalInfoBasic getInfo(int pos) {
        if ((pos << 1 | 1) >= getSize()) return null;
        return new WalInfoBasic(
                pos,
                getWalId(pos),
                getWalPart(pos),
                getWalValuePos(pos),
                getWalValueSize(pos),
                isPmem(pos)
        );
    }

    public int getSize() {
        return list.length;
    }

    public int getWalPart(int pos) {
        return (list[(pos << 1) | 1] >> BASE_DIS) & BASE;
    }

    public int getWalValueSize(int pos) {
        return list[(pos << 1) | 1] & BASE;
    }

    public int getWalId(int pos) {
        return (list[pos << 1] >> 1) & WAL_ID_BASE;
    }

    public int getWalValuePos(int pos) {
        return (list[pos << 1] >> 3) & WAL_VALUE_BASE;
    }

    public boolean isPmem(int pos) {
        return (list[pos << 1] & 1) == 1;
    }
}
