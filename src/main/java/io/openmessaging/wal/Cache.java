package io.openmessaging.wal;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.openmessaging.Constant.GB;
import static io.openmessaging.Constant.WRITE_BEFORE_QUERY;

public class Cache {

    private final Heap ROOT_HEAP;
    private final List<MemoryBlock> mbs = new ArrayList<>();
    private MemoryBlock tempMb;
    private boolean full = false;
    private int position = 0;
    private final Map<Integer, Idx> IDX;
    private final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    public Cache(int id, Map<Integer, Idx> IDX) {
        ROOT_HEAP = Heap.createHeap("/pmem/" + id, 15L * GB);
        this.IDX = IDX;
        tempMb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
        ROOT_HEAP.setRoot(tempMb.handle());
        mbs.add(tempMb);
    }

    public void write(WalInfoBasic info) {
        if (full) return;
        LOCK.writeLock().lock();
        try {
            if (full) return;
            if (position + info.value.limit() > WRITE_BEFORE_QUERY) {
                try {
                    tempMb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
                    ROOT_HEAP.setRoot(tempMb.handle());
                    mbs.add(tempMb);
                    position = 0;
                } catch (Exception e) {
                    full = true;
                    return;
                }
            }
            int cachePart = mbs.size() - 1;
            int cachePos = position;
            info.value.rewind();
            tempMb.copyFromArray(info.value.array(), 0, position, info.value.limit());
            position += info.value.limit();
            // 傲腾位置信息放入索引
            Idx idx = IDX.computeIfAbsent(info.getKey(), k -> new Idx());
            idx.add((int) info.pOffset, info.walId, cachePart, cachePos, info.valueSize, true);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    public void get(WalInfoBasic info) {
        LOCK.readLock().lock();
        try {
            MemoryBlock mb = mbs.get(info.walPart);
            mb.copyToArray(info.walPos, info.value.array(), 0, info.valueSize);
            info.value.limit(info.valueSize);
        } finally {
            LOCK.readLock().unlock();
        }
    }

}
