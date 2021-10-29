package io.openmessaging.wal;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Constant.GB;
import static io.openmessaging.Constant.WRITE_BEFORE_QUERY;
import static io.openmessaging.solve.LayneBMessageQueueImpl.IDX;

public class Cache {

    private final Heap ROOT_HEAP;
    private final List<MemoryBlock> mbs = new ArrayList<>();
    private AtomicInteger mbSize = new AtomicInteger();
    private boolean full = false;
    private int position = 0;
    private final Object LOCK = new Object();

    public Cache(int id) {
        ROOT_HEAP = Heap.createHeap("/pmem/" + id, 15L * GB);
    }

    public void write(WalInfoBasic info) {
        if (full) return;
        int cachePart;
        int cachePos;
        synchronized (LOCK) {
            if (full) return;
            if (mbs.isEmpty() || position + info.value.limit() > WRITE_BEFORE_QUERY) {
                try {
                    MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
                    ROOT_HEAP.setRoot(mb.handle());
                    mbs.add(mb);
                    mbSize.incrementAndGet();
                    position = 0;
                } catch (Exception e) {
                    full = true;
                    return;
                }
            }
            cachePart = mbSize.get() - 1;
            cachePos = position;
            info.value.rewind();
            MemoryBlock mb = mbs.get(cachePart);
            mb.copyFromArray(info.value.array(), 0, position, info.value.limit());
            position += info.valueSize;
        }
        // 傲腾位置信息放入索引
        Idx idx = IDX.computeIfAbsent(info.getKey(), k -> new Idx());
        idx.add((int) info.pOffset, info.walId, cachePart, cachePos, info.valueSize, true);
    }

    public void get(WalInfoBasic info) {
        MemoryBlock mb = mbs.get(info.walPart);
        mb.copyToArray(info.walPos, info.value.array(), 0, info.valueSize);
        info.value.limit(info.valueSize);
    }
}
