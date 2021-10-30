package io.openmessaging.wal;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Constant.GB;
import static io.openmessaging.Constant.WRITE_BEFORE_QUERY;

public class Cache {

    private final Heap ROOT_HEAP;
    private final List<MemoryBlock> mbs = new ArrayList<>();
    private boolean full = false;
    private int position = 0;
    private final Object LOCK = new Object();

    public Cache(int id) {
        ROOT_HEAP = Heap.createHeap("/pmem/" + id, 15L * GB);
        MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
        ROOT_HEAP.setRoot(mb.handle());
        mbs.add(mb);
    }

    public CacheResult write(WalInfoBasic info) {
        if (full) return null;
        synchronized (LOCK) {
            if (full) return null;
            if (position + info.value.limit() > WRITE_BEFORE_QUERY) {
                try {
                    MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
                    ROOT_HEAP.setRoot(mb.handle());
                    mbs.add(mb);
                    position = 0;
                } catch (Exception e) {
                    full = true;
                    return null;
                }
            }
            int cachePart = mbs.size() - 1;
            int cachePos = position;
            MemoryBlock mb = mbs.get(cachePart);
            mb.copyFromArray(info.value.array(), 0, position, info.value.limit());
            position += info.valueSize;
            return new CacheResult(cachePart, cachePos);
        }
    }

    public void get(WalInfoBasic info) {
        MemoryBlock mb = mbs.get(info.walPart);
        mb.copyToArray(info.walPos, info.value.array(), 0, info.valueSize);
        info.value.limit(info.valueSize);
    }

    public static class CacheResult {
        public int part;
        public int pos;

        public CacheResult(int part, int pos) {
            this.part = part;
            this.pos = pos;
        }
    }
}
