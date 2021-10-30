package io.openmessaging.wal;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.openmessaging.Constant.*;
import static io.openmessaging.solve.LayneBMessageQueueImpl.GET_RANGE_START;

public class Cache {

    private final Heap ROOT_HEAP;
    private final List<MemoryBlock> mbs = new CopyOnWriteArrayList<>();
    private boolean full_1 = false;
    private boolean full_2 = false;
    private int position = 0;
    private int oIndex = 0;
    private final Object LOCK = new Object();

    public Cache(int id) {
        ROOT_HEAP = Heap.createHeap("/pmem/" + id, 15L * GB);
        MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
        ROOT_HEAP.setRoot(mb.handle());
        mbs.add(mb);
    }

    public int[] write(WalInfoBasic info) {
        synchronized (LOCK) {
            if (GET_RANGE_START) {
                if (!full_2) {
                    if (position + info.value.limit() > WRITE_BEFORE_QUERY) {
                        // 新建
                        try {
                            MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
                            ROOT_HEAP.setRoot(mb.handle());
                            mbs.add(mb);
                            oIndex++;
                            position = 0;
                        } catch (Exception e) {
                            full_2 = true;
                            oIndex = 0;
                            position = 0;
                        }
                    }
                } else {
                    if (position + info.value.limit() > WRITE_BEFORE_QUERY) {
                        oIndex++;
                        position = 0;
                    }
                }
                if (oIndex >= 30) {
                    oIndex = 0;
                }
                int cachePart = oIndex;
                int cachePos = position;
                MemoryBlock mb = mbs.get(cachePart);
                mb.copyFromArray(info.value.array(), 0, cachePos, info.value.limit());
                position += info.valueSize;
                return new int[]{cachePart, cachePos};
            } else {
                if (info.valueSize <= 8 * KB) {
                    return writeR(info);
                }
            }
        }
        return null;
    }

    private int[] writeR(WalInfoBasic info) {
        if (full_1) return null;
        if (position + info.value.limit() > WRITE_BEFORE_QUERY) {
            if (mbs.size() < 18) {
                try {
                    MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
                    ROOT_HEAP.setRoot(mb.handle());
                    mbs.add(mb);
                    oIndex++;
                    position = 0;
                } catch (Exception e) {
                    full_1 = true;
                    return null;
                }
            } else {
                full_1 = true;
                return null;
            }
        }
        int cachePart = oIndex;
        int cachePos = position;
        MemoryBlock mb = mbs.get(cachePart);
        mb.copyFromArray(info.value.array(), 0, cachePos, info.value.limit());
        position += info.valueSize;
        return new int[]{cachePart, cachePos};
    }

    public void get(WalInfoBasic info) {
        MemoryBlock mb = mbs.get(info.walPart);
        mb.copyToArray(info.walPos, info.value.array(), 0, info.valueSize);
        info.value.limit(info.valueSize);
    }

}
