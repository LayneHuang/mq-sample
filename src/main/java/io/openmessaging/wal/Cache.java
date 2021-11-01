package io.openmessaging.wal;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

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
    private boolean lastWrite = false;

    public Cache(int id) {
        ROOT_HEAP = Heap.createHeap("/pmem/" + id, 15L * GB);
        MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
        ROOT_HEAP.setRoot(mb.handle());
        mbs.add(mb);
    }

    public int[] write(WalInfoBasic info) {
        synchronized (LOCK) {
            if (GET_RANGE_START) {
                return writeO(info);
            } else {
                return writeR(info);
            }
        }
    }

    private int[] writeO(WalInfoBasic info) {
        if (position + info.value.limit() > WRITE_BEFORE_QUERY) {
            if (full_2) {
                if (oIndex < 29) {
                    oIndex++;
                } else {
                    oIndex = 0;
                }
            } else {
                // 新建
                try {
                    MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
                    ROOT_HEAP.setRoot(mb.handle());
                    mbs.add(mb);
                    oIndex++;
                } catch (Exception e) {
                    full_2 = true;
                    oIndex = 0;
                }
            }
            position = 0;
        }
        return realWrite(info);
    }

    private int[] writeR(WalInfoBasic info) {
        if (full_1) return null;
        if (info.valueSize > 8 * KB) {
            if (lastWrite) {
                lastWrite = false;
                return null;
            }
        }
        if (position + info.value.limit() > WRITE_BEFORE_QUERY) {
            if (mbs.size() < 26) {
                MemoryBlock mb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
                ROOT_HEAP.setRoot(mb.handle());
                mbs.add(mb);
                oIndex++;
                position = 0;
            } else {
                full_1 = true;
                return null;
            }
        }
        lastWrite = true;
        return realWrite(info);
    }

    private int[] realWrite(WalInfoBasic info) {
        int[] res = {oIndex, position};
        MemoryBlock mb = mbs.get(oIndex);
        mb.copyFromArray(info.value.array(), 0, position, info.value.limit());
        position += info.valueSize;
        return res;
    }

    public void get(WalInfoBasic info) {
        MemoryBlock mb = mbs.get(info.walPart);
        mb.copyToArray(info.walPos, info.value.array(), 0, info.valueSize);
        info.value.limit(info.valueSize);
    }

}
