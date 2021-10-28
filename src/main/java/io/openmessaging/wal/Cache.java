package io.openmessaging.wal;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.openmessaging.Constant.GB;
import static io.openmessaging.Constant.WRITE_BEFORE_QUERY;

public class Cache {

    public static final String DIR_PMEM = "/pmem";
    public static final Heap ROOT_HEAP = Heap.createHeap(DIR_PMEM, 60L * GB);

    public List<MemoryBlock> mbs = new ArrayList<>();
    public MemoryBlock tempMb;
    private boolean full = false;
    private int position = 0;
    private final Map<Integer, Idx> IDX;

    public Cache(Map<Integer, Idx> IDX) {
        this.IDX = IDX;
        tempMb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
        ROOT_HEAP.setRoot(tempMb.handle());
        mbs.add(tempMb);
    }

    public void write(WalInfoBasic info) {
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
    }

    public void get(WalInfoBasic info) {
        MemoryBlock mb = mbs.get(info.walPart);
        mb.copyToArray(info.walPos, info.value.array(), 0, info.valueSize);
        info.value.limit(info.valueSize);
    }

}
