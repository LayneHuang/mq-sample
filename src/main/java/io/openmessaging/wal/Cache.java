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
    private long position = 0;
    private final Map<Integer, Idx> IDX;

    public Cache(Map<Integer, Idx> IDX) {
        this.IDX = IDX;
        setupLog();
    }

    public void openLog() {
        if (full) return;
        setupLog();
    }

    private void setupLog() {
        try {
            tempMb = ROOT_HEAP.allocateMemoryBlock(WRITE_BEFORE_QUERY);
            ROOT_HEAP.setRoot(tempMb.handle());
            mbs.add(tempMb);
        } catch (Exception e) {
            full = true;
        }
        position = 0;
    }

    public void write(WalInfoBasic info) {
        if (full) return;
//        tempMb.setByte(position, topic);
//        position += 1;
//        tempMb.setShort(position, queueId);
//        position += 2;
//        tempMb.setInt(position, offset);
//        position += 4;
//        tempMb.setShort(position, msgLen);
//        position += 2;
//        position += WalInfoBasic.BYTES;
        int cachePart = mbs.size() - 1;
        int cachePos = (int) position;
        info.value.rewind();
        tempMb.copyFromArray(info.value.array(), 0, position, info.value.limit());
        position += info.value.limit();
//        while (data.hasRemaining()) {
//            tempMb.setByte(position, data.get());
//            position++;
//        }
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
