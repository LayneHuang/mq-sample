package io.openmessaging.leo2;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.leo2.DataManager.G1;

public class Cache {

    public static final String DIR_PMEM = "/pmem";
    public static final int DIR_PMEM_SIZE = 60;
    public static final Heap ROOT_HEAP;
    public static volatile boolean FULL = false;

    static {
        File file = new File(DIR_PMEM);
        ROOT_HEAP = Heap.createHeap(DIR_PMEM, file.getFreeSpace());
    }

    public List<MemoryBlock> mbs = new ArrayList<>();

    public void write(ByteBuffer data) {
        if (FULL) return;
        MemoryBlock tempMb;
        try {
            tempMb = ROOT_HEAP.allocateMemoryBlock(G1);
            ROOT_HEAP.setRoot(tempMb.handle());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            FULL = true;
            return;
        }
        data.rewind();
        int position = 0;
        while (data.hasRemaining()) {
            tempMb.setByte(position, data.get());
            position++;
        }
        mbs.add(tempMb);
    }

    public MemoryBlock getMb(int index) {
        if (index < mbs.size()) {
            return mbs.get(index);
        }
        return null;
    }

}
