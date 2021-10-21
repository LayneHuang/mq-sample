package io.openmessaging.leo2;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.leo2.DataManager.G1;
import static io.openmessaging.leo2.DataManager.MSG_META_SIZE;

public class Cache {

    public static final String DIR_PMEM = "/pmem";
    public static final int DIR_PMEM_SIZE = 60;
    public static final Heap ROOT_HEAP = Heap.createHeap(DIR_PMEM, (long) G1 * DIR_PMEM_SIZE);

    public byte id;
    public byte logNumAdder = Byte.MIN_VALUE;
    public List<MemoryBlock> mbs = new ArrayList<>();
    public MemoryBlock tempMb;
    private boolean full = false;
    private long position = 0;

    public Cache(byte id) {
        this.id = id;
        setupLog();
    }

    public void openLog(byte logNumAdder) {
        if (full) return;
        this.logNumAdder = logNumAdder;
        setupLog();
    }

    private void setupLog() {
        try {
            tempMb = ROOT_HEAP.allocateMemoryBlock(G1);
            ROOT_HEAP.setRoot(tempMb.handle());
            mbs.add(tempMb);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            full = true;
        }
        position = 0;
    }

    public void write(ByteBuffer data) {
        if (full) return;
//        tempMb.setByte(position, topic);
//        position += 1;
//        tempMb.setShort(position, queueId);
//        position += 2;
//        tempMb.setInt(position, offset);
//        position += 4;
//        tempMb.setShort(position, msgLen);
//        position += 2;
        position += MSG_META_SIZE;
        data.rewind();
        while (data.hasRemaining()) {
            tempMb.setByte(position, data.get());
            position++;
        }
    }

    public MemoryBlock getMb(byte logNum) {
        int index = logNum - Byte.MIN_VALUE;
        if (index < mbs.size()) {
            return mbs.get(index);
        }
        return null;
    }

}
