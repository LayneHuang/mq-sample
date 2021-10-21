package io.openmessaging.leo2;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.leo2.DataManager.G1;

public class Cache {

    public static final String DIR_PMEM = "/pmem";
    public static final int DIR_PMEM_SIZE = 58;
    public static final Heap ROOT_HEAP = Heap.createHeap(DIR_PMEM, (long) G1 * DIR_PMEM_SIZE);

    public static final AtomicInteger size = new AtomicInteger();

    public byte id;
    public byte logNumAdder = Byte.MIN_VALUE;
    public List<MemoryBlock> mbs = new ArrayList<>(DIR_PMEM_SIZE);
    public MemoryBlock tempMb;
    private boolean full = false;
    private long position = 0;

    public Cache(byte id) {
        this.id = id;
        setupLog();
    }

    public void openLog(byte logNumAdder) {
        if (size.get() < DIR_PMEM_SIZE) {
            this.logNumAdder = logNumAdder;
            setupLog();
        } else {
            full = true;
        }
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
        size.getAndIncrement();
    }

    public void write(byte topic, short queueId, int offset, short msgLen, ByteBuffer data, short dataSize) {
        if (full) return;
        long startPos = position;
        tempMb.setByte(position, topic);
        position += 1;
        tempMb.setShort(position, queueId);
        position += 2;
        tempMb.setInt(position, offset);
        position += 4;
        tempMb.setShort(position, msgLen);
        position += 2;
        data.rewind();
        while (data.hasRemaining()) {
            tempMb.setByte(position, data.get());
            position++;
        }
//        tempMb.flush(startPos, dataSize);
    }

    public MemoryBlock getMb(byte logNum) {
        int index = logNum - Byte.MIN_VALUE;
        if (index < mbs.size()) {
            return mbs.get(index);
        }
        return null;
    }

}
