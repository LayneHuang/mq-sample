package io.openmessaging.wal;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.Constant.GB;
import static io.openmessaging.Constant.WRITE_BEFORE_QUERY;

public class Cache {

    public static final String DIR_PMEM = "/pmem";
    public static final Heap ROOT_HEAP = Heap.createHeap(DIR_PMEM, 60L * GB);

    public List<MemoryBlock> mbs = new ArrayList<>();
    public MemoryBlock tempMb;
    private boolean full = false;
    private long position = 0;

    public Cache() {
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
        position += WalInfoBasic.BYTES;
        data.rewind();
        tempMb.copyFromArray(data.array(), 0, position, data.limit());
        position += data.limit();
//        while (data.hasRemaining()) {
//            tempMb.setByte(position, data.get());
//            position++;
//        }
    }

    public MemoryBlock getMb(int part) {
        if (part < mbs.size()) {
            return mbs.get(part);
        }
        return null;
    }

}
