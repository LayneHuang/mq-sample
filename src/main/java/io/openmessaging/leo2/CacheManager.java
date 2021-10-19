package io.openmessaging.leo2;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import com.intel.pmem.llpl.Transaction;

import java.io.File;

public class CacheManager {

    public static final String DIR_PMEM = "/pmem";
//    public static final long PMEM_SIZE = new File(DIR_PMEM).length();
//    public static final Heap HEAP = Heap.createHeap(DIR_PMEM);

//    static {
//        System.out.println("PMEM_SIZE " + PMEM_SIZE);
//        // first run -- write values
//        MemoryBlock block = HEAP.allocateMemoryBlock(256, false);
//        HEAP.setRoot(block.handle());
//
//        // durable write at block offset 0
//        block.setLong(0, 12345);
//        block.flush(0, Long.BYTES);
//        System.out.println("wrote and flushed value 12345");
//
//        // transactional write at offset 0
//        Transaction.create(heap, () -> {
//            block.addToTransaction(8, Long.BYTES);
//            block.setLong(8, 23456);
//            System.out.println("wrote and flushed value 23456");
//        });
//
//        // allocate another block and link it to the first block
//        MemoryBlock otherBlock = heap.allocateMemoryBlock(256, false);
//        otherBlock.setInt(0, 111);
//        otherBlock.flush(0, Integer.BYTES);
//        block.setLong(16, otherBlock.handle());
//        block.flush(16, Long.BYTES);
//    }

//    public static void init(){
//        System.out.println("PMEM_SIZE " + PMEM_SIZE);
//    }

//    public static void write(){
//        // first run -- write values
//        MemoryBlock block = HEAP.allocateMemoryBlock(256, false);
//        HEAP.setRoot(block.handle());
//    }

}
