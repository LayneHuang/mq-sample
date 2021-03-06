package io.openmessaging;

import sun.misc.Cleaner;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;

public class Utils {

    public static final Unsafe UNSAFE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void unmap(MappedByteBuffer indexMapBuf) {
        Cleaner cleaner = ((DirectBuffer) indexMapBuf).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }

    public static void printMemory() {
        Runtime runtime = Runtime.getRuntime();
        long total = runtime.totalMemory() / (1024 * 1024);
        long free = runtime.freeMemory() / (1024 * 1024);
        System.out.println("Mem total " + total + " free " + free);
    }

}
