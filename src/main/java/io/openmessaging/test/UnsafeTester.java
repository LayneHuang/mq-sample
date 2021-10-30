package io.openmessaging.test;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.openmessaging.leo2.Utils.UNSAFE;

public class UnsafeTester {

    public static void main(String[] args) throws Exception {
        byte[] text = "WTF".getBytes(StandardCharsets.UTF_8);
        ByteBuffer srcBuf = ByteBuffer.allocateDirect(text.length);
        srcBuf.put(text);
        srcBuf.flip();
        ByteBuffer destBuf = ByteBuffer.allocate(text.length);
        long address = ((DirectBuffer)srcBuf).address();
        UNSAFE.copyMemory(null, address, destBuf.array(), 16, srcBuf.limit());
        byte[] text1 = new byte[3];
        destBuf.get(text1);
        System.out.println(new String(text1, StandardCharsets.UTF_8));
    }
}
