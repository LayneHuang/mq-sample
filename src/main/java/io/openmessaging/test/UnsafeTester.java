package io.openmessaging.test;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.openmessaging.leo2.Utils.UNSAFE;

public class UnsafeTester {

    public static void main(String[] args) throws Exception {
        byte[] text = "WTF".getBytes(StandardCharsets.UTF_8);
        ByteBuffer srcBuf = ByteBuffer.allocate(text.length);
        srcBuf.put(text);
        srcBuf.flip();
        ByteBuffer destBuf = ByteBuffer.allocateDirect(text.length);
        long address = ((DirectBuffer)destBuf).address();
        UNSAFE.copyMemory(srcBuf.array(), 16, null, address, srcBuf.limit());
        destBuf.get(text);
        System.out.println(new String(text, StandardCharsets.UTF_8));
    }
}
