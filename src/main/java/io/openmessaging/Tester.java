package io.openmessaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Tester {

    public static void main(String[] args) {
        DefaultMessageQueueImpl messageQueue = new DefaultMessageQueueImpl();
        for (int i = 0; i < 10000; i++) {
            String text = String.valueOf(i);
            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
            messageQueue.append("A", 1, buf);
        }
        Map<Integer, ByteBuffer> range = messageQueue.getRange("A", 1, 1000, 10);
        range.forEach((key, value) -> {
            System.out.println(key + ": " + new String(value.array()));
        });
    }
}
