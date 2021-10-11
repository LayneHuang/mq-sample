package io.openmessaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

/**
 * IdGenerator
 *
 * @author 86188
 * @since 2021/9/23
 */
public class IdGenerator {
    private static final Logger log = LoggerFactory.getLogger(IdGenerator.class);
    private static final HashMap<String, Integer> ID_MAP = new HashMap(128);

    private static int cnt = 0;

    private static final Object lock = new Object();

    public static int getId(String key) {
        if (ID_MAP.containsKey(key)) return ID_MAP.get(key);
        int result = -1;
        synchronized (lock) {
            if (ID_MAP.containsKey(key)) return ID_MAP.get(key);
            result = ++cnt;
            ID_MAP.put(key, result);
            save(key, result);
        }
        return result;
    }

    public static boolean load() {
        if (!Constant.getMetaPath().toFile().exists()) {
            log.info("no meta");
            return false;
        }
        ByteBuffer buffer = ByteBuffer.allocate(4 * 1024);
        try (FileChannel channel = FileChannel.open(Constant.getMetaPath(),
                StandardOpenOption.READ
        )) {
            while (channel.read(buffer) > 0) {
//            log.info("meta size: {}", cnt);
                buffer.flip();
                while (buffer.hasRemaining()) {
                    int value = buffer.getInt();
                    int keySize = buffer.getInt();
                    StringBuilder key = new StringBuilder();
                    for (int i = 0; i < keySize; ++i) {
                        key.append(buffer.getChar());
                    }
//                log.info("key: {}, value: {}", key.toString(), value);
                    ID_MAP.put(key.toString(), value);
                }
                buffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("mate reload finished");
        return true;
    }

    private static void save(String key, int value) {
        try (FileChannel channel = FileChannel.open(Constant.getMetaPath(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            char[] cs = key.toCharArray();
            ByteBuffer buffer = ByteBuffer.allocate(2 * Integer.BYTES + Character.BYTES * cs.length);
            buffer.putInt(value);
            buffer.putInt(cs.length);
            for (char c : cs) {
                buffer.putChar(c);
            }
            buffer.flip();
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
