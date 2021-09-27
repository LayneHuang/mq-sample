package io.openmessaging;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Constant
 *
 * @author 86188
 * @since 2021/9/17
 */
public class Constant {

    public static final Path DIR_ESSD =
            Files.exists(new File("/essd").toPath())
                    ? Paths.get("/essd")
                    : Paths.get(System.getProperty("user.dir") + File.separator + "essd");

    public static final String DIR_PMEM = "/pmem";

    public static final int WAL_FILE_COUNT = 10;

    public static final int MAX_TOPIC_COUNT = 100;

    public static final int MAX_QUEUE_COUNT = 5000;

    public static final int CACHE_LEN = 512;

    public static final int LOG_SEGMENT_SIZE = 1024;

    public static final int MSG_SIZE = 3 * Integer.BYTES + Long.BYTES;

    public static final int SIMPLE_MSG_SIZE = Integer.BYTES + Long.BYTES;

    public static final int READ_BUFFER_SIZE = 512 * SIMPLE_MSG_SIZE;

    public static final int WAL_BUFFER_SIZE = 128 * 1024 * 1024;

    public static Path getWALPath(int walId) {
        return DIR_ESSD.resolve("WAL-INFO-" + walId + ".md");
    }

    public static Path getWALValuePath(int walId) {
        return DIR_ESSD.resolve("WAL-VALUE-" + walId + ".md");
    }

    public static Path getPath(int topicId, int queueId) {
        return DIR_ESSD.resolve("PAGE-" + topicId + "-" + queueId + ".md");
    }

    public static String getKey(String topic, int queueId) {
        return topic + "-" + queueId;
    }

    public static int hash(String topic) {
        int hash = 0;
        int x;
        for (int i = 0; i < topic.length(); ++i) {
            hash = (hash << 4) + topic.charAt(i);
            if ((x = (int) (hash & 0xF0000000L)) != 0) {
                hash ^= (x >> 24);
                hash &= ~x;
            }
        }
        return (hash & 0x7FFFFFFF);
    }
}
