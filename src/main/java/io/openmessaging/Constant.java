package io.openmessaging;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Constant
 *
 * @author 86188
 * @since 2021/9/17
 */
public class Constant {

    public static final Path DIR_ESSD = Paths.get(System.getProperty("user.dir") + File.separator + "essd");

    public static final String DIR_PMEM = "/pmem";

    public static final int WAL_FILE_COUNT = 10;

    public static Path getWALPath(int walId) {
        return DIR_ESSD.resolve("WAL-" + walId + ".md");
    }

    public static Path getPath(String topic, int queueId) {
        return DIR_ESSD.resolve(getKey(topic, queueId) + ".md");
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
