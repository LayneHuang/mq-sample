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

    public static final int WRITE_SIZE = 8 * 1024;

    public static final int BQ_SIZE = 1 << 10;

    public static final int WRITE_BEFORE_QUERY = (1 << 28);

    public static Path getWALInfoPath(int walId) {
        return DIR_ESSD.resolve("WAL-INFO-" + walId + ".md");
    }

    public static Path getWALInfoPath(int walId, int part) {
        return DIR_ESSD.resolve("WAL-INFO-" + walId + "-" + part + ".md");
    }

    public static Path getMetaPath() {
        return DIR_ESSD.resolve("META.md");
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
