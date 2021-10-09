package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class Loader extends Thread {

    private final int walId;

    private final WriteAheadLog log;

    public Loader(int walId, WriteAheadLog log) {
        this.walId = walId;
        this.log = log;
    }

    public void run() {
        try (FileChannel fileChannel = FileChannel.open(
                Constant.getWALInfoPath(walId),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        )) {

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
