package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * WriteAheadLog
 *
 * @author layne
 * @since 2021/9/17
 */
public class WriteAheadLog {

    private Map<String, Integer> TOPIC_ID = new HashMap<>();

    private List<FileChannel> channels = new ArrayList<>();

    private List<ReadWriteLock> locks = new ArrayList<>();

    private WalOffset[] offsets = new WalOffset[Constant.WAL_FILE_COUNT];

    public WriteAheadLog() {
        initChannels();
    }

    public void initChannels() {
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            FileChannel channel = null;
            try {
                channel = FileChannel.open(
                        Constant.getWALPath(i),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
            channels.add(channel);
            offsets[i] = new WalOffset();
            locks.add(new ReentrantReadWriteLock());
        }
    }

    /**
     * log 落盘
     */
    public void flush(String topic, int queueId, ByteBuffer buffer) {
        int walId = TOPIC_ID.computeIfAbsent(topic, id -> Constant.hash(topic)) % Constant.WAL_FILE_COUNT;
        locks.get(walId).writeLock().lock();
        int topicLen = topic.length();
        ByteBuffer walBuffer = ByteBuffer.allocate(
                Character.BYTES * topicLen
                        + 2 * Integer.BYTES
                        + buffer.limit()
        );
        walBuffer.putInt(topicLen);
        for (int i = 0; i < topicLen; ++i) {
            walBuffer.putChar(topic.charAt(i));
        }
        walBuffer.putInt(queueId);
        walBuffer.put(buffer);
        walBuffer.flip();
        try {
            channels.get(walId).write(walBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        offsets[walId].endOffset = channels.size();
        locks.get(walId).writeLock().unlock();
    }

    public void showOffset() {
        for (WalOffset offset : offsets) {
            System.out.println("wal offset: " + offset.beginOffset + " " + offset.endOffset);
        }
    }
}
