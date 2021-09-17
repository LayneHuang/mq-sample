package io.openmessaging;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static io.openmessaging.DefaultMessageQueueImpl.DIR_ESSD;

public class FileChannelPool {

    public class ChannelBody {
        FileChannel channel;
        Semaphore semaphore = new Semaphore(1);
    }

    public static final ConcurrentHashMap<String, ChannelBody> POOL = new ConcurrentHashMap<>(1000);

    public FileChannel getFileChannel(String topic, int queueId) throws Exception {
        Path queuePath = DIR_ESSD.resolve(topic);
        Files.createDirectories(queuePath);
        ChannelBody channelBody = POOL.computeIfAbsent(topic + queueId, k -> {
            if (POOL.size() >= 1000) {
                Iterator<Map.Entry<String, ChannelBody>> iterator = POOL.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, ChannelBody> entry = iterator.next();
                    ChannelBody body = entry.getValue();
                    if (body.semaphore.tryAcquire()) {
                        try {
                            body.channel.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        iterator.remove();
                        break;
                    }
                }
            }
            ChannelBody body = new ChannelBody();
            try {
                body.channel = FileChannel.open(
                        queuePath.resolve(queueId + ".d"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
            return body;
        });
        channelBody.semaphore.acquire();
        return channelBody.channel;
    }

    public void release(String topic, int queueId) {
        POOL.computeIfPresent(topic + queueId, (s, body) -> {
            body.semaphore.release();
            return body;
        });
    }

}
