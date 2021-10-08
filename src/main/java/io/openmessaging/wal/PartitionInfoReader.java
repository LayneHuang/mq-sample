package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.solve.LayneMessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * PartitionInfoReader
 *
 * @author 86188
 * @since 2021/10/5
 */
public class PartitionInfoReader implements InfoReader {
    private static final Logger log = LoggerFactory.getLogger(PartitionInfoReader.class);

    @Override
    public List<WalInfoBasic> read(int topicId, int queueId, long offset, int fetchNum) {
        List<WalInfoBasic> result = new ArrayList<>();
        int size = 0;
        try (FileChannel infoChannel = FileChannel.open(
                Constant.getPath(topicId, queueId), StandardOpenOption.READ)) {
            ByteBuffer infoBuffer = ByteBuffer.allocate(Constant.SIMPLE_MSG_SIZE * fetchNum);
            infoChannel.read(infoBuffer, offset * Constant.SIMPLE_MSG_SIZE);
            while (size < fetchNum) {
                infoBuffer.flip();
                while (infoBuffer.hasRemaining()) {
                    WalInfoBasic infoBasic = new WalInfoBasic();
//                    infoBuffer = infoBasic.decodeSimple(infoBuffer);
//                    log.debug("info size: {}, pos: {}", infoSize, infoPos);
                    result.add(infoBasic);
                    size++;
                }
                infoBuffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
