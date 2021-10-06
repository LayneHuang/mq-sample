package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * WalInfoReader
 *
 * @author 86188
 * @since 2021/10/5
 */
public class WalInfoReader implements InfoReader {

    @Override
    public List<WalInfoBasic> read(int topicId, int queueId, long offset, int fetchNum) {
        List<WalInfoBasic> result = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.allocate(Constant.READ_BEFORE_QUERY);
        try (FileChannel indexChannel = FileChannel.open(
                Constant.getWALInfoPath(topicId % Constant.WAL_FILE_COUNT), StandardOpenOption.READ)) {
            while (result.size() < fetchNum) {
                indexChannel.read(buffer, offset);
                while (buffer.hasRemaining()) {
                    if (result.size() >= fetchNum) break;
                    WalInfoBasic infoBasic = new WalInfoBasic();
                    infoBasic.decode(buffer);
                    if (infoBasic.topicId == topicId && infoBasic.queueId == queueId && infoBasic.pOffset >= offset) {
                        result.add(infoBasic);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
