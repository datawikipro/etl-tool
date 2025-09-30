package pro.datawiki.diMigration.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;

public class CharsetDetector {
    public static String detectCharset(String fileLocation, String[] charsets) {
        try (RandomAccessFile aFile = new RandomAccessFile(fileLocation, "r");
             FileChannel inChannel = aFile.getChannel()) {

            MappedByteBuffer buffer = inChannel
                    .map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());

            buffer.load();
/*            for (int i = 0; i < buffer.limit(); i++) {
                System.out.print((char) buffer.get());
            }*/

            for (String charset : charsets) {
                try {
                    buffer.rewind();
                    Charset.forName(charset).newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(buffer);
                    return charset;
                } catch (CharacterCodingException e) {
                    // Ошибка: данная кодировка не подходит; продолжаем поиск
                }
                buffer.clear(); // do something with the data and clear/compact it.
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Неопределено";

    }
}