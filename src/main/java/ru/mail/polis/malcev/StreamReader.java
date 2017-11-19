package ru.mail.polis.malcev;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class StreamReader {

    private static final int SIZE_OF_BUFFER = 1024;

    @NotNull
    public static byte[] readDataFromStream(@NotNull final InputStream in) throws IOException {
        byte[] buffer = new byte[SIZE_OF_BUFFER];

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            int numberOfBytesRead;
            while ((numberOfBytesRead = in.read(buffer)) != -1) {
                baos.write(buffer,0, numberOfBytesRead);
            }
            baos.flush();
            return baos.toByteArray();
        }
    }
}
