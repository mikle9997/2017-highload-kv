package ru.mail.polis.malcev;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.NoSuchElementException;

public class MyFileDAO implements MyDAO {
    @NotNull
    private final File dir;

    public MyFileDAO(@NotNull final File dir) {
        this.dir = dir;
    }

    private File getFile(@NotNull final String key) {
        return new File(dir, key);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull final String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        final File file = getFile(key);

        if(!file.exists()) {
            throw new NoSuchElementException("Invalid key: " + key);
        }

        final int fileLength = (int) file.length();
        final byte[] value = new byte[fileLength];

        if (fileLength == 0) {
            return value;
        }

        try (BufferedInputStream f = new BufferedInputStream(new FileInputStream(file))) {
            if(f.available() == fileLength) {
                f.read(value);
                return value;

            } else {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()){
                    for (int j = f.read(value); j != -1; j = f.read(value)){
                        baos.write(value,0, j);
                    }
                    baos.flush();
                    return baos.toByteArray();
                }
            }
        }
    }

    @Override
    public void upsert(@NotNull final String key, @NotNull final byte[] value) throws IllegalArgumentException, IOException {
        try(OutputStream os = new FileOutputStream(getFile(key))) {
            os.write(value);
        }
    }

    @Override
    public void delete(@NotNull final String key) throws IllegalArgumentException, IOException {
        getFile(key).delete();
    }
}
