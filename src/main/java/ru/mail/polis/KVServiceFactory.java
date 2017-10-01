package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.malcev.MyFileDAO;
import ru.mail.polis.malcev.MyService;

import java.io.File;
import java.io.IOException;

/**
 * Constructs {@link KVService} instances.
 *
 * @author Vadim Tsesko <mail@incubos.org>
 */
final class KVServiceFactory {
    private static final long MAX_HEAP = 1024 * 1024 * 1024;

    private KVServiceFactory() {
        // Not supposed to be instantiated
    }

    /**
     * Construct a storage instance.
     *
     * @param port port to bind HTTP server to
     * @param data local disk folder to persist the data to
     * @return a storage instance
     */
    @NotNull
    static KVService create(
            final int port,
            @NotNull final File data) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 65536 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        if (!data.exists()) {
            throw new IllegalArgumentException("Path doesn't exist: " + data);
        }

        if (!data.isDirectory()) {
            throw new IllegalArgumentException("Path is not a directory: " + data);
        }

        return new MyService(port, new MyFileDAO(data));
    }
}
