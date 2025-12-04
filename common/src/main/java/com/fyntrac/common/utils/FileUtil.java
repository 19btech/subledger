package com.fyntrac.common.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtil {

    /**
     * Creates a file on disk from byte[] content.
     *
     * @param directoryPath the directory where the file will be created
     * @param fileName      the name of the file to create
     * @param content       the file content as byte array
     * @return the created File object
     * @throws IOException if writing fails
     */
    public static File createFile(String directoryPath, String fileName, byte[] content) throws IOException {
        // Ensure directory exists
        Files.createDirectories(new File(directoryPath).toPath());

        // Compose the file
        File file = new File(directoryPath, fileName);

        // Write the content to the file
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(content);
        }

        return file;
    }

    public static Path createDirectoryIfNotExists(String path) {
        try {
            return Files.createDirectories(Path.of(path));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create directory: " + path, e);
        }
    }

    public static String getFileNameWithoutExtension(Path file) {
        if (file == null || file.getFileName() == null) return null;

        String name = file.getFileName().toString();
        int lastDot = name.lastIndexOf('.');

        return lastDot == -1 ? name : name.substring(0, lastDot);
    }
}
