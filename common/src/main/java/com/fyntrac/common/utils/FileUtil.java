package com.fyntrac.common.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

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
}
