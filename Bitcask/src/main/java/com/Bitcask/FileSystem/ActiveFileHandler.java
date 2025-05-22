package com.Bitcask.FileSystem;

import java.io.File;
import java.io.IOException;


public class ActiveFileHandler {
    private final File file;
    public ActiveFileHandler(String filePath) throws IOException {
        this.file = new File(filePath);
    }

    public synchronized long append(byte[] data) throws IOException {
        return 0;
    }

    public synchronized byte[] read(long position, int length) throws IOException {
        return null;
    }

    public synchronized void close() throws IOException {
    }

    public File getFile() {
        return file;
    }

}