import java.io.IOException;
import java.nio.file.Path;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.Bitcask.Interface.Bitcask;


public class BitcaskImpl implements Bitcask{
    private static BitcaskImpl instance = null;
    private final Map<String, String> keydir = new ConcurrentHashMap<>();
    
    private BitcaskImpl(Path path, boolean isWriter) throws IOException {
        // initialize keydir by scanning files or hint files
        //loadKeydir(path);
        //if (isWriter) acquireLock(path);
    }

    public static synchronized BitcaskImpl getInstance(Path path, boolean isWriter) throws IOException {
        if (instance == null) {
            instance = new BitcaskImpl(path, isWriter);
        } else if (isWriter) {
            throw new IllegalStateException("Bitcask already opened; cannot open again for writing.");
        }
        return instance;
    }

    @Override
    public void put(String key, String value) throws IOException {
        // append to active file, update keydir
    }

    @Override
    public String get(String key) throws IOException {
        // lookup from keydir and read file
        return "";
    }

    @Override
    public Set<String> listKeys() {
        return keydir.keySet();
    }

    @Override
    public Map<String, String> getAll() {
        // scan all keys and read latest value for each
        return null;
    }

    @Override
    public void close() {
        // release file handles, clear lock
    }

    @Override
    public void sync() throws IOException {
        // fsync active file to disk
    }

    @Override
    public void merge() throws IOException {
        // implement file compaction logic
    }
}