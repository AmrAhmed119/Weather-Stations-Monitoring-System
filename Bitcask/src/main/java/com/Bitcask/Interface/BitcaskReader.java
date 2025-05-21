import java.io.IOException;
import java.nio.file.Path;
import com.Bitcask.Interface.BitcaskImpl;


public class BitcaskReader {
    private final BitcaskImpl sharedBitcask;

    public BitcaskReader(Path path) throws IOException {
        this.sharedBitcask = BitcaskImpl.getInstance(path, false);
    }

    public String get(String key) throws IOException {
        return sharedBitcask.get(key);
    }
}