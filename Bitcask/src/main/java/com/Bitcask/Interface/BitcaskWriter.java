package com.Bitcask.Interface;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.Bitcask.Model.FileRecord;

// there is a problem regarding getting the instance of BitcaskKeyDir. I need the writer 
// only to be able to write to the active file.
public class BitcaskWriter extends BitcaskReader {

    // consts
    private final int bytesToStoreTimestampIn = Long.BYTES;
    private final int bytesToStoreKeySizeIn = 4;
    private final int bytesToStoreValueSizeIn = 4;
    private final int segmentFileThreshold = 100; // TODO -> search on values

    // singletons
    private static BitcaskWriter instance = null;
    private final BitcaskImpl bitcask;
    
    // tracking objs
    private Path folderPath;
    private int activeFileSequenceNumber = 0;

    public static synchronized BitcaskWriter getInstance(Path path) throws IOException {
        if (instance == null) {
            BitcaskImpl bitcask = BitcaskImpl.getInstance(true);
            instance = new BitcaskWriter(path, bitcask);
            instance.initialize();
        }
        return instance;
    }

    private void initialize() throws IOException {
        // 1. Rebuild the keydir (load from disk, or scan segment files)
        loadKeydirFromExistedFiles(folderPath);
        
        // 2. Update activeFileSequenceNumber in BitcaskWriter
        activeFileSequenceNumber = getActiveFileSequenceNumber();
    }

    private void loadKeydirFromExistedFiles(Path folderPath2) {
        // // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'loadKeydirFromExistedFiles'");
    }

    private BitcaskWriter(Path folderPath, BitcaskImpl bitcaskObj) throws IOException {
        super(folderPath); // TODO
        this.bitcask = bitcaskObj;
        this.folderPath = folderPath;
    }

    public void put(Integer key, String value) throws Exception {
        // prepare the file
        int keySize = Integer.BYTES;
        int valueSize = value.getBytes().length;
        RandomAccessFile raf = prepareActiveFile(keySize, valueSize);

        // append
        long currentTime = System.currentTimeMillis();
        long PositionOfStartWritingValue = writeRecord(raf, key, value, currentTime);
        raf.getChannel().force(true); // force write to disk
        raf.close();

        // update keydir
        KeyDirValuePointer pointer = new KeyDirValuePointer(
            createNewOlderFileName(), 
            valueSize, 
            PositionOfStartWritingValue,
            currentTime
        );
        bitcask.put(key, pointer);
    }


    private String createNewOlderFileName() {
        return "older_" + String.valueOf(activeFileSequenceNumber) + ".data";    
    }

    private long writeRecord(RandomAccessFile raf, Integer key, String value, Long currentTime) throws IOException {
        FileRecord record = new FileRecord(currentTime, value.getBytes().length, key, value);
        byte[] serializedRecord = record.serialize();

        // get the position bytes to be accessed when reading the value given that file has older records
        long currentPosition = raf.getFilePointer() + bytesToStoreTimestampIn + bytesToStoreKeySizeIn + bytesToStoreValueSizeIn;

        raf.write(serializedRecord);

        return currentPosition;
    }

    public void printCurrentKeyDir(BitcaskReader reader) throws IOException {
        System.out.println("Current Key Directory:");
        for (Integer key : bitcask.listKeys()) {
            String value = reader.get(key, folderPath);
            System.out.println("Key: " + key + ", Value: " + value);
        }
    }

    private RandomAccessFile prepareActiveFile(int keySize, int valueSize) throws Exception {
        // make the path hold file path
        Path activePath = folderPath.resolve("active.data");

        // base case: first file to be created
        if (!activePath.toFile().exists()) activePath.toFile().createNewFile();
        
        // current file status checking
        RandomAccessFile raf = new RandomAccessFile(activePath.toFile(), "rw");
        long fileSize = raf.length();
        int nextRecordSize = bytesToStoreTimestampIn + bytesToStoreKeySizeIn + keySize + bytesToStoreValueSizeIn + valueSize; 

        // which segment file to write to
        if (fileSize + nextRecordSize <= segmentFileThreshold) {
            raf.seek(raf.length()); 
            return raf;
        } else {
            raf.close();

            // Rename current active file
            Path renamedFile = folderPath.resolve(createNewOlderFileName());
            Files.move(activePath, renamedFile, StandardCopyOption.REPLACE_EXISTING);

            // Create new active file
            activePath.toFile().createNewFile();
            raf = new RandomAccessFile(activePath.toFile(), "rw");
            activeFileSequenceNumber++;
            return raf;
        }
    }
    
    
    /**
     * Retrieves the next available sequence number for a new active data file in the Bitcask storage directory.
     * <p>
     * This method scans the storage folder for files matching the pattern "<sequence>.data",
     * determines the highest sequence number currently in use, and returns the next sequence number to be used.
     * <p>
     * This function is primarily used during recovery to ensure that new data files do not overwrite
     * existing files and to maintain the correct sequence of data files.
     *
     * @return the next available sequence number for a new data file
     */
    private int getActiveFileSequenceNumber() {
        File[] files = folderPath.toFile().listFiles(
            (dir, name) -> name.matches("\\d+\\.data")
        );
        int maxIndex = 0;
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                int idx = Integer.parseInt(fileName.substring(0, fileName.indexOf('.')));
                if (idx > maxIndex) {
                    maxIndex = idx;
                }
            }
        }
        return maxIndex + 1;
    }

    public void close() {
        if (instance != null) {
            instance = null;
        }
    }
}