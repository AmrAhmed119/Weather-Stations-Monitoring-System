package com.Bitcask.Interface;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;

public class BitcaskLocks {
    
    private static final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    
    public static final Lock readLock = rwLock.readLock();
    public static final Lock writeLock = rwLock.writeLock();

    // Utility methods (optional)
    public static void acquireReadLock() {
        readLock.lock();
    }

    public static void releaseReadLock() {
        readLock.unlock();
    }

    public static void acquireWriteLock() {
        writeLock.lock();
    }

    public static void releaseWriteLock() {
        writeLock.unlock();
    }
}