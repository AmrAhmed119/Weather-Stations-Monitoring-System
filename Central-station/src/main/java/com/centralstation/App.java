package com.centralstation;

import java.io.IOException;

import com.Bitcask.Interface.Bitcask;
import com.Bitcask.Interface.BitcaskImpl;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws IOException {
        Bitcask bitcask = BitcaskImpl.getInstance(null, false);
    }
}
