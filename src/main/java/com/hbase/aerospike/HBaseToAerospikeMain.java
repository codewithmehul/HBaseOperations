package com.hbase.aerospike;

import java.io.IOException;

public class HBaseToAerospikeMain {
    public static void main(String args[]) throws IOException {

        HBaseOperations hBaseOperations = new HBaseOperations();
        hBaseOperations.connect(args);
    }

}
